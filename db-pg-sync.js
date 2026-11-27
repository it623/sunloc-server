/**
 * db-pg-sync.js — Synchronous PostgreSQL adapter for Sunloc
 * Uses a pre-connected pool with synchronous wrappers via spawnSync stdin/stdout
 */

'use strict';

const { spawnSync } = require('child_process');
const os = require('os');
const path = require('path');
const fs = require('fs');

// ── Per-query sync helper ─────────────────────────────────────
// Spawns a fresh node process for each query, reads query from stdin
// This is reliable and has no size limits
const SYNC_HELPER = `
'use strict';
process.stdin.setEncoding('utf8');
let raw = '';
process.stdin.on('data', d => { raw += d; });
process.stdin.on('end', () => {
  const { Pool } = require(require('path').join(process.env.APP_DIR || '/app', 'node_modules', 'pg'));
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 1,
    connectionTimeoutMillis: 10000,
  });
  try {
    const args = JSON.parse(raw);
    let i = 0;
    const pgSql = args.sql.replace(/\\?/g, () => '$' + (++i));
    pool.query(pgSql, args.params || [])
      .then(r => {
        let out;
        if (args.method === 'get') out = { row: r.rows[0] || null };
        else if (args.method === 'all') out = { rows: r.rows };
        else out = { changes: r.rowCount };
        process.stdout.write(JSON.stringify(out));
        return pool.end();
      })
      .then(() => process.exit(0))
      .catch(e => {
        process.stdout.write(JSON.stringify({ error: e.message }));
        pool.end().finally(() => process.exit(0));
      });
  } catch(e) {
    process.stdout.write(JSON.stringify({ error: 'parse: ' + e.message }));
    process.exit(1);
  }
});
`;

const syncHelperPath = path.join(os.tmpdir(), 'sunloc-pg-sync.js');
fs.writeFileSync(syncHelperPath, SYNC_HELPER);

// v60R: every call here spawns a child node process and blocks the event loop for its whole life. Count them, time
// them and record the server.js call site, so the per-minute [v60M perf] line can name what is left to convert.
global._v60rSyncShim = global._v60rSyncShim || { n: 0, ms: 0, sites: {} };
function _v60rSite() {
  try {
    const st = String(new Error().stack || '').split('\n').slice(2);
    const f = st.find(l => /server\.js:\d+/.test(l)) || st[1] || '';
    const m = /server\.js:(\d+)/.exec(f); const fn = /at ([^ (]+)/.exec(f);
    return (fn ? fn[1] : '?') + (m ? ' server.js:' + m[1] : '');
  } catch (_) { return '?'; }
}
function pgQuerySync(sql, params, method) {
  const _t0 = Date.now();
  try { return _pgQuerySyncInner(sql, params, method); }
  finally {
    const ms = Date.now() - _t0, sh = global._v60rSyncShim; sh.n++; sh.ms += ms;
    const k = _v60rSite(); const e = sh.sites[k] || (sh.sites[k] = { n: 0, ms: 0 }); e.n++; e.ms += ms;
  }
}
function _pgQuerySyncInner(sql, params, method) {
  const input = JSON.stringify({ sql, params: params || [], method });
  const result = spawnSync(process.execPath, [syncHelperPath], {
    input,
    env: { ...process.env, NODE_PATH: path.join(process.cwd(), 'node_modules') },
    timeout: 30000,
    maxBuffer: 200 * 1024 * 1024,
    encoding: 'utf8',
  });

  if (result.error) throw result.error;
  if (result.status !== 0) {
    const msg = (result.stderr || '') + (result.stdout || '');
    throw new Error('PG helper failed: ' + msg.slice(0, 200));
  }

  const out = (result.stdout || '').trim();
  if (!out) throw new Error('Empty PG response');

  const parsed = JSON.parse(out);
  if (parsed.error) throw new Error(parsed.error);
  return parsed;
}

// ── Prepared statement shim ───────────────────────────────────
class PgStatement {
  constructor(sql) { this.sql = sql; }
  get(...params) { return pgQuerySync(this.sql, params.flat(), 'get').row || undefined; }
  all(...params) { return pgQuerySync(this.sql, params.flat(), 'all').rows || []; }
  run(...params) {
    const r = pgQuerySync(this.sql, params.flat(), 'run');
    return { changes: r.changes || 0, lastInsertRowid: null };
  }
  iterate(...params) { return this.all(...params)[Symbol.iterator](); }
}

// ── Database shim ─────────────────────────────────────────────
class PgDatabase {
  constructor() {
    this.isPostgres = true;
    try {
      pgQuerySync('SELECT 1 as ok', [], 'get');
      console.log('[DB] PostgreSQL connected successfully');
    } catch(e) {
      console.error('[DB] PostgreSQL connection failed:', e.message);
      throw e;
    }
  }

  prepare(sql) {
    const isIgnore = /\bINSERT OR IGNORE INTO\b/i.test(sql);
    const isReplace = /\bINSERT OR REPLACE INTO\b/i.test(sql);
    sql = sql
      .replace(/\bINSERT OR IGNORE INTO\b/gi, 'INSERT INTO')
      .replace(/\bINSERT OR REPLACE INTO\b/gi, 'INSERT INTO')
      .replace(/datetime\('now'\)/gi, 'NOW()')
      .replace(/datetime\(\\'now\\'\)/gi, 'NOW()')
      .replace(/AUTOINCREMENT/gi, '');
    if ((isIgnore || isReplace) && !/ON CONFLICT/i.test(sql)) {
      sql = sql.trimEnd().replace(/;$/, '') + ' ON CONFLICT DO NOTHING';
    }
    return new PgStatement(sql);
  }

  exec(sql) {
    const stmts = sql.split(';').map(s => s.trim()).filter(Boolean);
    for (const stmt of stmts) {
      const pgStmt = stmt
        .replace(/INTEGER PRIMARY KEY AUTOINCREMENT/gi, 'SERIAL PRIMARY KEY')
        .replace(/INTEGER PRIMARY KEY/gi, 'SERIAL PRIMARY KEY')
        .replace(/datetime\('now'\)/gi, 'NOW()')
        .replace(/AUTOINCREMENT/gi, '');
      try {
        pgQuerySync(pgStmt, [], 'run');
      } catch(e) {
        if (!e.message.includes('already exists') &&
            !e.message.includes('duplicate column') &&
            !e.message.includes('does not exist')) {
          console.error('[PG] DDL error:', e.message.slice(0, 120));
        }
      }
    }
  }

  pragma() {}
  transaction(fn) { return (...args) => fn(...args); }
}

module.exports = { PgDatabase };
