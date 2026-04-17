/**
 * db-pg-sync.js — Synchronous PostgreSQL adapter for Sunloc
 * Uses execFileSync with stdin for queries (no size limit issues)
 */

'use strict';

const { execFileSync, spawn } = require('child_process');
const os = require('os');
const path = require('path');
const fs = require('fs');

// ── Sync helper — reads query from stdin, writes result to stdout ──
const SYNC_HELPER = `
const { Pool } = require(require('path').join(process.env.APP_DIR || '/app', 'node_modules', 'pg'));
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 1 });
let raw = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', d => { raw += d; });
process.stdin.on('end', () => {
  try {
    const args = JSON.parse(raw);
    let i = 0;
    const pgSql = args.sql.replace(/\\?/g, () => '$' + (++i));
    pool.query(pgSql, args.params || [])
      .then(r => {
        let result;
        if (args.method === 'get') result = { row: r.rows[0] || null };
        else if (args.method === 'all') result = { rows: r.rows };
        else result = { changes: r.rowCount };
        process.stdout.write(JSON.stringify(result));
        pool.end().then(() => process.exit(0));
      })
      .catch(e => {
        process.stdout.write(JSON.stringify({ error: e.message }));
        pool.end().then(() => process.exit(0));
      });
  } catch(e) {
    process.stdout.write(JSON.stringify({ error: 'Parse error: ' + e.message }));
    process.exit(1);
  }
});
`;

const syncHelperPath = path.join(os.tmpdir(), 'sunloc-pg-sync.js');
fs.writeFileSync(syncHelperPath, SYNC_HELPER);

// ── Worker script (async, for background tasks) ───────────────
const WORKER_SRC = `
require('module').Module._initPaths();
const { Pool } = require(require('path').join(process.env.APP_DIR || '/app', 'node_modules', 'pg'));
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5 });
let buf = '';
process.stdin.on('data', d => {
  buf += d.toString();
  const nl = buf.indexOf('\\n');
  if (nl < 0) return;
  const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
  const req = JSON.parse(line);
  const { sql, params, method, id } = req;
  let i = 0;
  const pgSql = sql.replace(/\\?/g, () => '$' + (++i));
  pool.query(pgSql, params || [])
    .then(r => {
      if (method === 'get') process.stdout.write(JSON.stringify({ id, row: r.rows[0] || null }) + '\\n');
      else if (method === 'all') process.stdout.write(JSON.stringify({ id, rows: r.rows }) + '\\n');
      else process.stdout.write(JSON.stringify({ id, changes: r.rowCount }) + '\\n');
    })
    .catch(e => process.stdout.write(JSON.stringify({ id, error: e.message }) + '\\n'));
});
pool.connect().then(c => { c.release(); process.stdout.write(JSON.stringify({ id: '__ready__' }) + '\\n'); })
  .catch(e => { process.stderr.write('PG connect failed: ' + e.message + '\\n'); process.exit(1); });
`;

const workerPath = path.join(os.tmpdir(), 'sunloc-pg-worker.js');
fs.writeFileSync(workerPath, WORKER_SRC);

// ── Spawn async worker (non-blocking, for future use) ─────────
const worker = spawn(process.execPath, [workerPath], {
  env: { ...process.env, NODE_PATH: path.join(process.cwd(), 'node_modules') },
  stdio: ['pipe', 'pipe', 'inherit'],
});
worker.stdout.on('data', () => {});
worker.on('exit', code => console.error(`[PG Worker] exited with code ${code}`));

// ── Synchronous query via execFileSync + stdin ────────────────
function pgQuerySync(sql, params, method) {
  const args = JSON.stringify({ sql, params: params || [], method });
  try {
    const out = execFileSync(process.execPath, [syncHelperPath], {
      env: { ...process.env, NODE_PATH: path.join(process.cwd(), 'node_modules') },
      input: args,
      timeout: 60000,
      maxBuffer: 100 * 1024 * 1024,
      encoding: 'utf8',
    });
    if (!out || !out.trim()) throw new Error('Empty response from PG helper');
    const result = JSON.parse(out.trim());
    if (result.error) throw new Error(result.error);
    return result;
  } catch (e) {
    if (e.stdout) {
      try {
        const r = JSON.parse(e.stdout.trim());
        if (r.error) throw new Error(r.error);
        return r;
      } catch {}
    }
    throw e;
  }
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
