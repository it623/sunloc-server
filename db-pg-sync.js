/**
 * db-pg-sync.js — Synchronous PostgreSQL adapter for Sunloc
 */

'use strict';

const { execFileSync } = require('child_process');
const { spawn } = require('child_process');
const os = require('os');
const path = require('path');
const fs = require('fs');

// ── Worker script ─────────────────────────────────────────────
const WORKER_SRC = `
require('module').Module._initPaths();
const { Pool } = require(require('path').join(process.env.APP_DIR || '/app', 'node_modules', 'pg'));
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5 });
let buf = '';
process.stdin.on('data', d => {
  buf += d;
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
      else process.stdout.write(JSON.stringify({ id, changes: r.rowCount, lastInsertRowid: null }) + '\\n');
    })
    .catch(e => process.stdout.write(JSON.stringify({ id, error: e.message }) + '\\n'));
});
pool.connect().then(c => { c.release(); process.stdout.write(JSON.stringify({ id: '__ready__' }) + '\\n'); })
  .catch(e => { process.stderr.write('PG connect failed: ' + e.message + '\\n'); process.exit(1); });
`;

const workerPath = path.join(os.tmpdir(), 'sunloc-pg-worker.js');
fs.writeFileSync(workerPath, WORKER_SRC);

// ── Sync helper — reads from stdin ────────────────────────────
const SYNC_HELPER = `
const { Pool } = require(require('path').join(process.env.APP_DIR || '/app', 'node_modules', 'pg'));
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 1 });
let raw = '';
process.stdin.on('data', d => { raw += d; });
process.stdin.on('end', () => {
  const args = JSON.parse(raw);
  let i = 0;
  const pgSql = args.sql.replace(/\\?/g, () => '$' + (++i));
  pool.query(pgSql, args.params || [])
    .then(r => {
      if (args.method === 'get') process.stdout.write(JSON.stringify({ row: r.rows[0] || null }));
      else if (args.method === 'all') process.stdout.write(JSON.stringify({ rows: r.rows }));
      else process.stdout.write(JSON.stringify({ changes: r.rowCount }));
      pool.end();
    })
    .catch(e => { process.stdout.write(JSON.stringify({ error: e.message })); pool.end(); });
});
`;

const syncHelperPath = path.join(os.tmpdir(), 'sunloc-pg-sync.js');
fs.writeFileSync(syncHelperPath, SYNC_HELPER);

// ── Spawn worker ──────────────────────────────────────────────
let worker = null;
let pendingResolvers = {};
let readyResolve = null;
const readyPromise = new Promise(r => { readyResolve = r; });

function startWorker() {
  worker = spawn(process.execPath, [workerPath], {
    env: { ...process.env, NODE_PATH: path.join(process.cwd(), 'node_modules') },
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  let buf = '';
  worker.stdout.on('data', data => {
    buf += data.toString();
    let nl;
    while ((nl = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
      if (!line.trim()) continue;
      try {
        const msg = JSON.parse(line);
        if (msg.id === '__ready__') { readyResolve(); return; }
        const res = pendingResolvers[msg.id];
        if (res) { delete pendingResolvers[msg.id]; res(msg); }
      } catch(e) {}
    }
  });
  worker.on('exit', code => console.error(`[PG Worker] exited with code ${code}`));
}

startWorker();

// ── Synchronous query via stdin ───────────────────────────────
function pgQuerySync(sql, params, method) {
  const args = JSON.stringify({ sql, params: params || [], method });
  try {
    const out = execFileSync(process.execPath, [syncHelperPath], {
      env: { ...process.env, NODE_PATH: path.join(process.cwd(), 'node_modules') },
      input: args,
      timeout: 60000,
      maxBuffer: 50 * 1024 * 1024,
      encoding: 'utf8',
    });
    const result = JSON.parse(out);
    if (result.error) throw new Error(result.error);
    return result;
  } catch (e) {
    console.error('[PG] Query error:', e.code || e.message?.slice(0, 100));
    if (e.message && e.message.includes('{')) {
      try { const r = JSON.parse(e.message.match(/\{.*\}/)[0]); if (r.error) throw new Error(r.error); } catch{}
    }
    throw e;
  }
}

// ── Prepared statement shim ───────────────────────────────────
class PgStatement {
  constructor(sql) { this.sql = sql; }
  get(...params) { return pgQuerySync(this.sql, params.flat(), 'get').row || undefined; }
  all(...params) { return pgQuerySync(this.sql, params.flat(), 'all').rows || []; }
  run(...params) { const r = pgQuerySync(this.sql, params.flat(), 'run'); return { changes: r.changes || 0, lastInsertRowid: null }; }
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
