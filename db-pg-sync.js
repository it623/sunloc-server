/**
 * db-pg-sync.js — Synchronous PostgreSQL adapter for Sunloc
 * Uses a persistent worker process with synchronous IPC via Atomics.
 */

'use strict';

const { spawnSync, spawn } = require('child_process');
const os = require('os');
const path = require('path');
const fs = require('fs');

// ── Persistent worker script ──────────────────────────────────
const WORKER_SRC = `
require('module').Module._initPaths();
const { Pool } = require(require('path').join(process.env.APP_DIR || '/app', 'node_modules', 'pg'));
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 10 });

// Shared memory for sync IPC
const sab = new SharedArrayBuffer(4);
const flag = new Int32Array(sab);

let resultBuf = '';
let buf = '';

process.stdin.on('data', d => {
  buf += d.toString();
  let nl;
  while ((nl = buf.indexOf('\\n')) >= 0) {
    const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
    if (!line.trim()) continue;
    const req = JSON.parse(line);
    const { sql, params, method, id } = req;
    let i = 0;
    const pgSql = sql.replace(/\\?/g, () => '$' + (++i));
    pool.query(pgSql, params || [])
      .then(r => {
        let result;
        if (method === 'get') result = { id, row: r.rows[0] || null };
        else if (method === 'all') result = { id, rows: r.rows };
        else result = { id, changes: r.rowCount, lastInsertRowid: null };
        process.stdout.write(JSON.stringify(result) + '\\n');
      })
      .catch(e => process.stdout.write(JSON.stringify({ id, error: e.message }) + '\\n'));
  }
});

pool.connect()
  .then(c => { c.release(); process.stdout.write(JSON.stringify({ id: '__ready__' }) + '\\n'); })
  .catch(e => { process.stderr.write('PG connect failed: ' + e.message + '\\n'); process.exit(1); });
`;

const workerPath = path.join(os.tmpdir(), 'sunloc-pg-worker.js');
fs.writeFileSync(workerPath, WORKER_SRC);

// ── Spawn persistent worker ───────────────────────────────────
let worker = null;
let pendingResolvers = {};
let reqId = 0;
let workerReady = false;

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
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (!line.trim()) continue;
      try {
        const msg = JSON.parse(line);
        if (msg.id === '__ready__') { workerReady = true; return; }
        const res = pendingResolvers[msg.id];
        if (res) { delete pendingResolvers[msg.id]; res(msg); }
      } catch(e) {}
    }
  });

  worker.on('exit', code => {
    console.error(`[PG Worker] exited with code ${code}, restarting...`);
    workerReady = false;
    setTimeout(startWorker, 1000);
  });

  // Wait for ready
  const deadline = Date.now() + 10000;
  while (!workerReady && Date.now() < deadline) {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 50);
  }
}

startWorker();

// ── Synchronous query using worker + Atomics wait ─────────────
function pgQuerySync(sql, params, method) {
  if (!worker || !workerReady) throw new Error('PG worker not ready');
  
  const id = String(reqId++);
  const sab = new SharedArrayBuffer(4);
  const flag = new Int32Array(sab);
  let result = null;

  pendingResolvers[id] = (msg) => {
    result = msg;
    Atomics.store(flag, 0, 1);
    Atomics.notify(flag, 0);
  };

  worker.stdin.write(JSON.stringify({ sql, params: params || [], method, id }) + '\n');

  // Wait synchronously for result
  const deadline = Date.now() + 30000;
  while (result === null && Date.now() < deadline) {
    Atomics.wait(flag, 0, 0, 100);
  }

  delete pendingResolvers[id];

  if (result === null) throw new Error('PG query timeout');
  if (result.error) throw new Error(result.error);
  return result;
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
