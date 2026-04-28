/**
 * SUNLOC INTEGRATED SERVER v2
 * Stack: PostgreSQL (production) or SQLite (local dev)
 * Set DATABASE_URL env var to use PostgreSQL automatically.
 * All 107 db.prepare().get/run/all() calls work unchanged via the adapter.
 */

'use strict';

const express = require('express');
const cors    = require('cors');
const path    = require('path');
const fs      = require('fs');
const crypto  = require('crypto');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ─── Database — Dual Mode ──────────────────────────────────────
// When DATABASE_URL is set: uses PostgreSQL via db-pg-sync.js adapter
// (same synchronous db.prepare().get/run/all() API as better-sqlite3)
// When not set: uses better-sqlite3 directly (local dev / Railway SQLite)

let db;
const USE_POSTGRES = !!process.env.DATABASE_URL;

// ── Legacy order cutoff: orders with startDate on or before this date
//    are treated as legacy (already in plant) and exempt from 2-order limit
const LEGACY_CUTOFF = '2026-04-19';

let DB_PATH = 'postgres'; // overwritten below in SQLite mode; must be top-level so health endpoint never throws ReferenceError

if (USE_POSTGRES) {
  const { PgDatabase } = require('./db-pg-sync');
  db = new PgDatabase();
  console.log('[DB] Mode: PostgreSQL');
}

// Direct async pg pool for large queries
let pgPool = null;
if (USE_POSTGRES) {
  try {
    const { Pool } = require('pg');
    pgPool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5 });
    console.log('[DB] Direct pg pool ready');
  } catch(e) { console.error('[DB] pg pool error:', e.message); }
} else {
  const Database = require('better-sqlite3');

  function resolveDbPath() {
    if (process.env.DB_PATH) return process.env.DB_PATH;
    const volumePath = '/data/sunloc.db';
    try {
      const dir = '/data';
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.accessSync(dir, fs.constants.W_OK);
      console.log('[DB] Using persistent volume: ' + volumePath);
      return volumePath;
    } catch (e) {
      const localPath = path.join(__dirname, 'sunloc.db');
      console.log('[DB] Volume not available, using local: ' + localPath);
      return localPath;
    }
  }

  DB_PATH = resolveDbPath();
  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  console.log('[DB] Mode: SQLite at ' + DB_PATH);
}

// ─── Migration System ──────────────────────────────────────────
// SQLite migration SQL — the PgDatabase.exec() translates to Postgres automatically.

db.exec(`
  CREATE TABLE IF NOT EXISTS schema_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL UNIQUE,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
`);

const MIGRATIONS = [
  {
    version: 1,
    name: 'initial_schema',
    sql: `
      CREATE TABLE IF NOT EXISTS planning_state (
        id INTEGER PRIMARY KEY,
        state_json TEXT NOT NULL,
        saved_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS dpr_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        floor TEXT NOT NULL,
        date TEXT NOT NULL,
        data_json TEXT NOT NULL,
        saved_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(floor, date)
      );
      CREATE TABLE IF NOT EXISTS production_actuals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT,
        batch_number TEXT,
        machine_id TEXT NOT NULL,
        date TEXT NOT NULL,
        shift TEXT NOT NULL,
        run_index INTEGER NOT NULL DEFAULT 0,
        qty_lakhs REAL DEFAULT 0,
        floor TEXT,
        synced_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(machine_id, date, shift, run_index)
      );
      CREATE INDEX IF NOT EXISTS idx_actuals_order ON production_actuals(order_id);
      CREATE INDEX IF NOT EXISTS idx_actuals_batch ON production_actuals(batch_number);
      CREATE INDEX IF NOT EXISTS idx_actuals_machine ON production_actuals(machine_id, date);
      CREATE INDEX IF NOT EXISTS idx_dpr_date ON dpr_records(date);
    `
  },
  {
    version: 2,
    name: 'tracking_tables',
    sql: `
      CREATE TABLE IF NOT EXISTS tracking_labels (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        label_number INTEGER NOT NULL,
        size TEXT NOT NULL,
        qty REAL NOT NULL,
        is_partial INTEGER DEFAULT 0,
        is_orange INTEGER DEFAULT 0,
        parent_label_id TEXT,
        customer TEXT,
        colour TEXT,
        pc_code TEXT,
        po_number TEXT,
        machine_id TEXT,
        printing_matter TEXT,
        generated TEXT NOT NULL DEFAULT (datetime('now')),
        printed INTEGER DEFAULT 0,
        printed_at TEXT,
        voided INTEGER DEFAULT 0,
        void_reason TEXT,
        voided_at TEXT,
        voided_by TEXT,
        qr_data TEXT,
        UNIQUE(batch_number, label_number, is_orange)
      );
      CREATE TABLE IF NOT EXISTS tracking_scans (
        id TEXT PRIMARY KEY,
        label_id TEXT NOT NULL,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        type TEXT NOT NULL CHECK(type IN ('in','out')),
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        operator TEXT,
        size TEXT,
        qty REAL
      );
      CREATE TABLE IF NOT EXISTS tracking_stage_closure (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        closed INTEGER DEFAULT 1,
        closed_at TEXT NOT NULL DEFAULT (datetime('now')),
        closed_by TEXT,
        UNIQUE(batch_number, dept)
      );
      CREATE TABLE IF NOT EXISTS tracking_wastage (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        type TEXT NOT NULL CHECK(type IN ('salvage','remelt')),
        qty REAL NOT NULL,
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        by TEXT
      );
      CREATE TABLE IF NOT EXISTS tracking_dispatch_records (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        customer TEXT,
        qty REAL NOT NULL,
        boxes INTEGER NOT NULL,
        vehicle_no TEXT,
        invoice_no TEXT,
        remarks TEXT,
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        by TEXT
      );
      CREATE TABLE IF NOT EXISTS tracking_alerts (
        id TEXT PRIMARY KEY,
        label_id TEXT NOT NULL,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        scan_in_ts TEXT NOT NULL,
        hours_stuck REAL,
        resolved INTEGER DEFAULT 0,
        msg TEXT,
        UNIQUE(label_id, dept)
      );
      CREATE INDEX IF NOT EXISTS idx_scans_batch ON tracking_scans(batch_number, dept);
      CREATE INDEX IF NOT EXISTS idx_labels_batch ON tracking_labels(batch_number);
      CREATE INDEX IF NOT EXISTS idx_wastage_batch ON tracking_wastage(batch_number, dept);
    `
  },
  {
    version: 3,
    name: 'auth_and_audit',
    sql: `
      CREATE TABLE IF NOT EXISTS app_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        pin_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS app_sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        expires_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        ip TEXT,
        ts TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC);
      CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(username);
    `
  },
  {
    version: 4,
    name: 'temp_batch_system',
    sql: `
      CREATE TABLE IF NOT EXISTS temp_batches (
        id TEXT PRIMARY KEY,
        machine_id TEXT NOT NULL,
        machine_size TEXT NOT NULL,
        date TEXT NOT NULL,
        daily_cap_lakhs REAL NOT NULL,
        label_count INTEGER NOT NULL,
        pack_size_lakhs REAL NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        reconciled_order_id TEXT,
        reconciled_at TEXT,
        reconciled_by TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(machine_id, date)
      );
      CREATE TABLE IF NOT EXISTS pc_codes (
        id SERIAL PRIMARY KEY,
        size TEXT NOT NULL,
        code TEXT NOT NULL,
        colour TEXT NOT NULL,
        pack_size INTEGER DEFAULT 100000,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(size, code)
      );
      CREATE TABLE IF NOT EXISTS reconciliation_requests (
        id TEXT PRIMARY KEY,
        proposed_by TEXT NOT NULL,
        proposed_at TEXT NOT NULL DEFAULT (datetime('now')),
        approved_by TEXT,
        approved_at TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        order_id TEXT NOT NULL,
        order_details TEXT NOT NULL,
        back_date TEXT NOT NULL,
        temp_batch_mappings TEXT NOT NULL,
        total_boxes INTEGER NOT NULL,
        rejection_reason TEXT
      );
      CREATE TABLE IF NOT EXISTS temp_batch_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        machine_id TEXT NOT NULL,
        temp_batch_id TEXT NOT NULL,
        alert_date TEXT NOT NULL,
        sent_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(machine_id, alert_date)
      );
      CREATE INDEX IF NOT EXISTS idx_temp_batches_machine ON temp_batches(machine_id, date);
      CREATE INDEX IF NOT EXISTS idx_temp_batches_status ON temp_batches(status);
      CREATE INDEX IF NOT EXISTS idx_recon_status ON reconciliation_requests(status);
    `
  },
  {
    version: 5,
    name: 'temp_colour_and_wo_support',
    sql: `
      ALTER TABLE temp_batches ADD COLUMN colour TEXT;
      ALTER TABLE temp_batches ADD COLUMN pc_code TEXT;
      ALTER TABLE temp_batches ADD COLUMN colour_confirmed INTEGER DEFAULT 0;
      ALTER TABLE tracking_labels ADD COLUMN wo_status TEXT;
      CREATE TABLE IF NOT EXISTS wo_reconciliation_requests (
        id TEXT PRIMARY KEY,
        proposed_by TEXT NOT NULL,
        proposed_at TEXT NOT NULL DEFAULT (datetime('now')),
        approved_by TEXT,
        approved_at TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        order_id TEXT NOT NULL,
        customer TEXT NOT NULL,
        po_number TEXT,
        zone TEXT,
        qty_confirmed REAL,
        rejection_reason TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_wo_recon_status ON wo_reconciliation_requests(status);
    `
  },
  {
    version: 6,
    name: 'tracking_labels_extended_fields',
    sql: `
      ALTER TABLE tracking_labels ADD COLUMN ship_to TEXT;
      ALTER TABLE tracking_labels ADD COLUMN bill_to TEXT;
      ALTER TABLE tracking_labels ADD COLUMN is_excess INTEGER DEFAULT 0;
      ALTER TABLE tracking_labels ADD COLUMN excess_num INTEGER;
      ALTER TABLE tracking_labels ADD COLUMN excess_total INTEGER;
      ALTER TABLE tracking_labels ADD COLUMN normal_total INTEGER;
    `
  },
  {
    version: 7,
    name: 'dispatch_actuals',
    sql: `
      CREATE TABLE IF NOT EXISTS tracking_dispatch_actuals (
        batch_number TEXT PRIMARY KEY,
        dispatched_qty REAL DEFAULT 0,
        vehicle_no TEXT,
        invoice_no TEXT,
        updated_at TEXT
      );
    `
  },
  {
    version: 8,
    name: 'dpr_batch_closed',
    sql: `
      CREATE TABLE IF NOT EXISTS dpr_batch_closed (
        order_id TEXT PRIMARY KEY,
        batch_number TEXT,
        closed_at TEXT NOT NULL DEFAULT (datetime('now')),
        closed_by TEXT,
        notes TEXT
      );
    `
  },
  {
    version: 9,
    name: 'dpr_settings',
    sql: `
      CREATE TABLE IF NOT EXISTS dpr_settings (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
    `
  },
  {
    version: 10,
    name: 'tracking_scans_label_number',
    sql: `ALTER TABLE tracking_scans ADD COLUMN label_number INTEGER;`
  },
];

function runMigrations() {
  const applied = new Set(
    db.prepare('SELECT version FROM schema_migrations').all().map(r => r.version)
  );
  let ran = 0;
  for (const m of MIGRATIONS) {
    if (applied.has(m.version)) continue;
    console.log(`[Migration] Running v${m.version}: ${m.name}`);
    db.exec(m.sql);
    db.prepare('INSERT INTO schema_migrations (version, name) VALUES (?, ?)').run(m.version, m.name);
    console.log(`[Migration] v${m.version} applied successfully`);
    ran++;
  }
  if (ran === 0) console.log('[Migration] All migrations up to date');
  else console.log(`[Migration] ${ran} migration(s) applied`);
}

runMigrations();

// ─── Seed default users if none exist ─────────────────────────
function hashPin(pin) { return crypto.createHash('sha256').update(pin + 'sunloc_salt').digest('hex'); }

const seedUsers = [
  { username: 'GF',                pin: '1111', role: 'gf',               app: 'dpr'      },
  { username: 'FF',                pin: '2222', role: 'ff',               app: 'dpr'      },
  { username: 'DPR_Admin',         pin: '9999', role: 'admin',            app: 'dpr'      },
  { username: 'Planning_Manager',  pin: '3333', role: 'planning_manager', app: 'planning' },
  { username: 'Printing_Manager',  pin: '4444', role: 'printing_manager', app: 'planning' },
  { username: 'Dispatch_Manager',  pin: '5555', role: 'dispatch_manager', app: 'planning' },
  { username: 'Plan_Admin',        pin: '9999', role: 'admin',            app: 'planning' },
  { username: 'Track_Admin',       pin: '9999', role: 'admin',            app: 'tracking' },
];

const insertUser = db.prepare(`
  INSERT INTO app_users (username, pin_hash, role, app)
  VALUES (?, ?, ?, ?)
  ON CONFLICT (username) DO NOTHING
`);
for (const u of seedUsers) {
  insertUser.run(u.username, hashPin(u.pin), u.role, u.app);
}

// Clean expired sessions on startup (SQLite only — PostgreSQL handles via pgPool)
if (!USE_POSTGRES) {
  try { db.prepare(`DELETE FROM app_sessions WHERE expires_at < datetime('now')`).run(); } catch(e) {}
}


// ─── Helper: get latest planning state ────────────────────────
let _planningStateCache = null;
let _planningStateCacheTime = 0;

async function getPlanningStateAsync() {
  if (pgPool) {
    const r = await pgPool.query('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1');
    if (!r.rows[0]) return { orders: [], printOrders: [], dispatchPlans: [], dailyPrinting: [], machineMaster: [], printMachineMaster: [], packSizes: {} };
    try { return JSON.parse(r.rows[0].state_json); } catch { return {}; }
  }
  return getPlanningState();
}

function getPlanningState() {
  // Return cache if fresh
  if (_planningStateCache && _planningStateCache.orders && _planningStateCache.orders.length > 0 && Date.now() - _planningStateCacheTime < 30000) return _planningStateCache;
  // Try pgPool first (PostgreSQL)
  if (pgPool) {
    // Return cache while async fetch happens — warmPlanningCache() keeps this updated
    if (_planningStateCache) return _planningStateCache;
    return { orders: [], printOrders: [], dispatchPlans: [], dailyPrinting: [], machineMaster: [], printMachineMaster: [], packSizes: {} };
  }
  // SQLite fallback
  const row = db.prepare('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1').get();
  if (!row) return { orders: [], printOrders: [], dispatchPlans: [], dailyPrinting: [], machineMaster: [], printMachineMaster: [], packSizes: {} };
  try {
    _planningStateCache = JSON.parse(row.state_json);
    _planningStateCacheTime = Date.now();
    return _planningStateCache;
  } catch { return {}; }
}

async function ensurePostgresTables() {
  if (!pgPool) return;
  try {
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_labels (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        label_number INTEGER,
        size TEXT,
        qty REAL,
        is_partial INTEGER DEFAULT 0,
        is_orange INTEGER DEFAULT 0,
        parent_label_id TEXT,
        customer TEXT,
        colour TEXT,
        pc_code TEXT,
        po_number TEXT,
        machine_id TEXT,
        printing_matter TEXT,
        generated TEXT NOT NULL DEFAULT NOW()::TEXT,
        printed INTEGER DEFAULT 0,
        printed_at TEXT,
        voided INTEGER DEFAULT 0,
        void_reason TEXT,
        voided_at TEXT,
        voided_by TEXT,
        qr_data TEXT,
        wo_status TEXT,
        ship_to TEXT,
        bill_to TEXT,
        is_excess INTEGER DEFAULT 0,
        excess_num INTEGER,
        excess_total INTEGER,
        normal_total INTEGER
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_scans (
        id TEXT PRIMARY KEY,
        label_id TEXT,
        batch_number TEXT,
        label_number INTEGER,
        dept TEXT NOT NULL,
        type TEXT NOT NULL,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT,
        operator TEXT,
        size TEXT,
        qty REAL
      )
    `);
    // CRITICAL: ensure label_number column exists — missing column causes all scans to fail with 500
    await pgPool.query(`ALTER TABLE tracking_scans ADD COLUMN IF NOT EXISTS label_number INTEGER`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_scans_dept_ts ON tracking_scans(dept, ts DESC)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_scans_batch ON tracking_scans(batch_number, dept)`);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_labels_batch ON tracking_labels(batch_number)`);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_wastage (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        type TEXT,
        qty REAL,
        by TEXT,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT,
        note TEXT
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_stage_closure (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        closed INTEGER DEFAULT 1,
        closed_at TEXT,
        closed_by TEXT,
        UNIQUE(batch_number, dept)
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dpr_batch_closed (
        order_id TEXT PRIMARY KEY,
        batch_number TEXT,
        closed_at TEXT,
        closed_by TEXT,
        notes TEXT
      )
    `);
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dpr_settings (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // planning_state
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS planning_state (
        id SERIAL PRIMARY KEY,
        state_json TEXT NOT NULL,
        saved_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // dpr_records
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS dpr_records (
        id SERIAL PRIMARY KEY,
        floor TEXT NOT NULL,
        date TEXT NOT NULL,
        data_json TEXT NOT NULL,
        saved_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        UNIQUE(floor, date)
      )
    `);

    // production_actuals
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS production_actuals (
        id SERIAL PRIMARY KEY,
        order_id TEXT,
        batch_number TEXT,
        machine_id TEXT NOT NULL,
        date TEXT NOT NULL,
        shift TEXT NOT NULL,
        run_index INTEGER NOT NULL DEFAULT 0,
        qty_lakhs REAL NOT NULL,
        floor TEXT,
        synced_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        UNIQUE(machine_id, date, shift, run_index)
      )
    `);

    // app_users
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS app_users (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        pin_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        updated_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // app_sessions
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS app_sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        expires_at TEXT NOT NULL
      )
    `);

    // audit_log
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS audit_log (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        app TEXT NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        ip TEXT,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // temp_batches
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS temp_batches (
        id TEXT PRIMARY KEY,
        machine_id TEXT NOT NULL,
        machine_size TEXT NOT NULL,
        date TEXT NOT NULL,
        daily_cap_lakhs REAL NOT NULL,
        label_count INTEGER NOT NULL,
        pack_size_lakhs REAL NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        reconciled_order_id TEXT,
        reconciled_at TEXT,
        reconciled_by TEXT,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        UNIQUE(machine_id, date)
      )
    `);

    // temp_batch_alerts
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS temp_batch_alerts (
        id TEXT PRIMARY KEY,
        temp_batch_id TEXT NOT NULL,
        machine_id TEXT NOT NULL,
        date TEXT NOT NULL,
        alert_type TEXT NOT NULL,
        message TEXT,
        resolved INTEGER DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // tracking_alerts
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_alerts (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        dept TEXT NOT NULL,
        alert_type TEXT NOT NULL,
        message TEXT,
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        resolved INTEGER DEFAULT 0
      )
    `);

    // tracking_dispatch_records
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_dispatch_records (
        id TEXT PRIMARY KEY,
        batch_number TEXT NOT NULL,
        qty REAL,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT,
        operator TEXT,
        note TEXT
      )
    `);

    // tracking_dispatch_actuals
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS tracking_dispatch_actuals (
        id SERIAL PRIMARY KEY,
        batch_number TEXT NOT NULL,
        qty REAL,
        ts TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // reconciliation_requests
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS pc_codes (
        id SERIAL PRIMARY KEY,
        size TEXT NOT NULL,
        code TEXT NOT NULL,
        colour TEXT NOT NULL,
        pack_size INTEGER DEFAULT 100000,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(size, code)
      );
      CREATE TABLE IF NOT EXISTS reconciliation_requests (
        id TEXT PRIMARY KEY,
        temp_batch_id TEXT NOT NULL,
        order_id TEXT,
        batch_number TEXT,
        requested_by TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        resolved_at TEXT,
        resolved_by TEXT,
        notes TEXT
      );
      CREATE TABLE IF NOT EXISTS print_orders (
        id TEXT PRIMARY KEY,
        machine_id TEXT,
        customer TEXT,
        batch_number TEXT,
        pc_code TEXT,
        size TEXT,
        colour TEXT,
        print_matter TEXT,
        print_type TEXT,
        qty_to_print REAL,
        order_qty REAL,
        printed_to_date REAL DEFAULT 0,
        printed_to_date_manual BOOLEAN DEFAULT false,
        start_date TEXT,
        end_date TEXT,
        status TEXT DEFAULT 'pending',
        zone TEXT,
        remarks TEXT,
        production_order_id TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // wo_reconciliation_requests
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS wo_reconciliation_requests (
        id TEXT PRIMARY KEY,
        temp_batch_id TEXT NOT NULL,
        order_id TEXT,
        batch_number TEXT,
        requested_by TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
        resolved_at TEXT,
        resolved_by TEXT,
        notes TEXT
      )
    `);

    // schema_migrations (for tracking)
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id SERIAL PRIMARY KEY,
        version INTEGER NOT NULL UNIQUE,
        name TEXT NOT NULL,
        applied_at TEXT NOT NULL DEFAULT NOW()::TEXT
      )
    `);

    // Indexes for performance
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC)`).catch(()=>{});
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_prod_actuals_date ON production_actuals(date, machine_id)`).catch(()=>{});
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_dpr_records_date ON dpr_records(date)`).catch(()=>{});

    console.log('[DB] PostgreSQL tables verified/created');
  } catch(e) {
    console.error('[DB] ensurePostgresTables error:', e.message);
  }
}

async function warmPlanningCache() {
  if (!pgPool) return;
  // Refresh cache every 60 seconds
  setInterval(async () => {
    try {
      const r = await pgPool.query('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1');
      if (r.rows[0]) {
        _planningStateCache = JSON.parse(r.rows[0].state_json);
        _planningStateCacheTime = Date.now();
      }
    } catch(e) {}
  }, 60000);
  try {
    const r = await pgPool.query('SELECT state_json FROM planning_state ORDER BY id DESC LIMIT 1');
    if (r.rows[0]) {
      _planningStateCache = JSON.parse(r.rows[0].state_json);
      _planningStateCacheTime = Date.now();
      console.log('[DB] Planning state cache warmed:', (_planningStateCache.orders||[]).length, 'orders');
    }
  } catch(e) { console.error('[DB] Cache warm error:', e.message); }
}

// ─── Helper: get active orders for a machine ──────────────────
function getActiveOrdersForMachine(machineId) {
  const state = getPlanningState();
  const orders = state.orders || [];
  return orders.filter(o =>
    o.machineId === machineId &&
    o.status !== 'closed' &&
    !o.deleted
  ).map(o => ({
    id: o.id,
    batchNumber: o.batchNumber || '',
    poNumber: o.poNumber || '',
    customer: o.customer || '',
    size: o.size || '',
    colour: o.colour || '',
    qty: o.qty || 0,
    isPrinted: o.isPrinted || false,
    status: o.status || 'pending',
    zone: o.zone || '',
  }));
}

// Helper: get total actuals for an order (sums all runs across all machines/shifts)
let _actualsCache = null;
let _actualsCacheTime = 0;
async function warmActualsCache() {
  // Throttle to 60s — prevents DB hammering from every device's 30s auto-sync
  if (Date.now() - _actualsCacheTime < 60000 && _actualsCache) return;
  _actualsCacheTime = Date.now();
  if (!pgPool) return;
  try {
    const r = await pgPool.query('SELECT order_id, batch_number, SUM(qty_lakhs) as total FROM production_actuals GROUP BY order_id, batch_number');
    _actualsCache = {};
    for (const row of r.rows) {
      if (row.order_id) _actualsCache[row.order_id] = parseFloat(row.total) || 0;
      if (row.batch_number) _actualsCache[row.batch_number] = parseFloat(row.total) || 0;
    }
    console.log('[DB] Actuals cache warmed:', r.rows.length, 'entries');
  } catch(e) { console.error('[DB] Actuals cache error:', e.message); }
}

function getOrderActuals(orderId, batchNumber) {
  if (_actualsCache) {
    return _actualsCache[orderId] || _actualsCache[batchNumber] || 0;
  }
  // Falls back to SQLite only — cache should always be warm when pgPool is available
  let rows;
  if (orderId) {
    rows = db.prepare('SELECT SUM(qty_lakhs) as total FROM production_actuals WHERE order_id = ?').get(orderId);
    if (!rows?.total && batchNumber) {
      rows = db.prepare('SELECT SUM(qty_lakhs) as total FROM production_actuals WHERE batch_number = ?').get(batchNumber);
    }
  } else if (batchNumber) {
    rows = db.prepare('SELECT SUM(qty_lakhs) as total FROM production_actuals WHERE batch_number = ?').get(batchNumber);
  }
  return rows?.total || 0;
}

// ═══════════════════════════════════════════════════════════════
// PLANNING APP ROUTES
// ═══════════════════════════════════════════════════════════════

// GET /api/pc-codes — load all custom PC codes from DB
app.get('/api/pc-codes', async (req, res) => {
  try {
    let rows = [];
    if (pgPool) {
      try {
        const r = await pgPool.query('SELECT size, code, colour, pack_size FROM pc_codes ORDER BY size, code');
        rows = r.rows;
      } catch(e) {
        // Table may not exist yet — return empty
        return res.json({ ok: true, codes: {}, count: 0 });
      }
    } else {
      try { rows = db.prepare('SELECT size, code, colour, pack_size FROM pc_codes ORDER BY size, code').all(); } catch(e) {}
    }
    const bySize = {};
    rows.forEach(r => {
      if (!bySize[r.size]) bySize[r.size] = [];
      bySize[r.size].push({ c: r.code, n: r.colour, packSize: r.pack_size });
    });
    res.json({ ok: true, codes: bySize, count: rows.length });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/pc-codes — save new PC code to DB permanently
app.post('/api/pc-codes', async (req, res) => {
  try {
    const { size, code, colour, packSize } = req.body;
    if (!size || !code || !colour) return res.status(400).json({ ok: false, error: 'size, code, colour required' });
    if (pgPool) {
      await pgPool.query(
        'INSERT INTO pc_codes (size, code, colour, pack_size) VALUES ($1, $2, $3, $4) ON CONFLICT (size, code) DO UPDATE SET colour=$3, pack_size=$4',
        [size, code, colour, packSize || 100000]
      );
    } else {
      db.prepare('INSERT OR REPLACE INTO pc_codes (size, code, colour, pack_size) VALUES (?, ?, ?, ?)').run(size, code, colour, packSize || 100000);
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// DELETE /api/pc-codes — remove a PC code from DB
app.delete('/api/pc-codes', async (req, res) => {
  try {
    const { size, code } = req.body;
    if (!size || !code) return res.status(400).json({ ok: false, error: 'size, code required' });
    if (pgPool) {
      await pgPool.query('DELETE FROM pc_codes WHERE size=$1 AND code=$2', [size, code]);
    } else {
      db.prepare('DELETE FROM pc_codes WHERE size=? AND code=?').run(size, code);
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});


// ── Print Orders — dedicated table for permanent machine assignments ──────────
// GET /api/print-orders
app.get('/api/print-orders', async (req, res) => {
  try {
    if (!pgPool) return res.json({ ok: true, printOrders: [] });
    const r = await pgPool.query('SELECT * FROM print_orders ORDER BY updated_at DESC');
    res.json({ ok: true, printOrders: r.rows.map(row => ({
      id: row.id, machineId: row.machine_id, customer: row.customer,
      batchNumber: row.batch_number, pcCode: row.pc_code, size: row.size,
      colour: row.colour, printMatter: row.print_matter, printType: row.print_type,
      qtyToPrint: parseFloat(row.qty_to_print)||0, orderQty: parseFloat(row.order_qty)||0,
      printedToDate: parseFloat(row.printed_to_date)||0,
      printedToDateManual: row.printed_to_date_manual,
      startDate: row.start_date, endDate: row.end_date, status: row.status,
      zone: row.zone, remarks: row.remarks, productionOrderId: row.production_order_id,
    })) });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/print-orders/bulk
app.post('/api/print-orders/bulk', async (req, res) => {
  try {
    const { printOrders } = req.body;
    if (!Array.isArray(printOrders)) return res.status(400).json({ ok: false, error: 'printOrders array required' });
    if (!pgPool) return res.json({ ok: true, count: 0 });
    for (const p of printOrders) {
      if (!p.id) continue;
      await pgPool.query(`
        INSERT INTO print_orders (id,machine_id,customer,batch_number,pc_code,size,colour,
          print_matter,print_type,qty_to_print,order_qty,printed_to_date,printed_to_date_manual,
          start_date,end_date,status,zone,remarks,production_order_id,updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,NOW()::TEXT)
        ON CONFLICT(id) DO UPDATE SET machine_id=$2,customer=$3,batch_number=$4,pc_code=$5,
          size=$6,colour=$7,print_matter=$8,print_type=$9,qty_to_print=$10,order_qty=$11,
          printed_to_date=$12,printed_to_date_manual=$13,start_date=$14,end_date=$15,
          status=$16,zone=$17,remarks=$18,production_order_id=$19,updated_at=NOW()::TEXT
      `, [p.id,p.machineId||null,p.customer||null,p.batchNumber||null,p.pcCode||null,
          p.size||null,p.colour||null,p.printMatter||null,p.printType||null,
          p.qtyToPrint||null,p.orderQty||null,p.printedToDate||0,p.printedToDateManual||false,
          p.startDate||null,p.endDate||null,p.status||'pending',p.zone||null,
          p.remarks||null,p.productionOrderId||null]);
    }
    res.json({ ok: true, count: printOrders.length, savedAt: new Date().toISOString() });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET full planning state — uses direct pg pool for large JSON
app.get('/api/planning/state', async (req, res) => {
  try {
    const rawState = await getPlanningStateAsync();
    // CRITICAL: deep clone before mutating — never modify the cached object directly
    // Direct mutation corrupts the cache and causes order count drops (194→175 bug)
    const state = JSON.parse(JSON.stringify(rawState));
    if (state.orders && _actualsCache) {
      for (const ord of state.orders) {
        const actual = (_actualsCache[ord.id] || _actualsCache[ord.batchNumber] || 0);
        ord.actualProd = actual;
        if (actual > 0 && ord.status === 'pending') ord.status = 'running';
      }
    }
    const savedAt = pgPool
      ? (await pgPool.query('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1')).rows[0]?.saved_at
      : db.prepare('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1').get()?.saved_at;
    res.json({ ok: true, state, savedAt });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST save planning state — uses direct pg pool for large JSON
// ── Emergency restore from backup file ─────────────────────────
app.post('/api/planning/restore', async (req, res) => {
  try {
    const state = req.body;
    if (!state || !state.orders) return res.status(400).json({ ok: false, error: 'Invalid backup format' });
    const json = JSON.stringify(state);
    if (pgPool) {
      const existing = await pgPool.query('SELECT id FROM planning_state LIMIT 1');
      if (existing.rows.length > 0) {
        await pgPool.query('UPDATE planning_state SET state_json = $1, saved_at = NOW() WHERE id = $2', [json, existing.rows[0].id]);
      } else {
        await pgPool.query('INSERT INTO planning_state (state_json) VALUES ($1)', [json]);
      }
      _planningStateCache = state;
      _planningStateCacheTime = Date.now();
    }
    res.json({ ok: true, orders: state.orders.length, savedAt: new Date().toISOString() });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.post('/api/planning/state', async (req, res) => {
  try {
    const { state } = req.body;
    if (!state) return res.status(400).json({ ok: false, error: 'No state provided' });
    const json = JSON.stringify(state);
    if (pgPool) {
      const existing = await pgPool.query('SELECT id FROM planning_state LIMIT 1');
      if (existing.rows[0]) {
        await pgPool.query('UPDATE planning_state SET state_json = $1, saved_at = NOW() WHERE id = $2', [json, existing.rows[0].id]);
      } else {
        await pgPool.query('INSERT INTO planning_state (state_json) VALUES ($1)', [json]);
      }
      _planningStateCache = state;
      _planningStateCacheTime = Date.now();
    } else {
      const existing = db.prepare('SELECT id FROM planning_state LIMIT 1').get();
      if (existing) {
        db.prepare('UPDATE planning_state SET state_json = ?, saved_at = NOW() WHERE id = ?').run(json, existing.id);
      } else {
        db.prepare('INSERT INTO planning_state (state_json) VALUES (?)').run(json);
      }
    }
    res.json({ ok: true, savedAt: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET active orders for a machine (used by DPR dropdown)
app.get('/api/orders/machine/:machineId', (req, res) => {
  try {
    const orders = getActiveOrdersForMachine(req.params.machineId);
    res.json({ ok: true, orders });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET all active orders (summary for DPR to cache on load) — only 'running' status
app.get('/api/orders/active', async (req, res) => {
  try {
    // Refresh actuals in background — throttled to 60s, non-blocking
    warmActualsCache().catch(()=>{});
    const state = await getPlanningStateAsync();
    const running = (state.orders || []).filter(o => o.status === 'running' && !o.deleted);

    // Helper: extract YYYY-MM-DD from any startDate format (Date object, ISO string, etc.)
    const getDateStr = (d) => {
      if (!d) return '';
      const s = String(d);
      // ISO format: "2026-04-15T00:00:00.000Z" → "2026-04-15"
      if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0,10);
      // Try parsing as Date
      const dt = new Date(s);
      if (!isNaN(dt)) return dt.toISOString().slice(0,10);
      return '';
    };

    const mapOrder = o => {
      // Get actual production from DPR actuals cache — this is the real produced qty
      const actualFromCache = _actualsCache ? (_actualsCache[o.id] || _actualsCache[o.batchNumber] || 0) : 0;
      const actualQty = actualFromCache || o.actualQty || o.actualProd || 0;
      return {
        id: o.id,
        batchNumber: o.batchNumber || '',
        poNumber: o.poNumber || '',
        customer: o.customer || '',
        machineId: o.machineId || '',
        size: o.size || '',
        colour: o.colour || '',
        qty: o.qty || 0,
        grossQty: o.grossQty || o.qty || 0,
        actualQty,
        status: o.status || 'running',
        isPrinted: o.isPrinted || false,
        isLegacy: !o.startDate || getDateStr(o.startDate) <= LEGACY_CUTOFF,
      };
    };

    // Separate legacy (startDate <= CUTOFF) and new orders
    const legacyOrders = running.filter(o =>
      !o.startDate || getDateStr(o.startDate) <= LEGACY_CUTOFF
    );
    const newOrders = running.filter(o =>
      o.startDate && getDateStr(o.startDate) > LEGACY_CUTOFF
    );

    // For new orders: max 2 per machine — show LATEST 2 (most recently started)
    // Sort DESC so newest orders are kept, not oldest ones that should be closed
    const newOrdersFiltered = [];
    const newCountPerMachine = {};
    const newSorted = [...newOrders].sort((a,b) => String(b.startDate).localeCompare(String(a.startDate)));
    for (const o of newSorted) {
      const mc = o.machineId || 'unknown';
      if (!newCountPerMachine[mc]) newCountPerMachine[mc] = 0;
      if (newCountPerMachine[mc] < 2) {
        newOrdersFiltered.push(o);
        newCountPerMachine[mc]++;
      }
    }

    // Return ALL legacy orders + filtered new orders
    const orders = [...legacyOrders, ...newOrdersFiltered].map(mapOrder);
    res.json({ ok: true, orders });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// DPR APP ROUTES
// ═══════════════════════════════════════════════════════════════

// POST bulk import DPR records from backup
app.post('/api/dpr/bulk-import', async (req, res) => {
  try {
    const { records } = req.body;
    if (!records || !Array.isArray(records)) return res.status(400).json({ ok: false, error: 'No records provided' });
    let saved = 0;
    if (pgPool) {
      const client = await pgPool.connect();
      try {
        await client.query('BEGIN');
        for (const { floor, date, data } of records) {
          if (!floor || !date || !data) continue;
          await client.query(`INSERT INTO dpr_records (floor, date, data_json) VALUES ($1, $2, $3) ON CONFLICT(floor, date) DO UPDATE SET data_json = EXCLUDED.data_json, saved_at = NOW()`, [floor, date, JSON.stringify(data)]);
          // Extract actuals from DPR data
          await client.query('DELETE FROM production_actuals WHERE floor = $1 AND date = $2', [floor, date]);
          const shifts = data.shifts || {};
          for (const [shiftName, shiftData] of Object.entries(shifts)) {
            if (!shiftData.machines) continue;
            for (const [machineId, machineData] of Object.entries(shiftData.machines)) {
              const runs = machineData.runs || [{ orderId: machineData.orderId, batchNumber: machineData.batchNumber, qty: machineData.prod }];
              for (let ri = 0; ri < runs.length; ri++) {
                const run = runs[ri];
                const qty = parseFloat(run.qty) || 0;
                if (qty <= 0) continue;
                await client.query(`INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                  ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET
                  order_id=EXCLUDED.order_id, batch_number=EXCLUDED.batch_number, qty_lakhs=EXCLUDED.qty_lakhs`,
                  [run.orderId||null, run.batchNumber||null, machineId, date, shiftName, ri, qty, floor]);
              }
            }
          }
          saved++;
        }
        await client.query('COMMIT');
      } catch(e) { await client.query('ROLLBACK'); throw e; }
      finally { client.release(); }
      // Refresh actuals cache
      await warmActualsCache();
    }
    res.json({ ok: true, saved });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET DPR record for a floor + date
// POST save DPR record + extract actuals into bridge table
app.post('/api/dpr/save', async (req, res) => {
  try {
    const { floor, date, data, actuals } = req.body;
    if (!floor || !date || !data) return res.status(400).json({ ok: false, error: 'Missing floor, date, or data' });

    if (pgPool) {
      // Save full DPR record to PostgreSQL
      await pgPool.query(
        `INSERT INTO dpr_records (floor, date, data_json)
         VALUES ($1, $2, $3)
         ON CONFLICT(floor, date) DO UPDATE SET data_json = EXCLUDED.data_json, saved_at = NOW()`,
        [floor, date, JSON.stringify(data)]
      );

      // Delete old actuals for this floor+date, then re-insert
      await pgPool.query('DELETE FROM production_actuals WHERE floor = $1 AND date = $2', [floor, date]);

      const actualsToSave = [];
      if (actuals && actuals.length > 0) {
        for (const a of actuals) {
          if (!a.qty || a.qty <= 0) continue;
          actualsToSave.push([a.orderId||null, a.batchNumber||null, a.machineId, date, a.shift, a.runIndex||0, a.qty, a.floor||floor]);
        }
      } else {
        const shifts = data.shifts || {};
        for (const [shiftName, shiftData] of Object.entries(shifts)) {
          if (!shiftData.machines) continue;
          for (const [machineId, machineData] of Object.entries(shiftData.machines)) {
            const runs = machineData.runs || [{orderId:machineData.orderId,batchNumber:machineData.batchNumber,qty:machineData.prod}];
            runs.forEach((run,ri) => {
              const qty = parseFloat(run.qty)||0;
              if (qty <= 0) return;
              actualsToSave.push([run.orderId||null, run.batchNumber||null, machineId, date, shiftName, ri, qty, floor]);
            });
          }
        }
      }
      for (const row of actualsToSave) {
        await pgPool.query(
          `INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET
             order_id=EXCLUDED.order_id, batch_number=EXCLUDED.batch_number,
             qty_lakhs=EXCLUDED.qty_lakhs, synced_at=NOW()`,
          row
        );
      }

      // Update planning actuals cache (two-way sync) — warm cache so Planning sees fresh data
      try {
        await warmActualsCache();
      } catch(e) { console.warn('Planning sync error:', e.message); }

    } else {
      // SQLite fallback
      db.prepare(`INSERT INTO dpr_records (floor, date, data_json) VALUES (?, ?, ?) ON CONFLICT(floor, date) DO UPDATE SET data_json = excluded.data_json, saved_at = datetime('now')`).run(floor, date, JSON.stringify(data));
      db.prepare('DELETE FROM production_actuals WHERE floor = ? AND date = ?').run(floor, date);
      const upsert = db.prepare(`INSERT INTO production_actuals (order_id, batch_number, machine_id, date, shift, run_index, qty_lakhs, floor) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(machine_id, date, shift, run_index) DO UPDATE SET order_id=excluded.order_id, batch_number=excluded.batch_number, qty_lakhs=excluded.qty_lakhs, synced_at=datetime('now')`);
      const rows = actuals && actuals.length > 0 ? actuals.filter(a=>a.qty>0).map(a=>[a.orderId||null,a.batchNumber||null,a.machineId,date,a.shift,a.runIndex||0,a.qty,a.floor||floor]) : [];
      db.transaction(rows => rows.forEach(r => upsert.run(...r)))(rows);
    }

    // Refresh actuals cache so Planning sees new DPR data immediately (force — bypass throttle)
    _actualsCacheTime = 0; // bypass 60s throttle so save is visible immediately
    warmActualsCache().catch(e => console.warn('[DPR] cache warm failed:', e.message));
    res.json({ ok: true, savedAt: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET plant report data — all floors for a single date in one call
// Returns { ok, date, floors: { GF: data|null, '1F': data|null, '2F': data|null } }
app.get('/api/dpr/plant-report/:date', async (req, res) => {
  try {
    const { date } = req.params;
    const FLOOR_KEYS = ['GF', '1F', '2F'];
    const result = {};
    for (const fl of FLOOR_KEYS) {
      if (pgPool) {
        const r = await pgPool.query('SELECT data_json FROM dpr_records WHERE floor=$1 AND date=$2', [fl, date]);
        result[fl] = r.rows[0] ? JSON.parse(r.rows[0].data_json) : null;
      } else {
        const row = db.prepare('SELECT data_json FROM dpr_records WHERE floor = ? AND date = ?').get(fl, date);
        result[fl] = row ? JSON.parse(row.data_json) : null;
      }
    }
    res.json({ ok: true, date, floors: result });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET all DPR dates (for history navigation)
app.get('/api/dpr/dates/:floor', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query('SELECT DISTINCT date FROM dpr_records WHERE floor=$1 ORDER BY date DESC', [req.params.floor]);
      rows = r.rows;
    } else {
      rows = db.prepare('SELECT DISTINCT date FROM dpr_records WHERE floor = ? ORDER BY date DESC').all(req.params.floor);
    }
    res.json({ ok: true, dates: rows.map(r => r.date) });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST close a batch in DPR (manager action)
app.post('/api/dpr/batch-close', async (req, res) => {
  try {
    const { orderId, batchNumber, closedBy, notes } = req.body;
    if (!orderId) return res.status(400).json({ ok: false, error: 'orderId required' });
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO dpr_batch_closed (order_id, batch_number, closed_at, closed_by, notes)
         VALUES ($1,$2,NOW(),$3,$4)
         ON CONFLICT(order_id) DO UPDATE SET batch_number=EXCLUDED.batch_number, closed_at=NOW(), closed_by=EXCLUDED.closed_by, notes=EXCLUDED.notes`,
        [orderId, batchNumber||null, closedBy||null, notes||null]
      );
    } else {
      db.prepare(`INSERT OR REPLACE INTO dpr_batch_closed (order_id, batch_number, closed_at, closed_by, notes)
        VALUES (?, ?, datetime('now'), ?, ?)`).run(orderId, batchNumber||null, closedBy||null, notes||null);
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// DELETE reopen a batch in DPR (admin only)
app.delete('/api/dpr/batch-close/:orderId', async (req, res) => {
  try {
    if (pgPool) {
      await pgPool.query('DELETE FROM dpr_batch_closed WHERE order_id = $1', [req.params.orderId]);
    } else {
      db.prepare('DELETE FROM dpr_batch_closed WHERE order_id = ?').run(req.params.orderId);
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET all DPR-closed batches (used by Planning to gate close button)
app.get('/api/dpr/batch-closed', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query('SELECT order_id, batch_number, closed_at, closed_by FROM dpr_batch_closed');
      rows = r.rows;
    } else {
      rows = db.prepare('SELECT order_id, batch_number, closed_at, closed_by FROM dpr_batch_closed').all();
    }
    res.json({ ok: true, closed: rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET DPR record for a floor + date — MUST be after all specific /api/dpr/* routes
app.get('/api/dpr/:floor/:date', async (req, res) => {
  try {
    const { floor, date } = req.params;
    if (pgPool) {
      const r = await pgPool.query('SELECT data_json, saved_at FROM dpr_records WHERE floor = $1 AND date = $2', [floor, date]);
      if (!r.rows.length) return res.json({ ok: true, data: null });
      res.json({ ok: true, data: JSON.parse(r.rows[0].data_json), savedAt: r.rows[0].saved_at });
    } else {
      const row = db.prepare('SELECT data_json, saved_at FROM dpr_records WHERE floor = ? AND date = ?').get(floor, date);
      if (!row) return res.json({ ok: true, data: null });
      res.json({ ok: true, data: JSON.parse(row.data_json), savedAt: row.saved_at });
    }
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET actuals summary for a machine (for DPR to show cumulative vs planned)
app.get('/api/actuals/machine/:machineId', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT date,shift,qty_lakhs,order_id,batch_number FROM production_actuals WHERE machine_id=$1 ORDER BY date DESC, shift LIMIT 90`, [req.params.machineId]);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT date,shift,qty_lakhs,order_id,batch_number FROM production_actuals WHERE machine_id=? ORDER BY date DESC, shift LIMIT 90`).all(req.params.machineId);
    }
    res.json({ ok: true, actuals: rows });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET actuals for a specific order
app.get('/api/actuals/order/:orderId', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT date,shift,qty_lakhs,machine_id FROM production_actuals WHERE order_id=$1 OR batch_number=$1 ORDER BY date,shift`, [req.params.orderId]);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT date,shift,qty_lakhs,machine_id FROM production_actuals WHERE order_id=? OR batch_number=? ORDER BY date,shift`).all(req.params.orderId, req.params.orderId);
    }
    const total = rows.reduce((s, r) => s + r.qty_lakhs, 0);
    res.json({ ok: true, actuals: rows, total });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// HEALTH CHECK + INFO
// ═══════════════════════════════════════════════════════════════

// ─── Auth helper ──────────────────────────────────────────────
function generateToken() { return crypto.randomBytes(32).toString('hex'); }

function verifyToken(token) {
  if (!token) return null;
  try {
    // db wrapper reads from PostgreSQL when pgPool is active
    // Using simple token lookup (datetime comparison handled by expiry logic below)
    const session = db.prepare(`SELECT * FROM app_sessions WHERE token = ?`).get(token);
    if (!session) return null;
    // Check expiry in JS (works for both SQLite and PostgreSQL datetime formats)
    if (session.expires_at && new Date(session.expires_at) < new Date()) return null;
    return session;
  } catch(e) { return null; }
}

function logAudit(username, role, app, action, details, ip) {
  try {
    if (pgPool) {
      pgPool.query(`INSERT INTO audit_log (username,role,app,action,details,ip) VALUES ($1,$2,$3,$4,$5,$6)`,
        [username, role, app, action, details||null, ip||null]).catch(e=>console.error('Audit log error:',e.message));
    } else {
      db.prepare(`INSERT INTO audit_log (username,role,app,action,details,ip) VALUES (?,?,?,?,?,?)`).run(username,role,app,action,details||null,ip||null);
    }
  } catch(e) { console.error('Audit log error:', e.message); }
}

// POST /api/auth/login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { username, pin, app: appName } = req.body;
    if (!username || !pin || !appName) return res.status(400).json({ ok: false, error: 'Missing credentials' });
    let user;
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM app_users WHERE username=$1 AND app=$2', [username, appName]);
      user = r.rows[0];
    } else {
      user = db.prepare('SELECT * FROM app_users WHERE username=? AND app=?').get(username, appName);
    }
    if (!user) return res.status(401).json({ ok: false, error: 'User not found' });
    if (user.pin_hash !== hashPin(pin)) return res.status(401).json({ ok: false, error: 'Invalid PIN' });
    const token = generateToken();
    const expires = new Date(Date.now() + 8 * 60 * 60 * 1000).toISOString().replace('T',' ').slice(0,19);
    if (pgPool) {
      await pgPool.query('INSERT INTO app_sessions (token,user_id,username,role,app,expires_at) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(token) DO NOTHING',
        [token, user.id, user.username, user.role, appName, expires]);
    } else {
      db.prepare('INSERT INTO app_sessions (token,user_id,username,role,app,expires_at) VALUES (?,?,?,?,?,?)').run(token, user.id, user.username, user.role, appName, expires);
    }
    logAudit(user.username, user.role, appName, 'LOGIN', 'Successful login', req.ip);
    res.json({ ok: true, token, username: user.username, role: user.role });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/auth/verify
app.post('/api/auth/verify', (req, res) => {
  const { token } = req.body;
  const session = verifyToken(token);
  if (!session) return res.status(401).json({ ok: false, error: 'Invalid or expired session' });
  res.json({ ok: true, username: session.username, role: session.role, app: session.app });
});

// POST /api/auth/logout
app.post('/api/auth/logout', async (req, res) => {
  const { token } = req.body;
  if (token) {
    const session = verifyToken(token);
    if (session) {
      logAudit(session.username, session.role, session.app, 'LOGOUT', null, req.ip);
      if (pgPool) await pgPool.query('DELETE FROM app_sessions WHERE token=$1', [token]);
      else db.prepare('DELETE FROM app_sessions WHERE token=?').run(token);
    }
  }
  res.json({ ok: true });
});

// POST /api/auth/change-pin
app.post('/api/auth/change-pin', async (req, res) => {
  try {
    const { token, username, newPin } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin' && session.username !== username) {
      return res.status(403).json({ ok: false, error: 'Only admin can change other users PINs' });
    }
    if (pgPool) {
      await pgPool.query('UPDATE app_users SET pin_hash=$1, updated_at=NOW() WHERE username=$2 AND app=$3', [hashPin(newPin), username, session.app]);
    } else {
      db.prepare(`UPDATE app_users SET pin_hash=?, updated_at=datetime('now') WHERE username=? AND app=?`).run(hashPin(newPin), username, session.app);
    }
    logAudit(session.username, session.role, session.app, 'CHANGE_PIN', `Changed PIN for ${username}`, req.ip);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/audit/log
app.post('/api/audit/log', (req, res) => {
  try {
    const { token, action, details } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    logAudit(session.username, session.role, session.app, action, details, req.ip);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/audit/view — admin only
app.get('/api/audit/view', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const limit = parseInt(req.query.limit) || 200;
    const app = req.query.app || session.app;
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM audit_log WHERE app=$1 ORDER BY ts DESC LIMIT $2`, [app, limit]);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM audit_log WHERE app = ? ORDER BY ts DESC LIMIT ?`).all(app, limit);
    }
    res.json({ ok: true, logs: rows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/auth/users — admin only, list users for an app
app.get('/api/auth/users', async (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    let users;
    if (pgPool) {
      const r = await pgPool.query(`SELECT id,username,role,app,created_at,updated_at FROM app_users WHERE app=$1`, [req.query.app || session.app]);
      users = r.rows;
    } else {
      users = db.prepare(`SELECT id,username,role,app,created_at,updated_at FROM app_users WHERE app=?`).all(req.query.app || session.app);
    }
    res.json({ ok: true, users });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── TEMP Batch Colour/PC Code Update ────────────────────────

// POST /api/temp-batches/update-details — save colour + PC Code (one-time per TEMP batch)
app.post('/api/temp-batches/update-details', async (req, res) => {
  try {
    const { tempBatchId, colour, pcCode } = req.body;
    if (!tempBatchId) return res.status(400).json({ ok: false, error: 'Missing tempBatchId' });
    let updated;
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1', [tempBatchId]);
      if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'TEMP batch not found' });
      await pgPool.query(`UPDATE temp_batches SET colour=$1, pc_code=$2, colour_confirmed=1 WHERE id=$3`, [colour||null, pcCode||null, tempBatchId]);
      const r2 = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1', [tempBatchId]);
      updated = r2.rows[0];
    } else {
      const tb = db.prepare('SELECT * FROM temp_batches WHERE id = ?').get(tempBatchId);
      if (!tb) return res.status(404).json({ ok: false, error: 'TEMP batch not found' });
      db.prepare(`UPDATE temp_batches SET colour = ?, pc_code = ?, colour_confirmed = 1 WHERE id = ?`).run(colour||null, pcCode||null, tempBatchId);
      updated = db.prepare('SELECT * FROM temp_batches WHERE id = ?').get(tempBatchId);
    }
    logAudit('SYSTEM', 'system', 'dpr', 'TEMP_DETAILS_SET', `TEMP batch ${tempBatchId} — Colour: ${colour}, PC Code: ${pcCode}`);
    res.json({ ok: true, batch: updated });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── W/O (Without Order) Reconciliation ──────────────────────

// POST /api/wo/assign-customer — Planning Manager assigns customer to W/O order
app.post('/api/wo/assign-customer', async (req, res) => {
  try {
    const { token, orderId, customer, poNumber, zone, qtyConfirmed } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (!['planning_manager','admin'].includes(session.role)) {
      return res.status(403).json({ ok: false, error: 'Planning Manager or Admin required' });
    }
    // Update the planning state order
    const planState = getPlanningState();
    const ord = (planState.orders || []).find(o => o.id === orderId);
    if (!ord) return res.status(404).json({ ok: false, error: 'Order not found' });
    if (ord.woStatus !== 'wo') return res.status(400).json({ ok: false, error: 'Order is not a W/O order' });
    ord.customer = customer;
    ord.poNumber = poNumber || ord.poNumber;
    ord.zone = zone || ord.zone;
    if (qtyConfirmed) ord.qty = qtyConfirmed;
    ord.woCustomerAssignedAt = new Date().toISOString();
    ord.woCustomerAssignedBy = session.username;
    // Update dispatch plan customer too
    (planState.dispatchPlans || []).forEach(d => {
      if (d.productionOrderId === orderId) {
        d.customer = customer;
        d.poNumber = poNumber || d.poNumber;
        d.zone = zone || d.zone;
      }
    });
    if (pgPool) {
      await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json,saved_at=NOW()::TEXT`, [JSON.stringify(planState)]);
    } else {
      db.prepare(`INSERT INTO planning_state (id, state_json) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET state_json = excluded.state_json, updated_at = datetime('now')`).run(JSON.stringify(planState));
    }
    _planningStateCache = planState;
    logAudit(session.username, session.role, 'planning', 'WO_CUSTOMER_ASSIGNED',
      `W/O order ${orderId} assigned to customer: ${customer}`);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/propose-reconciliation — Planning Manager proposes W/O → real order
app.post('/api/wo/propose-reconciliation', async (req, res) => {
  try {
    const { token, orderId, customer, poNumber, zone, qtyConfirmed } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    if (!['planning_manager','admin'].includes(session.role)) {
      return res.status(403).json({ ok: false, error: 'Planning Manager or Admin required' });
    }
    if (!customer) return res.status(400).json({ ok: false, error: 'Customer name required' });
    const id = `WORECON-${Date.now()}`;
    const billTo = req.body.billTo || '';
    if (pgPool) {
      await pgPool.query(`INSERT INTO wo_reconciliation_requests (id,proposed_by,status,order_id,customer,po_number,zone,qty_confirmed) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [id, session.username, 'pending', orderId, customer, poNumber||null, zone||null, qtyConfirmed||null]);
      if (billTo && billTo !== customer) await pgPool.query('UPDATE wo_reconciliation_requests SET customer=$1 WHERE id=$2', [customer+'|||'+billTo, id]);
    } else {
      db.prepare(`INSERT INTO wo_reconciliation_requests (id,proposed_by,status,order_id,customer,po_number,zone,qty_confirmed) VALUES (?,?,?,?,?,?,?,?)`).run(id, session.username, 'pending', orderId, customer, poNumber||null, zone||null, qtyConfirmed||null);
      if (billTo && billTo !== customer) db.prepare('UPDATE wo_reconciliation_requests SET customer=? WHERE id=?').run(customer+'|||'+billTo, id);
    }
    logAudit(session.username, session.role, 'planning', 'WO_RECON_PROPOSED',
      `W/O reconciliation proposed: ${id} for order ${orderId} → customer ${customer}`);
    res.json({ ok: true, requestId: id, status: 'pending' });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/wo/pending — Admin views pending W/O reconciliation requests
app.get('/api/wo/pending', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    let woRows;
    if (pgPool) { const r = await pgPool.query(`SELECT * FROM wo_reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`); woRows=r.rows; }
    else { woRows = db.prepare(`SELECT * FROM wo_reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`).all(); }
    const planState = getPlanningState();
    const enriched = woRows.map(r => ({...r, orderDetails:(planState.orders||[]).find(o=>o.id===r.order_id)||{}}));
    res.json({ ok: true, requests: enriched });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/approve/:id — Admin approves W/O reconciliation
app.post('/api/wo/approve/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const request = pgPool ? (await pgPool.query('SELECT * FROM wo_reconciliation_requests WHERE id=$1',[req.params.id])).rows[0] : db.prepare('SELECT * FROM wo_reconciliation_requests WHERE id=?').get(req.params.id);
    if (!request) return res.status(404).json({ ok: false, error: 'Request not found' });
    if (request.status !== 'pending') return res.status(400).json({ ok: false, error: 'Already processed' });

    const approveWO = async () => {
      const now = new Date().toISOString();
      // 1. Update planning state: change woStatus to 'active', add customer
      const planState = getPlanningState();
      const ord = (planState.orders || []).find(o => o.id === request.order_id);
      if (ord) {
        const custParts = (request.customer||'').split('|||');
        ord.customer = custParts[0];
        ord.shipTo   = custParts[0];
        ord.billTo   = custParts[1] || '';
        ord.poNumber = request.po_number || ord.poNumber;
        ord.zone = request.zone || ord.zone;
        if (request.qty_confirmed) ord.qty = request.qty_confirmed;
        ord.woStatus = 'wo-reconciled';
        ord.woReconciledAt = now;
        ord.woReconciledBy = session.username;
        // Update dispatch plans
        (planState.dispatchPlans || []).forEach(d => {
          if (d.productionOrderId === request.order_id) {
            d.customer = request.customer;
            d.poNumber = request.po_number || d.poNumber;
            d.zone = request.zone || d.zone;
          }
        });
        if(pgPool){ await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json,saved_at=NOW()::TEXT`,[JSON.stringify(planState)]); _planningStateCache=planState; _planningStateCacheTime=Date.now(); }
        else { db.prepare(`INSERT INTO planning_state (id,state_json) VALUES (1,?) ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json,updated_at=datetime('now')`).run(JSON.stringify(planState)); }
      }
      // 2. Update all tracking labels for this order's batch
      if (ord) {
        if(pgPool) await pgPool.query(`UPDATE tracking_labels SET customer=$1,wo_status='wo-reconciled' WHERE batch_number=$2`,[request.customer,ord.batchNumber]);
        else db.prepare(`UPDATE tracking_labels SET customer=?,wo_status='wo-reconciled' WHERE batch_number=?`).run(request.customer,ord.batchNumber);
      }
      // 3. Mark request approved
      if(pgPool) await pgPool.query(`UPDATE wo_reconciliation_requests SET status='approved',approved_by=$1,approved_at=$2 WHERE id=$3`,[session.username,now,request.id]);
      else db.prepare(`UPDATE wo_reconciliation_requests SET status='approved',approved_by=?,approved_at=? WHERE id=?`).run(session.username,now,request.id);
      return { orderId: request.order_id, customer: request.customer };
    };

    const result = await approveWO();
    logAudit(session.username, session.role, 'planning', 'WO_RECON_APPROVED',
      `W/O reconciliation ${req.params.id} approved — order ${result.orderId} → ${result.customer}`);
    res.json({ ok: true, result, message: 'W/O reconciliation complete. Replacement labels ready for printing.' });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/wo/reject/:id
app.post('/api/wo/reject/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok: false, error: 'Admin only' });
    const { reason } = req.body;
    if (pgPool) {
      await pgPool.query(`UPDATE wo_reconciliation_requests SET status='rejected',approved_by=$1,approved_at=NOW(),rejection_reason=$2 WHERE id=$3`,
        [session.username, reason||'No reason given', req.params.id]);
    } else {
      db.prepare(`UPDATE wo_reconciliation_requests SET status='rejected',approved_by=?,approved_at=datetime('now'),rejection_reason=? WHERE id=?`).run(session.username, reason||'No reason given', req.params.id);
    }
    logAudit(session.username, session.role, 'planning', 'WO_RECON_REJECTED', `Rejected ${req.params.id}: ${reason}`);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/wo/history
app.get('/api/wo/history', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    let woHistRows;
    if (pgPool) { const r = await pgPool.query('SELECT * FROM wo_reconciliation_requests ORDER BY proposed_at DESC LIMIT 50'); woHistRows=r.rows; }
    else { woHistRows = db.prepare('SELECT * FROM wo_reconciliation_requests ORDER BY proposed_at DESC LIMIT 50').all(); }
    res.json({ ok: true, requests: woHistRows });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── Data Export / Import (Admin — for safe migrations) ────────

// GET /api/admin/export — full database export as JSON
app.get('/api/admin/export', (req, res) => {
  try {
    const token = req.headers['x-session-token'] || req.query.token;
    // Allow export with admin token OR with a special export key env var
    const exportKey = process.env.EXPORT_KEY || 'sunloc-export-2024';
    const isKeyAuth = req.query.key === exportKey;
    if (!isKeyAuth) {
      const session = verifyToken(token);
      if (!session || session.role !== 'admin') {
        return res.status(403).json({ ok: false, error: 'Admin access required' });
      }
    }

    const tables = [
      'planning_state', 'dpr_records', 'production_actuals',
      'tracking_labels', 'tracking_scans', 'tracking_stage_closure',
      'tracking_wastage', 'tracking_dispatch_records', 'tracking_alerts',
      'app_users', 'audit_log', 'schema_migrations'
    ];

    const exportData = {
      exported_at: new Date().toISOString(),
      db_path: DB_PATH,
      version: 'sunloc-v9',
      tables: {}
    };

    for (const table of tables) {
      try {
        exportData.tables[table] = db.prepare(`SELECT * FROM ${table}`).all();
      } catch (e) {
        exportData.tables[table] = []; // table may not exist yet
      }
    }

    const json = JSON.stringify(exportData, null, 2);
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="sunloc-backup-${new Date().toISOString().slice(0,10)}.json"`);
    res.send(json);
    console.log(`[Export] Full database exported — ${Object.values(exportData.tables).reduce((s,t)=>s+t.length,0)} total rows`);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/admin/import — restore database from JSON export
app.post('/api/admin/import', (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const exportKey = process.env.EXPORT_KEY || 'sunloc-export-2024';
    const isKeyAuth = req.query.key === exportKey;
    if (!isKeyAuth) {
      const session = verifyToken(token);
      if (!session || session.role !== 'admin') {
        return res.status(403).json({ ok: false, error: 'Admin access required' });
      }
    }

    const { tables, confirm } = req.body;
    if (confirm !== 'IMPORT_CONFIRMED') {
      return res.status(400).json({ ok: false, error: 'Must include confirm: "IMPORT_CONFIRMED"' });
    }
    if (!tables) return res.status(400).json({ ok: false, error: 'No tables data provided' });

    // Run migrations first to ensure schema is up to date
    runMigrations();

    const results = {};
    const importTransaction = db.transaction(() => {
      // Only import data tables — not sessions or migrations
      const importableTables = [
        'planning_state', 'dpr_records', 'production_actuals',
        'tracking_labels', 'tracking_scans', 'tracking_stage_closure',
        'tracking_wastage', 'tracking_dispatch_records', 'tracking_alerts'
      ];

      for (const table of importableTables) {
        const rows = tables[table];
        if (!rows || rows.length === 0) { results[table] = 0; continue; }
        try {
          // Get column names from first row
          const cols = Object.keys(rows[0]);
          const placeholders = cols.map(() => '?').join(',');
          const stmt = db.prepare(
            `INSERT OR REPLACE INTO ${table} (${cols.join(',')}) VALUES (${placeholders})`
          );
          let count = 0;
          for (const row of rows) {
            stmt.run(cols.map(c => row[c]));
            count++;
          }
          results[table] = count;
        } catch (e) {
          results[table] = `ERROR: ${e.message}`;
        }
      }
    });
    importTransaction();

    const totalRows = Object.values(results).reduce((s,v)=>typeof v==='number'?s+v:s, 0);
    console.log(`[Import] Restored ${totalRows} rows across ${Object.keys(results).length} tables`);
    res.json({ ok: true, results, totalRows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/admin/db-status — show DB path, size, migration status
app.get('/api/admin/db-status', (req, res) => {
  try {
    const migrations = db.prepare('SELECT * FROM schema_migrations ORDER BY version').all();
    const tableRowCounts = {};
    const tables = ['planning_state','dpr_records','production_actuals','tracking_labels',
      'tracking_scans','tracking_stage_closure','tracking_wastage','tracking_dispatch_records',
      'tracking_alerts','app_users','audit_log'];
    for (const t of tables) {
      try { tableRowCounts[t] = db.prepare(`SELECT COUNT(*) as c FROM ${t}`).get().c; }
      catch(e) { tableRowCounts[t] = 'N/A'; }
    }
    let dbSizeBytes = 0;
    try { dbSizeBytes = fs.statSync(DB_PATH).size; } catch(e){}
    res.json({
      ok: true,
      db_path: DB_PATH,
      db_size_mb: (dbSizeBytes / 1024 / 1024).toFixed(2),
      migrations_applied: migrations.length,
      migrations,
      table_row_counts: tableRowCounts
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ─── TEMP Batch System ─────────────────────────────────────────

// Helper: calculate label count from daily cap and pack size
function calcTempLabelCount(capLakhs, packSizeLakhs) {
  return Math.ceil(capLakhs / packSizeLakhs);
}

// Helper: generate TEMP batch ID
function tempBatchId(machineId, date) {
  return `TEMP-${machineId}-${date.replace(/-/g,'')}`;
}

// GET /api/temp-batches/check/:machineId — check if machine needs TEMP batch today
app.get('/api/temp-batches/check/:machineId', async (req, res) => {
  try {
    const { machineId } = req.params;
    const today = new Date().toISOString().split('T')[0];
    const planState = getPlanningState();
    const activeOrders = (planState.orders || []).filter(o =>
      o.machineId === machineId && o.status !== 'closed' && !o.deleted
    );
    const hasActiveOrder = activeOrders.length > 0;
    let existing = null, allTemp = [];
    if (pgPool) {
      const r1 = await pgPool.query(`SELECT * FROM temp_batches WHERE machine_id=$1 AND date=$2`, [machineId, today]);
      existing = r1.rows[0] || null;
      const r2 = await pgPool.query(`SELECT * FROM temp_batches WHERE machine_id=$1 AND status='active' ORDER BY date DESC`, [machineId]);
      allTemp = r2.rows;
    } else {
      existing = db.prepare(`SELECT * FROM temp_batches WHERE machine_id = ? AND date = ?`).get(machineId, today);
      allTemp = db.prepare(`SELECT * FROM temp_batches WHERE machine_id = ? AND status = 'active' ORDER BY date DESC`).all(machineId);
    }
    const mc = (planState.machineMaster || []).find(m => m.id === machineId);
    const packSizes = planState.packSizes || {};
    const packSizeLakhs = mc ? ((packSizes[mc.size] || 100000) / 100000) : 1;
    const capLakhs = mc ? (mc.cap || 8) : 8;
    const labelCount = mc ? calcTempLabelCount(capLakhs, packSizeLakhs) : 0;
    res.json({
      ok: true, machineId, hasActiveOrder,
      activeOrders: activeOrders.map(o => ({ id:o.id, batchNumber:o.batchNumber, qty:o.qty, status:o.status })),
      todayTempBatch: existing || null,
      needsTemp: !hasActiveOrder,
      machineInfo: mc ? { size: mc.size, capLakhs, packSizeLakhs, labelCount } : null,
      activeTempBatches: allTemp
    });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/temp-batches/create — create TEMP batch for a machine/date
app.post('/api/temp-batches/create', async (req, res) => {
  try {
    const { machineId, date } = req.body;
    const batchDate = date || new Date().toISOString().split('T')[0];
    const id = tempBatchId(machineId, batchDate);
    const planState = getPlanningState();
    const mc = (planState.machineMaster || []).find(m => m.id === machineId);
    if (!mc) return res.status(400).json({ ok:false, error:'Machine not found' });
    const packSizes = planState.packSizes || {};
    const packSizeLakhs = (packSizes[mc.size] || 100000) / 100000;
    const capLakhs = mc.cap || 8;
    const labelCount = calcTempLabelCount(capLakhs, packSizeLakhs);
    let batch;
    if (pgPool) {
      await pgPool.query(`INSERT INTO temp_batches (id,machine_id,machine_size,date,daily_cap_lakhs,label_count,pack_size_lakhs) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO NOTHING`,
        [id, machineId, mc.size, batchDate, capLakhs, labelCount, packSizeLakhs]);
      await pgPool.query(`INSERT INTO temp_batch_alerts (machine_id,temp_batch_id,alert_date) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
        [machineId, id, batchDate]);
      const r = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1', [id]);
      batch = r.rows[0];
    } else {
      db.prepare(`INSERT OR IGNORE INTO temp_batches (id,machine_id,machine_size,date,daily_cap_lakhs,label_count,pack_size_lakhs) VALUES (?,?,?,?,?,?,?)`).run(id, machineId, mc.size, batchDate, capLakhs, labelCount, packSizeLakhs);
      db.prepare(`INSERT OR IGNORE INTO temp_batch_alerts (machine_id,temp_batch_id,alert_date) VALUES (?,?,?)`).run(machineId, id, batchDate);
      batch = db.prepare(`SELECT * FROM temp_batches WHERE id = ?`).get(id);
    }
    logAudit('SYSTEM','system','dpr','TEMP_BATCH_CREATED',`TEMP batch created: ${id} — ${capLakhs}L → ${labelCount} labels (Size ${mc.size})`);
    res.json({ ok:true, batch });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/temp-batches/active — all active TEMP batches (for alerts)
// ── Delete TEMP batches for a specific date ─────────────────
app.delete('/api/temp-batches/by-date', async (req, res) => {
  try {
    const { date } = req.query; // date = YYYY-MM-DD
    if (!date) return res.status(400).json({ ok: false, error: 'date required' });
    let deleted = 0;
    if (pgPool) {
      const r = await pgPool.query(
        `DELETE FROM temp_batches WHERE date = $1 AND status != 'reconciled'`, [date]
      );
      deleted = r.rowCount;
    } else {
      const r = db.prepare(`DELETE FROM temp_batches WHERE date = ? AND status != 'reconciled'`).run(date);
      deleted = r.changes;
    }
    res.json({ ok: true, deleted, date });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.get('/api/temp-batches/active', async (req, res) => {
  try {
    let batches;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM temp_batches WHERE status='active' ORDER BY machine_id, date DESC`);
      batches = r.rows;
    } else {
      batches = db.prepare(`SELECT * FROM temp_batches WHERE status='active' ORDER BY machine_id, date DESC`).all();
    }
    const today = new Date().toISOString().split('T')[0];
    const TEMP_CUTOFF = '2026-04-27';
    // Ignore TEMP batches created before April 25 2026
    const filtered = batches.filter(b => (b.created_at||b.date||'') >= TEMP_CUTOFF);
    const enriched = filtered.map(b => ({...b, daysActive: Math.floor((new Date(today)-new Date(b.date))/86400000)+1}));
    res.json({ ok:true, batches: enriched, count: enriched.length });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/reconciliation/propose — Planning Manager proposes reconciliation
app.post('/api/reconciliation/propose', async (req, res) => {
  try {
    const { token, orderDetails, backDate, tempBatchMappings } = req.body;
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok:false, error:'Not authenticated' });
    if (!['planning_manager','admin'].includes(session.role)) {
      return res.status(403).json({ ok:false, error:'Planning Manager or Admin required' });
    }

    // Validate back-date: cannot be before earliest TEMP batch date
    const earliestTempDate = tempBatchMappings.reduce((min, m) => {
      return m.tempDate < min ? m.tempDate : min;
    }, '9999-12-31');

    if (backDate < earliestTempDate) {
      return res.status(400).json({
        ok:false,
        error: `Back-date (${backDate}) cannot be before earliest TEMP batch date (${earliestTempDate})`
      });
    }

    // Validate all TEMP batches exist and are active
    for (const mapping of tempBatchMappings) {
      let tb;
      if (pgPool) { const r = await pgPool.query('SELECT * FROM temp_batches WHERE id=$1',[mapping.tempBatchId]); tb=r.rows[0]; }
      else { tb = db.prepare('SELECT * FROM temp_batches WHERE id=?').get(mapping.tempBatchId); }
      if (!tb) return res.status(400).json({ ok:false, error:`TEMP batch ${mapping.tempBatchId} not found` });
      if (tb.status !== 'active') return res.status(400).json({ ok:false, error:`TEMP batch ${mapping.tempBatchId} is not active` });
    }

    const totalBoxes = tempBatchMappings.reduce((s,m) => s + (m.boxes || 0), 0);
    const id = `RECON-${Date.now()}`;

    if (pgPool) {
      await pgPool.query(`INSERT INTO reconciliation_requests (id,proposed_by,status,order_id,order_details,back_date,temp_batch_mappings,total_boxes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [id, session.username, 'pending', orderDetails.id||`ORDER-${Date.now()}`, JSON.stringify(orderDetails), backDate, JSON.stringify(tempBatchMappings), totalBoxes]);
    } else {
      db.prepare(`INSERT INTO reconciliation_requests (id,proposed_by,status,order_id,order_details,back_date,temp_batch_mappings,total_boxes) VALUES (?,?,?,?,?,?,?,?)`).run(
        id, session.username, 'pending', orderDetails.id||`ORDER-${Date.now()}`, JSON.stringify(orderDetails), backDate, JSON.stringify(tempBatchMappings), totalBoxes);
    }

    logAudit(session.username, session.role, 'planning', 'RECON_PROPOSED',
      `Reconciliation proposed: ${id} — ${tempBatchMappings.length} TEMP batches → Order, ${totalBoxes} boxes`);

    res.json({ ok:true, requestId: id, status:'pending', message:'Awaiting Admin approval' });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/reconciliation/pending — Admin views pending requests
app.get('/api/reconciliation/pending', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin only' });
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM reconciliation_requests WHERE status='pending' ORDER BY proposed_at DESC`).all();
    }
    const requests = rows.map(r => ({...r, order_details:JSON.parse(r.order_details||'{}'), temp_batch_mappings:JSON.parse(r.temp_batch_mappings||'[]')}));
    res.json({ ok:true, requests });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// POST /api/reconciliation/approve/:id — Admin approves and executes reconciliation
app.post('/api/reconciliation/approve/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') {
      return res.status(403).json({ ok:false, error:'Admin only' });
    }

    const request = pgPool ? (await pgPool.query('SELECT * FROM reconciliation_requests WHERE id=$1',[req.params.id])).rows[0] : db.prepare('SELECT * FROM reconciliation_requests WHERE id=?').get(req.params.id);
    if (!request) return res.status(404).json({ ok:false, error:'Request not found' });
    if (request.status !== 'pending') return res.status(400).json({ ok:false, error:'Request is not pending' });

    const orderDetails = JSON.parse(request.order_details);
    const mappings = JSON.parse(request.temp_batch_mappings);
    const orderId = request.order_id;

    // Execute reconciliation atomically
    const reconcileAsync = async () => {
      const now = new Date().toISOString();
      const results = { migratedScans:0, migratedLabels:0, migratedWastage:0, tempBatchesReconciled:0 };

      for (const mapping of mappings) {
        const { tempBatchId: tbId, boxes, startLabelNumber, endLabelNumber } = mapping;
        const tb = pgPool ? (await pgPool.query('SELECT * FROM temp_batches WHERE id=$1',[tbId])).rows[0] : db.prepare('SELECT * FROM temp_batches WHERE id=?').get(tbId);
        if (!tb) continue;

        // Determine production month from TEMP batch date (never changes)
        const prodMonth = tb.date.slice(0,7); // YYYY-MM

        // 1. Migrate tracking labels for this TEMP batch (within box range if partial)
        const labelFilter = (startLabelNumber && endLabelNumber)
          ? `batch_number = ? AND label_number >= ? AND label_number <= ?`
          : `batch_number = ?`;
        const labelArgs = (startLabelNumber && endLabelNumber)
          ? [tbId, startLabelNumber, endLabelNumber]
          : [tbId];

        const labelsToMigrate = pgPool ? (await pgPool.query(`SELECT * FROM tracking_labels WHERE ${labelFilter.replace('?','$1').replace('?','$2').replace('?','$3')}`, labelArgs)).rows : db.prepare(`SELECT * FROM tracking_labels WHERE ${labelFilter}`).all(...labelArgs);

        for (const label of labelsToMigrate) {
          const newLabelId = label.id.replace(tbId, orderId);
          if(pgPool){ await pgPool.query(`INSERT INTO tracking_labels SELECT replace(id,$1,$2) as id,$2 as batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data FROM tracking_labels WHERE id=$3 ON CONFLICT(id) DO NOTHING`,[tbId,orderId,label.id]); } else { db.prepare(`INSERT OR REPLACE INTO tracking_labels SELECT replace(id,?,?) as id,? as batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data FROM tracking_labels WHERE id=?`).run(tbId,orderId,orderId,label.id); }

          // Migrate scans for this label
          if(pgPool){ const sm=await pgPool.query(`UPDATE tracking_scans SET label_id=replace(label_id,$1,$2),batch_number=$2 WHERE label_id=$3`,[tbId,orderId,label.id]); results.migratedScans+=sm.rowCount||0; } else { const scanMigrated=db.prepare(`UPDATE tracking_scans SET label_id=replace(label_id,?,?),batch_number=? WHERE label_id=?`).run(tbId,orderId,orderId,label.id); results.migratedScans+=scanMigrated.changes; }
          results.migratedLabels++;

          // Remove old TEMP label if new one created
          if (newLabelId !== label.id) {
            if(pgPool) await pgPool.query('DELETE FROM tracking_labels WHERE id=$1 AND id!=$2',[label.id,newLabelId]); else db.prepare('DELETE FROM tracking_labels WHERE id=? AND id!=?').run(label.id,newLabelId);
          }
        }

        // 2. Migrate wastage records
        if(pgPool){ const wm=await pgPool.query('UPDATE tracking_wastage SET batch_number=$1 WHERE batch_number=$2',[orderId,tbId]); results.migratedWastage+=wm.rowCount||0; } else { const wastage=db.prepare('UPDATE tracking_wastage SET batch_number=? WHERE batch_number=?').run(orderId,tbId); results.migratedWastage+=wastage.changes; }

        // 3. Migrate stage closures
        if(pgPool) await pgPool.query('UPDATE tracking_stage_closure SET batch_number=$1 WHERE batch_number=$2',[orderId,tbId]); else db.prepare('UPDATE tracking_stage_closure SET batch_number=? WHERE batch_number=?').run(orderId,tbId);

        // 4. Migrate DPR production actuals
        if(pgPool) await pgPool.query('UPDATE production_actuals SET order_id=$1,batch_number=$2 WHERE batch_number=$3',[orderId,orderDetails.batchNumber||orderId,tbId]); else db.prepare('UPDATE production_actuals SET order_id=?,batch_number=? WHERE batch_number=?').run(orderId,orderDetails.batchNumber||orderId,tbId);

        // 5. Update dispatch records
        if(pgPool) await pgPool.query('UPDATE tracking_dispatch_records SET batch_number=$1 WHERE batch_number=$2',[orderId,tbId]); else db.prepare('UPDATE tracking_dispatch_records SET batch_number=? WHERE batch_number=?').run(orderId,tbId);

        // 6. Mark TEMP batch as reconciled (or partially reconciled)
        const isFullReconcile = !startLabelNumber; // full batch
        if(pgPool) await pgPool.query('UPDATE temp_batches SET status=$1,reconciled_order_id=$2,reconciled_at=$3,reconciled_by=$4 WHERE id=$5',[isFullReconcile?'reconciled':'partial',orderId,now,session.username,tbId]); else db.prepare('UPDATE temp_batches SET status=?,reconciled_order_id=?,reconciled_at=?,reconciled_by=? WHERE id=?').run(isFullReconcile?'reconciled':'partial',orderId,now,session.username,tbId);
        results.tempBatchesReconciled++;
      }

      // 7. Update planning state - add/update order with back-date and correct actualQty
      const planState = getPlanningState();
      if (planState.orders) {
        // Check if order already exists (Planning Manager may have pre-entered it)
        const existingIdx = planState.orders.findIndex(o => o.id === orderId);
        const orderToSave = {
          ...orderDetails,
          id: orderId,
          startDate: request.back_date,
          actualQty: mappings.reduce((s,m) => s + (m.actualLakhs || 0), 0),
          status: 'running'
        };
        if (existingIdx >= 0) {
          planState.orders[existingIdx] = { ...planState.orders[existingIdx], ...orderToSave };
        } else {
          planState.orders.push(orderToSave);
        }
        await pgPool.query(`INSERT INTO planning_state (id,state_json) VALUES (1,$1) ON CONFLICT(id) DO UPDATE SET state_json=EXCLUDED.state_json, saved_at=NOW()::TEXT`, [JSON.stringify(planState)]);
        _planningStateCache = planState; _planningStateCacheTime = Date.now();
      }

      // 8. Mark reconciliation request as approved
      await pgPool.query(`UPDATE reconciliation_requests SET status='approved',approved_by=$1,approved_at=$2 WHERE id=$3`,
        [session.username, now, request.id]);

      return results;
    };

    const results = await reconcileAsync();

    logAudit(session.username, session.role, 'planning', 'RECON_APPROVED',
      `Reconciliation ${req.params.id} approved — ${results.migratedLabels} labels, ${results.migratedScans} scans migrated`);

    res.json({ ok:true, results, message:'Reconciliation complete. Replacement labels ready for printing.' });
  } catch(err) {
    res.status(500).json({ ok:false, error:err.message });
  }
});

// POST /api/reconciliation/reject/:id — Admin rejects
app.post('/api/reconciliation/reject/:id', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session || session.role !== 'admin') return res.status(403).json({ ok:false, error:'Admin only' });
    const { reason } = req.body;
    if (pgPool) {
      await pgPool.query(`UPDATE reconciliation_requests SET status='rejected',approved_by=$1,approved_at=NOW(),rejection_reason=$2 WHERE id=$3`,
        [session.username, reason||'No reason given', req.params.id]);
    } else {
      db.prepare(`UPDATE reconciliation_requests SET status='rejected',approved_by=?,approved_at=datetime('now'),rejection_reason=? WHERE id=?`).run(session.username, reason||'No reason given', req.params.id);
    }
    logAudit(session.username, session.role, 'planning', 'RECON_REJECTED', `Rejected: ${req.params.id} — ${reason}`);
    res.json({ ok:true });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

// GET /api/reconciliation/history — all reconciliation requests
app.get('/api/reconciliation/history', async (req, res) => {
  try {
    const token = req.headers['x-session-token'];
    const session = verifyToken(token);
    if (!session) return res.status(401).json({ ok:false, error:'Not authenticated' });
    let rows;
    if (pgPool) {
      const r = await pgPool.query(`SELECT * FROM reconciliation_requests ORDER BY proposed_at DESC LIMIT 100`);
      rows = r.rows;
    } else {
      rows = db.prepare(`SELECT * FROM reconciliation_requests ORDER BY proposed_at DESC LIMIT 100`).all();
    }
    const requests = rows.map(r => ({...r, order_details:JSON.parse(r.order_details||'{}'), temp_batch_mappings:JSON.parse(r.temp_batch_mappings||'[]')}));
    res.json({ ok:true, requests });
  } catch(err) { res.status(500).json({ ok:false, error:err.message }); }
});

app.get('/api/health', (req, res) => {
  try {
    const planningRow  = db.prepare('SELECT saved_at FROM planning_state ORDER BY id DESC LIMIT 1').get();
    const dprCount     = db.prepare('SELECT COUNT(*) as c FROM dpr_records').get();
    const actualsCount = db.prepare('SELECT COUNT(*) as c FROM production_actuals').get();
    res.json({
      ok: true,
      server: 'Sunloc Integrated Server v1.0',
      db: DB_PATH,
      planningSavedAt: planningRow?.saved_at || null,
      dprRecords: dprCount?.c || 0,
      actualsEntries: actualsCount?.c || 0,
      uptime: Math.floor(process.uptime()) + 's',
    });
  } catch(err) {
    // Server is alive even if DB query fails (e.g. still warming up)
    res.json({ ok: true, server: 'Sunloc Integrated Server v1.0', db: DB_PATH, uptime: Math.floor(process.uptime())+'s', note: 'DB initialising: '+err.message });
  }
});

// NOTE: catch-all SPA fallback moved to END of file (after all API routes)
// so that /api/tracking/* routes are not intercepted by the wildcard.

// ═══════════════════════════════════════════════════════
// TRACKING APP SCHEMA (add to existing server.js)
// ═══════════════════════════════════════════════════════


// ─── TRACKING ROUTES ──────────────────────────────────────────

// GET /api/tracking/state — full tracking state
app.get('/api/tracking/label', async (req, res) => {
  // Direct label lookup by id or by batchNumber+labelNumber
  try {
    const { id, batchNumber, labelNumber } = req.query;
    let label = null;
    if (id) {
      if (pgPool) {
        label = (await pgPool.query('SELECT * FROM tracking_labels WHERE id=$1',[id])).rows[0] || null;
      } else {
        label = db.prepare('SELECT * FROM tracking_labels WHERE id=?').get(id);
      }
    }
    if (!label && batchNumber && labelNumber != null) {
      if (pgPool) {
        label = (await pgPool.query('SELECT * FROM tracking_labels WHERE batch_number=$1 AND ABS(label_number)=ABS($2)',[batchNumber, parseInt(labelNumber)])).rows[0] || null;
      } else {
        label = db.prepare('SELECT * FROM tracking_labels WHERE batch_number=? AND ABS(label_number)=ABS(?)').get(batchNumber, parseInt(labelNumber));
      }
    }
    res.json({ ok: true, label: label || null });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.get('/api/tracking/state', async (req, res) => {
  try {
    if (pgPool) {
      const [labels, scans, closure, wastage, dispatch, alerts] = await Promise.all([
        pgPool.query('SELECT * FROM tracking_labels ORDER BY generated DESC'),
        pgPool.query('SELECT * FROM tracking_scans ORDER BY ts ASC'),
        pgPool.query('SELECT * FROM tracking_stage_closure'),
        pgPool.query('SELECT * FROM tracking_wastage ORDER BY ts ASC'),
        pgPool.query('SELECT * FROM tracking_dispatch_records ORDER BY ts ASC'),
        pgPool.query('SELECT * FROM tracking_alerts WHERE resolved = 0'),
      ]);
      const mapLabel = r => ({ ...r, batchNumber: r.batch_number, labelNumber: r.label_number, isPartial: r.is_partial, isOrange: r.is_orange, parentLabelId: r.parent_label_id, pcCode: r.pc_code, poNumber: r.po_number, machineId: r.machine_id, printingMatter: r.printing_matter, printedAt: r.printed_at, voidReason: r.void_reason, voidedAt: r.voided_at, voidedBy: r.voided_by, qrData: r.qr_data, woStatus: r.wo_status, shipTo: r.ship_to, billTo: r.bill_to, isExcess: r.is_excess, excessNum: r.excess_num, excessTotal: r.excess_total, normalTotal: r.normal_total });
      const mapScan = r => ({ ...r, labelId: r.label_id, batchNumber: r.batch_number, labelNumber: r.label_number });
      const mapClosure = r => ({ ...r, batchNumber: r.batch_number, closedAt: r.closed_at, closedBy: r.closed_by });
      const mapWastage = r => ({ ...r, batchNumber: r.batch_number });
      const mapDispatch = r => ({ ...r, batchNumber: r.batch_number, vehicleNo: r.vehicle_no, invoiceNo: r.invoice_no });
      const mapAlert = r => ({ ...r, labelId: r.label_id, batchNumber: r.batch_number, scanInTs: r.scan_in_ts, hoursStuck: r.hours_stuck });
      res.json({ ok: true, state: {
        labels: labels.rows.map(mapLabel), scans: scans.rows.map(mapScan),
        stageClosure: closure.rows.map(mapClosure), wastage: wastage.rows.map(mapWastage),
        dispatchRecs: dispatch.rows.map(mapDispatch), alerts: alerts.rows.map(mapAlert)
      }});
    } else {
      const labels  = db.prepare('SELECT * FROM tracking_labels ORDER BY generated DESC').all();
      const scans   = db.prepare('SELECT * FROM tracking_scans ORDER BY ts ASC').all();
      const closure = db.prepare('SELECT * FROM tracking_stage_closure').all();
      const wastage = db.prepare('SELECT * FROM tracking_wastage ORDER BY ts ASC').all();
      const dispatch= db.prepare('SELECT * FROM tracking_dispatch_records ORDER BY ts ASC').all();
      const alerts  = db.prepare('SELECT * FROM tracking_alerts WHERE resolved = 0').all();
      res.json({ ok: true, state: { labels, scans, stageClosure: closure, wastage, dispatchRecs: dispatch, alerts } });
    }
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST /api/tracking/state — save full tracking state
app.post('/api/tracking/state', async (req, res) => {
  try {
    const { labels, scans, stageClosure, wastage, dispatchRecs, alerts } = req.body;
    if (pgPool) {
      const client = await pgPool.connect();
      try {
        await client.query('BEGIN');
        if (labels && labels.length) {
          for (const l of labels) {
            await client.query(`INSERT INTO tracking_labels
              (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data,wo_status,ship_to,bill_to,is_excess,excess_num,excess_total,normal_total)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)
              ON CONFLICT (id) DO UPDATE SET batch_number=EXCLUDED.batch_number,label_number=EXCLUDED.label_number,printed=EXCLUDED.printed,printed_at=EXCLUDED.printed_at,voided=EXCLUDED.voided,void_reason=EXCLUDED.void_reason,voided_at=EXCLUDED.voided_at,wo_status=EXCLUDED.wo_status,ship_to=EXCLUDED.ship_to,bill_to=EXCLUDED.bill_to`,
              [l.id,l.batchNumber,l.labelNumber,l.size,l.qty,l.isPartial?1:0,l.isOrange?1:0,l.parentLabelId||null,l.customer||null,l.colour||null,l.pcCode||null,l.poNumber||null,l.machineId||null,l.printingMatter||null,l.generated||new Date().toISOString(),l.printed?1:0,l.printedAt||null,l.voided?1:0,l.voidReason||null,l.voidedAt||null,l.voidedBy||null,l.qrData||null,l.woStatus||null,l.shipTo||null,l.billTo||null,l.isExcess?1:0,l.excessNum||null,l.excessTotal||null,l.normalTotal||null]);
          }
        }
        if (scans && scans.length) {
          for (const s of scans) {
            await client.query(`INSERT INTO tracking_scans (id,label_id,batch_number,dept,type,ts,operator,size,qty) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (id) DO NOTHING`,
              [s.id,s.labelId||s.label_id,s.batchNumber||s.batch_number,s.dept,s.type,s.ts,s.operator||null,s.size||null,s.qty||null]);
          }
        }
        if (stageClosure && stageClosure.length) {
          for (const s of stageClosure) {
            await client.query(`INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at,closed_by) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO UPDATE SET closed=EXCLUDED.closed,closed_at=EXCLUDED.closed_at`,
              [s.id,s.batchNumber||s.batch_number,s.dept,s.closed?1:0,s.closedAt||s.closed_at,s.closedBy||s.closed_by||null]);
          }
        }
        if (wastage && wastage.length) {
          for (const w of wastage) {
            await client.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts,by) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING`,
              [w.id,w.batchNumber||w.batch_number,w.dept,w.type,w.qty,w.ts,w.by||null]);
          }
        }
        if (dispatchRecs && dispatchRecs.length) {
          for (const d of dispatchRecs) {
            await client.query(`INSERT INTO tracking_dispatch_records (id,batch_number,customer,qty,boxes,vehicle_no,invoice_no,remarks,ts,by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (id) DO NOTHING`,
              [d.id,d.batchNumber||d.batch_number,d.customer||null,d.qty,d.boxes,d.vehicleNo||d.vehicle_no||null,d.invoiceNo||d.invoice_no||null,d.remarks||null,d.ts,d.by||null]);
          }
        }
        if (alerts && alerts.length) {
          for (const a of alerts) {
            await client.query(`INSERT INTO tracking_alerts (id,label_id,batch_number,dept,scan_in_ts,hours_stuck,resolved,msg) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO UPDATE SET resolved=EXCLUDED.resolved`,
              [a.id,a.labelId||a.label_id,a.batchNumber||a.batch_number,a.dept,a.scanInTs||a.scan_in_ts,a.hoursStuck||a.hours_stuck||null,a.resolved?1:0,a.msg||null]);
          }
        }
        await client.query('COMMIT');
      } catch(e) { await client.query('ROLLBACK'); throw e; }
      finally { client.release(); }
    } else {
      const saveAll = db.transaction(() => {
        if (labels?.length) { const stmt = db.prepare(`INSERT OR REPLACE INTO tracking_labels (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,customer,colour,pc_code,po_number,machine_id,printing_matter,generated,printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data,wo_status,ship_to,bill_to,is_excess,excess_num,excess_total,normal_total) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`); labels.forEach(l => stmt.run(l.id,l.batchNumber,l.labelNumber,l.size,l.qty,l.isPartial?1:0,l.isOrange?1:0,l.parentLabelId||null,l.customer||null,l.colour||null,l.pcCode||null,l.poNumber||null,l.machineId||null,l.printingMatter||null,l.generated||new Date().toISOString(),l.printed?1:0,l.printedAt||null,l.voided?1:0,l.voidReason||null,l.voidedAt||null,l.voidedBy||null,l.qrData||null,l.woStatus||null,l.shipTo||null,l.billTo||null,l.isExcess?1:0,l.excessNum||null,l.excessTotal||null,l.normalTotal||null)); }
        if (scans?.length) { const stmt = db.prepare(`INSERT OR IGNORE INTO tracking_scans (id,label_id,batch_number,dept,type,ts,operator,size,qty) VALUES (?,?,?,?,?,?,?,?,?)`); scans.forEach(s => stmt.run(s.id,s.labelId||s.label_id,s.batchNumber||s.batch_number,s.dept,s.type,s.ts,s.operator||null,s.size||null,s.qty||null)); }
        if (wastage?.length) { const stmt = db.prepare(`INSERT OR REPLACE INTO tracking_wastage (id,batch_number,dept,type,qty,ts,by) VALUES (?,?,?,?,?,?,?)`); wastage.forEach(w => stmt.run(w.id,w.batchNumber||w.batch_number,w.dept,w.type,w.qty,w.ts,w.by||null)); }
      });
      saveAll();
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});
// GET /api/tracking/batch-summary/:batchNumber
app.get('/api/tracking/batch-summary/:batchNumber', async (req, res) => {
  try {
    const { batchNumber } = req.params;
    let labels, scans, wastage, dispatch, alerts;
    if (pgPool) {
      [labels, scans, wastage, dispatch, alerts] = await Promise.all([
        pgPool.query('SELECT * FROM tracking_labels WHERE batch_number=$1', [batchNumber]).then(r=>r.rows),
        pgPool.query('SELECT * FROM tracking_scans WHERE batch_number=$1 ORDER BY ts', [batchNumber]).then(r=>r.rows),
        pgPool.query('SELECT * FROM tracking_wastage WHERE batch_number=$1', [batchNumber]).then(r=>r.rows),
        pgPool.query('SELECT * FROM tracking_dispatch_records WHERE batch_number=$1', [batchNumber]).then(r=>r.rows),
        pgPool.query('SELECT * FROM tracking_alerts WHERE batch_number=$1 AND resolved=0', [batchNumber]).then(r=>r.rows),
      ]);
    } else {
      labels   = db.prepare('SELECT * FROM tracking_labels WHERE batch_number = ?').all(batchNumber);
      scans    = db.prepare('SELECT * FROM tracking_scans WHERE batch_number=? ORDER BY ts').all(batchNumber);
      wastage  = db.prepare('SELECT * FROM tracking_wastage WHERE batch_number = ?').all(batchNumber);
      dispatch = db.prepare('SELECT * FROM tracking_dispatch_records WHERE batch_number = ?').all(batchNumber);
      alerts   = db.prepare('SELECT * FROM tracking_alerts WHERE batch_number = ? AND resolved = 0').all(batchNumber);
    }
    const deptMap = {};
    scans.forEach(s => {
      if (!deptMap[s.dept]) deptMap[s.dept] = { in: 0, out: 0 };
      deptMap[s.dept][s.type] = (deptMap[s.dept][s.type] || 0) + 1;
    });
    const labelStats = { total: labels.length, printed: labels.filter(l=>l.printed).length, voided: labels.filter(l=>l.voided).length };
    const dispatched = dispatch.reduce((s,d) => s + d.boxes, 0);
    res.json({ ok: true, deptMap, labelStats, wastage, alerts, dispatched, batchNumber });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/tracking/alerts — boxes stuck 48h+ grouped by batch+dept
app.get('/api/tracking/alerts', async (req, res) => {
  try {
    const ALERT_HOURS = 48;
    const ALERT_START = '2026-04-27T00:00:00';
    let alerts = [];
    if (pgPool) {
      const r = await pgPool.query(`
        SELECT s.batch_number as "batchNumber", s.dept,
          COUNT(DISTINCT s.label_id) as "stuckBoxes",
          MIN(s.ts) as "scanInTs",
          EXTRACT(EPOCH FROM (NOW() - MIN(s.ts)::timestamptz))/3600 as "hoursStuck",
          MAX(s.size) as "size"
        FROM tracking_scans s
        WHERE s.type = 'in' AND s.ts >= $2
          AND EXTRACT(EPOCH FROM (NOW() - s.ts::timestamptz))/3600 >= $1
          AND NOT EXISTS (
            SELECT 1 FROM tracking_scans o
            WHERE o.label_id = s.label_id AND o.dept = s.dept
              AND o.type = 'out' AND o.ts > s.ts
          )
        GROUP BY s.batch_number, s.dept
        HAVING COUNT(DISTINCT s.label_id) > 0
        ORDER BY MIN(s.ts) ASC LIMIT 100
      `, [ALERT_HOURS, ALERT_START]);
      alerts = r.rows.map(a => ({
        id: a.batchNumber+'_'+a.dept, batchNumber: a.batchNumber,
        stuckBoxes: parseInt(a.stuckBoxes), dept: a.dept,
        scanInTs: a.scanInTs, hoursStuck: parseFloat(a.hoursStuck).toFixed(1),
        size: a.size, resolved: 0
      }));
    }
    res.json({ ok: true, alerts, count: alerts.length });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/tracking/alerts/detail — individual box numbers for a batch+dept
app.get('/api/tracking/alerts/detail', async (req, res) => {
  try {
    const { batchNumber, dept } = req.query;
    if (!batchNumber || !dept) return res.status(400).json({ ok:false, error:'batchNumber and dept required' });
    const ALERT_START = '2026-04-27T00:00:00';
    let boxes = [];
    if (pgPool) {
      const r = await pgPool.query(`
        SELECT s.label_id as "labelId", ABS(l.label_number) as "boxNo",
          s.ts as "scanInTs",
          EXTRACT(EPOCH FROM (NOW() - s.ts::timestamptz))/3600 as "hoursStuck"
        FROM tracking_scans s
        LEFT JOIN tracking_labels l ON l.id = s.label_id
        WHERE s.type='in' AND s.batch_number=$1 AND s.dept=$2 AND s.ts>=$4
          AND EXTRACT(EPOCH FROM (NOW() - s.ts::timestamptz))/3600 >= $3
          AND NOT EXISTS (
            SELECT 1 FROM tracking_scans o
            WHERE o.label_id=s.label_id AND o.dept=s.dept
              AND o.type='out' AND o.ts>s.ts
          )
        ORDER BY l.label_number ASC
      `, [batchNumber, dept, 48, ALERT_START]);
      boxes = r.rows.map(b => ({
        labelId: b.labelId,
        boxNo: b.boxNo != null ? b.boxNo : '?',
        hoursStuck: parseFloat(b.hoursStuck).toFixed(1),
        scanInTs: b.scanInTs
      }));
    }
    res.json({ ok: true, batchNumber, dept, boxes });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// GET /api/tracking/wip-summary — scan counts + stage closures for Planning
app.get('/api/tracking/wip-summary', async (req, res) => {
  try {
    let summary, closures;
    if (pgPool) {
      const r1 = await pgPool.query('SELECT batch_number, dept, type, COUNT(*) as cnt FROM tracking_scans GROUP BY batch_number, dept, type');
      summary = r1.rows;
      try {
        const r2 = await pgPool.query("SELECT batch_number, dept, closed, closed_at FROM tracking_stage_closure WHERE closed = 1 OR closed::text = '1'");
        closures = r2.rows;
      } catch(ce) {
        try {
          const r2 = await pgPool.query('SELECT batch_number, dept, closed, closed_at FROM tracking_stage_closure WHERE closed IS NOT NULL');
          closures = r2.rows.filter(r => r.closed == 1 || r.closed === true);
        } catch(ce2) { closures = []; }
      }
    } else {
      summary = db.prepare('SELECT batch_number, dept, type, COUNT(*) as cnt FROM tracking_scans GROUP BY batch_number, dept, type').all();
      closures = db.prepare("SELECT batch_number, dept, closed, closed_at FROM tracking_stage_closure WHERE closed = 1").all();
    }
    res.json({ ok: true, scanSummary: summary, closures });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});


// ── Labels lookup by batchNumber (scanning fallback) ──
// ── Save new labels to PostgreSQL directly ──────────────────
app.post('/api/tracking/labels', async (req, res) => {
  try {
    const { labels } = req.body;
    if (!labels || !labels.length) return res.status(400).json({ ok: false, error: 'No labels' });
    if (pgPool) {
      for (const l of labels) {
        await pgPool.query(`
          INSERT INTO tracking_labels
            (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,
             customer,colour,pc_code,po_number,machine_id,printing_matter,generated,
             printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data,
             is_excess,excess_num,excess_total,normal_total)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)
          ON CONFLICT (id) DO UPDATE SET
            batch_number=EXCLUDED.batch_number, label_number=EXCLUDED.label_number,
            qty=EXCLUDED.qty, is_partial=EXCLUDED.is_partial,
            printed=EXCLUDED.printed, printed_at=EXCLUDED.printed_at,
            voided=EXCLUDED.voided, void_reason=EXCLUDED.void_reason,
            voided_at=EXCLUDED.voided_at, voided_by=EXCLUDED.voided_by,
            qr_data=EXCLUDED.qr_data, pc_code=EXCLUDED.pc_code,
            is_excess=EXCLUDED.is_excess, excess_num=EXCLUDED.excess_num,
            excess_total=EXCLUDED.excess_total, normal_total=EXCLUDED.normal_total`,
          [l.id, l.batchNumber||l.batch_number,
           // labelNumber may be "OL-15" (orange) or a number — always store as integer
           (()=>{ const n=l.labelNumber||l.label_number; if(n==null) return null; const s=String(n).replace(/^OL-/i,''); return parseInt(s)||null; })(),
           l.size, l.qty, l.isPartial?1:0, l.isOrange?1:0, l.parentLabelId||null,
           l.customer||null, l.colour||null, l.pcCode||null, l.poNumber||null,
           l.machineId||null, l.printingMatter||l.printMatter||null,
           l.generated||new Date().toISOString(),
           l.printed?1:0, l.printedAt||null, l.voided?1:0, l.voidReason||null,
           l.voidedAt||null, l.voidedBy||null, l.qrData||null,
           l.isExcess?1:0, l.excessNum||null, l.excessTotal||null, l.normalTotal||null]
        );
      }
    } else {
      const stmt = db.prepare(`INSERT OR IGNORE INTO tracking_labels
        (id,batch_number,label_number,size,qty,is_partial,is_orange,parent_label_id,
         customer,colour,pc_code,po_number,machine_id,printing_matter,generated,
         printed,printed_at,voided,void_reason,voided_at,voided_by,qr_data,
         is_excess,excess_num,excess_total,normal_total)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
      const parseLabelNum = n => { if(n==null) return null; const s=String(n).replace(/^OL-/i,''); return parseInt(s)||null; };
      labels.forEach(l => stmt.run(
        l.id, l.batchNumber||l.batch_number, parseLabelNum(l.labelNumber||l.label_number),
        l.size, l.qty, l.isPartial?1:0, l.isOrange?1:0, l.parentLabelId||null,
        l.customer||null, l.colour||null, l.pcCode||null, l.poNumber||null,
        l.machineId||null, l.printingMatter||l.printMatter||null,
        l.generated||new Date().toISOString(),
        l.printed?1:0, l.printedAt||null, l.voided?1:0, l.voidReason||null,
        l.voidedAt||null, l.voidedBy||null, l.qrData||null,
        l.isExcess?1:0, l.excessNum||null, l.excessTotal||null, l.normalTotal||null
      ));
    }
    res.json({ ok: true, saved: labels.length });
  } catch (err) {
    console.error('[LABEL ERROR]', err.message, '| first label:', JSON.stringify(req.body?.labels?.[0]||{}).substring(0,300));
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/tracking/labels', async (req, res) => {
  try {
    const { batchNumber } = req.query;
    if(!batchNumber) return res.status(400).json({ok:false,error:'batchNumber required'});
    if (pgPool) {
      const r = await pgPool.query(
        'SELECT * FROM tracking_labels WHERE batch_number = $1 AND voided = 0', [batchNumber]
      );
      res.json({ok:true, labels: r.rows});
    } else {
      const labels = db.prepare(
        'SELECT * FROM tracking_labels WHERE batch_number = ? AND voided = 0'
      ).all(batchNumber);
      res.json({ok:true, labels});
    }
  } catch(err) { res.status(500).json({ok:false,error:err.message}); }
});

// ── All labels fast endpoint ──
app.get('/api/tracking/labels-all', async (req, res) => {
  try {
    const m=r=>({id:r.id,batchNumber:r.batch_number,labelNumber:r.label_number,size:r.size,qty:r.qty,isPartial:!!r.is_partial,isOrange:!!r.is_orange,parentLabelId:r.parent_label_id||null,customer:r.customer||'',colour:r.colour||'',pcCode:r.pc_code||'',poNumber:r.po_number||'',machineId:r.machine_id||'',printingMatter:r.printing_matter||'',generated:r.generated,printed:!!r.printed,printedAt:r.printed_at||null,voided:!!r.voided,voidReason:r.void_reason||'',voidedAt:r.voided_at||null,voidedBy:r.voided_by||null,qrData:r.qr_data||'',woStatus:r.wo_status||null,shipTo:r.ship_to||'',billTo:r.bill_to||'',isExcess:!!r.is_excess,excessNum:r.excess_num||null,excessTotal:r.excess_total||null,normalTotal:r.normal_total||null});
    if(pgPool){const r=await pgPool.query('SELECT * FROM tracking_labels ORDER BY generated DESC');res.json({ok:true,labels:r.rows.map(m)});}
    else{const labels=db.prepare('SELECT * FROM tracking_labels ORDER BY generated DESC').all();res.json({ok:true,labels:labels.map(m)});}
  }catch(err){res.status(500).json({ok:false,error:err.message});}
});
// ── Recent scans fast endpoint ──
app.get('/api/tracking/scans-recent', async (req, res) => {
  try {
    const mapScan = r => ({
      id: r.id,
      labelId: r.label_id,
      batchNumber: r.batch_number,
      dept: r.dept,
      type: r.type,
      ts: r.ts,
      operator: r.operator || null,
      size: r.size || null,
      qty: r.qty || null,
      labelNumber: r.label_number || null
    });
    if (pgPool) {
      // Try with label_number column first (after migration v10)
      let rows;
      try {
        const r = await pgPool.query(
          'SELECT * FROM tracking_scans ORDER BY ts DESC LIMIT 2000'
        );
        rows = r.rows;
      } catch(e) {
        // Fallback if column issues — select without label_number
        const r = await pgPool.query(
          'SELECT id,label_id,batch_number,dept,type,ts,operator,size,qty FROM tracking_scans ORDER BY ts DESC LIMIT 2000'
        );
        rows = r.rows;
      }
      res.json({ ok: true, scans: rows.map(mapScan) });
    } else {
      let scans;
      try {
        scans = db.prepare('SELECT * FROM tracking_scans ORDER BY ts DESC LIMIT 2000').all();
      } catch(e) {
        scans = db.prepare('SELECT id,label_id,batch_number,dept,type,ts,operator,size,qty FROM tracking_scans ORDER BY ts DESC LIMIT 2000').all();
      }
      res.json({ ok: true, scans: scans.map(mapScan) });
    }
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Wastage fast endpoint ──
app.get('/api/tracking/wastage', async (req, res) => {
  try {
    const m=r=>({...r,batchNumber:r.batch_number});
    if(pgPool){const r=await pgPool.query('SELECT * FROM tracking_wastage ORDER BY ts DESC');res.json({ok:true,wastage:r.rows.map(m)});}
    else{const wastage=db.prepare('SELECT * FROM tracking_wastage ORDER BY ts DESC').all();res.json({ok:true,wastage:wastage.map(m)});}
  }catch(err){res.status(500).json({ok:false,error:err.message});}
});
// ── Individual scan save (called after each scan in/out) ──
app.post('/api/tracking/scan', async (req, res) => {
  try {
    const { scan } = req.body;
    if(!scan || !scan.id) return res.status(400).json({ok:false,error:'Missing scan'});
    const labelId = scan.labelId||scan.label_id;
    const batchNumber = scan.batchNumber||scan.batch_number;
    // HARD BLOCK: Unprinted batches can never be scanned at Printing or PI
    // Check planning state to get isPrinted for this batch
    if (scan.dept === 'printing' || scan.dept === 'pi') {
      const planState = await getPlanningStateAsync();
      const order = (planState.orders||[]).find(o =>
        o.batchNumber === batchNumber || o.id === batchNumber
      );
      if (order && order.isPrinted === false) {
        return res.json({ok:false, blocked:true,
          error:`Batch ${batchNumber} is UNPRINTED — scanning at ${scan.dept} is not allowed. Unprinted batches go AIM → Packing directly.`
        });
      }
    }

    if (pgPool) {
      // Server-side duplicate check: one IN and one OUT max per label per dept per batch
      // Scoped to batch_number so same label in a new batch is never blocked
      const existing = await pgPool.query(
        `SELECT type FROM tracking_scans WHERE label_id=$1 AND dept=$2 AND batch_number=$3`,
        [labelId, scan.dept, batchNumber]
      );
      const doneTypes = existing.rows.map(r=>r.type);
      if(doneTypes.includes(scan.type)){
        return res.json({ok:false, duplicate:true, error:'Already scanned '+scan.type.toUpperCase()+' at '+scan.dept});
      }
      await pgPool.query(
        `INSERT INTO tracking_scans (id,label_id,batch_number,label_number,dept,type,ts,operator,size,qty)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (id) DO NOTHING`,
        [scan.id, labelId, batchNumber, scan.labelNumber||null, scan.dept, scan.type, scan.ts,
         scan.operator||null, scan.size||null, scan.qty||null]
      );
    } else {
      db.prepare(`INSERT OR IGNORE INTO tracking_scans
        (id,label_id,batch_number,label_number,dept,type,ts,operator,size,qty)
        VALUES (?,?,?,?,?,?,?,?,?,?)`).run(
        scan.id, labelId, batchNumber, scan.labelNumber||null, scan.dept, scan.type, scan.ts,
        scan.operator||null, scan.size||null, scan.qty||null
      );
    }
    res.json({ok:true});
  } catch(err) { res.status(500).json({ok:false,error:err.message}); }
});

// ── A-Grade summary per batch — for Planning live update ──────
app.get('/api/tracking/agrade-summary', async (req, res) => {
  try {
    // Pack sizes for fallback calculation
    const PACK_SIZES = {'0':1.5,'00':1.5,'000':1.5,'1':1.25,'2':1.0,'3':0.75,'4':0.5,'5':0.333};

    // Get batch sizes from planning state for fallback
    const planState = getPlanningState();
    const batchSizeMap = {};
    (planState.orders||[]).forEach(o => { if(o.batchNumber) batchSizeMap[o.batchNumber.toUpperCase()] = String(o.size||'2'); });

    // Scan counts per batch per dept per type
    let scans, wastage, prodActuals;
    if (pgPool) {
      const [r1, r2, r3] = await Promise.all([
        pgPool.query('SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans GROUP BY batch_number, dept, type'),
        pgPool.query('SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage GROUP BY batch_number, dept, type'),
        pgPool.query('SELECT batch_number, SUM(qty_lakhs) as gross_prod FROM production_actuals GROUP BY batch_number'),
      ]);
      scans = r1.rows; wastage = r2.rows; prodActuals = r3.rows;
    } else {
      scans = db.prepare('SELECT batch_number, dept, type, COUNT(*) as cnt, SUM(qty) as total_qty FROM tracking_scans GROUP BY batch_number, dept, type').all();
      wastage = db.prepare('SELECT batch_number, dept, type, SUM(qty) as total_qty FROM tracking_wastage GROUP BY batch_number, dept, type').all();
      prodActuals = db.prepare('SELECT batch_number, SUM(qty_lakhs) as gross_prod FROM production_actuals GROUP BY batch_number').all();
    }
    const grossProdMap = {};
    prodActuals.forEach(r => { if(r.batch_number) grossProdMap[r.batch_number.toUpperCase()] = parseFloat(r.gross_prod||0); });

    // Build per-batch summary
    const batches = {};
    scans.forEach(s => {
      const bn = (s.batch_number||'').toUpperCase(); // normalize to uppercase
      if (!batches[bn]) batches[bn] = {};
      if (!batches[bn][s.dept]) batches[bn][s.dept] = {in:0,out:0,inQty:0,outQty:0};
      batches[bn][s.dept][s.type] = parseInt(s.cnt||0, 10);
      // Use SUM(qty) if available, else fallback to COUNT * packSize
      const sumQty = parseFloat(s.total_qty||0);
      const ps = PACK_SIZES[batchSizeMap[bn]||batchSizeMap[s.batch_number]||'2'] || 1.0;
      const effectiveQty = sumQty > 0 ? sumQty : parseInt(s.cnt||0,10) * ps;
      batches[bn][s.dept][s.type+'Qty'] = effectiveQty;
    });

    wastage.forEach(w => {
      const bn = (w.batch_number||'').toUpperCase(); // normalize to uppercase
      if (!batches[bn]) batches[bn] = {};
      if (!batches[bn][w.dept]) batches[bn][w.dept] = {in:0,out:0,inQty:0,outQty:0};
      if (!batches[bn][w.dept].wastage) batches[bn][w.dept].wastage = {};
      batches[bn][w.dept].wastage[w.type] = parseFloat(w.total_qty||0);
    });

    // Calculate A-grade per batch per stage
    const result = {};
    Object.entries(batches).forEach(([batchNo, depts]) => {
      const aim = depts['aim'] || {};
      const print = depts['printing'] || {};
      const pi = depts['pi'] || {};
      const pack = depts['packing'] || {};

      const aimWaste = (aim.wastage?.salvage||0) + (aim.wastage?.remelt||0);
      const printWaste = (print.wastage?.salvage||0) + (print.wastage?.remelt||0);
      const piWaste = (pi.wastage?.salvage||0) + (pi.wastage?.remelt||0);

      const aimOut = aim.outQty || 0;
      const aimInspected = aimOut + aimWaste;
      const printOut = print.outQty || 0;
      const printInspected = printOut + printWaste;
      const piOut = pi.outQty || 0;
      const piInspected = piOut + piWaste;

      const grossProd = grossProdMap[batchNo.toUpperCase()] || 0;
      const packOutQty = pack.outQty || 0;
      // WIP = everything produced but not yet packed out
      const wipLakhs = Math.max(0, grossProd - packOutQty);

      result[batchNo.toUpperCase()] = { // normalize to uppercase for consistent lookup
        aim: {
          inQty: aim.inQty||0, outQty: aimOut,
          wastage: aimWaste, inspected: aimInspected,
          aGradePct: aimInspected>0 ? (aimOut/aimInspected*100) : null
        },
        printing: {
          inQty: print.inQty||0, outQty: printOut,
          wastage: printWaste, inspected: printInspected,
          aGradePct: printInspected>0 ? (printOut/printInspected*100) : null
        },
        pi: {
          inQty: pi.inQty||0, outQty: piOut,
          wastage: piWaste, inspected: piInspected,
          aGradePct: piInspected>0 ? (piOut/piInspected*100) : null
        },
        packing: { inQty: pack.inQty||0, outQty: packOutQty, in: pack.in||0, out: pack.out||0 }, // .out = box count (scan count)
        grossProd,
        wipLakhs  // Lakhs still in pipeline (not yet packed out)
      };
    });

    res.json({ ok: true, batches: result });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Dispatch record from Tracking app ──────────────────────────
app.post('/api/tracking/dispatch-record', async (req, res) => {
  try {
    const { record } = req.body;
    if(!record || !record.id) return res.status(400).json({ok:false,error:'Missing record'});
    if (pgPool) {
      await pgPool.query(`INSERT INTO tracking_dispatch_records (id,batch_number,customer,qty,boxes,vehicle_no,invoice_no,remarks,ts,"by") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(id) DO NOTHING`,
        [record.id, record.batchNumber||record.batch_number, record.customer||null, record.qty, record.boxes, record.vehicleNo||record.vehicle_no||null, record.invoiceNo||record.invoice_no||null, record.remarks||null, record.ts, record.by||null]);
    } else {
      db.prepare(`INSERT OR IGNORE INTO tracking_dispatch_records (id,batch_number,customer,qty,boxes,vehicle_no,invoice_no,remarks,ts,by) VALUES (?,?,?,?,?,?,?,?,?,?)`).run(record.id, record.batchNumber||record.batch_number, record.customer||null, record.qty, record.boxes, record.vehicleNo||record.vehicle_no||null, record.invoiceNo||record.invoice_no||null, record.remarks||null, record.ts, record.by||null);
    }
    res.json({ok:true});
  } catch(err) { res.status(500).json({ok:false,error:err.message}); }
});

// ── Dispatch actual update — syncs Tracking qty back to Planning ──
app.post('/api/tracking/dispatch-update', async (req, res) => {
  try {
    const { batchNumber, dispatchedQty, vehicleNo, invoiceNo } = req.body;
    if(!batchNumber) return res.status(400).json({ok:false,error:'Missing batchNumber'});
    if (pgPool) {
      await pgPool.query(`INSERT INTO tracking_dispatch_actuals (batch_number,dispatched_qty,vehicle_no,invoice_no,updated_at) VALUES ($1,$2,$3,$4,NOW()) ON CONFLICT(batch_number) DO UPDATE SET dispatched_qty=EXCLUDED.dispatched_qty, vehicle_no=EXCLUDED.vehicle_no, invoice_no=EXCLUDED.invoice_no, updated_at=NOW()`,
        [batchNumber, dispatchedQty||0, vehicleNo||null, invoiceNo||null]);
    } else {
      db.prepare(`INSERT OR REPLACE INTO tracking_dispatch_actuals (batch_number,dispatched_qty,vehicle_no,invoice_no,updated_at) VALUES (?,?,?,?,datetime('now'))`).run(batchNumber, dispatchedQty||0, vehicleNo||null, invoiceNo||null);
    }
    res.json({ok:true});
  } catch(err) { res.status(500).json({ok:false,error:err.message}); }
});

// ── Get dispatch actuals for Planning app ──
app.get('/api/tracking/dispatch-actuals', async (req, res) => {
  // pgPool used below
  try {
    const rows = pgPool ? (await pgPool.query('SELECT * FROM tracking_dispatch_actuals')).rows : db.prepare('SELECT * FROM tracking_dispatch_actuals').all();
    res.json({ok:true, actuals: rows});
  } catch(err) { res.status(500).json({ok:false,error:err.message}); }
});

// ── Admin Backfill — manual entry of historical scan data ──
app.post('/api/tracking/backfill', async (req, res) => {
  try {
    const { scans } = req.body;
    if (!scans || !Array.isArray(scans)) return res.status(400).json({ ok: false, error: 'scans array required' });
    let count = 0;
    for (const scan of scans) {
      if (!scan.id) continue;
      if (pgPool) {
        await pgPool.query(
          `INSERT INTO tracking_scans (id,label_id,batch_number,label_number,dept,type,ts,operator,size,qty) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(id) DO NOTHING`,
          [scan.id, scan.labelId||scan.label_id||null, scan.batchNumber||scan.batch_number||null, scan.labelNumber||null, scan.dept, scan.type, scan.ts, scan.operator||null, scan.size||null, scan.qty||null]
        );
      } else {
        db.prepare(`INSERT OR IGNORE INTO tracking_scans (id,label_id,batch_number,label_number,dept,type,ts,operator,size,qty) VALUES (?,?,?,?,?,?,?,?,?,?)`).run(scan.id, scan.labelId||scan.label_id||null, scan.batchNumber||scan.batch_number||null, scan.labelNumber||null, scan.dept, scan.type, scan.ts, scan.operator||null, scan.size||null, scan.qty||null);
      }
      count++;
    }
    res.json({ ok: true, imported: count });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
app.post('/api/tracking/backfill-wastage', async (req, res) => {
  try {
    const { wastage } = req.body;
    if (!wastage || !Array.isArray(wastage)) return res.status(400).json({ ok: false, error: 'wastage array required' });
    let count = 0;
    for (const w of wastage) {
      if (!w.id) continue;
      if (pgPool) {
        await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`,
          [w.id, w.batchNumber||w.batch_number||null, w.dept, w.type||null, w.qty||null, w.ts||new Date().toISOString()]);
      } else {
        db.prepare(`INSERT OR IGNORE INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES (?,?,?,?,?,?)`).run(w.id, w.batchNumber||w.batch_number||null, w.dept, w.type||null, w.qty||null, w.ts||new Date().toISOString());
      }
      count++;
    }
    res.json({ ok: true, imported: count });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});
app.get('/jsqr.min.js', (req, res) => {
  if (jsqrCache) {
    res.setHeader('Content-Type', 'application/javascript');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    return res.send(jsqrCache);
  }
  const https = require('https');
  https.get('https://cdnjs.cloudflare.com/ajax/libs/jsQR/1.4.0/jsQR.min.js', r => {
    let data = '';
    r.on('data', c => data += c);
    r.on('end', () => {
      jsqrCache = data;
      res.setHeader('Content-Type', 'application/javascript');
      res.setHeader('Cache-Control', 'public, max-age=86400');
      res.send(data);
    });
  }).on('error', () => res.status(503).send('// jsQR fetch failed'));
});

// ── Label void — mark label voided in DB ──────────────────────
app.post('/api/tracking/label-void', async (req, res) => {
  try {
    const { labelId, reason, voidedBy } = req.body;
    if (!labelId) return res.status(400).json({ ok: false, error: 'labelId required' });
    const ts = new Date().toISOString();
    if (pgPool) {
      await pgPool.query(`UPDATE tracking_labels SET voided=1, void_reason=$1, voided_at=$2, voided_by=$3 WHERE id=$4`, [reason||'', ts, voidedBy||'', labelId]);
    } else {
      db.prepare(`UPDATE tracking_labels SET voided=1, void_reason=?, voided_at=?, voided_by=? WHERE id=?`).run(reason||'', ts, voidedBy||'', labelId);
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// POST update label qty (edit)
// ── Recent scans — lightweight endpoint (last 200 scans per dept) ─
app.get('/api/tracking/scans', async (req, res) => {
  try {
    // Return deduplicated scans — keep earliest scan per (label_id, dept, type, minute)
    const dedupeSQL = `
      SELECT DISTINCT ON (label_id, dept, type, date_trunc('minute', ts::timestamp))
        id, label_id, batch_number, dept, type, ts, operator, size, qty
      FROM tracking_scans
      ORDER BY label_id, dept, type, date_trunc('minute', ts::timestamp), ts ASC
      LIMIT 1000
    `;
    if (pgPool) {
      const r = await pgPool.query(dedupeSQL);
      res.json({ ok: true, scans: r.rows });
    } else {
      const scans = db.prepare('SELECT * FROM tracking_scans ORDER BY ts DESC LIMIT 500').all();
      res.json({ ok: true, scans });
    }
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.post('/api/tracking/label-update', async (req, res) => {
  try {
    const { labelId, qty, printed, printedAt } = req.body;
    if (!labelId) return res.status(400).json({ ok: false, error: 'labelId required' });
    if (printed !== undefined && qty === undefined) {
      // Update printed status only
      const pVal = printed ? 1 : 0;
      const pAt  = printedAt || new Date().toISOString();
      if (pgPool) {
        await pgPool.query('UPDATE tracking_labels SET printed = $1, printed_at = $2 WHERE id = $3', [pVal, pAt, labelId]);
      } else {
        db.prepare('UPDATE tracking_labels SET printed = ?, printed_at = ? WHERE id = ?').run(pVal, pAt, labelId);
      }
    } else if (qty) {
      // Update qty and mark for reprint
      if (pgPool) {
        await pgPool.query('UPDATE tracking_labels SET qty = $1, printed = 0 WHERE id = $2', [qty, labelId]);
      } else {
        db.prepare('UPDATE tracking_labels SET qty = ?, printed = 0 WHERE id = ?').run(qty, labelId);
      }
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Clean duplicate scans from DB ─────────────────────────────
app.post('/api/tracking/cleanup-scans', async (req, res) => {
  try {
    if (!pgPool) return res.json({ ok: false, error: 'PostgreSQL only' });
    // Delete duplicate scans — keep only the earliest per (label_id, dept, type, minute)
    const result = await pgPool.query(`
      DELETE FROM tracking_scans
      WHERE id NOT IN (
        SELECT DISTINCT ON (label_id, dept, type, date_trunc('minute', ts::timestamp))
          id
        FROM tracking_scans
        ORDER BY label_id, dept, type, date_trunc('minute', ts::timestamp), ts ASC
      )
    `);
    res.json({ ok: true, deleted: result.rowCount });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Clean duplicate scans — keep only first scan per label+dept+type ─
app.post('/api/admin/clean-duplicate-scans', async (req, res) => {
  try {
    if (!pgPool) return res.status(400).json({ ok: false, error: 'PostgreSQL only' });
    // Delete duplicates: keep the earliest scan per label_id+dept+type combination
    const result = await pgPool.query(`
      DELETE FROM tracking_scans
      WHERE id NOT IN (
        SELECT DISTINCT ON (label_id, dept, type) id
        FROM tracking_scans
        ORDER BY label_id, dept, type, ts ASC
      )
    `);
    res.json({ ok: true, deleted: result.rowCount });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Stage status — record which departments are closed per batch ─
app.post('/api/tracking/stage-status', async (req, res) => {
  try {
    const { batchNumber, statusMap } = req.body;
    if (!batchNumber || !statusMap) return res.status(400).json({ ok: false, error: 'batchNumber and statusMap required' });
    const ts = new Date().toISOString();
    for (const [dept, status] of Object.entries(statusMap)) {
      const closed = status === 'closed' ? 1 : 0;
      if (pgPool) {
        await pgPool.query(`INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(batch_number,dept) DO UPDATE SET closed=EXCLUDED.closed, closed_at=EXCLUDED.closed_at`, [`${batchNumber}-${dept}`, batchNumber, dept, closed, ts]);
      } else {
        db.prepare(`INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at) VALUES (?,?,?,?,?) ON CONFLICT(batch_number,dept) DO UPDATE SET closed=excluded.closed, closed_at=excluded.closed_at`).run(`${batchNumber}-${dept}`, batchNumber, dept, closed, ts);
      }
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Stage Close — mark a dept stage as closed for a batch ──────
app.post('/api/tracking/stage-close', async (req, res) => {
  try {
    const { batchNumber, dept, closedBy } = req.body;
    if (!batchNumber || !dept) return res.status(400).json({ ok: false, error: 'Missing batchNumber or dept' });
    const id = `${batchNumber}-${dept}`;
    const ts = new Date().toISOString();
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO tracking_stage_closure (id, batch_number, dept, closed, closed_at, closed_by)
         VALUES ($1,$2,$3,1,$4,$5)
         ON CONFLICT(batch_number, dept) DO UPDATE SET closed=1, closed_at=EXCLUDED.closed_at, closed_by=EXCLUDED.closed_by`,
        [id, batchNumber, dept, ts, closedBy||null]
      );
    } else {
      db.prepare(`INSERT INTO tracking_stage_closure (id,batch_number,dept,closed,closed_at,closed_by)
        VALUES (?,?,?,1,?,?) ON CONFLICT(batch_number,dept) DO UPDATE SET closed=1,closed_at=excluded.closed_at,closed_by=excluded.closed_by`)
        .run(id, batchNumber, dept, ts, closedBy||null);
    }
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Wastage — save salvage/remelt records ─────────────────────
app.post('/api/tracking/wastage', async (req, res) => {
  try {
    const { batchNumber, dept, salvage, remelt } = req.body;
    if (!batchNumber || !dept) return res.status(400).json({ ok: false, error: 'batchNumber and dept required' });
    const ts = new Date().toISOString();
    const genId = () => Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
    if (pgPool) {
      if (parseFloat(salvage) > 0) await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`, [genId(),batchNumber,dept,'salvage',parseFloat(salvage),ts]);
      if (parseFloat(remelt)  > 0) await pgPool.query(`INSERT INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`, [genId(),batchNumber,dept,'remelt',parseFloat(remelt),ts]);
    } else {
      const insert = db.prepare(`INSERT OR IGNORE INTO tracking_wastage (id,batch_number,dept,type,qty,ts) VALUES (?,?,?,?,?,?)`);
      if (parseFloat(salvage) > 0) insert.run(genId(),batchNumber,dept,'salvage',parseFloat(salvage),ts);
      if (parseFloat(remelt)  > 0) insert.run(genId(),batchNumber,dept,'remelt',parseFloat(remelt),ts);
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Wastage edit — admin/planning correction ──────────────────
app.post('/api/tracking/wastage-edit', async (req, res) => {
  try {
    const { id, qty, editedBy } = req.body;
    if (!id || qty === undefined) return res.status(400).json({ ok: false, error: 'id and qty required' });
    if (pgPool) {
      const r = await pgPool.query(`UPDATE tracking_wastage SET qty=$1, "by"=COALESCE("by",'') || ' [edited by ' || $2 || ']' WHERE id=$3`, [parseFloat(qty), editedBy||'admin', id]);
      if (r.rowCount === 0) return res.status(404).json({ ok: false, error: 'Wastage entry not found' });
    } else {
      const result = db.prepare(`UPDATE tracking_wastage SET qty=?, by=COALESCE(by,'')||' [edited by '||?||']' WHERE id=?`).run(parseFloat(qty), editedBy||'admin', id);
      if (result.changes === 0) return res.status(404).json({ ok: false, error: 'Wastage entry not found' });
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Wastage delete — admin/planning correction ─────────────────
app.post('/api/tracking/wastage-delete', async (req, res) => {
  try {
    const { id, deletedBy } = req.body;
    if (!id) return res.status(400).json({ ok: false, error: 'id required' });
    if (pgPool) {
      const r = await pgPool.query('SELECT * FROM tracking_wastage WHERE id=$1', [id]);
      if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'Not found' });
      const entry = r.rows[0];
      await pgPool.query(`INSERT INTO audit_log (username,role,app,action,details) VALUES ($1,'admin','tracking','WASTAGE_DELETE',$2)`, [deletedBy||'admin', JSON.stringify({id,batch_number:entry.batch_number,dept:entry.dept,type:entry.type,qty:entry.qty})]);
      await pgPool.query('DELETE FROM tracking_wastage WHERE id=$1', [id]);
    } else {
      const entry = db.prepare('SELECT * FROM tracking_wastage WHERE id=?').get(id);
      if (!entry) return res.status(404).json({ ok: false, error: 'Not found' });
      db.prepare(`INSERT INTO audit_log (username,role,app,action,details) VALUES (?,'admin','tracking','WASTAGE_DELETE',?)`).run(deletedBy||'admin', JSON.stringify({id,batch_number:entry.batch_number,dept:entry.dept,type:entry.type,qty:entry.qty}));
      db.prepare('DELETE FROM tracking_wastage WHERE id=?').run(id);
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Reprint log — audit trail for damaged label replacements ──
app.post('/api/tracking/reprint-log', async (req, res) => {
  try {
    const { log } = req.body;
    if (!log) return res.status(400).json({ ok: false, error: 'log required' });
    if (pgPool) {
      await pgPool.query(`INSERT INTO audit_log (username,role,app,action,details) VALUES ($1,$2,$3,$4,$5)`,
        [log.requestedBy||'tracking', 'tracking', 'tracking', 'LABEL_REPRINT', JSON.stringify(log)]);
    } else {
      db.prepare(`INSERT INTO audit_log (username,role,app,action,details) VALUES (?,?,?,?,?)`).run(log.requestedBy||'tracking', 'tracking', 'tracking', 'LABEL_REPRINT', JSON.stringify(log));
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── DPR Settings — GET all settings ──────────────────────────
app.get('/api/dpr/settings', async (req, res) => {
  try {
    let rows;
    if (pgPool) {
      const r = await pgPool.query('SELECT key, value_json FROM dpr_settings');
      rows = r.rows;
    } else {
      rows = db.prepare('SELECT key, value_json FROM dpr_settings').all();
    }
    const settings = {};
    rows.forEach(r => {
      try { settings[r.key] = JSON.parse(r.value_json); } catch { settings[r.key] = r.value_json; }
    });
    res.json({ ok: true, settings });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── DPR Settings — POST save/update one or more settings ─────
app.post('/api/dpr/settings', async (req, res) => {
  try {
    const { settings } = req.body;
    if (!settings || typeof settings !== 'object') return res.status(400).json({ ok: false, error: 'settings object required' });
    if (pgPool) {
      for (const [key, value] of Object.entries(settings)) {
        await pgPool.query(
          `INSERT INTO dpr_settings (key, value_json, updated_at)
           VALUES ($1, $2, NOW())
           ON CONFLICT(key) DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = NOW()`,
          [key, JSON.stringify(value)]
        );
      }
    } else {
      const upsert = db.prepare(`
        INSERT INTO dpr_settings (key, value_json, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
      `);
      for (const [key, value] of Object.entries(settings)) {
        upsert.run(key, JSON.stringify(value));
      }
    }
    res.json({ ok: true, saved: Object.keys(settings).length });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ── Catch-all: serve index.html for unknown routes (SPA fallback) ──
// MUST be last — after all /api/* routes so they are not intercepted
app.get('*', (req, res) => {
  const idx = path.join(__dirname, 'public', 'index.html');
  if (fs.existsSync(idx)) res.sendFile(idx);
  else res.json({ ok: false, error: 'No frontend found. Place Planning App and DPR App in /public folder.' });
});

// ── Start server ──────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`[Sunloc] Server running on port ${PORT}`);
  console.log(`[Sunloc] DB: ${DB_PATH}`);
  // Ensure PostgreSQL tables exist (handles cases where PgDatabase migrations didn't create them)
  ensurePostgresTables().then(()=>{
    warmPlanningCache();
    warmActualsCache();
  });
});
