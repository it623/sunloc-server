// ═══════════════════════════════════════════════════════════════════════════════════════════════
// assistant-engine.js — Sunloc Admin Assistant engine (v52)
//
// Everything the assistant does lives in this file: the metric registry, the data snapshot,
// the 12-hour digest (sections → styled xlsx → email), the scheduler, the LLM planner client,
// and the chat resolver. server.js only wires routes to the exports.
//
// HARD RULES (validator-enforced):
//   • READ-ONLY toward every pre-existing table. The only tables this module writes are
//     assistant_digests, assistant_audit, assistant_metrics, assistant_requests.
//   • All heavy SQL runs on a dedicated client with SET LOCAL statement_timeout (pool-starvation
//     fence, same pattern as _V51ZA_GAP_TIMEOUT_MS).
//   • Quantities NEVER leave the network. The LLM sees: the question, the metric catalogue, and
//     (for "why" synthesis) remarks TEXT with quantities stripped. Never figures.
//   • Frozen formulas come from sunloc-core.js (byte-identical to tracking.html). This module
//     contains NO arithmetic re-implementation of any frozen formula.
//   • Single-flight: one digest OR one chat resolution at a time (global.state shim safety).
// ═══════════════════════════════════════════════════════════════════════════════════════════════
'use strict';

const https = require('https');
const http = require('http');
const path = require('path');
const core = require('./public/sunloc-core.js');

// deps injected by server.js at init
let pgPool = null, db = null, PORT = 3000, log = console.log;

const IST_OFFSET_MS = 5.5 * 3600 * 1000;
const nowIST = () => new Date(Date.now() + IST_OFFSET_MS);
const istStamp = () => {
  const d = nowIST();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')} ${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')} IST`;
};
const istDate = () => istStamp().slice(0, 10);
const r2 = (v) => Math.round((Number(v) || 0) * 100) / 100;

// ─── single-flight lock ────────────────────────────────────────────────────────────────────────
let _busy = false;
async function withLock(label, fn) {
  if (_busy) throw new Error(`Assistant busy (${label}) — one request at a time; retry shortly`);
  _busy = true;
  try { return await fn(); } finally { _busy = false; }
}

// ─── fenced SQL (dedicated client + statement timeout, released in finally) ────────────────────
const SQL_TIMEOUT_MS = parseInt(process.env.ASSISTANT_SQL_TIMEOUT_MS || '20000', 10);
async function fencedQuery(sql, params) {
  if (!pgPool) { // SQLite fallback: better-sqlite3 is synchronous; no fence needed
    return { rows: db.prepare(sql.replace(/\$\d+/g, '?')).all(...(params || [])) };
  }
  const client = await pgPool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL statement_timeout = ${SQL_TIMEOUT_MS}`);
    const r = await client.query(sql, params);
    await client.query('COMMIT');
    return r;
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    if (e.code === '57014') throw new Error('query timed out (fence) — section skipped');
    throw e;
  } finally {
    client.release();
  }
}

// ─── local API fetch (reuses the live endpoints' exact assembly logic) ─────────────────────────
function localGet(pathname) {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: '127.0.0.1', port: PORT, path: pathname, timeout: 30000,
      headers: { 'x-assistant-internal': '1' } }, (res) => {
      let buf = '';
      res.on('data', (c) => buf += c);
      res.on('end', () => {
        try { resolve(JSON.parse(buf)); }
        catch (e) { reject(new Error(`non-JSON from ${pathname}`)); }
      });
    });
    req.on('timeout', () => { req.destroy(new Error(`timeout on ${pathname}`)); });
    req.on('error', reject);
  });
}

// ─── snapshot: populate the core's 14 globals + assistant-side extras ──────────────────────────
async function buildSnapshot() {
  const snap = { builtAt: new Date().toISOString(), builtAtIST: istStamp(), errors: [] };
  const grab = async (key, p) => {
    try { snap[key] = await localGet(p); }
    catch (e) { snap[key] = null; snap.errors.push(`${key}: ${e.message}`); }
  };
  // v52F (Ishan, 19 Aug — first-digest audit): the snapshot now mirrors the tracking client's OWN
  // load sequence instead of guessing shapes. Batches live in planning/state (orders), labels in
  // labels-all, and every report-grade number in scan-summary (summary/wastage/grossByBatch/
  // grossOverrides) — the tracking/state endpoint only carries closure/wastage/dispatch/scans.
  // Getting this wrong was why t2/t5/t6 showed all-zero WIP and t3 bucketed everything as 'older'.
  await grab('tracking', '/api/tracking/state');
  await grab('scanSummary', '/api/tracking/scan-summary');
  await grab('labelsAll', '/api/tracking/labels-all');
  await grab('retired', '/api/batch/retired');
  await grab('reconOverrides', '/api/batch/reconcile-overrides');
  await grab('orders', '/api/orders/all');
  await grab('machines', '/api/machines/master');
  await grab('planningKV', '/api/planning/all-kv');
  await grab('planningState', '/api/planning/state?reconcile=1');
  await grab('printSalvagePct', '/api/daily-printing/salvage-pct');
  const monthStart = istDate().slice(0, 8) + '01';
  await grab('invoices', `/api/invoice/received?from_date=${monthStart}&limit=5000`);
  await grab('handoverGaps', '/api/tracking/handover-gap-counts');

  // dpr_records — no range GET exists; direct fenced SELECT (read-only), last 62 days
  try {
    const since = new Date(Date.now() - 62 * 86400000).toISOString().slice(0, 10);
    const r = await fencedQuery(
      `SELECT floor, date, data_json AS data FROM dpr_records WHERE date >= $1 ORDER BY date ASC`, [since]);   // v52F: real column is data_json
    snap.dprRecords = (r.rows || []).map(x => {
      try { return { floor: x.floor, date: x.date, data: typeof x.data === 'string' ? JSON.parse(x.data) : x.data }; }
      catch (_) { return { floor: x.floor, date: x.date, data: null }; }
    });
  } catch (e) { snap.dprRecords = []; snap.errors.push('dprRecords: ' + e.message); }

  // deemed scan-outs + regularise audit (read-only)
  try {
    // v52F: is_deemed_scan_out lives on invoices_received, not tracking_scans
    const r = await fencedQuery(
      `SELECT sap_doc_num, batch_number, total_boxes, total_qty_lakhs, deemed_reason, deemed_by, dispatched_at
       FROM invoices_received WHERE is_deemed_scan_out = 1 ORDER BY dispatched_at DESC NULLS LAST LIMIT 200`, []);
    snap.deemedScanOuts = r.rows || [];
  } catch (e) { snap.deemedScanOuts = []; snap.errors.push('deemed: ' + e.message); }
  try {
    const r = await fencedQuery(
      `SELECT username, action, details, created_at FROM audit_log
       WHERE action ILIKE '%regulari%' ORDER BY created_at DESC LIMIT 200`, []);
    snap.regulariseAudit = r.rows || [];
  } catch (e) { snap.regulariseAudit = []; snap.errors.push('regularise: ' + e.message); }
  return snap;
}

// install snapshot into the core's globals (Node shim). Callers hold the single-flight lock.
function installState(snap) {
  const t = (snap.tracking && (snap.tracking.state || snap.tracking.data || snap.tracking)) || {};
  const pj = (snap.planningState && snap.planningState.state) || {};
  const sj = snap.scanSummary || {};
  global.window = global.window || {};
  global.state = {
    // v52F: batches = planning orders (client STEP 1, v37F: all non-deleted), enriched identically
    batches: ((pj.orders || []).filter(o => !o.deleted)).map(b => ({ ...b,
      actualQty: b.actualQty || b.actualProd || 0, actualProd: b.actualProd || b.actualQty || 0 })),
    scans: t.scans || [], labels: (snap.labelsAll && snap.labelsAll.labels) || t.labels || [],
    wastage: t.wastage || [], dispatchRecs: t.dispatchRecs || t.dispatches || [],
    stageClosure: t.stageClosure || {},
    // v52F: report-grade aggregates come from scan-summary (client STEP 3.6), field-for-field
    scanSummary: sj.summary || null, wastageSummary: sj.wastage || null,
    grossSummary: sj.grossByBatch || null, grossOverrides: sj.grossOverrides || {},
  };
  global.window._retiredSet = new Set(((snap.retired && (snap.retired.batches || snap.retired.data)) || []).map(b => (b.batchNumber || b.batch_number || b)));
  global.window._reconOverrides = (snap.reconOverrides && (snap.reconOverrides.overrides || snap.reconOverrides.data)) || {};
  // v52F: _printSalvagePct is a per-batch MAP served by its own endpoint (client ~line 2226),
  // not a scalar in planning kv — the wrong shape silently zeroed print-salvage netting.
  global.window._printSalvagePct = (snap.printSalvagePct && snap.printSalvagePct.pct) || {};
  global.window._trkProdMonthFilter = null; // digest computes per-month explicitly
}

// ─── small shared helpers (selection/formatting only — NO formula arithmetic) ──────────────────
const B = () => global.state.batches || [];
const bNo = (b) => b.batchNumber || b.batch_number || b.batch || '';
const bCust = (b) => (b.customer || '').trim();
const bFloor = (b) => (b.floor || b.aimFloor || '').toUpperCase();
const prodMonthOf = (b) => { try { return core._v50bCohortMonth(bNo(b)) || (b.prodMonth || '').slice(0, 7); } catch (_) { return (b.prodMonth || b.createdAt || '').slice(0, 7); } };
const top = (arr, n, key) => [...arr].sort((a, b2) => (b2[key] || 0) - (a[key] || 0)).slice(0, n);

function batchesFromJuly() {
  return B().filter(b => { const m = prodMonthOf(b); return m && m >= '2026-07'; });
}

// ─── DIGEST SECTIONS (template v1 — Ishan, 17 Aug 2026) ────────────────────────────────────────
// Every section: { title, headers, rows, note? } or { title, error }. A section failure never
// kills the digest.
const SECTIONS = [];
const section = (id, group, title, fn) => SECTIONS.push({ id, group, title, fn });

// —— PLANNING ——
section('p1_machines_48h', 'Planning', 'Machines with orders only for the next 48 hours', (snap) => {
  const orders = ((snap.orders && (snap.orders.orders || snap.orders.data)) || [])
    .filter(o => !o.deleted && o.status !== 'closed' && o.status !== 'completed');
  const horizon = Date.now() + 48 * 3600000;
  const byMc = {};
  for (const o of orders) {
    const mc = o.machineId || o.machine_id || o.machine; if (!mc) continue;
    const end = Date.parse(o.endDate || o.end_date || o.plannedEnd || o.dispatchDate || '') || 0;
    if (!byMc[mc] || end > byMc[mc].end) byMc[mc] = { end, order: o };
  }
  const mcs = ((snap.machines && (snap.machines.machines || snap.machines.data)) || []).filter(m => m.active !== false && (m.type || '').toLowerCase() !== 'print');
  const rows = [];
  for (const m of mcs) {
    const e = byMc[m.id];
    if (!e) { rows.push([m.id, m.size || '', '— no open order —', 'LOAD NOW']); continue; }
    if (e.end && e.end < horizon) rows.push([m.id, m.size || '', new Date(e.end).toISOString().slice(0, 10), 'runs dry < 48h']);
  }
  return { headers: ['Machine', 'Size', 'Last scheduled day', 'Flag'], rows, note: rows.length ? '' : 'All machines loaded beyond 48h.' };
});

section('p2_below_target', 'Planning', 'Machines below target — production & A-Grade (target vs actual)', (snap) => {
  const mcs = ((snap.machines && (snap.machines.machines || snap.machines.data)) || []);
  const tByMc = {}; for (const m of mcs) tByMc[m.id] = { cap: Number(m.cap) || 0, ag: Number(m.aGrade ?? m.a_grade) || 0 };
  // actuals from DPR blobs, last 7 days: per machine — avg daily prod, weighted A%
  const cut = new Date(Date.now() - 7 * 86400000).toISOString().slice(0, 10);
  const acc = {};
  for (const rec of (snap.dprRecords || [])) {
    if (!rec.data || String(rec.date).slice(0, 10) < cut) continue;
    for (const sh of Object.values(rec.data.shifts || {})) {
      for (const row of (sh.machines || sh.rows || [])) {
        const mc = row.mc || row.machineId || row.machine; if (!mc) continue;
        const a = acc[mc] || (acc[mc] = { prod: 0, agNum: 0, agDen: 0, days: new Set() });
        const q = Number(row.prod ?? row.qty ?? row.output) || 0;
        a.prod += q; a.days.add(String(rec.date).slice(0, 10));
        const ag = Number(row.aGrade ?? row.agrade ?? row.aGradePct);
        if (isFinite(ag) && q > 0) { a.agNum += ag * q; a.agDen += q; }
      }
    }
  }
  const rows = [];
  for (const [mc, a] of Object.entries(acc)) {
    const t = tByMc[mc] || { cap: 0, ag: 0 };
    const avg = a.days.size ? a.prod / a.days.size : 0;
    const agAct = a.agDen ? a.agNum / a.agDen : null;
    const prodBad = t.cap > 0 && avg < t.cap;
    const agBad = t.ag > 0 && agAct !== null && agAct < t.ag;
    if (prodBad || agBad) rows.push([mc, r2(t.cap), r2(avg), r2(avg - t.cap), t.ag || '—', agAct === null ? '—' : r2(agAct), agAct === null || !t.ag ? '—' : r2(agAct - t.ag)]);
  }
  rows.sort((x, y) => (x[3] || 0) - (y[3] || 0));
  return { headers: ['Machine', 'Target/day (L)', 'Actual avg/day (L)', 'Prod variance', 'A% target', 'A% actual', 'A% variance'], rows, note: '7-day window; negative variance = red flag.' };
});

section('p3_size_top5', 'Planning', 'Size-wise planning — top 5 customers with expected dispatch dates', (snap) => {
  const orders = ((snap.orders && (snap.orders.orders || snap.orders.data)) || []).filter(o => !o.deleted && o.status !== 'closed');
  const byCust = {};
  for (const o of orders) {
    const c = (o.customer || '').trim() || '—';
    const a = byCust[c] || (byCust[c] = { qty: 0, sizes: {}, disp: [] });
    const q = Number(o.qty ?? o.orderQty ?? o.quantity) || 0; a.qty += q;
    const s = String(o.size ?? '—'); a.sizes[s] = (a.sizes[s] || 0) + q;
    const d = o.dispatchDate || o.expectedDispatch || o.endDate || o.end_date; if (d) a.disp.push(String(d).slice(0, 10));
  }
  const rows = top(Object.entries(byCust).map(([c, a]) => ({ c, ...a })), 5, 'qty').map(a => [
    a.c, r2(a.qty),
    Object.entries(a.sizes).sort((x, y) => y[1] - x[1]).map(([s, q]) => `${s}: ${r2(q)}L`).join('  |  '),
    a.disp.sort().slice(0, 4).join(', ') + (a.disp.length > 4 ? ' …' : ''),
  ]);
  return { headers: ['Customer', 'Open qty (L)', 'Size split', 'Expected dispatch dates'], rows };
});

section('pp1_wastage_opm', 'Planning', 'Printing — cumulative wastage % (Daily Printing Log) & top 5 OPMs', (snap) => {
  const dp = (((snap.planningKV || {}).data || {}).dailyPrinting) || [];
  const byOpm = {};
  for (const e of dp) {
    const mc = e.machineId || e.opm; if (!mc) continue;
    const a = byOpm[mc] || (byOpm[mc] = { out: 0, sal: 0 });
    a.out += Number(e.totalOutput) || 0; a.sal += Number(e.salvage) || 0;
  }
  const all = Object.entries(byOpm).map(([mc, a]) => ({ mc, out: a.out, sal: a.sal, pct: a.out ? a.sal / a.out * 100 : 0 }));
  const totOut = all.reduce((s, x) => s + x.out, 0), totSal = all.reduce((s, x) => s + x.sal, 0);
  const rows = top(all, 5, 'pct').map(x => [x.mc, r2(x.out), r2(x.sal), r2(x.pct)]);
  return { headers: ['OPM', 'Output', 'Salvage', 'Wastage %'], rows, note: `Plant cumulative wastage: ${totOut ? r2(totSal / totOut * 100) : 0}% (${r2(totSal)} / ${r2(totOut)}).` };
});

section('p4_wo_july', 'Planning', 'W/O details — July production month onwards', (snap) => {
  // v52F (Ishan, 19 Aug — digest audit): W/O means orders CURRENTLY classified as W/O
  // (woStatus === 'wo', the planning app's own test) — not every batch since July. The first
  // digest listed 466 closed customer batches here; the section now shows the live W/O book only.
  const orders = ((snap.orders && (snap.orders.orders || snap.orders.data)) || [])
    .filter(o => !o.deleted && o.woStatus === 'wo');
  const rows = [];
  for (const o of orders) {
    const b = o.batchNumber || o.batch_number || '';
    const m = (() => { try { return core._v50bCohortMonth(b) || ''; } catch (_) { return ''; } })() || String(o.prodMonth || o.startDate || '').slice(0, 7);
    if (m && m < '2026-07') continue;
    rows.push([m || '—', o.woNumber || o.wo_number || o.wo || o.workOrder || '—', b, (o.customer || '').trim(), String(o.size ?? ''), r2(o.qty ?? o.orderQty ?? 0), o.status || 'open']);
  }
  rows.sort((a, b2) => a[0] < b2[0] ? -1 : a[0] > b2[0] ? 1 : 0);
  return { headers: ['Prod month', 'W/O', 'Batch', 'Customer', 'Size', 'Qty (L)', 'Status'], rows };
});

// —— DPR ——
section('d2_resort_top5', 'DPR', 'Top 5 machines by Re-Sort — with reasons (target: zero)', (snap) => {
  const byMc = {};
  for (const rec of (snap.dprRecords || [])) {
    if (!rec.data) continue;
    for (const rs of (rec.data.resort || [])) {
      const mc = rs.mc || rs.machineId; if (!mc) continue;
      const a = byMc[mc] || (byMc[mc] = { qty: 0, reasons: new Set() });
      a.qty += Number(rs.qty) || 0;
      const rr = (rs.reason || rs.remarks || '').trim(); if (rr) a.reasons.add(rr);
    }
  }
  const rows = top(Object.entries(byMc).map(([mc, a]) => ({ mc, qty: a.qty, reasons: [...a.reasons].join('; ') })), 5, 'qty')
    .map(x => [x.mc, r2(x.qty), x.reasons || '—']);
  return { headers: ['Machine', 'Re-Sort qty (L)', 'Reasons (verbatim)'], rows, note: 'Any Re-Sort is a negative variance (target = 0). 62-day window.' };
});

section('d3_dt_pct', 'DPR', 'Cumulative downtime by category — % of floor total, per floor', (snap) => {
  const byFloor = {};
  for (const rec of (snap.dprRecords || [])) {
    if (!rec.data) continue;
    const fl = (rec.floor || '').toUpperCase() || '—';
    const f = byFloor[fl] || (byFloor[fl] = {});
    for (const sh of Object.values(rec.data.shifts || {})) {
      for (const d of (sh.dt || sh.downtime || [])) {
        const cat = d.cat || d.category || 'other';
        f[cat] = (f[cat] || 0) + (Number(d.mins ?? d.minutes ?? d.hrs * 60) || 0);
      }
    }
  }
  const rows = [];
  for (const [fl, cats] of Object.entries(byFloor)) {
    const tot = Object.values(cats).reduce((s, v) => s + v, 0) || 1;
    for (const [cat, mins] of Object.entries(cats).sort((a, b2) => b2[1] - a[1]))
      rows.push([fl, cat, r2(mins / 60), r2(mins / tot * 100)]);
  }
  return { headers: ['Floor', 'Category', 'Hours', '% of floor DT'], rows, note: '62-day window; % out of 100 per floor.' };
});

section('d4_operators', 'DPR', 'Operators — top 5 and bottom 5 vs targets (attribution)', (snap) => {
  const kv = ((snap.planningKV || {}).data || {});
  const targets = kv.dpr_targets || kv.dprTargets || {};
  const ops = {};
  for (const rec of (snap.dprRecords || [])) {
    if (!rec.data) continue;
    for (const [shName, sh] of Object.entries(rec.data.shifts || {})) {
      for (const row of (sh.machines || sh.rows || [])) {
        const who = (row.operator || row.op || row.name || '').trim(); if (!who) continue;
        const mc = row.mc || row.machineId || ''; const q = Number(row.prod ?? row.qty) || 0;
        const t = Number(targets[`${mc}|${shName}`] ?? targets[mc]) || 0;
        const a = ops[who] || (ops[who] = { prod: 0, tgt: 0, shifts: 0 });
        a.prod += q; a.tgt += t; a.shifts++;
      }
    }
  }
  const scored = Object.entries(ops).filter(([, a]) => a.tgt > 0 && a.shifts >= 3)
    .map(([who, a]) => ({ who, ...a, pct: a.prod / a.tgt * 100 }));
  scored.sort((a, b2) => b2.pct - a.pct);
  const rows = [...scored.slice(0, 5).map(x => ['TOP', x.who, x.shifts, r2(x.prod), r2(x.tgt), r2(x.pct)]),
                ...scored.slice(-5).reverse().map(x => ['BOTTOM', x.who, x.shifts, r2(x.prod), r2(x.tgt), r2(x.pct)])];
  return { headers: ['', 'Operator', 'Shifts', 'Actual (L)', 'Target (L)', '% of target'], rows,
    note: 'Operator-to-machine-shift attribution vs stored machine|shift targets (min 3 shifts). 62-day window.' };
});

// —— TRACKING ——
section('t1_inout', 'Tracking', 'Boxes & quantity IN / OUT — Printing, PI, Packing', () => {
  const scans = global.state.scans || [];
  const monthStart = istDate().slice(0, 8) + '01';
  const h12 = Date.now() - 12 * 3600000;
  const agg = {};
  for (const s of scans) {
    const dept = (s.dept || s.department || '').toUpperCase();
    if (!['PRINTING', 'PI', 'PACKING'].includes(dept)) continue;
    const dir = String(s.type || '').toLowerCase() === 'out' ? 'OUT' : 'IN';   // v52F: scans carry type in/out
    const ts = Date.parse(s.ts || s.scannedAt || s.created_at || '') || 0;
    const day = new Date(ts).toISOString().slice(0, 10);
    const q = Number(s.qty) || 0;   // v52F: tracking_scans rows carry the label qty in `qty`
    for (const win of [day >= monthStart ? 'MTD' : null, ts >= h12 ? '12H' : null]) {
      if (!win) continue;
      const k = `${win}|${dept}|${dir}`;
      const a = agg[k] || (agg[k] = { boxes: 0, qty: 0 });
      a.boxes++; a.qty += q;
    }
  }
  const rows = [];
  for (const win of ['12H', 'MTD']) for (const dept of ['PRINTING', 'PI', 'PACKING']) for (const dir of ['IN', 'OUT']) {
    const a = agg[`${win}|${dept}|${dir}`] || { boxes: 0, qty: 0 };
    rows.push([win === '12H' ? 'Last 12 hours' : 'Month to date', dept, dir, a.boxes, r2(a.qty)]);
  }
  return { headers: ['Window', 'Department', 'Direction', 'Boxes', 'Qty (L)'], rows };
});

section('t2_wip_top', 'Tracking', 'Department-wise WIP (Report D top table)', () => {
  const sums = { prodWip: 0, aim: 0, printing: 0, pi: 0, packing: 0 };
  for (const b of B()) {
    if (core._isRetired(bNo(b))) continue;
    try {
      const w = core.getBatchWIPBreakdown(bNo(b)); if (!w) continue;
      sums.prodWip += w.preAIM ?? w.prodWip ?? 0;
      sums.aim += w.aimWIP ?? w.aimDeptWIP ?? 0;
      sums.printing += w.printWIP ?? w.printingWIP ?? 0;
      sums.pi += w.piWIP ?? 0;
      sums.packing += w.packWIP ?? w.packingWIP ?? 0;
    } catch (_) {}
  }
  return { headers: ['Unscanned/Prod WIP (L)', 'AIM (L)', 'Printing (L)', 'PI (L)', 'Packing (L)', 'TOTAL (L)'],
    rows: [[r2(sums.prodWip), r2(sums.aim), r2(sums.printing), r2(sums.pi), r2(sums.packing),
            r2(sums.prodWip + sums.aim + sums.printing + sums.pi + sums.packing)]],
    note: 'Frozen getBatchWIPBreakdown, all live batches.' };
});

section('t3_dispatch_buckets', 'Tracking', 'Dispatches this month — by production-month age', () => {
  const monthStart = istDate().slice(0, 8) + '01';
  const curM = monthStart.slice(0, 7);
  const prevM = (() => { const d = new Date(curM + '-01T00:00:00Z'); d.setUTCMonth(d.getUTCMonth() - 1); return d.toISOString().slice(0, 7); })();
  const buckets = { current: 0, previous: 0, older: 0 };
  for (const rec of (global.state.dispatchRecs || [])) {
    const day = String(rec.date || rec.dispatchDate || rec.ts || '').slice(0, 10);
    if (day < monthStart) continue;
    const q = Number(rec.qtyLakhs ?? rec.qty_lakhs ?? rec.qty) || 0;
    const bm = (() => { try { return core._v50bCohortMonth(rec.batchNumber || rec.batch_number || '') || ''; } catch (_) { return ''; } })();
    if (bm === curM) buckets.current += q;
    else if (bm === prevM) buckets.previous += q;
    else buckets.older += q;
  }
  return { headers: ['Bucket', 'Qty (L)'], rows: [
    ['Current production month, dispatched this month', r2(buckets.current)],
    ['Immediately previous production month', r2(buckets.previous)],
    ['Past periods (2+ months prior)', r2(buckets.older)],
    ['TOTAL', r2(buckets.current + buckets.previous + buckets.older)]] };
});

section('t4_top_invoiced', 'Tracking', 'Top 5 customers invoiced this month — qty & value', (snap) => {
  const inv = ((snap.invoices && (snap.invoices.invoices || snap.invoices.requests || snap.invoices.data)) || []);
  const byCust = {};
  for (const i of inv) {
    const c = (i.customer || '').trim() || '—';
    // v52F: invoices_received carries total_qty_lakhs and the SAP-billed total_amount — the
    // rate_per_lakh field belongs to invoice_requests and was never here (0-qty/0-value audit find)
    const q = Number(i.total_qty_lakhs) || 0;
    const v = Number(i.total_amount) || 0;
    const a = byCust[c] || (byCust[c] = { qty: 0, val: 0, n: 0 });
    a.qty += q; a.val += v; a.n++;
  }
  const rows = top(Object.entries(byCust).map(([c, a]) => ({ c, ...a })), 5, 'val')
    .map(a => [a.c, a.n, r2(a.qty), Math.round(a.val).toLocaleString('en-IN')]);
  return { headers: ['Customer', 'Invoices', 'Qty (L)', 'Value (₹)'], rows, note: 'Current calendar month, irrespective of production month. Value = qty × rate/lakh.' };
});

section('t5_alkem_wip', 'Tracking', 'Alkem — month-wise WIP (July onwards)', () => {
  const rows = [];
  const byM = {};
  for (const b of batchesFromJuly()) {
    if (!/alkem/i.test(bCust(b)) || core._isRetired(bNo(b))) continue;
    try {
      const w = core.getBatchWIPBreakdown(bNo(b)); if (!w) continue;
      const m = prodMonthOf(b);
      const a = byM[m] || (byM[m] = { prod: 0, aim: 0, print: 0, pi: 0, pack: 0 });
      a.prod += w.preAIM ?? 0; a.aim += w.aimWIP ?? w.aimDeptWIP ?? 0;
      a.print += w.printWIP ?? 0; a.pi += w.piWIP ?? 0; a.pack += w.packWIP ?? 0;
    } catch (_) {}
  }
  for (const m of Object.keys(byM).sort()) {
    const a = byM[m];
    rows.push([m, r2(a.prod), r2(a.aim), r2(a.print), r2(a.pi), r2(a.pack), r2(a.prod + a.aim + a.print + a.pi + a.pack)]);
  }
  return { headers: ['Prod month', 'Unscanned', 'AIM', 'Printing', 'PI', 'Packing', 'Total WIP (L)'], rows };
});

section('t6_dept_wip_months', 'Tracking', 'Department-wise WIP per production month (July onwards)', (snap) => {
  const byM = {};
  for (const b of batchesFromJuly()) {
    if (core._isRetired(bNo(b))) continue;
    try {
      const w = core.getBatchWIPBreakdown(bNo(b)); if (!w) continue;
      const m = prodMonthOf(b), fl = bFloor(b) || '—';
      const a = byM[m] || (byM[m] = { aimScanned: {}, aimUnscanned: {}, print: 0, pi: 0 });
      a.aimScanned[fl] = (a.aimScanned[fl] || 0) + (w.aimWIP ?? w.aimDeptWIP ?? 0);
      a.aimUnscanned[fl] = (a.aimUnscanned[fl] || 0) + (w.preAIM ?? 0);
      a.print += w.printWIP ?? 0; a.pi += w.piWIP ?? 0;
    } catch (_) {}
  }
  const gaps = ((snap.handoverGaps || {}).counts || (snap.handoverGaps || {}).data || {});
  const rows = [];
  for (const m of Object.keys(byM).sort()) {
    const a = byM[m];
    const flStr = (o) => Object.entries(o).map(([f, v]) => `${f}: ${r2(v)}`).join('  |  ') || '—';
    rows.push([m, flStr(a.aimScanned), flStr(a.aimUnscanned), r2(a.print), r2(a.pi)]);
  }
  return { headers: ['Prod month', 'AIM scanned (GF | FF/SF)', 'AIM unscanned (GF | FF/SF)', 'Printing scanned (L)', 'PI scanned (L)'], rows,
    note: 'Scanned per Report B logic (frozen). Printing/PI unscanned = handover gaps (Report F): ' +
      (typeof gaps === 'object' ? Object.entries(gaps).slice(0, 6).map(([k, v]) => `${k}=${v}`).join(', ') : String(gaps)) };
});

section('t7_stale_boxes', 'Tracking', 'Boxes in Packing > 7 days, not dispatched', () => {
  const cut = Date.now() - 7 * 86400000;
  // v52F: rec.boxes is a COUNT (the 'number 9 is not iterable' crash in the first digest);
  // the box/label ids live in rec.scannedLabels. Scan rows identify their box via label_id.
  const dispatched = new Set();
  for (const rec of (global.state.dispatchRecs || []))
    if (Array.isArray(rec.scannedLabels)) for (const bx of rec.scannedLabels) dispatched.add(String(bx && bx.labelId || bx));
  const rows = [];
  for (const s of (global.state.scans || [])) {
    const dept = (s.dept || '').toUpperCase();
    if (dept !== 'PACKING' || (s.type || '').toUpperCase().includes('OUT')) continue;
    const ts = Date.parse(s.ts || '') || 0;
    if (!ts || ts > cut) continue;
    const box = String(s.labelId || s.label_id || ''); if (!box || dispatched.has(box)) continue;
    rows.push([box, s.batchNumber || s.batch_number || '', new Date(ts).toISOString().slice(0, 10), Math.floor((Date.now() - ts) / 86400000)]);
  }
  rows.sort((a, b2) => b2[3] - a[3]);
  return { headers: ['Box', 'Batch', 'Packed on', 'Days idle'], rows: rows.slice(0, 100),
    note: rows.length > 100 ? `${rows.length} total; top 100 shown.` : '' };
});

section('t8_partial', 'Tracking', 'Partial dispatches (less than available scanned qty) — henceforth', () => {
  const epoch = process.env.ASSISTANT_EPOCH || '2026-08-17';
  const rows = [];
  for (const rec of (global.state.dispatchRecs || [])) {
    const day = String(rec.date || rec.dispatchDate || '').slice(0, 10);
    if (day < epoch) continue;
    const avail = Number(rec.availableQty ?? rec.available_qty);
    const q = Number(rec.qtyLakhs ?? rec.qty_lakhs ?? rec.qty) || 0;
    if (isFinite(avail) && avail > 0 && q < avail)
      rows.push([day, rec.batchNumber || rec.batch_number || '', (rec.customer || '').trim(), r2(q), r2(avail), r2(avail - q)]);
  }
  return { headers: ['Date', 'Batch', 'Customer', 'Dispatched (L)', 'Available (L)', 'Left behind (L)'], rows,
    note: `From ${epoch} onwards only (no backfill, per standing rule). Records without an available-qty field are not judged.` };
});

section('t8b_deemed', 'Tracking', 'Dispatches without scan-out (deemed) & admin regularisations', (snap) => {
  const rows = (snap.deemedScanOuts || []).slice(0, 50).map(d =>
    ['Deemed scan-out', d.batch_number || '', 'inv ' + (d.sap_doc_num || ''), r2(d.total_qty_lakhs), d.deemed_by || '', String(d.dispatched_at || '').slice(0, 16) + ' — ' + String(d.deemed_reason || '').slice(0, 60)]);
  for (const a of (snap.regulariseAudit || []).slice(0, 50))
    rows.push(['Admin regularise', '', '', '', a.username || '', String(a.created_at || '').slice(0, 16) + ' — ' + String(a.details || '').slice(0, 80)]);
  return { headers: ['Type', 'Batch', 'Box', 'Qty (L)', 'By', 'When / details'], rows };
});

// —— DATA INTEGRITY ——
section('di1_dpr_vs_gross', 'Data Integrity', 'Batches where DPR ≠ Gross', () => {
  const rows = [];
  for (const b of B()) {
    const no = bNo(b); if (core._isRetired(no)) continue;
    try {
      const dpr = core._dprOvrGross(no);
      const gross = core._grossFor(no);
      if (dpr != null && gross != null && Math.abs(dpr - gross) > 0.05)
        rows.push([no, bCust(b), r2(gross), r2(dpr), r2(dpr - gross)]);
    } catch (_) {}
  }
  rows.sort((a, b2) => Math.abs(b2[4]) - Math.abs(a[4]));
  return { headers: ['Batch', 'Customer', 'Gross (L)', 'DPR (L)', 'Difference'], rows: rows.slice(0, 50) };
});

section('di2_inspected_gt_dpr', 'Data Integrity', 'Batches where Inspected > DPR', () => {
  const rows = [];
  for (const b of B()) {
    const no = bNo(b); if (core._isRetired(no)) continue;
    try {
      const g = core.getBatchAGrade(no); if (!g) continue;
      const inspected = g.inspected || 0;      // frozen definition: Inspected = ScanIn + Salvage
      const dpr = core._dprOvrGross(no);
      if (dpr != null && inspected > dpr + 0.05)
        rows.push([no, bCust(b), r2(dpr), r2(inspected), r2(inspected - dpr)]);
    } catch (_) {}
  }
  rows.sort((a, b2) => b2[4] - a[4]);
  return { headers: ['Batch', 'Customer', 'DPR (L)', 'Inspected (L)', 'Excess'], rows: rows.slice(0, 50) };
});

// ─── run all sections ──────────────────────────────────────────────────────────────────────────
async function computeDigest(snap) {
  installState(snap);
  const out = { generatedAt: snap.builtAtIST, sections: [], snapshotErrors: snap.errors };
  for (const s of SECTIONS) {
    try {
      const r = await s.fn(snap);
      out.sections.push({ id: s.id, group: s.group, title: s.title, ...r });
    } catch (e) {
      out.sections.push({ id: s.id, group: s.group, title: s.title, error: e.message });
    }
  }
  return out;
}

// ─── xlsx render (xlsx-js-style — same library family as Report E) ─────────────────────────────
function renderXlsx(digest) {
  const XLSX = require('xlsx-js-style');
  const wb = XLSX.utils.book_new();
  const S = {
    title: { font: { bold: true, sz: 16, color: { rgb: 'FFFFFF' } }, fill: { fgColor: { rgb: '1F4E79' } }, alignment: { vertical: 'center' } },
    banner: { font: { bold: true, sz: 12, color: { rgb: 'FFFFFF' } }, fill: { fgColor: { rgb: '2E75B6' } } },
    hdr: { font: { bold: true, color: { rgb: 'FFFFFF' } }, fill: { fgColor: { rgb: '595959' } }, border: { bottom: { style: 'thin' } } },
    neg: { font: { color: { rgb: '9C0006' }, bold: true }, fill: { fgColor: { rgb: 'FFC7CE' } } },
    note: { font: { italic: true, sz: 9, color: { rgb: '7F7F7F' } } },
    err: { font: { color: { rgb: '9C0006' }, italic: true } },
  };
  const groups = ['Summary', 'Planning', 'DPR', 'Tracking', 'Data Integrity'];
  const byGroup = {};
  for (const s of digest.sections) (byGroup[s.group] = byGroup[s.group] || []).push(s);

  // Summary sheet
  {
    const aoa = [[{ v: `SUNLOC ADMIN DIGEST — ${digest.generatedAt}`, s: S.title }], []];
    for (const g of groups.slice(1)) {
      aoa.push([{ v: g, s: S.banner }]);
      for (const s of (byGroup[g] || [])) {
        const n = s.error ? 'ERROR' : (s.rows ? s.rows.length : 0);
        aoa.push([s.title, s.error ? { v: s.error, s: S.err } : `${n} row(s)`]);
      }
      aoa.push([]);
    }
    if (digest.snapshotErrors && digest.snapshotErrors.length)
      aoa.push([{ v: 'Snapshot warnings: ' + digest.snapshotErrors.join(' | '), s: S.note }]);
    const ws = XLSX.utils.aoa_to_sheet(aoa);
    ws['!cols'] = [{ wch: 70 }, { wch: 40 }];
    XLSX.utils.book_append_sheet(wb, ws, 'Summary');
  }

  for (const g of groups.slice(1)) {
    const aoa = [];
    for (const s of (byGroup[g] || [])) {
      aoa.push([{ v: s.title, s: S.banner }]);
      if (s.error) { aoa.push([{ v: 'Section failed: ' + s.error, s: S.err }]); aoa.push([]); continue; }
      aoa.push((s.headers || []).map(h => ({ v: h, s: S.hdr })));
      const varCols = (s.headers || []).map((h, i) => /variance|difference|excess|left behind/i.test(h) ? i : -1).filter(i => i >= 0);
      for (const row of (s.rows || [])) {
        aoa.push(row.map((c, i) => {
          const isNeg = varCols.includes(i) && Number(c) < 0;
          const isPos = varCols.includes(i) && Number(c) > 0 && /excess|difference|left behind/i.test(s.headers[i] || '');
          return (isNeg || isPos) ? { v: c, s: S.neg } : { v: c };
        }));
      }
      if (s.note) aoa.push([{ v: s.note, s: S.note }]);
      aoa.push([]);
    }
    const ws = XLSX.utils.aoa_to_sheet(aoa.length ? aoa : [['—']]);
    ws['!cols'] = Array.from({ length: 8 }, (_, i) => ({ wch: i === 0 ? 34 : 20 }));
    XLSX.utils.book_append_sheet(wb, ws, g.replace(/[\\/?*[\]]/g, '').slice(0, 31));
  }
  return XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
}

// ─── email ─────────────────────────────────────────────────────────────────────────────────────
async function sendDigestEmail(xlsxBuf, digest) {
  const host = process.env.SMTP_HOST, user = process.env.SMTP_USER, pass = process.env.SMTP_PASS;
  const to = process.env.DIGEST_EMAIL_TO;
  if (!host || !user || !pass || !to) return { sent: false, reason: 'SMTP env not configured' };
  const nodemailer = require('nodemailer');
  const transporter = nodemailer.createTransport({
    host, port: parseInt(process.env.SMTP_PORT || '587', 10), secure: false,
    auth: { user, pass },
  });
  const nSections = digest.sections.length, nErr = digest.sections.filter(s => s.error).length;
  const fname = `Sunloc_Admin_Digest_${digest.generatedAt.replace(/[: ]/g, '-')}.xlsx`;
  await transporter.sendMail({
    from: `Sunloc Assistant <${user}>`, to,
    subject: `Sunloc Admin Digest — ${digest.generatedAt}`,
    text: `Attached: the ${digest.generatedAt} digest. ${nSections} sections${nErr ? `, ${nErr} failed (see Summary sheet)` : ''}.\n\nGenerated automatically by the Sunloc Admin Assistant (v52).`,
    attachments: [{ filename: fname, content: xlsxBuf }],
  });
  return { sent: true };
}

// ─── digest persistence + run ──────────────────────────────────────────────────────────────────
async function storeDigest(slotKey, digest, xlsxBuf, emailResult) {
  const id = 'dg_' + Date.now().toString(36);
  await fencedQuery(
    `INSERT INTO assistant_digests (id, slot_key, generated_at, digest_json, xlsx_base64, email_sent, email_note)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [id, slotKey, new Date().toISOString(), JSON.stringify(digest), xlsxBuf.toString('base64'),
     emailResult.sent ? 1 : 0, emailResult.reason || '']);
  return id;
}

async function runDigest(slotKey, trigger) {
  return withLock('digest', async () => {
    log(`[assistant] digest run start — slot=${slotKey} trigger=${trigger}`);
    const snap = await buildSnapshot();
    const digest = await computeDigest(snap);
    const xlsxBuf = renderXlsx(digest);
    let emailResult = { sent: false, reason: 'not attempted' };
    try { emailResult = await sendDigestEmail(xlsxBuf, digest); }
    catch (e) { emailResult = { sent: false, reason: e.message }; }
    const id = await storeDigest(slotKey, digest, xlsxBuf, emailResult);
    log(`[assistant] digest ${id} stored — email: ${emailResult.sent ? 'sent' : emailResult.reason}`);
    return { id, sections: digest.sections.length, failed: digest.sections.filter(s => s.error).length, email: emailResult };
  });
}

// ─── scheduler: 08:00 & 20:00 IST, restart-safe ────────────────────────────────────────────────
function currentSlotKey() {
  const d = nowIST();
  const hh = d.getUTCHours();
  const slot = hh >= 20 ? '20' : hh >= 8 ? '08' : null;
  if (slot === null) return null;
  // the 20:00 slot of "today IST"; before 08:00 no slot is due yet today
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}_${slot}`;
}
function startScheduler() {
  if (process.env.SUNLOC_DISABLE_BG_JOBS === '1') { log('[assistant] scheduler disabled (SUNLOC_DISABLE_BG_JOBS)'); return; }
  setInterval(async () => {
    try {
      const slot = currentSlotKey(); if (!slot) return;
      const r = await fencedQuery(`SELECT 1 FROM assistant_digests WHERE slot_key = $1 LIMIT 1`, [slot]);
      if (r.rows && r.rows.length) return;                 // this slot already generated (restart-safe)
      await runDigest(slot, 'schedule');
    } catch (e) {
      if (!/busy/.test(e.message)) log('[assistant] scheduler:', e.message);
    }
  }, 60 * 1000);
  log('[assistant] scheduler armed — 08:00 & 20:00 IST');
}

// ─── LLM client (raw https, no SDK; key server-side only) ──────────────────────────────────────
function anthropic(messages, system, maxTokens) {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) return Promise.reject(new Error('ANTHROPIC_API_KEY not configured'));
  const body = JSON.stringify({
    model: process.env.ASSISTANT_MODEL || 'claude-sonnet-4-6',
    max_tokens: maxTokens || 1500, system, messages,
  });
  return new Promise((resolve, reject) => {
    const req = https.request({
      host: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': key, 'anthropic-version': '2023-06-01',
                 'content-length': Buffer.byteLength(body) },
      timeout: 60000,
    }, (res) => {
      let buf = ''; res.on('data', c => buf += c);
      res.on('end', () => {
        try {
          const j = JSON.parse(buf);
          if (j.error) return reject(new Error(j.error.message || 'API error'));
          resolve((j.content || []).filter(c => c.type === 'text').map(c => c.text).join('\n'));
        } catch (e) { reject(e); }
      });
    });
    req.on('timeout', () => req.destroy(new Error('LLM timeout')));
    req.on('error', reject);
    req.write(body); req.end();
  });
}

// ─── METRIC REGISTRY (chat) ────────────────────────────────────────────────────────────────────
// Each metric: id, desc, dims (accepted filter params), resolve(params, snap) → {headers, rows, note?}
// Section functions double as resolvers; chat-only metrics are added below. Params are always
// optional; resolvers must tolerate absence.
const bySection = Object.fromEntries(SECTIONS.map(s => [s.id, s]));
const REGISTRY = [];
const metric = (id, desc, dims, resolve) => REGISTRY.push({ id, desc, dims, resolve });

// re-expose every digest section as a metric
for (const s of SECTIONS)
  metric('digest.' + s.id, `[digest section] ${s.title}`, [], (p, snap) => s.fn(snap));

metric('wip.by_batch', 'WIP breakdown for one batch or all batches (frozen formula)', ['batch', 'customer', 'month', 'dept'], () => {
  const rows = [];
  for (const b of B()) {
    const no = bNo(b);
    if (core._isRetired(no)) continue;
    try {
      const w = core.getBatchWIPBreakdown(no); if (!w) continue;
      rows.push([no, bCust(b), prodMonthOf(b), r2(w.preAIM ?? 0), r2(w.aimWIP ?? w.aimDeptWIP ?? 0), r2(w.printWIP ?? 0), r2(w.piWIP ?? 0), r2(w.packWIP ?? 0)]);
    } catch (_) {}
  }
  return { headers: ['Batch', 'Customer', 'Prod month', 'Unscanned', 'AIM', 'Printing', 'PI', 'Packing'], rows };
});

metric('wip.by_dept', 'Total WIP per department', ['month'], (p, snap) => bySection.t2_wip_top.fn(snap));
metric('wip.by_customer', 'WIP totals grouped by customer', ['month'], () => {
  const byC = {};
  for (const b of B()) {
    const no = bNo(b); if (core._isRetired(no)) continue;
    try {
      const w = core.getBatchWIPBreakdown(no); if (!w) continue;
      byC[bCust(b) || '—'] = (byC[bCust(b) || '—'] || 0) + (w.totalWIP ?? 0);
    } catch (_) {}
  }
  return { headers: ['Customer', 'Total WIP (L)'], rows: Object.entries(byC).sort((a, b2) => b2[1] - a[1]).map(([c, v]) => [c, r2(v)]) };
});

metric('agrade.by_batch', 'A-Grade cascade (AIM / post-Print / post-PI) per batch — frozen formulas', ['batch', 'customer', 'month'], () => {
  const rows = [];
  for (const b of B()) {
    const no = bNo(b); if (core._isRetired(no)) continue;
    try {
      const g = core.getBatchAGrade(no); if (!g) continue;
      const aimPct = g.stages && g.stages.aim ? r2(g.stages.aim.pct) : r2(g.pct);
      const postPrint = (g.postPrintPct != null) ? r2(g.postPrintPct) : '—';
      const postPI = (g.postPIPct != null) ? r2(g.postPIPct)
                   : (g.stages && g.stages.pi && g.stages.pi.pct != null) ? r2(g.stages.pi.pct) : '—';
      rows.push([no, bCust(b), aimPct, postPrint, postPI]);
    } catch (_) {}
  }
  return { headers: ['Batch', 'Customer', 'AIM A%', 'Post-Print A%', 'Post-PI A%'], rows };
});

metric('balance.to_dispatch', 'Balance quantity remaining to dispatch, by customer and production month', ['customer', 'month'], () => {
  const by = {};
  for (const b of B()) {
    const no = bNo(b); if (core._isRetired(no)) continue;
    try {
      const gross = core._grossFor(no) || 0;
      const disp = core._v40_dispatchedLakhs(no) || 0;
      const k = `${bCust(b) || '—'}|${prodMonthOf(b)}`;
      const a = by[k] || (by[k] = { gross: 0, disp: 0 });
      a.gross += gross; a.disp += disp;
    } catch (_) {}
  }
  const rows = Object.entries(by).map(([k, a]) => {
    const [c, m] = k.split('|');
    return [c, m, r2(a.gross), r2(a.disp), r2(Math.max(0, a.gross - a.disp))];
  }).sort((x, y) => y[4] - x[4]);
  return { headers: ['Customer', 'Prod month', 'Gross (L)', 'Dispatched (L)', 'Balance (L)'], rows };
});

metric('planned.qty', 'Planned production quantity by customer / month / size', ['customer', 'month', 'size'], (p, snap) => {
  const orders = ((snap.orders && (snap.orders.orders || snap.orders.data)) || []).filter(o => !o.deleted);
  const by = {};
  for (const o of orders) {
    const k = `${(o.customer || '—').trim()}|${String(o.prodMonth || o.startDate || '').slice(0, 7)}|${o.size ?? '—'}`;
    by[k] = (by[k] || 0) + (Number(o.qty ?? o.orderQty) || 0);
  }
  return { headers: ['Customer', 'Month', 'Size', 'Planned qty (L)'],
    rows: Object.entries(by).map(([k, v]) => [...k.split('|'), r2(v)]).sort((a, b2) => b2[3] - a[3]) };
});

metric('orders.trend', 'Order intake trend, size-wise and customer-wise', ['customer', 'size'], (p, snap) => {
  const orders = ((snap.orders && (snap.orders.orders || snap.orders.data)) || []).filter(o => !o.deleted);
  const by = {};
  for (const o of orders) {
    const m = String(o.createdAt || o.orderDate || o.startDate || '').slice(0, 7) || '—';
    const k = `${m}|${o.size ?? '—'}`;
    by[k] = (by[k] || 0) + (Number(o.qty ?? o.orderQty) || 0);
  }
  return { headers: ['Month received', 'Size', 'Qty (L)'],
    rows: Object.entries(by).map(([k, v]) => [...k.split('|'), r2(v)]).sort() };
});

metric('scanning.efficiency', 'Department scanning efficiency — In vs Out per department', [], () => {
  const agg = {};
  for (const s of (global.state.scans || [])) {
    const dept = (s.dept || '').toUpperCase() || '—';
    const dir = (s.direction || s.type || '').toUpperCase().includes('OUT') ? 'out' : 'in';
    const q = Number(s.qtyLakhs ?? s.qty_lakhs) || 0;
    const a = agg[dept] || (agg[dept] = { in: 0, out: 0 });
    a[dir] += q;
  }
  return { headers: ['Department', 'In (L)', 'Out (L)', 'Out/In %'],
    rows: Object.entries(agg).map(([d, a]) => [d, r2(a.in), r2(a.out), a.in ? r2(a.out / a.in * 100) : '—']) };
});

metric('boxes.idle', 'Boxes/batches not moved in the last N days (default 15)', ['days'], (p) => {
  const days = Math.max(1, parseInt((p && p.days) || 15, 10));
  const cut = Date.now() - days * 86400000;
  const lastByBox = {};
  for (const s of (global.state.scans || [])) {
    const box = String(s.labelId || s.label_id || ''); if (!box) continue;   // v52F: box id = label id
    const ts = Date.parse(s.ts || '') || 0;
    if (!lastByBox[box] || ts > lastByBox[box].ts) lastByBox[box] = { ts, dept: (s.dept || '').toUpperCase(), batch: s.batchNumber || s.batch_number || '' };
  }
  const dispatched = new Set();
  for (const rec of (global.state.dispatchRecs || []))
    if (Array.isArray(rec.scannedLabels)) for (const bx of rec.scannedLabels) dispatched.add(String(bx && bx.labelId || bx));   // v52F: boxes is a count
  const rows = Object.entries(lastByBox)
    .filter(([box, x]) => x.ts && x.ts < cut && !dispatched.has(box))
    .map(([box, x]) => [box, x.batch, x.dept, new Date(x.ts).toISOString().slice(0, 10), Math.floor((Date.now() - x.ts) / 86400000)])
    .sort((a, b2) => b2[4] - a[4]);
  return { headers: ['Box', 'Batch', 'Last dept', 'Last scan', 'Days idle'], rows: rows.slice(0, 200) };
});

metric('gpr.status', 'GPR module metrics', [], () => ({
  headers: ['Status'], rows: [['GPR is not live yet — its metrics will appear here when the GPR module deploys.']] }));

// ─── plan validation + resolution ──────────────────────────────────────────────────────────────
const REG_IDS = new Set(REGISTRY.map(m => m.id));
function validatePlan(plan) {
  if (!plan || !Array.isArray(plan.metrics) || !plan.metrics.length) return 'plan has no metrics';
  if (plan.metrics.length > 6) return 'plan too broad (max 6 metrics)';
  for (const m of plan.metrics) {
    if (!REG_IDS.has(m.id)) return `unknown metric: ${m.id}`;
    if (m.params && typeof m.params !== 'object') return `bad params on ${m.id}`;
  }
  return null;
}
async function resolvePlan(plan, snap) {
  installState(snap);
  const results = [];
  for (const m of plan.metrics) {
    const reg = REGISTRY.find(r => r.id === m.id);
    try {
      let r = await reg.resolve(m.params || {}, snap);
      // generic post-filters (selection only — never arithmetic)
      if (m.params && r && r.rows && r.headers) {
        for (const [key, val] of Object.entries(m.params)) {
          const ci = r.headers.findIndex(h => h.toLowerCase().includes(key.toLowerCase()));
          if (ci >= 0 && val) r = { ...r, rows: r.rows.filter(row => String(row[ci]).toLowerCase().includes(String(val).toLowerCase())) };
        }
        if (m.params.top) r = { ...r, rows: r.rows.slice(0, parseInt(m.params.top, 10) || 10) };
      }
      results.push({ id: m.id, ...r });
    } catch (e) { results.push({ id: m.id, error: e.message }); }
  }
  return results;
}

const PLANNER_SYSTEM = () => `You are the query planner for the Sunloc admin assistant (pharma capsule plant).
Turn the admin's question into a JSON plan. You NEVER see data — only choose metrics.
Respond with ONLY JSON, no prose, no markdown fences. One of:
1. {"plan":{"metrics":[{"id":"<metric id>","params":{...}}]}} — params may include: batch, customer, month (YYYY-MM), size, dept, days, top.
2. {"clarify":"<one short question>"} — if the question is ambiguous or no metric fits.
3. {"tier1":{"name":"<proposed metric name>","base":"<existing metric id>","transform":"<filter/ratio/groupBy in words>"},"clarify":"<ask admin to approve>"} — if derivable from existing metrics.
4. {"tier2":"<what new base data would be needed>"} — if it needs data no metric exposes.
Available metrics:
${REGISTRY.map(m => `- ${m.id}: ${m.desc}${m.dims.length ? ' (params: ' + m.dims.join(', ') + ')' : ''}`).join('\n')}`;

async function ask(question, user) {
  return withLock('ask', async () => {
    const snap = await buildSnapshot();
    let planText, parsed;
    try {
      planText = await anthropic([{ role: 'user', content: question }], PLANNER_SYSTEM(), 1200);
      parsed = JSON.parse(planText.replace(/```json|```/g, '').trim());
    } catch (e) {
      // deterministic fallback: no LLM → serve the registry for structured mode
      await audit(user, 'ask-fallback', question, e.message);
      return { mode: 'fallback', note: 'LLM unavailable (' + e.message + ') — use structured mode below.',
               registry: REGISTRY.map(m => ({ id: m.id, desc: m.desc, dims: m.dims })) };
    }
    if (parsed.clarify && !parsed.tier1) { await audit(user, 'clarify', question, parsed.clarify); return { mode: 'clarify', question: parsed.clarify }; }
    if (parsed.tier1) { await audit(user, 'tier1-proposal', question, JSON.stringify(parsed.tier1)); return { mode: 'tier1', proposal: parsed.tier1, clarify: parsed.clarify || 'Approve this derived metric?' }; }
    if (parsed.tier2) {
      await fencedQuery(`INSERT INTO assistant_requests (id, question, need, requested_by, created_at) VALUES ($1,$2,$3,$4,$5)`,
        ['rq_' + Date.now().toString(36), question, String(parsed.tier2).slice(0, 2000), user, new Date().toISOString()]);
      await audit(user, 'tier2-queued', question, parsed.tier2);
      return { mode: 'tier2', note: 'This needs new base data. Logged as a build request: ' + parsed.tier2 };
    }
    const bad = validatePlan(parsed.plan);
    if (bad) { await audit(user, 'plan-rejected', question, bad); return { mode: 'clarify', question: `I could not form a safe plan (${bad}). Could you rephrase?` }; }
    const results = await resolvePlan(parsed.plan, snap);
    await audit(user, 'answered', question, JSON.stringify(parsed.plan));
    return { mode: 'answer', plan: parsed.plan, results, asOf: snap.builtAtIST };
  });
}

// "why" synthesis: remarks TEXT out (option ii) — quantities stripped before egress
async function synthesizeRemarks(scopeQuestion, user) {
  const snap = await buildSnapshot();
  const texts = [];
  for (const rec of (snap.dprRecords || []).slice(-40)) {
    if (!rec.data) continue;
    for (const rm of (rec.data.remarks || []))
      texts.push(`[${rec.floor} ${String(rec.date).slice(0, 10)}${rm.mc ? ' ' + rm.mc : ''}${rm.shift ? ' shift ' + rm.shift : ''}] ${String(rm.text || rm.remark || rm).replace(/\d+(\.\d+)?/g, '#')}`);
    for (const rs of (rec.data.resort || []))
      if (rs.reason) texts.push(`[Re-Sort ${rec.floor} ${String(rec.date).slice(0, 10)} ${rs.mc || ''}] ${String(rs.reason).replace(/\d+(\.\d+)?/g, '#')}`);
  }
  for (const w of (((snap.tracking || {}).state || snap.tracking || {}).wastage || []).slice(-100))
    if (w.note) texts.push(`[Wastage ${w.stage || ''} ${w.batchNumber || ''}] ${String(w.note).replace(/\d+(\.\d+)?/g, '#')}`);
  if (!texts.length) return { synthesis: 'No remarks found in the current window.' };
  const out = await anthropic(
    [{ role: 'user', content: `Question: ${scopeQuestion}\n\nOperator remarks (quantities masked as #):\n${texts.slice(0, 250).join('\n')}` }],
    'You synthesize pharma plant floor remarks. Identify recurring themes, name machines/shifts where patterns repeat, stay strictly within the remarks given. 4-8 sentences, plain prose.', 800);
  await audit(user, 'remarks-synthesis', scopeQuestion, `${texts.length} remark(s) sent (quantities masked)`);
  return { synthesis: out, remarksUsed: texts.length };
}

async function audit(user, action, question, detail) {
  try {
    await fencedQuery(`INSERT INTO assistant_audit (username, action, question, detail, created_at) VALUES ($1,$2,$3,$4,$5)`,
      [user || 'admin', action, String(question || '').slice(0, 2000), String(detail || '').slice(0, 4000), new Date().toISOString()]);
  } catch (e) { log('[assistant] audit failed:', e.message); }
}

// ─── init + exports ────────────────────────────────────────────────────────────────────────────
function init(deps) {
  pgPool = deps.pgPool; db = deps.db; PORT = deps.port || PORT; log = deps.log || log;
  startScheduler();
}

module.exports = {
  init, runDigest, ask, synthesizeRemarks, audit, fencedQuery,
  registry: () => REGISTRY.map(m => ({ id: m.id, desc: m.desc, dims: m.dims })),
  resolveOne: async (id, params, user) => withLock('query', async () => {
    const reg = REGISTRY.find(r => r.id === id);
    if (!reg) throw new Error('unknown metric: ' + id);
    const snap = await buildSnapshot();
    const results = await resolvePlan({ metrics: [{ id, params }] }, snap);
    await audit(user, 'structured-query', id, JSON.stringify(params || {}));
    return { results, asOf: snap.builtAtIST };
  }),
};
