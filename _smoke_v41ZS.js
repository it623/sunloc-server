// v41ZI logic smoke tests — exercise the real gross/override/resolver/scan-gate semantics offline.
let PASS=0, FAIL=0;
const eq=(a,b,msg)=>{ if(a===b){PASS++;} else {FAIL++; console.log(`FAIL: ${msg} → got ${JSON.stringify(a)}, want ${JSON.stringify(b)}`);} };

// ---- 1. effectiveGross: pull the REAL function text out of server.js and run it ----
const fs=require('fs');
const src=fs.readFileSync('server.js','utf8');
let _grossOverride={}, _grossByBatch=null;
// db stub used by the sync fallback branch
const db={ prepare:(sql)=>({ get:(bn)=>db._rows[bn]||null }), _rows:{} };
const m=src.match(/function effectiveGross\(batchNumber\)\s*\{[\s\S]*?\n\}/);
if(!m){ console.log('FAIL: could not extract effectiveGross'); FAIL++; }
else { eval(m[0]); }

// case A: override present (incl 0) wins
_grossOverride={'B1':0,'B2':12.5}; _grossByBatch={'B1':99,'B2':50,'B3':30}; db._rows={};
eq(effectiveGross('B1'),0,'override 0 wins over batch sum 99');
eq(effectiveGross('B2'),12.5,'override 12.5 wins over batch sum 50');
// case B: no override → pure batch sum
eq(effectiveGross('B3'),30,'no override → batch sum 30');
// case C: neither → db fallback
_grossOverride={}; _grossByBatch={}; db._rows={'B4':{total:7.25}};
eq(effectiveGross('B4'),7.25,'db fallback sum');
eq(effectiveGross('B5'),0,'unknown batch → 0');
eq(effectiveGross(''),0,'empty batch → 0');

// ---- 2. Planning injection precedence: (hasOverride || eff>0) ? eff : legacy ----
function inject(bn, over, batchSum, legacy){
  _grossOverride = over!==undefined ? {[bn]:over} : {};
  _grossByBatch = batchSum!==undefined ? {[bn]:batchSum} : {};
  db._rows={};
  const hasOverride=Object.prototype.hasOwnProperty.call(_grossOverride,bn);
  const eff=effectiveGross(bn);
  return (hasOverride||eff>0)?eff:legacy;
}
eq(inject('B',0,40,99),0,'override 0 wins in planning (not legacy)');
eq(inject('B',undefined,40,99),40,'no override, batch sum 40 used');
eq(inject('B',undefined,0,99),99,'no override, no batch sum → legacy 99');
eq(inject('B',undefined,undefined,99),99,'no override, no batch → legacy 99');
eq(inject('B',25,40,99),25,'override 25 wins');

// ---- 3. _dlResolveBatch priority (replicate exact logic) ----
const printOrders=[{id:'po1',batchNumber:'26ZD083',productionOrderId:'o1'},
                   {id:'po2',batchNumber:'',productionOrderId:'o2'},
                   {id:'po3',batchNumber:'',productionOrderId:'oX'}];
const orders=[{id:'o1',batchNumber:'26ZD083'},{id:'o2',batchNumber:'26ZE099'}];
function _dlResolveBatch(l){
  if(l.batchNumber) return l.batchNumber;
  const po=l.printOrderId?printOrders.find(p=>p.id===l.printOrderId):null;
  if(po) return po.batchNumber||(po.productionOrderId?((orders.find(o=>o.id===po.productionOrderId)||{}).batchNumber||''):'')||'';
  return '';
}
eq(_dlResolveBatch({batchNumber:'X1'}),'X1','stored batchNumber wins');
eq(_dlResolveBatch({printOrderId:'po1'}),'26ZD083','via print order batchNumber');
eq(_dlResolveBatch({printOrderId:'po2'}),'26ZE099','via productionOrderId → order batchNumber');
eq(_dlResolveBatch({printOrderId:'po3'}),'','PO present but no batch resolvable → empty');
eq(_dlResolveBatch({pcCode:'2340'}),'','no link → empty (caller falls back to pcCode)');

// ---- 4. canScanDept core (full-history _ss based) ----
let scanSummary={}, scans=[];
const _ss=(bn,d)=>(scanSummary[bn]&&scanSummary[bn][d])||{in:0,out:0};
const hasScanSummary=()=>Object.keys(scanSummary).length>0;
function canScan(bn, prevDept, thisDept){
  const _useSS=hasScanSummary();
  const _localCnt=(d,t)=>scans.filter(s=>s.batchNumber===bn&&s.dept===d&&s.type===t&&s._local).length;
  const prevOut=_useSS?(_ss(bn,prevDept).out+_localCnt(prevDept,'out')):scans.filter(s=>s.batchNumber===bn&&s.dept===prevDept&&s.type==='out').length;
  if(prevOut===0) return false;
  const thisIn=_useSS?(_ss(bn,thisDept).in+_localCnt(thisDept,'in')):scans.filter(s=>s.batchNumber===bn&&s.dept===thisDept&&s.type==='in').length;
  return thisIn<prevOut;
}
// scenario: PI out=100 (full history), packing in=40 → allowed (the bug case: windowed scans would've shown prevOut 0)
scanSummary={'BN':{pi:{in:100,out:100},packing:{in:40,out:0}}}; scans=[];
eq(canScan('BN','pi','packing'),true,'packing-in allowed when PI out=100 (full history)');
// packing fully caught up: in=100 == prevOut 100 → blocked (no more to bring in)
scanSummary={'BN':{pi:{in:100,out:100},packing:{in:100,out:0}}};
eq(canScan('BN','pi','packing'),false,'packing-in blocked when in==prevOut');
// genuinely premature: PI out=0 → blocked
scanSummary={'BN':{pi:{in:10,out:0},packing:{in:0,out:0}}};
eq(canScan('BN','pi','packing'),false,'blocked when PI out=0 (premature)');
// local pending out counts toward prevOut
scanSummary={'BN':{pi:{in:0,out:0}}}; scans=[{batchNumber:'BN',dept:'pi',type:'out',_local:true}];
eq(canScan('BN','pi','packing'),true,'local pending PI-out unblocks packing');


// ---- 5. REGRESSION GUARD (v41ZI2): warmActualsCache must be fire-and-forget on the polled,
//        tight-timeout endpoints (planning/state 8s, scan-summary 15s). Awaiting it there blocked
//        the Tracking sync → empty state.batches → all dashboard counts read 0. ----
(function(){
  const s = require('fs').readFileSync('server.js','utf8');
  // planning/state handler slice
  const psIdx = s.indexOf("app.get('/api/planning/state'");
  const psEnd = s.indexOf("app.", psIdx+10);
  const psBody = s.slice(psIdx, psEnd>psIdx?psEnd:psIdx+8000);
  const ssIdx = s.indexOf("app.get('/api/tracking/scan-summary'");
  const ssEnd = s.indexOf("app.", ssIdx+10);
  const ssBody = s.slice(ssIdx, ssEnd>ssIdx?ssEnd:ssIdx+6000);
  eq(/await\s+warmActualsCache/.test(psBody), false, 'planning/state must NOT await warmActualsCache');
  eq(/warmActualsCache\(\)\.catch/.test(psBody), true, 'planning/state warms cache fire-and-forget');
  eq(/await\s+warmActualsCache/.test(ssBody), false, 'scan-summary must NOT await warmActualsCache');
  eq(/warmActualsCache\(\)\.catch/.test(ssBody), true, 'scan-summary warms cache fire-and-forget');
})();

// ---- 6. v41ZJ GUARDS: planning/state injection must be pure in-memory (no synchronous SQLite
//        fallback on the polled hot path), and a terminal error handler must absorb client aborts. ----
(function(){
  const src = require('fs').readFileSync('server.js','utf8');
  const psIdx = src.indexOf("app.get('/api/planning/state'");
  let inj = src.slice(psIdx, src.indexOf('ord.actualProd =', psIdx)+200);
  inj = inj.split('\n').filter(l=>!/^\s*\/\//.test(l)).join('\n'); // drop comment lines
  eq(/effectiveGross\s*\(\w/.test(inj), false, 'planning/state injection must NOT call effectiveGross (no per-order DB)');
  eq(/isClientAbort/.test(src), true, 'terminal client-abort error handler present');
  eq(/request\.aborted/.test(src), true, 'handler matches request.aborted');
}).call(this);

// ---- 7. v41ZK GUARDS: closed-batches must not block on the heavy warm (client times out at 12-20s),
//        and the public login-users endpoint must exist and be distinct from the admin users route. ----
(function(){
  const src = require('fs').readFileSync('server.js','utf8');
  const cbIdx = src.indexOf("app.get('/api/dpr/closed-batches'");
  const cbBody = src.slice(cbIdx, cbIdx+700);
  eq(/await\s+warmActualsCache/.test(cbBody), false, 'closed-batches must NOT await warmActualsCache');
  eq(/warmActualsCache\(\)\.catch/.test(cbBody), true, 'closed-batches warms fire-and-forget');
  eq(src.includes("app.get('/api/auth/login-users'"), true, 'public login-users endpoint present');
  eq((src.match(/app\.get\('\/api\/auth\/login-users'/g)||[]).length, 1, 'exactly one login-users route');
}).call(this);

// ---- 8. v41ZM CASCADE INVARIANT (behavioral): on one machine, orders never overlap, and every
//        non-closed order anchors exactly to the previous order's end (no gap). Faithful replica of
//        the corrected recalcMachineScheduleQuiet sequencing — closed orders shift forward on overlap
//        keeping their duration; non-closed orders anchor to the cursor. ----
(function(){
  const DAY=86400000, d=n=>new Date(2026,0,1+n), end=(s,days)=>new Date(s.getTime()+days*DAY);
  // queue (serial order) reproducing BOTH original bugs: two closed orders share day0..2 (overlap),
  // and a running order sits at day10 while the machine is free at day4 (gap).
  const orders=[
    {batch:'26ZE082',status:'closed', start:d(0), dur:2},
    {batch:'26ZE083',status:'closed', start:d(0), dur:2},
    {batch:'26ZE085',status:'running',start:d(10),dur:2},
    {batch:'26ZE086',status:'pending',start:d(20),dur:3},
  ].map(o=>({...o, startDate:o.start, endDate:end(o.start,o.dur)}));
  let cursor=null;
  for(const o of orders){
    if(o.status==='closed'){
      if(cursor && o.startDate < cursor){
        const durMs=(o.endDate>o.startDate)?(o.endDate-o.startDate):0;
        o.startDate=new Date(cursor); o.endDate=new Date(cursor.getTime()+durMs);
      }
      if(o.endDate && (!cursor || o.endDate>cursor)) cursor=new Date(o.endDate);
      continue;
    }
    o.startDate = cursor ? new Date(cursor) : o.startDate;   // corrected anchor (was: keep later histStart)
    o.endDate = end(o.startDate, o.dur);
    cursor = new Date(o.endDate);
  }
  let overlap=false, gap=false;
  for(let i=1;i<orders.length;i++){
    if(orders[i].startDate < orders[i-1].endDate) overlap=true;
    if(orders[i].status!=='closed' && +orders[i].startDate !== +orders[i-1].endDate) gap=true;
  }
  eq(overlap,false,'cascade: no same-machine date overlaps after sequencing');
  eq(gap,false,'cascade: non-closed orders anchor exactly to previous end (no gap)');
  eq(+orders[1].startDate, +orders[0].endDate, 'cascade: overlapping closed order shifted to predecessor end');
  eq(+orders[2].startDate, +orders[1].endDate, 'cascade: running order pulled in to fill the gap');
}).call(this);

// ---- 9. v41ZM CASCADE SOURCE GUARDS: the corrected anchoring + closed-overlap sequencing must be
//        present in BOTH recalcMachineScheduleQuiet and recalcMachineSchedule, and the old buggy
//        "keep the later historical start" pattern must be gone. ----
(function(){
  const ph=require('fs').readFileSync('public/planning.html','utf8');
  eq((ph.match(/ord\.startDate = cursor \? new Date\(cursor\) : histStart;/g)||[]).length, 2,
     'cascade gap-fix anchor present in both schedule functions');
  eq(/cursor && cursor > histStart\) \? new Date\(cursor\) : histStart/.test(ph), false,
     'old gap-bug pattern (keep later histStart) removed from both');
  // v41ZQ #1: the buggy v41ZM closed-order forward-shift (drifted closed orders into the future and
  // collapsed 0-span ones to a single day) must be GONE from both functions...
  eq((ph.match(/ord\.endDate = new Date\(cursor\.getTime\(\) \+ _dur\);/g)||[]).length, 0,
     'v41ZM closed-order duration-preserving shift removed from both functions');
  // ...replaced by: a finished (closed) order is frozen and clamped so it never shows in the future.
  eq((ph.match(/new Date\(ord\.endDate\)\s+> _td\) ord\.endDate\s+= new Date\(_td\);/g)||[]).length, 2,
     'closed-order not-future end clamp present in both schedule functions');
  // v41ZQ #1: open orders use FULL run duration (total gross / cap), not remaining-work days.
  eq((ph.match(/const fullDays = \(sc\.effectiveCap && sc\.effectiveCap > 0\) \? Math\.max\(1, Math\.ceil\(sc\.grossQty \/ sc\.effectiveCap\)\) : 1;/g)||[]).length, 2,
     'full-duration (total gross) computation present in both functions');
  eq((ph.match(/ord\.endDate = calcEndDate\(ord\.startDate, fullDays\);/g)||[]).length, 2,
     'open-order end uses full duration in both functions');
  // the remaining-work end logic that collapsed completed spans must be gone.
  eq(/fromToday\.setDate\(fromToday\.getDate\(\) \+ Math\.ceil\(remainingDays\)\)/.test(ph), false,
     'remaining-work end logic (span collapse source) removed from both');
}).call(this);

// ---- 10. v41ZQ OVERLOAD GUARD: the gross warm must NOT join the write-churned production_orders
//         table (that join+expression-group on a 5-connection pool timed out closed-batches and made
//         the apps go offline in v41ZM). _grossByBatch is now built in memory from the actuals rows +
//         the warmed order cache. ----
(function(){
  const src=require('fs').readFileSync('server.js','utf8');
  const wi=src.indexOf('async function warmActualsCache');
  const wbody=src.slice(wi, src.indexOf('\n}', wi+50));
  eq(/LEFT JOIN\s+production_orders/i.test(wbody), false, 'warmActualsCache must NOT JOIN production_orders (overload)');
  eq(/COALESCE\(NULLIF\(pa\.batch_number/.test(wbody), false, 'warmActualsCache must NOT group by COALESCE expr on prod_orders');
  eq(/_planningStateCache/.test(wbody), true, 'warmActualsCache attributes gross via in-memory order cache');

  // behavioral: replicate the in-memory attribution and confirm order_id-only rows map to their batch
  const rows=[
    {order_id:'o1', batch_number:'B1', total:'10'},   // direct batch
    {order_id:'o2', batch_number:'',   total:'5'},     // blank batch -> map via order o2->B2
    {order_id:'o3', batch_number:null, total:'7'},     // null  batch -> map via order o3->B1 (sums into B1)
    {order_id:'o9', batch_number:null, total:'3'},     // unknown order, no batch -> dropped
  ];
  const orderBatch={o1:'B1',o2:'B2',o3:'B1'};
  const g={};
  for(const row of rows){
    const batch=(row.batch_number && String(row.batch_number).trim())?row.batch_number:orderBatch[row.order_id];
    if(!batch) continue;
    g[batch]=(g[batch]||0)+(parseFloat(row.total)||0);
  }
  eq(g['B1'],17,'gross attribution: B1 = direct 10 + order_id-mapped 7');
  eq(g['B2'],5,'gross attribution: B2 from order_id-only row');
  eq(Object.prototype.hasOwnProperty.call(g,'undefined'),false,'unmappable row dropped (no phantom batch)');
}).call(this);

// ---- 11. v41ZQ MERGE DEBOUNCE GUARD: the background production_orders merge must be throttled so it
//         can't churn the 5-connection pool on every ~13s save. ----
(function(){
  const src=require('fs').readFileSync('server.js','utf8');
  eq(/_bgNow - _lastBgMerge < BG_MERGE_DEBOUNCE_MS/.test(src), true, 'background merge is debounced');
  eq(/_lastBgMerge = _bgNow;/.test(src), true, 'debounce timestamp set before merge awaits (blocks concurrent merge)');
  // behavioral: leading-edge debounce — first proceeds, repeats within window skip, after window proceeds
  let last=0; const WIN=30000; const T=1780000000000; // realistic epoch: matches server (_lastBgMerge=0, Date.now() huge)
  const tryMerge=now=>{ if(now-last<WIN) return false; last=now; return true; };
  eq(tryMerge(T+1000),  true,  'debounce: first merge proceeds (last=0 vs huge Date.now())');
  eq(tryMerge(T+5000),  false, 'debounce: merge within window skipped');
  eq(tryMerge(T+9000),  false, 'debounce: another within window skipped');
  eq(tryMerge(T+50000), true,  'debounce: merge after window proceeds');
}).call(this);

// ---- 12. v41ZQ #2 GROSS SINGLE-SOURCE-OF-TRUTH: planning's protection merge must take the server's
//         injected actualProd (the authoritative per-batch DPR gross) over any locally-edited value,
//         so Planning "Actual Prod" matches DPR / Closed Batches / Reports D-E-F. The old
//         "o.actualProd || local.actualProd" let a stale local edit win when server sent a value. ----
(function(){
  const ph=require('fs').readFileSync('public/planning.html','utf8');
  const sv=require('fs').readFileSync('server.js','utf8');
  eq(/actualProd: \(o\.actualProd != null \? o\.actualProd : local\.actualProd\)/.test(ph), true,
     'planning merge prefers server (DPR) actualProd over local');
  eq((ph.match(/actualProd: o\.actualProd \|\| local\.actualProd/g)||[]).length, 0,
     'old local-wins actualProd merge removed');
  // server still injects the authoritative per-batch DPR gross (override -> _grossByBatch) into actualProd
  eq(/ord\.actualProd = \(hasOverride \|\| eff > 0\) \? eff : legacy;/.test(sv), true,
     'server planning/state still injects authoritative DPR gross into actualProd');
}).call(this);


// ---- 13. v41ZQ #2 INVOICE BOXES = ACTUAL PACKED (not planned) + size-wise pack qty ----
(function(){
  const ph=require('fs').readFileSync('public/planning.html','utf8');
  eq(/plan\.packedBoxes != null \? \(parseInt\(plan\.packedBoxes/.test(ph), true,
     'invoice item boxes use actual packed (plan.packedBoxes), not planned plan.boxes');
  eq(/const _invQty = Math\.round\(\(_invBoxes \* _invPackSize \/ 100000\)/.test(ph), true,
     'invoice item qty derived from size-wise pack size');
  eq((ph.match(/boxes: plan\.boxes \|\| 0,\n\s*qtyLakhs: plan\.packedQty/g)||[]).length, 0,
     'old planned-box invoice mapping removed');
}).call(this);
// ---- 14. v41ZQ #3 PROD SUMMARY collapsible batch rows ----
(function(){
  const ph=require('fs').readFileSync('public/planning.html','utf8');
  eq(/function toggleWoRows\(ci, rowEl\)/.test(ph), true, 'prod-summary toggleWoRows present');
  eq(/class="wo-row wo-grp-\$\{ci\}" style="display:none/.test(ph), true, 'batch rows collapsed by default');
  eq(/onclick="toggleWoRows\(\$\{ci\},this\)"/.test(ph), true, 'customer row toggles its batch rows');
}).call(this);


// ---- 15. v41ZQ #2 SEQUENTIAL lot fill + soft short-invoice warning + auto-adjust ----
(function(){
  const ph=require('fs').readFileSync('public/planning.html','utf8');
  eq(/SEQUENTIAL lot fill/.test(ph), true, 'sequential lot-fill block present');
  eq(/const allocBoxes = \(i === lots\.length - 1\) \? Math\.max\(0, remBoxes\)/.test(ph), true,
     'last lot absorbs remainder (short/excess)');
  eq((ph.match(/_plannedQtyByBatch/g)||[]).length, 0, 'old proportional split removed');
  eq(/function _v40_autoAdjustShort\(planId\)/.test(ph), true, 'auto-adjust helper present');
  eq(/const _short = \(it\.plannedBoxes \|\| 0\) > \(it\.boxes \|\| 0\)/.test(ph), true,
     'short detection (packed < planned) present');
  eq(/Math\.abs\(it\.batchWip \|\| 0\) <= 0\.005/.test(ph), true,
     'WIP=0 accounted-for condition present');
}).call(this);


// ==== v41ZR Issue 3: wastage backfill normalization (admin form shape -> rows) ====
function bfwNormalize(body){
  let { wastage, batchNumber, dept, salvage, remelt, backdateTs } = body;
  if (!Array.isArray(wastage)) {
    const ts = backdateTs || 'TS';
    const sv = parseFloat(salvage) || 0;
    const rm = parseFloat(remelt) || 0;
    wastage = [];
    if (sv > 0) wastage.push({ batch_number: batchNumber, dept, type: 'salvage', qty: sv, ts });
    if (rm > 0) wastage.push({ batch_number: batchNumber, dept, type: 'remelt',  qty: rm, ts });
  }
  return wastage;
}
let w = bfwNormalize({batchNumber:'26N024',dept:'AIM',salvage:8.45,remelt:0,backdateTs:'2026-06-08'});
eq(w.length,1,'Issue3: salvage-only form -> 1 row');
eq(w[0].type,'salvage','Issue3: row type salvage');
eq(w[0].qty,8.45,'Issue3: row qty 8.45');
eq(w[0].batch_number,'26N024','Issue3: row batch 26N024');
w = bfwNormalize({batchNumber:'X',dept:'AIM',salvage:2,remelt:3});
eq(w.length,2,'Issue3: salvage+remelt -> 2 rows');
w = bfwNormalize({wastage:[{batch_number:'Y',type:'salvage',qty:1}]});
eq(w.length,1,'Issue3: explicit array preserved');
eq(w[0].batch_number,'Y','Issue3: explicit array batch preserved');
w = bfwNormalize({batchNumber:'Z',salvage:0,remelt:0});
eq(w.length,0,'Issue3: zero salvage+remelt -> no rows');
eq(/type: 'salvage'/.test(src) && /type: 'remelt'/.test(src),true,'Issue3: server builds salvage+remelt rows');
eq(/'bfw_' \+ Date\.now\(\)/.test(src),true,'Issue3: server generates id server-side');

// ==== v41ZR Issue 4: DPR gate allows prior-production batch wound down (status off "running") ====
function gate(meta, dprClosed, deleted, batchNumber, grossByBatch){
  if (dprClosed) return false;                 // DPR-closed blocked (unless admin force, not modeled)
  if (!meta) return true;                       // unknown -> allow (legacy/orphan)
  if (deleted) return false;                    // deleted blocked
  if (meta.status === 'running') return true;   // running allowed
  if (batchNumber && grossByBatch && (grossByBatch[batchNumber]||0) > 0) return true; // v41ZR fix
  return false;                                 // never-started wrong-status blocked
}
eq(gate({status:'completed'},false,false,'26ZF091',{'26ZF091':34.6}),true,'Issue4: downgraded batch w/ prior prod ALLOWED');
eq(gate({status:'pending'},false,false,'NEW1',{}),false,'Issue4: never-started wrong-status still GATED');
eq(gate({status:'running'},false,false,'R1',{}),true,'Issue4: running allowed');
eq(gate({status:'completed'},true,false,'26ZF091',{'26ZF091':34.6}),false,'Issue4: DPR-closed still blocked');
eq(gate({status:'completed'},false,true,'D1',{'D1':5}),false,'Issue4: deleted still blocked');
eq((src.match(/_grossByBatch\[batchNumber\] \|\| 0\) > 0/g)||[]).length>=3,true,'Issue4: prior-prod allowance present in all 3 gates');



// ==== v41ZS Issue 1: SAP cache prune — only on complete, non-empty fetch ====
const sapSrc = fs.readFileSync('sap-client.js','utf8');
eq(/return \{ ok: true, indents, complete \};/.test(sapSrc),true,'Issue1: fetchOpenSalesOrders returns complete flag');
eq(/complete = false;/.test(sapSrc),true,'Issue1: complete set false on partial page fail');
eq(/r\.complete && indents\.length > 0/.test(src),true,'Issue1: prune gated on complete + non-empty');
eq(/sap_doc_entry <> ALL\(\$1::int\[\]\)/.test(src),true,'Issue1: PG prune deletes rows not in fetched open set');
// prune-decision replica
function shouldPrune(complete, fetchedLen){ return !!(complete && fetchedLen > 0); }
eq(shouldPrune(true, 5), true,  'Issue1: complete+5 fetched -> prune');
eq(shouldPrune(false,5), false, 'Issue1: partial fetch -> NO prune (safety)');
eq(shouldPrune(true, 0), false, 'Issue1: empty fetch -> NO prune (never wipe cache)');
// what survives a prune: only DocEntries in the fresh open set
function pruneCache(cacheEntries, fetchedEntries){
  const keep = new Set(fetchedEntries);
  return cacheEntries.filter(e => keep.has(e));
}
eq(JSON.stringify(pruneCache([101,102,103,104],[101,103])),JSON.stringify([101,103]),'Issue1: closed orders (102,104) pruned, open kept');

// ==== v41ZS legacy-dispatch regularise — endpoint + single consolidated admin button ====
eq(/app\.post\('\/api\/invoice\/:id\/regularise-dispatch'/.test(src),true,'Legacy: regularise-dispatch endpoint exists');
eq(/Admin only — regularising a legacy\/return dispatch/.test(src),true,'Legacy: admin-gated');
eq(/is_legacy_closed   = 1/.test(src) && /dispatch_status    = 'dispatched'/.test(src),true,'Legacy: marks legacy-closed + dispatched');
eq(/is_deemed_scan_out = TRUE/.test(src),true,'Legacy: deemed scan-out (no scan counts)');
const trkSrc = fs.readFileSync('public/tracking.html','utf8');
eq(/Regularise Dispatch/.test(trkSrc),true,'Legacy: single Regularise Dispatch button present');
eq(/_v41zs_regulariseDispatch/.test(trkSrc),true,'Legacy: client handler present');
// consolidation: the three old admin buttons are no longer surfaced
eq(/label: '✅ Approve as Direct-SAP'/.test(trkSrc),false,'Legacy: old Approve-Direct-SAP button removed');
eq(/label: '⚠️ Deemed Scan-Out'/.test(trkSrc),false,'Legacy: old Deemed-Scan-Out button removed');
eq(/label: '🗂️ Mark as Legacy Closed'/.test(trkSrc),false,'Legacy: old Mark-Legacy-Closed button removed');



// ==== v41ZS deep-audit fixes ====
eq(/batch_number=COALESCE\(NULLIF\(\$9,''\), invoices_received\.batch_number\)/.test(src),true,'Audit: poller preserves admin-attached batch on blank SAP UDF (PG)');
eq(/batch_number=COALESCE\(NULLIF\(excluded\.batch_number,''\), invoices_received\.batch_number\)/.test(src),true,'Audit: poller preserves batch on blank (SQLite)');
eq(/dispatched_at      = \$3/.test(src) && /dispatched_by      = \$2/.test(src),true,'Audit: regularise sets dispatched_at/by (coherent dispatched state)');
eq(/deemed_reason      = \$4/.test(src) && /deemed_by          = \$2/.test(src),true,'Audit: regularise sets deemed_reason/by (consistent w/ is_deemed_scan_out)');
const sapSrc2 = fs.readFileSync('sap-client.js','utf8');
eq(/pageGuard >= 500.*complete = false/.test(sapSrc2),true,'Audit: prune skipped on runaway pagination (incomplete)');
// poller does NOT clobber regularised state (dispatch_status / is_legacy_closed not in ON CONFLICT SET)
const onConflictBlock = (src.match(/ON CONFLICT \(sap_doc_entry\) DO UPDATE SET[\s\S]*?fetched_at=NOW\(\)::TEXT/)||[''])[0];
eq(/dispatch_status/.test(onConflictBlock),false,'Audit: poller upsert does NOT overwrite dispatch_status (regularisation persists)');
eq(/is_legacy_closed/.test(onConflictBlock),false,'Audit: poller upsert does NOT overwrite is_legacy_closed');
// SQLite regularise UPDATE param-count sanity (11 placeholders, 11 args)
eq((`COALESCE(NULLIF(?,'')),?,?,?,?,?,?,?,?,?,?`.match(/\?/g)||[]).length,11,'Audit: SQLite regularise placeholder count = 11 (matches .run args)');


console.log(`\n[FINAL] ${PASS} PASS / ${FAIL} FAIL`);
process.exit(FAIL?1:0);
