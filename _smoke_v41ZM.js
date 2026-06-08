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
  eq((ph.match(/shift it forward by the overlap/g)||[]).length, 2,
     'closed-order overlap sequencing present in both schedule functions');
  eq((ph.match(/ord\.endDate = new Date\(cursor\.getTime\(\) \+ _dur\);/g)||[]).length, 2,
     'closed-order duration-preserving shift present in both functions');
}).call(this);

console.log(`\n[FINAL] ${PASS} PASS / ${FAIL} FAIL`);
process.exit(FAIL?1:0);
