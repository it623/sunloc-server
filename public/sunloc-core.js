// ═══════════════════════════════════════════════════════════════════════════════════════════════
// sunloc-core.js — the FROZEN Sunloc compute core (v52)
//
// These 39 functions are extracted VERBATIM from tracking.html and MUST remain byte-identical to
// their originals there. The build validator extracts both copies and fails the build on any
// divergence — a formula change must land in tracking.html and be re-extracted here in the same
// build (Ishan's formula-uniformity rule, mechanically enforced).
//
// The core is environment-pure: no DOM, no localStorage, no fetch. Its entire interface is the
// global `state` object (batches, scans, labels, wastage, dispatchRecs, stageClosure,
// grossOverrides, scanSummary, grossSummary, wastageSummary) and four `window` fields
// (_printSalvagePct, _reconOverrides, _retiredSet, _trkProdMonthFilter).
//
// In the BROWSER (assistant.html): define `state` and `window._*` before calling.
// In NODE (assistant-engine.js): set global.state and global.window before calling; the digest
// job runs single-flight so the globals are never contended.
// DO NOT EDIT THE FUNCTION BODIES IN THIS FILE. Edit tracking.html and re-extract.
// ═══════════════════════════════════════════════════════════════════════════════════════════════

function today(){ return _istToday() }

function now(){ return new Date().toISOString() }

function _istDayKey(ts){
  const d=new Date(ts);
  if(isNaN(d.getTime())) return String(ts||'').slice(0,10);
  const s=new Date(d.getTime()-30*60*1000);
  return `${s.getUTCFullYear()}-${String(s.getUTCMonth()+1).padStart(2,'0')}-${String(s.getUTCDate()).padStart(2,'0')}`;
}

function _istToday(){ return _istDayKey(new Date().toISOString()); }

function _trkProdStartMonth(b){
  const start = b.startDate || b.orderStartDate;
  if (!start) return '';
  const d = new Date(start);
  if (isNaN(d.getTime())) return '';
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
}

function getProdMonthBatches(excludeRetired){
  // v50G: fall back to the HEADER month, never to 'ALL' — an unset filter used to silently widen
  // every report to all months, which is the defect this build removes.
  const pm = window._trkProdMonthFilter || _trkSelectedMonth || 'ALL';
  return state.batches.filter(b => {
    if (b.deleted) return false;
    if (excludeRetired && _isRetired(b.batchNumber)) return false;
    if (pm === 'ALL') return true;
    // v50B: same cohort rule as getTrkMonthBatches — start month, else planning month, else first scan.
    return _v50bCohortMonth(b) === pm;
  });
}

function _v50cLastProdMonth(b) {
  const last = b.dprLastDate || b.endDate || b.orderEndDate || b.dprFirstDate || b.startDate || b.orderStartDate;
  if (!last) return '';
  const d = new Date(last);
  return isNaN(d.getTime()) ? '' : `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
}

function _v50bCohortMonth(b) {
  const m = _v50bCohortMonthRaw(b);
  if (m && m < _V50C_COHORT_START) {
    const lastM = _v50cLastProdMonth(b);
    if (lastM && lastM >= _V50C_COHORT_START) return _V50C_COHORT_START;   // absorbed into July
  }
  return m;
}

function _v50bCohortMonthRaw(b) {
  if (!b) return '';
  const start = b.startDate || b.orderStartDate;
  if (start) { const d = new Date(start); if (!isNaN(d.getTime())) return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`; }
  // No production dates → planning month (planMonth/month on the order blob), then first scan.
  const pm = b.planMonth || b.month || b.planningMonth;
  if (pm && /^\d{4}-\d{2}$/.test(String(pm))) return String(pm);
  if (pm) { const d = new Date(pm); if (!isNaN(d.getTime())) return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`; }
  return _v50bFirstScanMonth(b.batchNumber);
}

function _v50bFirstScanMonth(batchNo) {
  if (!batchNo) return '';
  let earliest = null;
  (state.scans||[]).forEach(sc => { if (sc.batchNumber===batchNo && sc.ts && (!earliest || sc.ts < earliest)) earliest = sc.ts; });
  const fs = (state.scanSummary && state.scanSummary[batchNo] && state.scanSummary[batchNo].firstScan) || null;
  if (fs && (!earliest || fs < earliest)) earliest = fs;
  if (!earliest) return '';
  const d = new Date(earliest);
  return isNaN(d.getTime()) ? '' : `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
}

function hoursAgo(ts){ return (Date.now()-new Date(ts).getTime())/3600000 }

function boxToLakh(boxes, size){ const ps=PACK_SIZES[String(size)]; return ps ? boxes*ps : boxes; }

function getBatch(batchNo){ return state.batches.find(b=>b.batchNumber===batchNo||b.id===batchNo)||null }

function _ss(batchNo, dept) {
  // Returns { in, out, inQty, outQty } from scanSummary (full history, no limit)
  const s = state.scanSummary?.[batchNo]?.[dept];
  return s || { in:0, out:0, inQty:0, outQty:0 };
}

function _sw(batchNo, dept) {
  // Returns { salvage, remelt } from wastageSummary (full history)
  const w = state.wastageSummary?.[batchNo]?.[dept];
  return w || { salvage:0, remelt:0 };
}

function hasScanSummary() {
  return state.scanSummary && Object.keys(state.scanSummary).length > 0;
}

function _boxLakh(sc, fallbackSize){
  let q = parseFloat(sc && sc.qty);
  if (Number.isFinite(q)) return q;                       // scan stores its box's label qty (0 ok = specimen)
  const lab = (sc && sc.labelId) ? (state.labels||[]).find(l=>l.id===sc.labelId) : null;
  if (lab){ const lq = parseFloat(lab.qty); if (Number.isFinite(lq)) return lq; }
  const ps = PACK_SIZES[String((lab&&lab.size)||(sc&&sc.size)||fallbackSize||'0')];
  return ps || 0;
}

function _scanLakhs(batchNo, dept, type, size){
  if (hasScanSummary()){
    const s = _ss(batchNo, dept);
    const serverQ = (type==='in' ? s.inQty : s.outQty) || 0;
    const localQ = (state.scans||[])
      .filter(sc=>sc.batchNumber===batchNo && sc.dept===dept && sc.type===type && sc._local)
      .reduce((a,sc)=>a+_boxLakh(sc,size),0);
    return serverQ + localQ;
  }
  // summary not loaded yet — sum the (windowed) local scan set directly, valued per box
  return (state.scans||[])
    .filter(sc=>sc.batchNumber===batchNo && sc.dept===dept && sc.type===type)
    .reduce((a,sc)=>a+_boxLakh(sc,size),0);
}

function _grossFor(batchNo, b) {
  return _v50fGross(batchNo, b);
}

function _v40_dispatchedLakhs(batchNumber, batchSize) {
  const localOut = state.scans.filter(s => s.batchNumber === batchNumber && s.dept === 'dispatch' && s.type === 'out' && s._local).length;
  const legacyBoxes = hasScanSummary()
    ? (_ss(batchNumber, 'dispatch').out + localOut)
    : state.scans.filter(s => s.batchNumber === batchNumber && s.dept === 'dispatch' && s.type === 'out').length;
  const legacyLakhs = _scanLakhs(batchNumber,'dispatch','out',batchSize);  // v47G: per-box label sum
  const phase18Lakhs = state.dispatchRecs.filter(r => r.batchNumber === batchNumber).reduce((s, r) => s + (r.qty || 0), 0);
  return legacyLakhs + phase18Lakhs;
}

function getLabelsByBatch(batchNo){ return state.labels.filter(l=>l.batchNumber===batchNo&&!l.voided) }

function getWastageForStage(batchNo, dept){
  return state.wastage.filter(w=>w.batchNumber===batchNo&&w.dept===dept);
}

function getTotalWastage(batchNo, dept){
  // Use wastageSummary (full history, no limit) when available
  if (hasScanSummary() && state.wastageSummary) {
    const w = _sw(batchNo, dept);
    // Add any local pending wastage entries
    const localEntries = state.wastage.filter(e=>e.batchNumber===batchNo&&e.dept===dept&&e._local);
    const localSalvage = localEntries.filter(e=>e.type==='salvage').reduce((s,e)=>s+e.qty,0);
    const localRemelt  = localEntries.filter(e=>e.type==='remelt').reduce((s,e)=>s+e.qty,0);
    const salvage = w.salvage + localSalvage;
    const remelt  = w.remelt  + localRemelt;
    return { salvage, remelt, total: salvage + remelt };
  }
  // Fallback to state.wastage (full — wastage endpoint has no LIMIT)
  const entries = getWastageForStage(batchNo, dept);
  const salvage = entries.filter(e=>e.type==='salvage').reduce((s,e)=>s+e.qty,0);
  const remelt  = entries.filter(e=>e.type==='remelt').reduce((s,e)=>s+e.qty,0);
  return {salvage, remelt, total:salvage+remelt};
}

function isStageComplete(batchNo, dept){
  return state.stageClosure.some(s=>s.batchNumber===batchNo&&s.dept===dept&&s.closed);
}

function _isRetired(bn){ return !!(bn && window._retiredSet.has(String(bn).toUpperCase())); }

function _reconOverride(bn){ return bn ? (window._reconOverrides[String(bn).toUpperCase()]||null) : null; }

function _reconWip(ov, bn){
  if(!ov) return 0;
  // v49Y: the remainder derives from the EFFECTIVE gross — DPR closed-batch correction (top authority)
  // when present, else the reconcile record's gross. A correction that raises gross raises the unpacked
  // remainder (and vice versa), so WIP collateral follows the correction across every report.
  const _g = (bn!=null && _dprOvrGross(bn)!=null) ? _dprOvrGross(bn) : ov.gross;
  if(_g!=null && ov.packing!=null) return Math.max(0, (_g||0) - (ov.packing||0) - (ov.wastage||0));
  return ov.wip||0;
}

function _dprOvrGross(bn){
  const o = state.grossOverrides && state.grossOverrides[bn];
  if(!o) return null;
  const g = +o.gross;
  return (isFinite(g) && g >= 0) ? g : null;
}

function _v47yReconStage(batchNo){
  const b = getBatch(batchNo);
  if (b && !b.isPrinted) return 'aim';
  if (_ss(batchNo,'pi').in > 0 || _ss(batchNo,'pi').out > 0) return 'pi';
  if (_ss(batchNo,'printing').in > 0 || _ss(batchNo,'printing').out > 0) return 'printing';
  return 'aim';
}

function _v47ySplit(batchNo, ov){
  const b = getBatch(batchNo) || {};
  const sz = b.size;
  const g  = (_dprOvrGross(batchNo)!=null) ? _dprOvrGross(batchNo) : (ov.gross||0);
  const ws = ov.wastage||0;
  let rem = _isRetired(batchNo) ? 0 : _reconWip(ov, batchNo);
  const L = (d,t)=>_scanLakhs(batchNo,d,t,sz);
  const aIn=L('aim','in'), aOut=L('aim','out'), pIn=L('printing','in'), pOut=L('printing','out'),
        iIn=L('pi','in'), iOut=L('pi','out'), kIn=L('packing','in');
  const take = c => { const t=Math.min(rem, Math.max(0,c)); rem-=t; return t; };
  const packT = take(iOut - kIn);                                  // pi→packing transit (live gap leg)
  const pi    = take(iIn - iOut);                                  // held at PI (dept leg)
  const piT   = take(pOut - iIn);                                  // printing→pi transit (live gap leg)
  const printing = take(pIn - pOut);                               // held at Printing (dept leg)
  const aimT  = take(aOut - (b.isPrinted ? pIn : kIn));            // aim→next transit (live gap leg)
  // Reconciled wastage carries no recorded location — net it where it can physically sit: first
  // inside the AIM dept slice (post-scan-in salvage/remelt lives within in − out), any residue
  // against the unscanned leg (pre-box salvage). Nets exactly once; conservation holds either way.
  const aimHeldRaw = Math.max(0, aIn - aOut);
  const wsAtAim = Math.min(ws, aimHeldRaw);
  const aim   = take(aimHeldRaw - wsAtAim);                        // held in AIM dept (dept leg)
  const prod  = take(g - aIn - (ws - wsAtAim));                    // unscanned at AIM (production leg)
  return { prod, aim, printing, pi, aimT, piT, packT,
           total: prod + aim + printing + pi + aimT + piT + packT };
}

function _v50fGross(batchNo, b){
  const _dg = _dprOvrGross(batchNo);
  if(_dg!=null) return _dg;                                   // (1) deliberate DPR correction
  if (b && _v50dJulGross && _v50dIsAbsorbed(b) && Object.prototype.hasOwnProperty.call(_v50dJulGross, batchNo)) {
    return parseFloat(_v50dJulGross[batchNo]) || 0;           // (2) absorbed straddler → July slice
  }
  if (state.grossSummary && Object.prototype.hasOwnProperty.call(state.grossSummary, batchNo)) {
    return state.grossSummary[batchNo] || 0;                  // (3) authoritative apportioned DPR sum
  }
  // v50H (Ishan, 4 Aug): the summary is the complete set of batches with DPR production (a
  // batch_gross_override injects its key too). So once it is LOADED, a batch missing from it has
  // produced nothing and its gross is 0. Falling through to the blob's actualProd showed a stale
  // figure as production long after DPR said 0.00 — 26U136 read 5.00L with no DPR entry at all, and
  // 26ZA078 read 22.85L that had actually been keyed against 26ZA079, double-counting the family.
  // The blob remains the fallback only BEFORE the summary loads, so a cold screen still shows a
  // figure rather than a misleading zero.
  if (state.grossSummary && Object.keys(state.grossSummary).length) return 0;
  return (b && (b.actualProd || b.actualQty)) || 0;           // (4) pre-load fallback only
}

function _reconGross(b){
  if(!b) return 0;
  // The admin reconcile override sits between the DPR correction and the DPR-derived sum: it is an
  // explicit human decision about this batch, but it must never beat a deliberate DPR correction.
  const _dg = _dprOvrGross(b.batchNumber);
  if(_dg!=null) return _dg;
  const o=_reconOverride(b.batchNumber); if(o&&o.gross!=null) return o.gross;
  return _v50fGross(b.batchNumber, b);
}

function getBatchWIPBreakdown(batchNo, opts){
  const batch = getBatch(batchNo);
  if(!batch) return null;
  const _raw = !!(opts && opts.raw);   // v46N: raw=true fetches the pre-override figures (for the hover tooltip)
  if(_isRetired(batchNo) && !_raw){
    // Retired: WIP excluded to 0 (batch declared physically gone). Gross kept (it was produced);
    // A-Grade is computed separately from scan data, so it is unaffected by this exclusion.
    const _g = batch.actualProd || batch.actualQty || 0;
    return { grossProd:_g, aimIn:0, aimOut:0, packIn:0, packOut:0, preAIM:0, aimWIP:0, printWIP:0,
             piWIP:0, toPackTransit:0, packWIP:0, packedNotDisp:0, totalWIP:0,
             wAIM:{salvage:0,remelt:0}, wPrint:{salvage:0,remelt:0}, wPI:{salvage:0,remelt:0}, retired:true };
  }
  // v44F Issue#1: admin reconciliation override — authoritative typed totals replace scan-derived WIP.
  const _ovW = _reconOverride(batchNo);
  if(_ovW && !_raw){
    // v49Y: DPR closed-batch correction (top authority, v47S) beats the reconcile record's gross.
    const g=(_dprOvrGross(batchNo)!=null?_dprOvrGross(batchNo):(_ovW.gross||0)), ws=_ovW.wastage||0;
    // v49M (confirmed by Ishan): the reconciled unpacked remainder self-cures at the STAGE WHERE IT SITS
    // (_v47yReconStage), against that stage's own clearing event — NOT universally against packing.
    // v49L wrongly capped by packing, which would have held an AIM-parked remainder (e.g. 26ZC091) all
    // the way until packing though it clears the moment it is scanned in at AIM. Correct: AIM clears on
    // scan-IN (avail − aimIn); PI/printing clear on scan-OUT (avail − stageOut). This keeps Report D /
    // Batch Tracker / Planning identical to Report B — one model — and clears each remainder at its true
    // stage. Gross / A-Grade / wastage stay authoritative from the override; only this WIP self-cures.
    // v49Z (confirmed by Ishan): stage buckets from the scan-position SPLIT (_v47ySplit) — the
    // customer stage matrix and every breakdown consumer now place the frozen remainder where the
    // boxes physically are (26ZG141: 45.30L Printing + 5.00L PI), instead of a single collapsed
    // total with empty stage cells. Bucket mapping mirrors the matrix footnote — each dept cell
    // already includes its inbound in-transit boxes; pi→packing (and aim→packing for unprinted)
    // transit rides the Packing column. Total = allocated sum = the self-cured remainder.
    const sp = _v47ySplit(batchNo, _ovW);
    const _pkDisp = _scanLakhs(batchNo,'packing','in', batch.size) || (_ovW.packing||0);
    return { grossProd:g, aimIn:0, aimOut:0, packIn:_pkDisp, packOut:_pkDisp, preAIM:sp.prod,
             aimWIP:sp.aim, printWIP:sp.printing + (batch.isPrinted ? sp.aimT : 0),
             piWIP:sp.pi + sp.piT, toPackTransit:sp.packT + (batch.isPrinted ? 0 : sp.aimT),
             packWIP:0, packedNotDisp:0, totalWIP:sp.total,
             wAIM:{salvage:ws,remelt:0}, wPrint:{salvage:0,remelt:0}, wPI:{salvage:0,remelt:0}, reconciled:true };
  }
  const sz = batch.size;
  const useSummary = hasScanSummary();

  // v40 P18.14: UNIFORM DEFINITION — "packed" = packing.in (boxes scanned INTO packing).
  // packing.out scans are NOT used in the workflow (boxes sit at packing dock until trucks
  // pick up via dispatch). The 'packOut' variable below is a legacy misnomer kept for
  // backward compatibility — it intentionally reads packing.in too, so any caller gets the
  // same canonical "packed" count whether they ask for packIn or packOut.
  // This same definition is enforced everywhere: Dashboard, Reports A/B/C/D/E/H/K, Dispatch page, exports.
  let aimInBoxes, aimOutBoxes, printOutBoxes, piOutBoxes, packInBoxes, packOutBoxes;
  if (useSummary) {
    const localExtra = (dept, type) =>
      state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept===dept&&sc.type===type&&sc._local).length;
    aimInBoxes   = _ss(batchNo,'aim').in         + localExtra('aim','in');
    aimOutBoxes  = _ss(batchNo,'aim').out        + localExtra('aim','out');
    printOutBoxes= _ss(batchNo,'printing').out   + localExtra('printing','out');
    piOutBoxes   = _ss(batchNo,'pi').out          + localExtra('pi','out');
    packInBoxes  = _ss(batchNo,'packing').in      + localExtra('packing','in');
    packOutBoxes = _ss(batchNo,'packing').in     + localExtra('packing','out');
  } else {
    aimInBoxes   = state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='aim'&&s.type==='in').length;
    aimOutBoxes  = state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='aim'&&s.type==='out').length;
    printOutBoxes= state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='printing'&&s.type==='out').length;
    piOutBoxes   = state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='pi'&&s.type==='out').length;
    packInBoxes  = state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='packing'&&s.type==='in').length;
    packOutBoxes = state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='packing'&&s.type==='in').length;
  }

  // Convert to Lakhs
  // v47G (confirmed by Ishan): scan-derived legs use the SUMMED per-box label qty (partial-aware),
  // not box-count × uniform pack size. The box counts above stay for the count columns; the frozen
  // WIP / A-Grade formulas below are untouched — only the Lakhs quantity fed into them is corrected.
  const aimIn    = _scanLakhs(batchNo,'aim','in',       sz);
  const aimOut   = _scanLakhs(batchNo,'aim','out',      sz);
  const printOut = _scanLakhs(batchNo,'printing','out', sz);
  const piOut    = _scanLakhs(batchNo,'pi','out',       sz);
  const packIn   = _scanLakhs(batchNo,'packing','in',   sz);
  const packOut  = _scanLakhs(batchNo,'packing','in',   sz);  // canonical "packed" = packing.in (legacy misnomer)

  const wAIM   = getTotalWastage(batchNo,'aim');
  const wPrint = getTotalWastage(batchNo,'printing');
  const wPI    = getTotalWastage(batchNo,'pi');
  const grossProd = batch.actualProd || batch.actualQty || 0;

  // v47I (confirmed by Ishan): printing salvage is logged in the DPL (Planning), never in Tracking, so
  // getTotalWastage('printing') reads 0 and the printing loss was previously stuck in WIP. Feed the
  // SAME figure Report E uses for Printing Rejection — DPL cumulative salvage % × Print-TD (printing
  // scan-out) — so Printing WIP shrinks by exactly the quantum the A-Grade % already removes. Pure DPL
  // (replaces the empty Tracking term; not additive) → WIP and A-Grade use one identical number and
  // cannot diverge. Printing has no remelt (salvage only). Defaults to 0 until the DPL overlay loads.
  const _dplPrintPct   = (window._printSalvagePct && window._printSalvagePct[(batchNo||'').toUpperCase()]) || 0;
  const printSalvageDPL = batch.isPrinted ? printOut * (_dplPrintPct/100) : 0;

  // v46E (confirmed by Ishan): canonical AIM WIP split — salvage removed pre-box (lives in the
  // Unsorted/Unscanned leg), remelt declared post scan-in (lives in the AIM Dept leg). This matches
  // Report E / Report B / Report D exactly — one model across all WIP reports:
  //   Unscanned (pre-AIM) WIP = Gross − AIM Salvage − AIM Scan In        (= Gross − Inspected)
  //   AIM Dept WIP            = AIM Scan In − AIM Remelt − AIM Scan Out   (= A-Grade − Scan Out)
  // Both floored at 0. The sum is unchanged vs the prior split (remelt merely moves from the
  // Unsorted leg into the AIM leg), so downstream (Printing/PI) WIP and totalWIP stay UNCHANGED and
  // the breakdown still sums to totalWIP. Overshoot (Scan In + Salvage > Gross) clamps here and is
  // surfaced by the DPR-vs-Inspected integrity check rather than absorbed silently.
  const preAIM   = Math.max(0, grossProd - wAIM.salvage - aimIn);
  const aimWIP   = Math.max(0, aimIn - wAIM.remelt - aimOut);
  const printWIP = batch.isPrinted ? Math.max(0, aimOut - printOut - printSalvageDPL - wPrint.remelt) : 0;
  const piWIP    = batch.isPrinted ? Math.max(0, printOut - piOut - wPI.salvage - wPI.remelt) : 0;
  // v37E: Transit gap between last production stage and packing receipt
  const lastProdOut   = batch.isPrinted ? piOut : aimOut;
  const toPackTransit = Math.max(0, lastProdOut - packIn);
  const packWIP       = Math.max(0, packIn - packOut); // FG queue (NOT in totalWIP) — legacy (≈0; no pack-out scan)
  // v45ZL item 2b (confirmed by Ishan): TRUE packed-not-dispatched = lifetime pack-in − lifetime
  // dispatched, i.e. Report G's Balance (L). The legacy packWIP above nets pack-in against a
  // non-existent pack-OUT scan (pack-in IS the FG cutoff), so it reads ~0 and never reflected the
  // FG actually sitting packed and awaiting dispatch. Additive display field — no frozen formula
  // (WIP / totalWIP / packWIP) is changed. Same source Report G uses (_v40_dispatchedLakhs).
  const _dispL45zl = (typeof _v40_dispatchedLakhs==='function') ? _v40_dispatchedLakhs(batchNo, sz) : 0;
  const packedNotDisp = Math.max(0, packIn - _dispL45zl);

  // totalWIP formula: Gross Production − All Wastage (salvage+remelt across all stages) − Pack-In Quantity
  // ("Pack-In Quantity" = boxes that have arrived at packing dept = cleared all production stages).
  // FG awaiting dispatch (packIn − packOut) is NOT in WIP — handled by next-vehicle scheduling.
  const totalSalvageWIP = wAIM.salvage + printSalvageDPL + wPI.salvage;
  const totalRemeltWIP  = wAIM.remelt  + wPrint.remelt  + wPI.remelt;
  // v37E WIP-fix: use packIn — material at packing is FG, not WIP
  const totalWIP = Math.max(0, grossProd - totalSalvageWIP - totalRemeltWIP - packIn);

  return {
    grossProd, aimIn, aimOut, packIn, packOut,
    preAIM, aimWIP, printWIP, piWIP, toPackTransit, packWIP, packedNotDisp, totalWIP,
    wAIM, wPrint, wPI
  };
}

function getBatchAGrade(batchNo, stage, opts){
  // A-Grade formula: OUT / (OUT + Salvage + Remelt) * 100
  // Uses scanSummary (full history, no LIMIT) when available
  // Falls back to state.scans (recent only) if scanSummary not yet loaded
  const batch = getBatch(batchNo);
  if(!batch) return {inspected:0,aGrade:0,pct:0,salvage:0,remelt:0,wip:0};

  // v44F Issue#1: admin reconciliation override — A-Grade(lakhs) + Wastage are admin-typed; keep the
  // frozen A-Grade% shape (Out/(Out+Salvage+Remelt)) using those values. v46N: raw skips the override.
  const _ovA = (opts && opts.raw) ? null : _reconOverride(batchNo);
  if(_ovA){
    const a=_ovA.aGrade||0, ws=_ovA.wastage||0, denom=a+ws;
    return { inspected:a+ws, aGrade:a, pct: denom>0?(a/denom*100):0, salvage:ws, remelt:0, wip:_reconWip(_ovA, batchNo), reconciled:true }; /* v46Q; v49Y: bn → effective gross */
  }

  const useSummary = hasScanSummary();

  if(stage==='unprinted'){
    let aimInBoxes, aimOutBoxes;
    if (useSummary) {
      const s = _ss(batchNo,'aim');
      aimInBoxes  = s.in  + state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept==='aim'&&sc.type==='in'&&sc._local).length;
      aimOutBoxes = s.out + state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept==='aim'&&sc.type==='out'&&sc._local).length;
    } else {
      aimInBoxes  = state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='aim'&&s.type==='in').length;
      aimOutBoxes = state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='aim'&&s.type==='out').length;
    }
    const w = getTotalWastage(batchNo,'aim');
    // v47G (confirmed by Ishan): per-box label-qty sum, not box-count × pack. Inspected = ScanIn +
    // Salvage and A-Grade = ScanIn − Remelt derive from these, so both correct automatically.
    const aimIn  = _scanLakhs(batchNo,'aim','in',  batch.size);  // Scan-In
    const aimOut = _scanLakhs(batchNo,'aim','out', batch.size);  // Scan-Out (moved onward) — dept-WIP + frozen post-print base
    // v45V (confirmed by Ishan): AIM A-Grade = Scan-In − AIM Remelt (salvage removed pre-box, remelt
    // post-box). Denominator = A-Grade + Salvage + Remelt = Scan-In + Salvage (remelt already sits
    // inside Scan-In, so it is never re-added; denominator maxes at Scan-In + Salvage at completion).
    // Prior basis was Scan-Out; this makes AIM yield reflect boxing time, not movement time. The
    // post-print/PI chain (stage 'printed', below) is UNCHANGED — it still uses Scan-Out as its base.
    const aGrade = Math.max(0, aimIn - w.remelt);
    const inspected = aGrade + w.salvage + w.remelt;    // = Scan-In + Salvage
    const wip = Math.max(0, aGrade - aimOut);           // Dept WIP: good boxed at AIM, not yet moved onward
    return { aimIn, aimOut, inspected, aGrade, aimAGrade: aGrade, agNum: aGrade, agDen: inspected, salvage:w.salvage, remelt:w.remelt, wip,
             pct: inspected>0 ? (aGrade/inspected*100) : 0 };
  }

  if(stage==='printed'){
    let aimOutBoxes, printOutBoxes, piOutBoxes;
    if (useSummary) {
      aimOutBoxes   = _ss(batchNo,'aim').out       + state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept==='aim'&&sc.type==='out'&&sc._local).length;
      printOutBoxes = _ss(batchNo,'printing').out  + state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept==='printing'&&sc.type==='out'&&sc._local).length;
      piOutBoxes    = _ss(batchNo,'pi').out         + state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept==='pi'&&sc.type==='out'&&sc._local).length;
    } else {
      aimOutBoxes   = state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='aim'&&s.type==='out').length;
      printOutBoxes = state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='printing'&&s.type==='out').length;
      piOutBoxes    = state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='pi'&&s.type==='out').length;
    }
    const aimOut   = _scanLakhs(batchNo,'aim','out',      batch.size);  // v47G: per-box label-qty sum
    const printOut = _scanLakhs(batchNo,'printing','out', batch.size);
    const piOut    = _scanLakhs(batchNo,'pi','out',       batch.size);
    const wAIM   = getTotalWastage(batchNo,'aim');
    const wPrint = getTotalWastage(batchNo,'printing');
    const wPI    = getTotalWastage(batchNo,'pi');
    const aimInspected   = aimOut   + wAIM.total;
    const printInspected = printOut + wPrint.total;
    const piInspected    = piOut    + wPI.total;
    const printWIP = Math.max(0, aimOut - printInspected);
    const piWIP    = Math.max(0, printOut - piInspected);
    // v47H (confirmed by Ishan): realized A-Grade = PI Scan-Out — the boxes that cleared Print
    // Inspection, which is ALREADY net of PI wastage (do NOT subtract it again). Everything not yet
    // through PI is WIP (Reports B/F). The Post-Printing A-Grade % is the convergence TARGET, not the
    // running value: % = (AIM A-Grade − Printing Rejection − PI wastage) ÷ (AIM A-Grade + AIM Salvage),
    // where AIM A-Grade = Scan-In − AIM Remelt and Printing Rejection = DPL cumulative salvage % ×
    // Print-TD (printing scan-out). Reports D & E both read this stage. 26ZD104: PI Scan-Out 36.75L
    // (running, → converges to 47.86L); % = (52.5 − 0.89 − 3.75) ÷ (52.5 + 2.2) = 87.5%.
    const aimInAG        = _scanLakhs(batchNo,'aim','in', batch.size);
    const aimAGradeBase  = Math.max(0, aimInAG - wAIM.remelt);         // AIM A-Grade = Scan-In − AIM Remelt
    const _dplPctAG      = (window._printSalvagePct && window._printSalvagePct[(batchNo||'').toUpperCase()]) || 0;
    const _printRejAG    = printOut * (_dplPctAG/100);                 // Printing Rejection = DPL % × Print-TD
    const _postPrintDen  = aimAGradeBase + wAIM.salvage;
    const _postPrintPct  = _postPrintDen>0 ? Math.max(0, (aimAGradeBase - _printRejAG - wPI.total)/_postPrintDen*100) : 0;
    const netAGrade      = piOut;                                      // realized A-Grade = PI Scan-Out (net of PI wastage)
    const totalSalvage = wAIM.salvage + wPrint.salvage + wPI.salvage;
    const totalRemelt  = wAIM.remelt  + wPrint.remelt  + wPI.remelt;
    return {
      aimIn: aimOut, inspected: aimInspected, aGrade: netAGrade,
      aimAGrade: aimAGradeBase, printRej: _printRejAG, postPrintPct: _postPrintPct,
      agNum: Math.max(0, aimAGradeBase - _printRejAG - wPI.total), agDen: _postPrintDen, wip: printWIP + piWIP,
      salvage: totalSalvage, remelt: totalRemelt,
      pct: _postPrintPct,
      stages: {
        aim:   { out:aimOut,   wastage:wAIM.total,   inspected:aimInspected,   pct:aimInspected>0?(aimOut/aimInspected*100):0 },
        print: { out:printOut, wastage:wPrint.total,  inspected:printInspected, pct:printInspected>0?(Math.max(0,printInspected-wPrint.total)/printInspected*100):0 }, // v45ZH: stage yield nets its own wastage — 'Print: 100%' beside a non-zero print salvage contradicted the headline post-print %
        pi:    { out:piOut,    wastage:wPI.total,     inspected:piInspected,    pct:piInspected>0?(piOut/piInspected*100):0 },
      }
    };
  }
  return {inspected:0,aGrade:0,pct:0,wip:0};
}

function _v50dIsAbsorbed(b){
  if (!_V50B_COHORT || !b) return false;
  const raw = _v50bCohortMonthRaw(b);
  if (!raw || raw >= _V50C_COHORT_START) return false;
  const lastM = _v50cLastProdMonth(b);
  return !!(lastM && lastM >= _V50C_COHORT_START);
}

function _rptBUniverse(){
  const _bMd = (_v41_useMonthAttributed && _v41_agradeMonthCache[_trkSelectedMonth]) ? _v41_agradeMonthCache[_trkSelectedMonth] : null;
  const _bIsCurMonth = _trkSelectedMonth === today().slice(0,7);
  // v50B (confirmed by Ishan): ∑ Cumulative now means ALL MONTHS — the true floor position in one
  // screen — instead of lifetime figures narrowed to one month's cohort (which returned an empty
  // table whenever the month's own batches had no scans yet). Month of Production stays cohort-scoped.
  const _cumAll50b = _V50B_COHORT && !_v41_useMonthAttributed;
  const monthBatchNos = new Set((_cumAll50b
      ? state.batches.filter(b => !b.deleted && !_isRetired(b.batchNumber))
      : getProdMonthBatches(true)).map(b=>b.batchNumber).filter(Boolean));
  const _ghostMc45y = {};
  const _retiredRows45z = new Set();
  if (_bMd) {
    const _addIf = bn => {
      const b = getBatch(bn);
      if (_isRetired(bn)) { monthBatchNos.add(bn); _retiredRows45z.add(bn); if (!b) _ghostMc45y[bn] = (_bMd.monthMachine || {})[bn] || null; return; }
      if (b) { if (!b.deleted) monthBatchNos.add(bn); return; }
      monthBatchNos.add(bn);
      _ghostMc45y[bn] = (_bMd.monthMachine || {})[bn] || null;
    };
    Object.keys(_bMd.summary||{}).forEach(_addIf);
    Object.keys(_bMd.wastage||{}).forEach(_addIf);
    Object.entries(_bMd.monthGross||{}).forEach(([bn,g])=>{ if((parseFloat(g)||0)>0) _addIf(bn); });
  }
  const _ffsfSet = new Set([...(FLOORS.FF?.machines||[]),...(FLOORS.SF?.machines||[])].map(normMcId));
  return { _bMd, _bIsCurMonth, monthBatchNos, _ghostMc45y, _retiredRows45z, _ffsfSet };
}

function _rptBRow(batchNo, dept, U){
  const { _bMd, _bIsCurMonth, _ghostMc45y, _retiredRows45z, _ffsfSet } = U;
  const hasWastage = dept==='aim'||dept==='printing'||dept==='pi';
  const batch=getBatch(batchNo);
  const _bSize45y = (batch && batch.size != null) ? batch.size : (getLabelsByBatch(batchNo)[0]?.size ?? '0');
  const localIn  = state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept===dept&&sc.type==='in'&&sc._local).length;
  const localOut = state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept===dept&&sc.type==='out'&&sc._local).length;
  const _mdS = _bMd ? (((_bMd.summary||{})[batchNo]||{})[dept] || {inBoxes:0,outBoxes:0,inReconQty:0,outReconQty:0,inRealQty:0,outRealQty:0}) : null;
  const _mdW = _bMd ? (((_bMd.wastage||{})[batchNo]||{})[dept] || {salvage:0,remelt:0}) : null;
  const inn = _bMd ? (_mdS.inBoxes  + (_bIsCurMonth?localIn:0))
                   : (hasScanSummary() ? (_ss(batchNo,dept).in  + localIn)  : state.scans.filter(s=>s.batchNumber===batchNo&&s.dept===dept&&s.type==='in').length);
  const out = _bMd ? (_mdS.outBoxes + (_bIsCurMonth?localOut:0))
                   : (hasScanSummary() ? (_ss(batchNo,dept).out + localOut) : state.scans.filter(s=>s.batchNumber===batchNo&&s.dept===dept&&s.type==='out').length);
  // v47G (confirmed by Ishan): per-box label-qty sum for this dept's Scan-In/Out (not box-count × pack).
  // Month branch = server inRealQty/outRealQty + recon Lakhs (+ current-month local scans valued per box);
  // cumulative branch = the live _scanLakhs primitive. inn/out box counts above stay for the WIP-box column.
  const _locInQty  = _bMd && _bIsCurMonth ? state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept===dept&&sc.type==='in'&&sc._local).reduce((a,sc)=>a+_boxLakh(sc,_bSize45y),0) : 0;
  const _locOutQty = _bMd && _bIsCurMonth ? state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept===dept&&sc.type==='out'&&sc._local).reduce((a,sc)=>a+_boxLakh(sc,_bSize45y),0) : 0;
  const innQty = _bMd ? ((_mdS.inRealQty||0)  + (_mdS.inReconQty||0)  + _locInQty)  : _scanLakhs(batchNo, dept, 'in',  _bSize45y);
  const outQty = _bMd ? ((_mdS.outRealQty||0) + (_mdS.outReconQty||0) + _locOutQty) : _scanLakhs(batchNo, dept, 'out', _bSize45y);
  const w = hasWastage ? (_bMd ? { total:(_mdW.salvage+_mdW.remelt), salvage:_mdW.salvage, remelt:_mdW.remelt }
                               : getTotalWastage(batchNo, dept))
                       : {total:0, salvage:0, remelt:0};
  const ps = PACK_SIZES[String(_bSize45y)]||1;
  const wastageBoxes = hasWastage ? Math.round(w.total/ps) : 0;
  // v47A (regression fix): REVERT to the frozen WIP-box formula. The v46Z box-stages-map count broke the
  // fundamental invariant WIP = scan-in − scan-out: it counted every label whose CURRENT stage equals this
  // dept (an all-time, cross-month set), so month-scoped in/out no longer reconciled with the WIP column
  // (26W062 showed 7 on 4-in/3-out; AIM totalled 104 boxes on 843-in/822-out). The frozen net formula is
  // the single source of truth for the WIP-box count — box count and WIP qty derive from it and stay
  // consistent. (The expandable chip list remains a current-stage view; it may differ from this net count,
  // which is acceptable — the WIP number must be scan-in − scan-out − wastage, per Ishan.)
  // v47B (confirmed by Ishan): month-scoped WIP box SET from the server (agrade-by-month wipBoxes) — the
  // boxes scanned into this dept this month with no scan-out. The COUNT is the length of that exact list,
  // and _rptB_wipBoxChips renders the SAME list, so clicking N always shows those N boxes. Falls back to
  // the frozen net formula only when the month set isn't loaded (e.g. Excel export without the cache).
  const _wipSet = (_bMd && _bMd.wipBoxes && (_bMd.wipBoxes[batchNo]||{})[dept]) || null;
  // v49D (confirmed by Ishan): PI is split away from Printing, which netted nothing on the month-set path
  // and so reported phantom WIP for every batch carrying PI wastage.
  //   QTY  - base is the ACTUAL summed quantity of the labels still in the dept (server wipQtys), not
  //          box-count x nominal pack size. Nominal overstates any batch whose last box is a partial
  //          (generateLabels isPartialLast), which is routine. Salvage AND remelt are both subtracted:
  //          on July data the floor records PI salvage as a wastage row while the labels keep their full
  //          quantity (26ZE101: printing scan-out is exactly 33 x 1.75 = 57.75L with 0.52L salvage
  //          logged), so the wastage row is the only record of it and must come off here.
  //   BOXES - remelt only. Salvage takes material OUT of boxes that remain physically present and keep
  //          travelling; remelt destroys whole boxes that are never scanned out and would otherwise sit
  //          in the WIP set forever. Rounded at nominal pack size.
  // NOT changed: getBatchWIPBreakdown and getBatchAGrade already subtract PI salvage and stay as they
  // are, so July figures are untouched. If the floor later amends label quantities at PI to carry
  // salvage, all three sites double-count and must be revisited together - that is the August spec.
  // Printing and AIM are untouched here.
  const _piSal49d     = (dept === 'pi' && hasWastage) ? (w.salvage || 0) : 0;
  const _piRem49d     = (dept === 'pi' && hasWastage) ? (w.remelt || 0) : 0;
  const _piRemBox49d  = _piRem49d > 0 ? Math.round(_piRem49d / ps) : 0;
  // ═══ v51Z PRINTING WASTAGE SOURCE (Ishan, 12 Aug) ════════════════════════════════════════════
  // Printing wastage lives in the Planning Daily Printing Log — v44J made that the single source
  // for the A-grade cascade, Report E and the frozen WIP formula (all net DPL% and IGNORE tracking
  // printing salvage; see 3489). Report B was the one consumer never converted: it netted ONLY
  // tracking-entered wastage, so its printing WIP stayed gross of DPL — which is why admin was
  // reduced to hand-mirroring DPL lumpsums into the Tracking wastage tab (the Alkem 34L).
  // Now identical to getBatchWIPBreakdown's printWIP (3472): net = in − out − DPL%×Print-TD −
  // tracking remelt. Tracking printing SALVAGE is deliberately NOT netted — same exclusion as every
  // other consumer — so the existing manual mirror entries cause no double-netting while they still
  // exist; they are simply inert here and can be deleted after deploy. Remelt has no DPL
  // counterpart and stays tracking-sourced. wipQty is qty-exact (in − out − deductions), no longer
  // nominal box×packsize, so partial last boxes stop overstating.
  const _dplPct51z = (dept === 'printing') ? ((window._printSalvagePct && window._printSalvagePct[(batchNo||'').toUpperCase()]) || 0) : 0;
  const _dplSal51z = (dept === 'printing') ? (outQty * (_dplPct51z / 100)) : 0;
  const _prRem51z  = (dept === 'printing' && hasWastage) ? (w.remelt || 0) : 0;
  const _rawRemain49d = _wipSet ? _wipSet.length : Math.max(0, inn - out);
  const _piWipQty49d  = (dept === 'pi' && _bMd && _bMd.wipQtys)
    ? ((_bMd.wipQtys[batchNo] || {})[dept]) : null;
  const wip = dept === 'pi'
    ? Math.max(0, _rawRemain49d - _piRemBox49d)
    : dept === 'printing'
      ? Math.max(0, inn - out - Math.round((_dplSal51z + _prRem51z) / ps))
      : (_wipSet ? _wipSet.length : Math.max(0, inn - out - (dept==='aim' ? 0 : wastageBoxes)));
  // v46E (confirmed by Ishan): AIM Dept WIP (qty) nets remelt — Scan In − Remelt − Scan Out — so
  // Report B/D reconcile with Report E and getBatchWIPBreakdown. WIP boxes stay the physical inn−out
  // count. Printing nets DPL salvage + tracking remelt (v51Z above); PI is handled above (v49D).
  const wipQty = dept==='aim'
    ? Math.max(0, innQty - w.remelt - outQty)
    : dept==='pi'
      ? Math.max(0, (_piWipQty49d != null ? _piWipQty49d
      // v52A (Ishan, 17 Aug — 26T093: B said 3.34 while E said 2.19): the fallback valued the
      // remaining boxes at NOMINAL box-count × pack size, but a partial last box is routine, so B
      // overstated exactly the batches E prices correctly (E rides getBatchWIPBreakdown's qty-exact
      // in − out). Fallback is now the same qty-exact net the month path's wipQtys approximates:
      // innQty − outQty (26T093: 12.85 − 10.50 = 2.35, − 0.16 sal = 2.19 ≡ Report E). Uniformity:
      // every cumulative-view consumer of the PI row (Report B, dashboard stage tiles, Report D's
      // stage primitive) gets the corrected figure; the v49D month path with server wipQtys is
      // untouched, as are boxes counts (physical inn − out stays).
                                           : Math.max(0, innQty - outQty)) - _piSal49d - _piRem49d)
      : dept==='printing'
        ? Math.max(0, innQty - outQty - _dplSal51z - _prRem51z)
        : boxToLakh(wip,_bSize45y);
  const grossProd = _bMd
    ? ((batchNo in (_bMd.monthGross||{})) ? (parseFloat(_bMd.monthGross[batchNo])||0)
       : (((parseFloat(batch?.actualProd)||0) === 0 && _trkProdStartMonth(batch||{})===_trkSelectedMonth) ? _reconGross(batch) : 0))
    : _reconGross(batch);
  const aimInBoxes = _bMd
    ? ((((_bMd.summary||{})[batchNo]||{}).aim||{inBoxes:0}).inBoxes + (_bIsCurMonth ? state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept==='aim'&&sc.type==='in'&&sc._local).length : 0))
    : (hasScanSummary()
      ? (_ss(batchNo,'aim').in + state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept==='aim'&&sc.type==='in'&&sc._local).length)
      : state.scans.filter(s=>s.batchNumber===batchNo&&s.dept==='aim'&&s.type==='in').length);
  // v47G: AIM Scan-In as per-box label sum (drives Unscanned WIP = Gross − Salvage − ScanIn).
  const _aimS = _bMd ? (((_bMd.summary||{})[batchNo]||{}).aim || {inRealQty:0,inReconQty:0}) : null;
  const aimInQty = _bMd
    ? ((_aimS.inRealQty||0) + (_aimS.inReconQty||0) + (_bIsCurMonth ? state.scans.filter(sc=>sc.batchNumber===batchNo&&sc.dept==='aim'&&sc.type==='in'&&sc._local).reduce((a,sc)=>a+_boxLakh(sc,_bSize45y),0) : 0))
    : _scanLakhs(batchNo,'aim','in',_bSize45y);
  const aimWaste = _bMd ? (((_bMd.wastage||{})[batchNo]||{}).aim || {salvage:0,remelt:0}) : getTotalWastage(batchNo,'aim');
  // v46E (confirmed by Ishan): Unscanned WIP = Gross − Salvage − Scan In (= Gross − Inspected).
  // Remelt is NOT deducted here — it's post scan-in, so it lives in the AIM Dept WIP leg instead.
  const _prodWipRaw=_isRetired(batchNo)?0:Math.max(0, grossProd - aimWaste.salvage - aimInQty);
  const firstIn=state.scans.filter(s=>s.batchNumber===batchNo&&s.dept===dept&&s.type==='in').sort((a,b)=>new Date(a.ts)-new Date(b.ts))[0];
  const hrs=firstIn?hoursAgo(firstIn.ts):0;
  const _closed = isStageComplete(batchNo,dept);
  // v46M (confirmed by Ishan): admin reconcile override takes precedence across ALL reports. When a
  // batch is reconciled, its authoritative quantities (Gross, WIP, Wastage, Packed) come from the typed
  // override — not raw scans/DPR — exactly as getBatchWIPBreakdown already does; A-Grade already routes
  // through the override via _v41_agradeFor/getBatchAGrade. Physical box counts (inn/out) stay raw. The
  // pre-override (raw) figures are preserved in reconOrig so reports can surface them on hover. Reconciled
  // WIP is placed on the production-WIP leg with the dept legs zeroed — matches getBatchWIPBreakdown's
  // single-total collapse and the point-2 agreement (reconcile-to-0 ⇒ every WIP figure 0).
  const _rcOv = _reconOverride(batchNo);
  let _gross=grossProd, _prodWip=_prodWipRaw, _wip=wip, _wipQty=wipQty, _w=w, _aimWaste=aimWaste, _reconOrig=null, _frozenHere=false;
  if(_rcOv){
    _reconOrig = { grossProd, prodWip:_prodWipRaw, wip, wipQty, wastage:(w?w.total:0),
                   aimSalvage:(aimWaste?aimWaste.salvage:0), avail:Math.max(0,grossProd-(aimWaste?aimWaste.salvage:0)) };
    if(_rcOv.gross!=null && _dprOvrGross(batchNo)==null) _gross=_rcOv.gross;   // v49Y: DPR correction (already in grossProd via _reconGross) is top authority
    // v47Y (confirmed by Ishan): the frozen remainder AMOUNT stays Gross−Packed−Wastage; its LOCATION is
    // the scan-trail-derived stage (_v47yReconStage — printed: PI→Printing→AIM deepest-first; unprinted:
    // AIM). Park it ONLY on that stage's row, 0 elsewhere — so a batch that has cleared AIM shows 0 AIM
    // production WIP and its remainder surfaces at PI/Printing. AIM as the derived stage keeps it on the
    // production leg; deeper stages carry it on the dept-WIP leg. Replaces v46M's "always park on AIM".
    // v49K (confirmed by Ishan): the frozen remainder must SELF-CURE as material moves. v47Y froze the
    // AMOUNT at reconcile time (from ov.packing) and never re-derived it, so Report B kept showing e.g.
    // 4L WIP on 26ZC091 long after the material was scanned in — while Report E, which derives unscanned
    // WIP live (avail − scanIn), correctly showed 0. Fix: cap the frozen remainder by the CURRENT
    // unscanned WIP at its stage, so once scan-in ≥ available the WIP falls to 0 across every report,
    // matching Report E. The override stays authoritative for gross/A-grade/wastage; only this live WIP
    // leg follows real scans.
    // v49M (confirmed by Ishan): self-cure the frozen remainder at the STAGE WHERE IT IS PARKED, against
    // THAT stage's own clearing event — not universally against packing. The remainder sits at
    // _v47yReconStage (aim / pi / printing). It clears when the material moves past that stage:
    //   • parked at AIM     → clears as it is SCANNED IN at AIM  (avail − aimIn). This is 26ZC091: its
    //                         remainder was unscanned-at-AIM, so once aimIn ≥ avail it is 0 — packing is
    //                         irrelevant to it. (v49L wrongly held it until packing; corrected here.)
    //   • parked at PI/print→ clears as it is SCANNED OUT of that stage (moved onward): avail − stageOut.
    // A remainder that legitimately sits at packing (batch reconciled with FG already at the pack dock)
    // still clears against packing.in via the same avail-minus-stage-progress shape. Override stays
    // authoritative for gross/A-grade/wastage; only this WIP leg follows real scans, stage-appropriately.
    // v49Z (confirmed by Ishan): frozen remainder SPLIT by actual scan positions — each dept row
    // carries exactly what physically sits in that dept (in − out capacity, allocation capped by the
    // remainder); unscanned residue stays on AIM's production leg. Transit slices render via the live
    // handover-gap legs (consumed inside _v47ySplit — no double count). Self-cure intrinsic.
    const _sp47z = _v47ySplit(batchNo, _rcOv);
    if (dept==='aim')            { _prodWip=_sp47z.prod; _wip=0; _wipQty=_sp47z.aim; }
    else if (dept==='printing')  { _prodWip=0; _wip=0; _wipQty=_sp47z.printing; }
    else if (dept==='pi')        { _prodWip=0; _wip=0; _wipQty=_sp47z.pi; }
    else                         { _prodWip=0; _wip=0; _wipQty=0; }
    _frozenHere = (_prodWip + _wipQty) > 0.005;
    const _ovWaste=_rcOv.wastage||0; _w={total:_ovWaste,salvage:_ovWaste,remelt:0}; _aimWaste={salvage:_ovWaste,remelt:0};
  }
  const statusKey = _closed ? 'closed' : ((_wip>0 || _frozenHere) ? 'indept' : (out>0 ? 'leftdept' : '')); // v47Y: frozen remainder here ⇒ In Dept
  // v46E: Available to Scan In = Gross − Salvage (salvage removed pre-box; remelt is post scan-in). Matches Report E.
  const _avail = Math.max(0, _gross - _aimWaste.salvage);
  const floor = _ffsfSet.has(normMcId(getBatch(batchNo)?.machineId || _ghostMc45y[batchNo])) ? 'ffsf' : 'gf';
  // v51Z: the dept's DISPLAYED wastage — printing shows what the WIP actually nets (DPL salvage +
  // tracking remelt); tracking salvage at printing is inert everywhere and no longer shown as dept
  // wastage. Other depts unchanged (their tracking entries remain the single source).
  const _wDisp51z = (dept === 'printing' && !_rcOv) ? (_dplSal51z + _prRem51z) : (_w ? _w.total : 0);   // recon override stays authoritative (v46M)
  return { batchNo, batch, size:_bSize45y, hasWastage, inn, out, innQty, outQty, w:_w, wDisp:_wDisp51z, wastageBoxes, wip:_wip, wipQty:_wipQty,
           grossProd:_gross, aimInBoxes, aimInQty, aimWaste:_aimWaste, prodWip:_prodWip, hrs, closed:_closed, statusKey,
           avail:_avail, floor, retired:_retiredRows45z.has(batchNo), reconciled:!!_rcOv, reconOrig:_reconOrig, frozenHere:_frozenHere };
}

function _rptBDeptSet(dept, U){
  const { monthBatchNos } = U;
  let set;
  if (hasScanSummary()) {
    set = [...monthBatchNos].filter(bn => _ss(bn,dept).in > 0 || _ss(bn,dept).out > 0);
  } else {
    set = [...new Set(state.scans.filter(s=>s.dept===dept&&s.type==='in').map(s=>s.batchNumber))];
  }
  if (dept==='aim') {
    const have = new Set(set);
    [...monthBatchNos].forEach(bn => {
      if (have.has(bn) || _isRetired(bn)) return;
      if (_rptBRow(bn,'aim',U).prodWip > 0.005) set.push(bn);
    });
  }
  return set.slice().sort(_v44zk_batchCmp);
}

function normMcId(id){ return (id||'').replace(/[.\s]/g,'').toUpperCase(); }

// ─── Node export shim (ignored by browsers) ────────────────────────────────────────────────────
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { today, now, _istDayKey, _istToday, _trkProdStartMonth, getProdMonthBatches, _v50cLastProdMonth, _v50bCohortMonth, _v50bCohortMonthRaw, _v50bFirstScanMonth, hoursAgo, boxToLakh, getBatch, _ss, _sw, hasScanSummary, _boxLakh, _scanLakhs, _grossFor, _v40_dispatchedLakhs, getLabelsByBatch, getWastageForStage, getTotalWastage, isStageComplete, _isRetired, _reconOverride, _reconWip, _dprOvrGross, _v47yReconStage, _v47ySplit, _v50fGross, _reconGross, getBatchWIPBreakdown, getBatchAGrade, _v50dIsAbsorbed, _rptBUniverse, _rptBRow, _rptBDeptSet, normMcId };
}
