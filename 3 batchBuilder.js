/*******************************************************
 * batchBuilder.js (clean + config-driven)
 *
 * WHAT IT DOES
 * - Reads OrderItems
 * - Creates/reuses Batches (AUTO + MISC) based on CFG.BATCH rules
 * - Assigns OrderItems.PrintBatchID (machine key)
 * - Updates Batch metrics (even when reusing existing OPEN batches)
 *
 * FULL DAYS ONLY
 * - If CFG.BATCH.FULL_DAYS_ONLY = true, "today" (UK) is excluded
 * - Manual override: createBatchesIncludeTodayOverride()
 *
 * IMPORTANT
 * - Does NOT define onOpen() to avoid collisions.
 *   Menus now live in menu.js
 *******************************************************/

/*******************************************************
 * PUBLIC ENTRYPOINTS
 *******************************************************/
function createBatchesAuto(options) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    _createBatchesAuto_(options || {});
  } finally {
    lock.releaseLock();
  }
}

function createBatchesIncludeTodayOverride() {
  createBatchesAuto({ includeToday: true });
}

/*******************************************************
 * CORE BATCH CREATION
 *******************************************************/
function _createBatchesAuto_(options) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // ---- Config safety
  const batchCfg = (typeof CFG !== "undefined" && CFG.BATCH) ? CFG.BATCH : {};
  const blankishValues = getBlankishValues_();

  // ---- Sheets
  const shOI = ss.getSheetByName(CFG.SHEETS.ORDER_ITEMS);
  const shB  = ss.getSheetByName(CFG.SHEETS.BATCHES);
  if (!shOI) throw new Error(`Missing sheet: ${CFG.SHEETS.ORDER_ITEMS}`);
  if (!shB)  throw new Error(`Missing sheet: ${CFG.SHEETS.BATCHES}`);

  // ---- Options (with defaults)
  const dateMode = options.dateModeOverride || batchCfg.DATE_MODE || "ORDER_DATE";
  const lookbackDays = (options.lookbackDaysOverride !== undefined)
    ? options.lookbackDaysOverride
    : (batchCfg.LOOKBACK_DAYS !== undefined ? batchCfg.LOOKBACK_DAYS : 7);

  const minLI = (batchCfg.MIN_LINEITEMS_FOR_AUTO !== undefined) ? batchCfg.MIN_LINEITEMS_FOR_AUTO : 2;
  const minPU = (batchCfg.MIN_PRINTUNITS_FOR_AUTO !== undefined) ? batchCfg.MIN_PRINTUNITS_FOR_AUTO : 2;
  const maxPU = (batchCfg.MAX_PRINTUNITS_PER_BATCH !== undefined) ? batchCfg.MAX_PRINTUNITS_PER_BATCH : 80;

  const includeToday = (options.includeToday === true);
  const enforceFullDaysOnly = (batchCfg.FULL_DAYS_ONLY === true) && !includeToday;

  const TYPE_AUTO = batchCfg.TYPE_AUTO || "AUTO";
  const TYPE_MISC = batchCfg.TYPE_MISC || "MISC";
  const STATUS_OPEN = batchCfg.STATUS_OPEN || "Open";

  const CREATE_MISC_PER_DATE = (batchCfg.CREATE_MISC_PER_DATE !== false);
  const MISC_PROFILE_KEY = batchCfg.MISC_PROFILE_KEY || "MISC";
  const MISC_CATEGORY = batchCfg.MISC_CATEGORY || "MISC";

  // ---- Time anchors (UK)
  const today = startOfDay_(new Date());
  const todayKey = ymd_(today);
  const cutoff = new Date(today.getTime() - (lookbackDays * 86400000));

  // ---- Header maps (helpers.js)
  const oiMap = headerMap_(shOI);
  const bMap  = headerMap_(shB);

  // ---- Required OrderItems columns
  const cOI = CFG.COLS.ORDER_ITEMS;
  const iCreatedAt  = requireCol_(oiMap, cOI.CreatedAt);
  const iOrderName  = requireCol_(oiMap, cOI.OrderName);
  const iUnits      = requireCol_(oiMap, cOI.PrintUnits);
  const iCategory   = requireCol_(oiMap, cOI.PrintCategory);
  const iBatchID    = requireCol_(oiMap, cOI.PrintBatchID);
  const iReady      = requireCol_(oiMap, cOI.ReadyForOrders);
  const iProfileKey = requireCol_(oiMap, cOI.PrintProfileKey);

  // ---- Read OrderItems
  const oiLastRow = shOI.getLastRow();
  const oiLastCol = shOI.getLastColumn();
  if (oiLastRow < 2) {
    ss.toast("No OrderItems rows found.", "Batching", 5);
    return;
  }
  const oiValues = shOI.getRange(2, 1, oiLastRow - 1, oiLastCol).getValues();

  // ---- Read Batches + index (reuse + seq + metrics)
  const batchIndex = indexExistingBatches_(shB);

  // ---- Bucket candidates/outliers per dateKey
  // buckets: dateKey -> { dateObj, candidates[], outliers[] }
  const buckets = new Map();

  for (let r = 0; r < oiValues.length; r++) {
    const row = oiValues[r];

    // Skip already batched
    if (String(row[iBatchID] || "").trim()) continue;

    // Must be ReadyForOrders
    if (!isTrue_(row[iReady])) continue;

    // Exclude non-print
    const cat = String(row[iCategory] || "").trim();
    if (!cat || isBlankish_(cat, blankishValues)) continue;
    if (cat.toUpperCase() === "NONE") continue;

    // Determine bucket date
    const createdAt = parseDate_(row[iCreatedAt]);
    if (dateMode === "ORDER_DATE") {
      if (!createdAt) continue;
      if (createdAt < cutoff) continue;
    }

    const bucketDate = (dateMode === "PRINT_DAY")
      ? today
      : startOfDay_(createdAt);

    const dateKey = ymd_(bucketDate); // YYYY-MM-DD

    // Full-days-only caveat
    if (enforceFullDaysOnly && dateKey === todayKey) continue;

    // Profile key (required for AUTO)
    const pKey = String(row[iProfileKey] || "").trim();
    const units = toInt_(row[iUnits], 0);
    const orderName = String(row[iOrderName] || "").trim();

    // Outlier rules (print-related but not clean)
    const isMixed = (cat.toUpperCase() === "MIXED");
    const isUnknown = (cat.toUpperCase() === "UNKNOWN");
    const badUnits = units <= 0;
    const isOutlier = isMixed || isUnknown || !pKey || badUnits;

    if (!buckets.has(dateKey)) {
      buckets.set(dateKey, { dateObj: bucketDate, candidates: [], outliers: [] });
    }

    const rec = {
      rowIndex0: r,     // index into oiValues (0-based; sheet row = r+2)
      orderName,
      printUnits: units,
      printProfileKey: pKey,
      printCategory: cat
    };

    if (isOutlier) buckets.get(dateKey).outliers.push(rec);
    else buckets.get(dateKey).candidates.push(rec);
  }

  if (buckets.size === 0) {
    ss.toast("No eligible unbatched items found (given current rules).", "Batching", 6);
    return;
  }

  // ---- Assignment plan: rowIndex0 -> BatchID
  const rowToBatchId = new Map();

  // ---- New batch rows to append
  const newBatchRows = [];

  // ---- Track which existing batch rows need metric updates
  // batchId -> { tuAdd, liAdd, orders:Set }
  const existingMetricAdds = new Map();

  // ---- Process each bucket
  for (const [dateKey, bucket] of buckets.entries()) {
    // Group candidates by PrintProfileKey
    const groups = groupBy_(bucket.candidates, x => x.printProfileKey);

    for (const [profileKey, items] of groups.entries()) {
      const totalUnits = items.reduce((a, it) => a + (it.printUnits || 0), 0);
      const qualifies = (items.length >= minLI) && (totalUnits >= minPU);

      if (!qualifies) {
        // FIX: syntax bug; push all items to outliers
        bucket.outliers.push(...items);
        continue;
      }

      // Split if too large
      const splits = splitByMaxUnits_(items, maxPU);

      for (const splitItems of splits) {
        const printCategoryCode = primaryCodeFromProfileKey_(profileKey);

        const batchInfo = ensureOpenBatch_({
          batchIndex,
          newBatchRows,
          batchDateKey: dateKey,
          batchDateObj: bucket.dateObj,
          batchType: TYPE_AUTO,
          printProfileKey: profileKey,
          printCategory: printCategoryCode,
          status: STATUS_OPEN
        });

        for (const it of splitItems) rowToBatchId.set(it.rowIndex0, batchInfo.batchId);

        // Metrics
        addMetricsToBatch_(batchIndex, batchInfo, splitItems, existingMetricAdds);
      }
    }

    // MISC for outliers
    const miscItems = bucket.outliers;
    if (CREATE_MISC_PER_DATE && miscItems.length) {
      const miscBatch = ensureOpenBatch_({
        batchIndex,
        newBatchRows,
        batchDateKey: dateKey,
        batchDateObj: bucket.dateObj,
        batchType: TYPE_MISC,
        printProfileKey: MISC_PROFILE_KEY,
        printCategory: MISC_CATEGORY,
        status: STATUS_OPEN
      });

      for (const it of miscItems) rowToBatchId.set(it.rowIndex0, miscBatch.batchId);

      addMetricsToBatch_(batchIndex, miscBatch, miscItems, existingMetricAdds);
    }
  }

  // ---- Write: append new batch rows (if any)
  if (newBatchRows.length) {
    const bHeaders = batchIndex.headers;
    shB.getRange(shB.getLastRow() + 1, 1, newBatchRows.length, bHeaders.length).setValues(newBatchRows);
  }

  // ---- Write: update metrics for reused existing OPEN batches
  const updatedExistingCount = writeExistingBatchMetricUpdates_(shB, batchIndex, existingMetricAdds);

  // ---- Write: set OrderItems.PrintBatchID (single bulk write)
  let assigned = 0;
  if (rowToBatchId.size) {
    const out = new Array(oiValues.length);
    for (let i = 0; i < oiValues.length; i++) out[i] = [oiValues[i][iBatchID]];
    for (const [rowIndex0, batchId] of rowToBatchId.entries()) {
      out[rowIndex0] = [batchId];
      assigned++;
    }
    shOI.getRange(2, iBatchID + 1, oiValues.length, 1).setValues(out);
  }

  ss.toast(
    `Batching complete. New batches: ${newBatchRows.length}, Items assigned: ${assigned}, Existing batches updated: ${updatedExistingCount}`,
    "Batching",
    8
  );
}

/*******************************************************
 * BATCH INDEXING / REUSE / IDs / METRICS
 *******************************************************/
function indexExistingBatches_(shB) {
  const headers = getHeaders_(shB);
  const colMap = {};
  headers.forEach((h, i) => colMap[h] = i);

  const lastRow = shB.getLastRow();
  const lastCol = shB.getLastColumn();
  const values = (lastRow >= 2) ? shB.getRange(2, 1, lastRow - 1, lastCol).getValues() : [];

  const bCols = CFG.COLS.BATCHES;

  const idxId   = colMap[bCols.BatchID];
  const idxDate = colMap[bCols.BatchDate];
  const idxType = colMap[bCols.BatchType];
  const idxKey  = colMap[bCols.PrintProfileKey];
  const idxCat  = colMap[bCols.PrintCategory];
  const idxStat = colMap[bCols.Status];

  const idxTU = colMap[bCols.TotalPrintUnits];
  const idxLI = colMap[bCols.LineItemCount];
  const idxOC = colMap[bCols.OrderCount];

  // reuseKey -> { batchId, existingRowIndex0 }
  const openByKey = new Map();

  // seqMaxKey (YYYYMMDD|TYPE|CODE) -> maxSeq
  const seqMax = new Map();

  // batchId -> { rowIndex0 }
  const existingMetrics = new Map();

  for (let i = 0; i < values.length; i++) {
    const r = values[i];

    const id = String(r[idxId] || "").trim();
    if (!id) continue;

    const type = String(r[idxType] || "").trim().toUpperCase();
    const pKey = String(r[idxKey] || "").trim();
    const cat  = String(r[idxCat] || "").trim().toUpperCase();

    const dt = (idxDate !== undefined) ? parseDate_(r[idxDate]) : null;
    const dateKey = dt ? ymd_(startOfDay_(dt)) : "";

    // Reuse OPEN batches
    const statusOpen = (CFG.BATCH && CFG.BATCH.STATUS_OPEN) ? CFG.BATCH.STATUS_OPEN : "Open";
    const status = (idxStat !== undefined) ? String(r[idxStat] || "").trim() : "";

    if (status && status === statusOpen && dateKey && type && pKey) {
      const reuseKey = `${dateKey}|${type}|${pKey}`;
      if (!openByKey.has(reuseKey)) {
        openByKey.set(reuseKey, { batchId: id, existingRowIndex0: i });
      }
    }

    // Track metrics for existing rows (rowIndex only; values are in batchIndex.values)
    existingMetrics.set(id, { rowIndex0: i });

    // Track next sequence from BatchID format: B-YYYYMMDD-TYPE-CODE-### (preferred)
    const m = id.match(/^B-(\d{8})-([A-Z]+)-([A-Z0-9]+)-(\d{3})$/i);
    if (m) {
      const key = `${m[1]}|${m[2].toUpperCase()}|${m[3].toUpperCase()}`;
      const seq = parseInt(m[4], 10);
      if (!seqMax.has(key) || seq > seqMax.get(key)) seqMax.set(key, seq);
    } else {
      // Fallback: if IDs ever deviate, seed key
      if (dateKey && type && cat) {
        const ymd8 = dateKey.replace(/-/g, "");
        const key = `${ymd8}|${type}|${cat.replace(/[^A-Z0-9]/g, "").slice(0, 12)}`;
        if (!seqMax.has(key)) seqMax.set(key, 0);
      }
    }

    // Ensure existing metrics are numeric (in-memory values array)
    if (idxTU !== undefined) values[i][idxTU] = toInt_(values[i][idxTU], 0);
    if (idxLI !== undefined) values[i][idxLI] = toInt_(values[i][idxLI], 0);
    if (idxOC !== undefined) values[i][idxOC] = toInt_(values[i][idxOC], 0);
  }

  return { headers, colMap, values, openByKey, seqMax, existingMetrics };
}

function ensureOpenBatch_(args) {
  const {
    batchIndex,
    newBatchRows,
    batchDateKey,
    batchDateObj,
    batchType,
    printProfileKey,
    printCategory,
    status
  } = args;

  const reuseKey = `${batchDateKey}|${String(batchType).toUpperCase()}|${printProfileKey}`;
  const existing = batchIndex.openByKey.get(reuseKey);
  if (existing) {
    return { batchId: existing.batchId, existingRowIndex0: existing.existingRowIndex0, isNew: false };
  }

  // Create new batch row
  const batchId = generateBatchId_(batchIndex, batchDateKey, batchType, printCategory);

  const bCols = CFG.COLS.BATCHES;
  const headers = batchIndex.headers;
  const row = new Array(headers.length).fill("");

  const set = (h, v) => {
    if (batchIndex.colMap[h] !== undefined) row[batchIndex.colMap[h]] = v;
  };

  set(bCols.BatchID, batchId);
  set(bCols.BatchDate, batchDateObj);
  set(bCols.BatchType, String(batchType).toUpperCase());
  set(bCols.PrintProfileKey, printProfileKey);
  set(bCols.PrintCategory, String(printCategory || "").trim());
  set(bCols.Status, status);
  set(bCols.CreatedAt, new Date());

  if (bCols.PrintBatchName && batchIndex.colMap[bCols.PrintBatchName] !== undefined) {
    set(bCols.PrintBatchName, makePrintBatchName_(batchDateObj, printCategory, batchId, ""));
  }

  // Initialise metrics
  if (bCols.TotalPrintUnits && batchIndex.colMap[bCols.TotalPrintUnits] !== undefined) set(bCols.TotalPrintUnits, 0);
  if (bCols.LineItemCount  && batchIndex.colMap[bCols.LineItemCount]  !== undefined) set(bCols.LineItemCount, 0);
  if (bCols.OrderCount     && batchIndex.colMap[bCols.OrderCount]     !== undefined) set(bCols.OrderCount, 0);

  newBatchRows.push(row);

  // Mark as reusable within this run
  batchIndex.openByKey.set(reuseKey, { batchId, existingRowIndex0: null });

  return { batchId, existingRowIndex0: null, isNew: true, _newRowRef: row };
}

function generateBatchId_(batchIndex, dateKeyYmd, batchType, printCategoryCode) {
  const ymd8 = dateKeyYmd.replace(/-/g, "");
  const type = String(batchType || "").toUpperCase();
  const code = String(printCategoryCode || "GEN").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 12) || "GEN";

  const seqKey = `${ymd8}|${type}|${code}`;
  const next = (batchIndex.seqMax.get(seqKey) || 0) + 1;
  batchIndex.seqMax.set(seqKey, next);

  const seq = String(next).padStart(3, "0");
  return `B-${ymd8}-${type}-${code}-${seq}`;
}

function addMetricsToBatch_(batchIndex, batchInfo, items, existingMetricAdds) {
  const totalUnits = items.reduce((a, it) => a + (it.printUnits || 0), 0);
  const liCount = items.length;
  const orders = new Set(items.map(it => it.orderName).filter(Boolean));
  const orderCount = orders.size;

  const bCols = CFG.COLS.BATCHES;
  const idxTU = batchIndex.colMap[bCols.TotalPrintUnits];
  const idxLI = batchIndex.colMap[bCols.LineItemCount];
  const idxOC = batchIndex.colMap[bCols.OrderCount];

  // New batch row: update the row array directly (correct + immediate)
  if (batchInfo.isNew && batchInfo._newRowRef) {
    const row = batchInfo._newRowRef;

    if (idxTU !== undefined) row[idxTU] = toInt_(row[idxTU], 0) + totalUnits;
    if (idxLI !== undefined) row[idxLI] = toInt_(row[idxLI], 0) + liCount;
    if (idxOC !== undefined) row[idxOC] = toInt_(row[idxOC], 0) + orderCount;
    return;
  }

  // Existing batch row: store increments for later write-back
  const id = batchInfo.batchId;
  if (!existingMetricAdds.has(id)) {
    existingMetricAdds.set(id, { tu: 0, li: 0, orders: new Set() });
  }
  const m = existingMetricAdds.get(id);
  m.tu += totalUnits;
  m.li += liCount;
  for (const o of orders) m.orders.add(o);
}

/**
 * Updates only the 3 metric columns (TU/LI/OC) for touched existing batch rows.
 * Avoids overwriting the rest of the row.
 */
function writeExistingBatchMetricUpdates_(shB, batchIndex, existingMetricAdds) {
  if (!existingMetricAdds || existingMetricAdds.size === 0) return 0;

  const bCols = CFG.COLS.BATCHES;
  const idxTU = batchIndex.colMap[bCols.TotalPrintUnits];
  const idxLI = batchIndex.colMap[bCols.LineItemCount];
  const idxOC = batchIndex.colMap[bCols.OrderCount];

  if (idxTU === undefined && idxLI === undefined && idxOC === undefined) return 0;

  const touchedRowIndices = [];
  for (const [batchId, add] of existingMetricAdds.entries()) {
    const em = batchIndex.existingMetrics.get(batchId);
    if (!em) continue;

    const row = batchIndex.values[em.rowIndex0];
    if (!row) continue;

    if (idxTU !== undefined) row[idxTU] = toInt_(row[idxTU], 0) + (add.tu || 0);
    if (idxLI !== undefined) row[idxLI] = toInt_(row[idxLI], 0) + (add.li || 0);
    if (idxOC !== undefined) row[idxOC] = toInt_(row[idxOC], 0) + (add.orders ? add.orders.size : 0);

    touchedRowIndices.push(em.rowIndex0);
  }

  if (!touchedRowIndices.length) return 0;

  // Batch contiguous runs of rows (rowIndex0 is 0-based within values array; sheet row = 2 + rowIndex0)
  touchedRowIndices.sort((a, b) => a - b);

  const runs = [];
  let start = touchedRowIndices[0];
  let prev = start;
  for (let i = 1; i < touchedRowIndices.length; i++) {
    const cur = touchedRowIndices[i];
    if (cur === prev + 1) prev = cur;
    else { runs.push([start, prev]); start = cur; prev = cur; }
  }
  runs.push([start, prev]);

  for (const [s, e] of runs) {
    const num = (e - s + 1);

    if (idxTU !== undefined) {
      const vals = new Array(num);
      for (let i = 0; i < num; i++) vals[i] = [batchIndex.values[s + i][idxTU]];
      shB.getRange(2 + s, idxTU + 1, num, 1).setValues(vals);
    }
    if (idxLI !== undefined) {
      const vals = new Array(num);
      for (let i = 0; i < num; i++) vals[i] = [batchIndex.values[s + i][idxLI]];
      shB.getRange(2 + s, idxLI + 1, num, 1).setValues(vals);
    }
    if (idxOC !== undefined) {
      const vals = new Array(num);
      for (let i = 0; i < num; i++) vals[i] = [batchIndex.values[s + i][idxOC]];
      shB.getRange(2 + s, idxOC + 1, num, 1).setValues(vals);
    }
  }

  return touchedRowIndices.length;
}

/*******************************************************
 * OPTIONAL HUMAN-FRIENDLY BATCH NAME
 *******************************************************/
function makePrintBatchName_(batchDateObj, printCategory, batchId, royalMailBatchNumber) {
  const datePart = Utilities.formatDate(batchDateObj, CFG.TIMEZONE, "dd/MM/yyyy");
  const cat = String(printCategory || "").trim() || "GEN";

  const rm = String(royalMailBatchNumber || "").trim();
  if (rm) return `${datePart} - ${cat} - ${rm}`;

  // Fallback while RM batch number is blank (keeps names unique/usable)
  const runNo = extractRunNumberFromBatchId_(batchId);
  return `${datePart} - ${cat} - Run ${runNo}`;
}


function extractRunNumberFromBatchId_(batchId) {
  const m = String(batchId || "").match(/-(\d{3})$/);
  return m ? String(parseInt(m[1], 10)) : "";
}

/*******************************************************
 * GROUPING + SPLITTING
 *******************************************************/
function groupBy_(arr, fnKey) {
  const m = new Map();
  for (const x of arr) {
    const k = String(fnKey(x));
    if (!m.has(k)) m.set(k, []);
    m.get(k).push(x);
  }
  return m;
}

function splitByMaxUnits_(items, maxUnits) {
  const out = [];
  let cur = [];
  let sum = 0;

  for (const it of items) {
    const u = it.printUnits || 0;
    if (cur.length && (sum + u) > maxUnits) {
      out.push(cur);
      cur = [];
      sum = 0;
    }
    cur.push(it);
    sum += u;
  }
  if (cur.length) out.push(cur);
  return out;
}

function primaryCodeFromProfileKey_(profileKey) {
  const first = String(profileKey || "").split("|")[0].trim();
  return first.split(":")[0].trim() || "GEN";
}

/*******************************************************
 * DATE HELPERS (batching-specific; keep local for now)
 *******************************************************/
function startOfDay_(d) {
  const x = new Date(d.getTime());
  x.setHours(0, 0, 0, 0);
  return x;
}

function ymd_(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

/*************************
 * Royal Mail Batch Number
 **************************/
function refreshBatchNamesFromRoyalMailBatchNumber() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    const changed = refreshBatchNamesFromRM_();
    ss.toast(`Batch names refreshed: ${changed} updated`, "Batches", 6);
  } finally {
    lock.releaseLock();
  }
}

function refreshBatchNamesFromRM_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const shB = ss.getSheetByName(CFG.SHEETS.BATCHES);
  if (!shB) throw new Error(`Missing sheet: ${CFG.SHEETS.BATCHES}`);

  const bMap = headerMap_(shB);
  const bCols = CFG.COLS.BATCHES;

  const iId   = requireCol_(bMap, bCols.BatchID);
  const iDate = requireCol_(bMap, bCols.BatchDate);
  const iCat  = requireCol_(bMap, bCols.PrintCategory);
  const iName = requireCol_(bMap, bCols.PrintBatchName);
  const iRM   = requireCol_(bMap, bCols.RoyalMailBatchNumber);

  const lastRow = shB.getLastRow();
  const lastCol = shB.getLastColumn();
  if (lastRow < 2) return 0;

  const values = shB.getRange(2, 1, lastRow - 1, lastCol).getValues();

  const outNames = new Array(values.length);
  let changed = 0;

  for (let r = 0; r < values.length; r++) {
    const row = values[r];
    const rm = String(row[iRM] || "").trim();
    const batchId = String(row[iId] || "").trim();
    const dt = parseDate_(row[iDate]);
    const cat = String(row[iCat] || "").trim();

    const current = String(row[iName] || "").trim();
    const desired = (dt && batchId)
      ? makePrintBatchName_(dt, cat, batchId, rm)
      : current;

    outNames[r] = [desired];
    if (desired !== current) changed++;
  }

  if (changed) {
    shB.getRange(2, iName + 1, outNames.length, 1).setValues(outNames);
  }

  return changed;
}

