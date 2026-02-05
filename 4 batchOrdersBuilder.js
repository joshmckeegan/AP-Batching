/*******************************************************
 * batchOrdersBuilder.js
 *
 * Incremental-style rebuild of BatchOrders from OrderItems truth source,
 * joined with Batches + Orders. Avoids full clear/write by diffing on key.
 *******************************************************/

function rebuildBatchOrders() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    _rebuildBatchOrders_();
  } finally {
    lock.releaseLock();
  }
}

function _rebuildBatchOrders_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  const shOI = ss.getSheetByName(CFG.SHEETS.ORDER_ITEMS);
  const shB  = ss.getSheetByName(CFG.SHEETS.BATCHES);
  const shO  = ss.getSheetByName(CFG.SHEETS.ORDERS);
  const shBO = ss.getSheetByName(CFG.SHEETS.BATCH_ORDERS);

  if (!shOI) throw new Error(`Missing sheet: ${CFG.SHEETS.ORDER_ITEMS}`);
  if (!shB)  throw new Error(`Missing sheet: ${CFG.SHEETS.BATCHES}`);
  if (!shO)  throw new Error(`Missing sheet: ${CFG.SHEETS.ORDERS}`);
  if (!shBO) throw new Error(`Missing sheet: ${CFG.SHEETS.BATCH_ORDERS}`);

  const oiMap = headerMap_(shOI);
  const bMap  = headerMap_(shB);
  const oMap  = headerMap_(shO);
  const boMap = headerMap_(shBO);

  const iOiOrder = requireCol_(oiMap, CFG.COLS.ORDER_ITEMS.OrderName);
  const iOiBatch = requireCol_(oiMap, CFG.COLS.ORDER_ITEMS.PrintBatchID);
  const iOiUnits = requireCol_(oiMap, CFG.COLS.ORDER_ITEMS.PrintUnits);

  const iBBatch = requireCol_(bMap, CFG.COLS.BATCHES.BatchID);
  const iBName  = requireCol_(bMap, CFG.COLS.BATCHES.PrintBatchName);
  const iBRM    = requireCol_(bMap, CFG.COLS.BATCHES.RoyalMailBatchNumber);

  const iOOrder   = requireCol_(oMap, CFG.COLS.ORDERS.OrderName);
  const iOCreated = requireCol_(oMap, CFG.COLS.ORDERS.CreatedAt);
  const iOStatus  = requireCol_(oMap, CFG.COLS.ORDERS.Status);

  const boCols = CFG.COLS.BATCH_ORDERS;
  const iBoID     = requireCol_(boMap, boCols.BatchOrderID);
  const iBoBatch  = requireCol_(boMap, boCols.BatchID);
  const iBoName   = requireCol_(boMap, boCols.PrintBatchName);
  const iBoOrder  = requireCol_(boMap, boCols.OrderName);
  const iBoOCreat = requireCol_(boMap, boCols.OrderCreatedAt);
  const iBoStat   = requireCol_(boMap, boCols.OrderStatus);
  const iBoCount  = requireCol_(boMap, boCols.OrderItemCount);
  const iBoUnits  = requireCol_(boMap, boCols.PrintUnits);
  const iBoUpd    = requireCol_(boMap, boCols.LastUpdatedAt);
  const iBoRM     = requireCol_(boMap, boCols.RoyalMailBatchNumber);

  const oi = readDataRange_(shOI);
  const b  = readDataRange_(shB);
  const o  = readDataRange_(shO);
  const bo = readDataRange_(shBO);

  const batchNameById = new Map();
  const batchRmById = new Map();
  for (const r of b.values) {
    const id = String(r[iBBatch] || "").trim();
    if (!id) continue;
    batchNameById.set(id, String(r[iBName] || "").trim());
    batchRmById.set(id, String(r[iBRM] || "").trim());
  }

  const orderInfoByName = new Map();
  for (const r of o.values) {
    const name = String(r[iOOrder] || "").trim();
    if (!name) continue;
    orderInfoByName.set(name, {
      createdAt: parseDate_(r[iOCreated]) || "",
      status: String(r[iOStatus] || "").trim()
    });
  }

  const agg = new Map();
  for (const r of oi.values) {
    const batchId = String(r[iOiBatch] || "").trim();
    if (!batchId) continue;

    const orderName = String(r[iOiOrder] || "").trim();
    if (!orderName) continue;

    const key = `${batchId}|${orderName}`;
    if (!agg.has(key)) agg.set(key, { batchId, orderName, itemCount: 0, units: 0 });

    const a = agg.get(key);
    a.itemCount += 1;
    a.units += toInt_(r[iOiUnits], 0);
  }

  const headers = getHeaders_(shBO);
  const now = new Date();

  // Desired rows by BatchOrderID
  const desiredById = new Map();
  const desiredIds = Array.from(agg.keys()).sort((a, b) => a.localeCompare(b));

  for (const key of desiredIds) {
    const a = agg.get(key);
    const row = new Array(headers.length).fill("");

    row[iBoID] = key;
    row[iBoBatch] = a.batchId;
    row[iBoName] = batchNameById.get(a.batchId) || "";
    row[iBoOrder] = a.orderName;
    row[iBoRM] = batchRmById.get(a.batchId) || "";

    const oi2 = orderInfoByName.get(a.orderName);
    row[iBoOCreat] = oi2 ? oi2.createdAt : "";
    row[iBoStat] = oi2 ? oi2.status : CFG.STATUS.NEW;

    row[iBoCount] = a.itemCount;
    row[iBoUnits] = a.units;
    row[iBoUpd] = now;

    desiredById.set(key, row);
  }

  // Existing rows by key
  const existingById = new Map();
  for (let i = 0; i < bo.values.length; i++) {
    const id = String(bo.values[i][iBoID] || "").trim();
    if (id) existingById.set(id, i);
  }

  const changedRowIndices = [];
  const appendRows = [];

  // Update + append
  for (const id of desiredIds) {
    const desired = desiredById.get(id);

    if (!existingById.has(id)) {
      appendRows.push(desired);
      continue;
    }

    const idx = existingById.get(id);
    const existingRow = bo.values[idx];

    // Preserve LastUpdatedAt from desired (now) while comparing all other values
    if (!rowsEquivalentForBatchOrders_(existingRow, desired, iBoUpd)) {
      bo.values[idx] = desired;
      changedRowIndices.push(idx);
    }
  }

  // Clear stale rows (keys no longer present)
  const staleRowIndices = [];
  for (const entry of existingById.entries()) {
    const id = entry[0];
    const idx = entry[1];
    if (!desiredById.has(id)) staleRowIndices.push(idx);
  }

  if (changedRowIndices.length) {
    writeContiguousRows_(shBO, bo.values, changedRowIndices, headers.length);
  }

  if (appendRows.length) {
    shBO.getRange(shBO.getLastRow() + 1, 1, appendRows.length, headers.length).setValues(appendRows);
  }

  if (staleRowIndices.length) {
    clearContiguousRows_(shBO, staleRowIndices, headers.length);
  }

  const totalRows = desiredIds.length;
  const rowsToFormat = Math.max(totalRows, 1);
  shBO.getRange(2, iBoOCreat + 1, rowsToFormat, 1).setNumberFormat(CFG.FORMATS.DATETIME_UK);
  shBO.getRange(2, iBoUpd + 1, rowsToFormat, 1).setNumberFormat(CFG.FORMATS.DATETIME_UK);
  shBO.getRange(2, iBoCount + 1, rowsToFormat, 1).setNumberFormat("0");
  shBO.getRange(2, iBoUnits + 1, rowsToFormat, 1).setNumberFormat("0");

  ss.toast(
    `BatchOrders synced: desired ${desiredIds.length}, updated ${changedRowIndices.length}, appended ${appendRows.length}, cleared ${staleRowIndices.length}`,
    "BatchOrders",
    6
  );
}

function rowsEquivalentForBatchOrders_(a, b, updatedAtIndex) {
  const len = Math.max(a.length, b.length);
  for (let i = 0; i < len; i++) {
    if (i === updatedAtIndex) continue;

    const av = a[i];
    const bv = b[i];

    if (av instanceof Date || bv instanceof Date) {
      const ad = parseDate_(av);
      const bd = parseDate_(bv);
      const at = ad ? ad.getTime() : "";
      const bt = bd ? bd.getTime() : "";
      if (at !== bt) return false;
      continue;
    }

    if (String(av || "") !== String(bv || "")) return false;
  }
  return true;
}

function writeContiguousRows_(sh, values, rowIndices0, width) {
  const sorted = Array.from(new Set(rowIndices0)).sort((a, b) => a - b);
  if (!sorted.length) return;

  const runs = [];
  let start = sorted[0];
  let prev = start;
  for (let i = 1; i < sorted.length; i++) {
    const cur = sorted[i];
    if (cur === prev + 1) prev = cur;
    else { runs.push([start, prev]); start = cur; prev = cur; }
  }
  runs.push([start, prev]);

  for (const run of runs) {
    const s = run[0];
    const e = run[1];
    const num = e - s + 1;
    const block = new Array(num);
    for (let i = 0; i < num; i++) block[i] = values[s + i];
    sh.getRange(2 + s, 1, num, width).setValues(block);
  }
}

function clearContiguousRows_(sh, rowIndices0, width) {
  const sorted = Array.from(new Set(rowIndices0)).sort((a, b) => a - b);
  if (!sorted.length) return;

  let start = sorted[0];
  let prev = start;
  for (let i = 1; i < sorted.length; i++) {
    const cur = sorted[i];
    if (cur === prev + 1) {
      prev = cur;
    } else {
      sh.getRange(2 + start, 1, prev - start + 1, width).clearContent();
      start = cur;
      prev = cur;
    }
  }
  sh.getRange(2 + start, 1, prev - start + 1, width).clearContent();
}

/**
 * Reads data rows (row 2+) across all columns that exist, returning:
 * { values: any[][], lastRow: number, lastCol: number }
 */
function readDataRange_(sh) {
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return { values: [], lastRow, lastCol };
  return {
    values: sh.getRange(2, 1, lastRow - 1, lastCol).getValues(),
    lastRow,
    lastCol
  };
}
