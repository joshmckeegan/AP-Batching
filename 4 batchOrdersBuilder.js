/*******************************************************
 * batchOrdersBuilder.js
 *
 * Rebuilds BatchOrders from OrderItems, joined to:
 * - Batches (for PrintBatchName)
 * - Orders  (for OrderCreatedAt, OrderStatus)
 *
 * Truth source: OrderItems
 *
 * NOTES
 * - Uses helpers.js for headerMap_/requireCol_/parseDate_/toInt_
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

  // --- Header maps (helpers.js)
  const oiMap = headerMap_(shOI);
  const bMap  = headerMap_(shB);
  const oMap  = headerMap_(shO);
  const boMap = headerMap_(shBO);

  // --- Required indices (0-based)
  const iOiOrder = requireCol_(oiMap, CFG.COLS.ORDER_ITEMS.OrderName);
  const iOiBatch = requireCol_(oiMap, CFG.COLS.ORDER_ITEMS.PrintBatchID);
  const iOiUnits = requireCol_(oiMap, CFG.COLS.ORDER_ITEMS.PrintUnits);

  const iBBatch = requireCol_(bMap, CFG.COLS.BATCHES.BatchID);
  const iBName  = requireCol_(bMap, CFG.COLS.BATCHES.PrintBatchName);
  const iBRM = requireCol_(bMap, CFG.COLS.BATCHES.RoyalMailBatchNumber);

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
  const iBoRM = requireCol_(boMap, boCols.RoyalMailBatchNumber);

  // --- Read source tables (bulk)
  const oi = readDataRange_(shOI);
  const b  = readDataRange_(shB);
  const o  = readDataRange_(shO);

  // --- Lookup: BatchID -> PrintBatchName
  const batchNameById = new Map();
  const batchRmById = new Map();

  for (const r of b.values) {
  const id = String(r[iBBatch] || "").trim();
  if (!id) continue;

  batchNameById.set(id, String(r[iBName] || "").trim());
  batchRmById.set(id, String(r[iBRM] || "").trim());
}

  // --- Lookup: OrderName -> {createdAt, status}
  const orderInfoByName = new Map();
  for (const r of o.values) {
    const name = String(r[iOOrder] || "").trim();
    if (!name) continue;
    orderInfoByName.set(name, {
      createdAt: parseDate_(r[iOCreated]) || "",
      status: String(r[iOStatus] || "").trim()
    });
  }

  // --- Aggregate OrderItems by (BatchID, OrderName)
  // key = `${BatchID}|${OrderName}`
  const agg = new Map();

  for (const r of oi.values) {
    const batchId = String(r[iOiBatch] || "").trim();
    if (!batchId) continue;

    const orderName = String(r[iOiOrder] || "").trim();
    if (!orderName) continue;

    const key = `${batchId}|${orderName}`;
    if (!agg.has(key)) {
      agg.set(key, { batchId, orderName, itemCount: 0, units: 0 });
    }

    const a = agg.get(key);
    a.itemCount += 1;
    a.units += toInt_(r[iOiUnits], 0);
  }

  // --- Build output rows aligned to BatchOrders headers
  const headers = getHeaders_(shBO); // helpers.js
  const now = new Date();

  // Stable ordering (prevents rows jumping around on each rebuild)
  const keys = Array.from(agg.keys()).sort((a, b) => a.localeCompare(b));

  const out = [];
  for (const key of keys) {
    const a = agg.get(key);
    const row = new Array(headers.length).fill("");

    row[iBoID]    = key;
    row[iBoBatch] = a.batchId;
    row[iBoName]  = batchNameById.get(a.batchId) || "";
    row[iBoOrder] = a.orderName;
    row[iBoRM] = batchRmById.get(a.batchId) || "";

    const oi2 = orderInfoByName.get(a.orderName);
    row[iBoOCreat] = oi2 ? oi2.createdAt : "";
    row[iBoStat] = oi2 ? oi2.status : CFG.STATUS.NEW;

    row[iBoCount] = a.itemCount;
    row[iBoUnits] = a.units;
    row[iBoUpd]   = now;

    out.push(row);
  }

  // --- Clear existing data rows only (not headers)
  const existingLastRow = shBO.getLastRow();
  const boLastCol = shBO.getLastColumn();
  if (existingLastRow >= 2) {
    shBO.getRange(2, 1, existingLastRow - 1, boLastCol).clearContent();
  }

  // --- Write fresh
  if (out.length) {
    shBO.getRange(2, 1, out.length, headers.length).setValues(out);
  }

  // --- Formats (only apply to written rows, not getMaxRows())
  const rowsToFormat = Math.max(out.length, 1);
  shBO.getRange(2, iBoOCreat + 1, rowsToFormat, 1).setNumberFormat(CFG.FORMATS.DATETIME_UK);
  shBO.getRange(2, iBoUpd + 1,   rowsToFormat, 1).setNumberFormat(CFG.FORMATS.DATETIME_UK);
  shBO.getRange(2, iBoCount + 1, rowsToFormat, 1).setNumberFormat("0");
  shBO.getRange(2, iBoUnits + 1, rowsToFormat, 1).setNumberFormat("0");

  ss.toast(`BatchOrders rebuilt: ${out.length} rows`, "BatchOrders", 6);
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
