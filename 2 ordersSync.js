/***************************************
 * ordersSync.js
 *
 * Orchestration + triggers + Step 3:
 * - syncOrdersFromOrderItems()
 * - install/remove time trigger
 * - upsert Orders from READY OrderItems (delta-write focused)
 ***************************************/

function syncOrdersFromOrderItems() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    const res = normalizeAndEnrichOrderItems_();
    const up = upsertOrdersFromReadyOrderItems_({
      startRow: Math.max(2, (res.checkpointSetToRow - getDefaultOrderItemsOverlap_())),
      endRow: Math.max(1, res.checkpointSetToRow)
    });

    ss.toast(
      `Sync complete. Scanned: ${res.scanned}, Changed: ${res.changed}, Exceptions: ${res.exceptions}, Orders touched: ${up.touchedOrders}, Orders updated: ${up.updatedRows}, Orders appended: ${up.appendedRows}`,
      "Sync",
      8
    );
  } finally {
    lock.releaseLock();
  }
}

function installOrdersSyncTrigger() {
  removeOrdersSyncTrigger();

  const fn = (CFG.TRIGGER && CFG.TRIGGER.FUNCTION_NAME) ? CFG.TRIGGER.FUNCTION_NAME : "syncOrdersFromOrderItems";
  ScriptApp.newTrigger(fn)
    .timeBased()
    .everyMinutes(CFG.TRIGGER.EVERY_MINUTES)
    .create();
}

function removeOrdersSyncTrigger() {
  const fn = (CFG.TRIGGER && CFG.TRIGGER.FUNCTION_NAME) ? CFG.TRIGGER.FUNCTION_NAME : "syncOrdersFromOrderItems";
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === fn) ScriptApp.deleteTrigger(t);
  });
}

function forceRepairReadyForOrdersOneOff() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    resetOrderItemsCheckpoint_();
    normalizeAndEnrichOrderItems_({ overlapRows: 0 });
    upsertOrdersFromReadyOrderItems_({ forceFullOrderRecompute: true });

    ss.toast("Repair complete: derived fields recomputed (full rescan).", "Repair", 6);
  } finally {
    lock.releaseLock();
  }
}

/*******************************************************
 * STEP 3 — UPSERT Orders from OrderItems (READY ONLY)
 *
 * Performance notes:
 * - Uses scan window to discover touched OrderNames.
 * - Re-aggregates those touched orders across OrderItems truth source.
 * - Writes only changed Orders rows + appends missing orders.
 *******************************************************/
function upsertOrdersFromReadyOrderItems_(options) {
  const opts = options || {};

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const shOI = ss.getSheetByName(CFG.SHEETS.ORDER_ITEMS);
  const shO  = ss.getSheetByName(CFG.SHEETS.ORDERS);
  if (!shOI) throw new Error(`Missing sheet: ${CFG.SHEETS.ORDER_ITEMS}`);
  if (!shO)  throw new Error(`Missing sheet: ${CFG.SHEETS.ORDERS}`);

  const oiMap = headerMap_(shOI);
  const oMap  = headerMap_(shO);

  const cOI = CFG.COLS.ORDER_ITEMS;
  const cO  = CFG.COLS.ORDERS;

  const iOiOrder  = requireCol_(oiMap, cOI.OrderName);
  const iOiCreate = requireCol_(oiMap, cOI.CreatedAt);
  const iOiReady  = requireCol_(oiMap, cOI.ReadyForOrders);

  const iOiBatch  = optionalCol_(oiMap, cOI.PrintBatchID);
  const iOiPrint  = optionalCol_(oiMap, cOI.PrintedAt);
  const iOiPack   = optionalCol_(oiMap, cOI.PackedAt);
  const iOiPackBy = optionalCol_(oiMap, cOI.PackedBy);

  const oOrder    = requireCol_(oMap, cO.OrderName);
  const oCreated  = requireCol_(oMap, cO.CreatedAt);
  const oStatus   = requireCol_(oMap, cO.Status);
  const oPackedAt = optionalCol_(oMap, cO.PackedAt);
  const oPackedBy = optionalCol_(oMap, cO.PackedBy);

  const oiLastRow = shOI.getLastRow();
  const oiLastCol = shOI.getLastColumn();
  if (oiLastRow < 2) return { touchedOrders: 0, updatedRows: 0, appendedRows: 0 };

  const touchedOrderNames = new Set();

  if (opts.forceFullOrderRecompute === true) {
    const allOi = shOI.getRange(2, 1, oiLastRow - 1, oiLastCol).getValues();
    for (const row of allOi) {
      const orderName = String(row[iOiOrder] || "").trim();
      if (orderName) touchedOrderNames.add(orderName);
    }
  } else {
    const startRow = Math.max(2, parseInt(opts.startRow, 10) || 2);
    const endRow = Math.min(oiLastRow, parseInt(opts.endRow, 10) || oiLastRow);

    if (endRow >= startRow) {
      const scanVals = shOI.getRange(startRow, 1, endRow - startRow + 1, oiLastCol).getValues();
      for (const row of scanVals) {
        const orderName = String(row[iOiOrder] || "").trim();
        if (orderName) touchedOrderNames.add(orderName);
      }
    }
  }

  if (!touchedOrderNames.size) return { touchedOrders: 0, updatedRows: 0, appendedRows: 0 };

  // Re-aggregate touched orders across full OrderItems truth source.
  const oi = shOI.getRange(2, 1, oiLastRow - 1, oiLastCol).getValues();
  const sums = new Map();

  for (const r of oi) {
    const orderName = String(r[iOiOrder] || "").trim();
    if (!orderName || !touchedOrderNames.has(orderName)) continue;

    if (!sums.has(orderName)) {
      sums.set(orderName, {
        orderName,
        allReady: true,
        earliestCreatedAt: null,
        anyInProdSignal: false,
        allPrinted: true,
        allPacked: true,
        maxPackedAt: null,
        lastPackedBy: ""
      });
    }

    const s = sums.get(orderName);
    const ready = isTrue_(r[iOiReady]);
    if (!ready) s.allReady = false;

    const createdAt = parseDate_(r[iOiCreate]);
    if (createdAt && (!s.earliestCreatedAt || createdAt.getTime() < s.earliestCreatedAt.getTime())) {
      s.earliestCreatedAt = createdAt;
    }

    if (iOiBatch >= 0 && String(r[iOiBatch] || "").trim()) s.anyInProdSignal = true;
    if (iOiPrint >= 0 && r[iOiPrint]) s.anyInProdSignal = true;
    if (iOiPack >= 0 && r[iOiPack]) s.anyInProdSignal = true;

    if (iOiPrint >= 0) {
      if (!r[iOiPrint]) s.allPrinted = false;
    } else {
      s.allPrinted = false;
    }

    if (iOiPack >= 0) {
      if (!r[iOiPack]) s.allPacked = false;
      const packedAt = parseDate_(r[iOiPack]);
      if (packedAt && (!s.maxPackedAt || packedAt.getTime() > s.maxPackedAt.getTime())) s.maxPackedAt = packedAt;
    } else {
      s.allPacked = false;
    }

    if (iOiPackBy >= 0) {
      const pb = String(r[iOiPackBy] || "").trim();
      if (pb) s.lastPackedBy = pb;
    }
  }

  const readyOrders = [];
  for (const s of sums.values()) if (s.allReady) readyOrders.push(s);
  if (!readyOrders.length) return { touchedOrders: touchedOrderNames.size, updatedRows: 0, appendedRows: 0 };

  const oLastRow = shO.getLastRow();
  const oLastCol = shO.getLastColumn();
  const oValues = (oLastRow >= 2) ? shO.getRange(2, 1, oLastRow - 1, oLastCol).getValues() : [];

  const byName = new Map();
  for (let i = 0; i < oValues.length; i++) {
    const nm = String(oValues[i][oOrder] || "").trim();
    if (nm) byName.set(nm, i);
  }

  const headers = getHeaders_(shO);
  const toAppend = [];
  const changedRowIndices = [];

  for (const s of readyOrders) {
    const derivedStatus = deriveOrderStatus_(s);

    if (!byName.has(s.orderName)) {
      const row = new Array(headers.length).fill("");
      row[oOrder] = s.orderName;
      row[oCreated] = s.earliestCreatedAt || "";
      row[oStatus] = derivedStatus;

      if (oPackedAt >= 0 && derivedStatus === CFG.STATUS.PACKED) row[oPackedAt] = s.maxPackedAt || "";
      if (oPackedBy >= 0 && derivedStatus === CFG.STATUS.PACKED) row[oPackedBy] = s.lastPackedBy || "";

      toAppend.push(row);
      continue;
    }

    const idx = byName.get(s.orderName);
    const row = oValues[idx];
    let rowChanged = false;

    const existingCreated = parseDate_(row[oCreated]);
    if (!existingCreated || (s.earliestCreatedAt && existingCreated.getTime() > s.earliestCreatedAt.getTime())) {
      row[oCreated] = s.earliestCreatedAt || row[oCreated];
      rowChanged = true;
    }

    const currentStatus = String(row[oStatus] || "").trim();
    if (!isManualOrFinalOrderStatus_(currentStatus) && currentStatus !== derivedStatus) {
      row[oStatus] = derivedStatus;
      rowChanged = true;
    }

    if (derivedStatus === CFG.STATUS.PACKED) {
      if (oPackedAt >= 0 && !row[oPackedAt] && s.maxPackedAt) { row[oPackedAt] = s.maxPackedAt; rowChanged = true; }
      if (oPackedBy >= 0 && !String(row[oPackedBy] || "").trim() && s.lastPackedBy) { row[oPackedBy] = s.lastPackedBy; rowChanged = true; }
    }

    if (rowChanged) changedRowIndices.push(idx);
  }

  if (toAppend.length) {
    shO.getRange(shO.getLastRow() + 1, 1, toAppend.length, headers.length).setValues(toAppend);
  }

  if (changedRowIndices.length) {
    changedRowIndices.sort((a, b) => a - b);
    const runs = [];
    let start = changedRowIndices[0];
    let prev = start;
    for (let i = 1; i < changedRowIndices.length; i++) {
      const cur = changedRowIndices[i];
      if (cur === prev + 1) prev = cur;
      else { runs.push([start, prev]); start = cur; prev = cur; }
    }
    runs.push([start, prev]);

    for (const run of runs) {
      const s = run[0];
      const e = run[1];
      const num = e - s + 1;
      const block = new Array(num);
      for (let i = 0; i < num; i++) block[i] = oValues[s + i];
      shO.getRange(2 + s, 1, num, headers.length).setValues(block);
    }
  }

  const createdAtColO1 = requireCol_(oMap, CFG.COLS.ORDERS.CreatedAt) + 1;
  shO.getRange(1, createdAtColO1, Math.max(shO.getLastRow(), 1), 1).setNumberFormat(CFG.FORMATS.DATETIME_UK);

  return {
    touchedOrders: touchedOrderNames.size,
    updatedRows: changedRowIndices.length,
    appendedRows: toAppend.length
  };
}

function deriveOrderStatus_(s) {
  if (s.allPacked) return CFG.STATUS.PACKED;
  if (s.allPrinted) return CFG.STATUS.READY;
  if (s.anyInProdSignal) return CFG.STATUS.IN_PROD;
  return CFG.STATUS.NEW;
}

/*******************************************************
 * ADMIN — CHECKPOINT TOOLS
 *******************************************************/

/**
 * Resets the OrderItems checkpoint to force a full rescan on the next run.
 * Does not itself modify OrderItems.
 */
function adminResetOrderItemsCheckpoint() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    resetOrderItemsCheckpoint_();
    ss.toast("OrderItems checkpoint reset. Next sync will rescan the full sheet.", "Admin", 6);
  } finally {
    lock.releaseLock();
  }
}

/**
 * Resets checkpoint and immediately performs a full normalize/enrich pass.
 */
function adminRunFullOrderItemsRescanNow() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    resetOrderItemsCheckpoint_();

    const res = normalizeAndEnrichOrderItems_({ overlapRows: 0 });
    const up = upsertOrdersFromReadyOrderItems_({ forceFullOrderRecompute: true });

    ss.toast(
      `Full rescan complete. Scanned: ${res.scanned}, Changed: ${res.changed}, Exceptions: ${res.exceptions}, Orders updated: ${up.updatedRows}, Orders appended: ${up.appendedRows}.`,
      "Admin",
      8
    );
  } finally {
    lock.releaseLock();
  }
}
