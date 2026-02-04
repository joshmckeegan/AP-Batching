/***************************************
 * ordersSync.js
 *
 * Orchestration + triggers + Step 3:
 * - syncOrdersFromOrderItems()
 * - install/remove time trigger
 * - upsert Orders from READY OrderItems
 ***************************************/

function syncOrdersFromOrderItems() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    const res = normalizeAndEnrichOrderItems_();
    upsertOrdersFromReadyOrderItems_();

    ss.toast(
      `Sync complete. Scanned: ${res.scanned}, Changed: ${res.changed}, Exceptions: ${res.exceptions}, Checkpoint row: ${res.checkpointSetToRow}`,
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
    // Full rescan by resetting checkpoint
    resetOrderItemsCheckpoint_();
    normalizeAndEnrichOrderItems_({ overlapRows: 0 });

    ss.toast("Repair complete: derived fields recomputed (full rescan).", "Repair", 6);
  } finally {
    lock.releaseLock();
  }
}

/*******************************************************
 * STEP 3 — UPSERT Orders from OrderItems (READY ONLY)
 *******************************************************/
function upsertOrdersFromReadyOrderItems_() {
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

  const iOiBatch  = (oiMap[cOI.PrintBatchID] !== undefined) ? oiMap[cOI.PrintBatchID] : -1;
  const iOiPrint  = (oiMap[cOI.PrintedAt] !== undefined) ? oiMap[cOI.PrintedAt] : -1;
  const iOiPack   = (oiMap[cOI.PackedAt] !== undefined) ? oiMap[cOI.PackedAt] : -1;
  const iOiPackBy = (oiMap[cOI.PackedBy] !== undefined) ? oiMap[cOI.PackedBy] : -1;

  const oOrder    = requireCol_(oMap, cO.OrderName);
  const oCreated  = requireCol_(oMap, cO.CreatedAt);
  const oStatus   = requireCol_(oMap, cO.Status);
  const oPackedAt = (oMap[cO.PackedAt] !== undefined) ? oMap[cO.PackedAt] : -1;
  const oPackedBy = (oMap[cO.PackedBy] !== undefined) ? oMap[cO.PackedBy] : -1;

  const oiLastRow = shOI.getLastRow();
  const oiLastCol = shOI.getLastColumn();
  if (oiLastRow < 2) return;

  const oi = shOI.getRange(2, 1, oiLastRow - 1, oiLastCol).getValues();

  const sums = new Map();

  for (const r of oi) {
    const orderName = String(r[iOiOrder] || "").trim();
    if (!orderName) continue;

    const ready = isTrue_(r[iOiReady]);

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
    if (!ready) s.allReady = false;

    const createdAt = parseDate_(r[iOiCreate]);
    if (createdAt && (!s.earliestCreatedAt || createdAt.getTime() < s.earliestCreatedAt.getTime())) {
      s.earliestCreatedAt = createdAt;
    }

    if (iOiBatch >= 0 && String(r[iOiBatch] || "").trim()) s.anyInProdSignal = true;
    if (iOiPrint >= 0 && r[iOiPrint]) s.anyInProdSignal = true;
    if (iOiPack >= 0 && r[iOiPack]) s.anyInProdSignal = true;

    if (iOiPrint >= 0) { if (!r[iOiPrint]) s.allPrinted = false; } else { s.allPrinted = false; }

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
  if (!readyOrders.length) return;

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
  let changed = false;

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

    const existingCreated = parseDate_(row[oCreated]);
    if (!existingCreated || (s.earliestCreatedAt && existingCreated.getTime() > s.earliestCreatedAt.getTime())) {
      row[oCreated] = s.earliestCreatedAt || row[oCreated];
      changed = true;
    }

    const currentStatus = String(row[oStatus] || "").trim();
      // Do not overwrite manual/API-controlled statuses
      if (!isManualOrFinalOrderStatus_(currentStatus) && currentStatus !== derivedStatus) {
      row[oStatus] = derivedStatus;
      changed = true;
    }


    if (derivedStatus === CFG.STATUS.PACKED) {
      if (oPackedAt >= 0 && !row[oPackedAt] && s.maxPackedAt) { row[oPackedAt] = s.maxPackedAt; changed = true; }
      if (oPackedBy >= 0 && !String(row[oPackedBy] || "").trim() && s.lastPackedBy) { row[oPackedBy] = s.lastPackedBy; changed = true; }
    }
  }

  if (toAppend.length) {
    shO.getRange(shO.getLastRow() + 1, 1, toAppend.length, headers.length).setValues(toAppend);
  }
  if (changed && oValues.length) {
    shO.getRange(2, 1, oValues.length, headers.length).setValues(oValues);
  }

  const createdAtColO1 = requireCol_(oMap, CFG.COLS.ORDERS.CreatedAt) + 1;
  shO.getRange(1, createdAtColO1, Math.max(shO.getLastRow(), 1), 1)
     .setNumberFormat(CFG.FORMATS.DATETIME_UK);
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
 * This does NOT upsert Orders unless you explicitly want that (kept separate).
 */
function adminRunFullOrderItemsRescanNow() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    resetOrderItemsCheckpoint_();

    const res = normalizeAndEnrichOrderItems_({ overlapRows: 0 });

    ss.toast(
      `Full rescan complete. Scanned: ${res.scanned}, Changed: ${res.changed}, Exceptions: ${res.exceptions}.`,
      "Admin",
      8
    );
  } finally {
    lock.releaseLock();
  }
}
