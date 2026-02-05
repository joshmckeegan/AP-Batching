/***************************************
 * orderItemsSetup.js
 *
 * Step 1 + Step 2:
 * - Normalize OrderItems.CreatedAt
 * - Enrich OrderItems from SKU_Matrix:
 *     PrintCategory, PrintProfileKey, PrintUnits, ReadyForOrders
 *
 * Includes checkpointed scanning for performance.
 ***************************************/

function normalizeAndEnrichOrderItems_(opts) {
  const options = opts || {};

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const shOI = ss.getSheetByName(CFG.SHEETS.ORDER_ITEMS);
  const shM  = ss.getSheetByName(CFG.SHEETS.SKU_MATRIX);
  const shX  = ss.getSheetByName(CFG.SHEETS.EXCEPTIONS); // optional

  if (!shOI) throw new Error(`Missing sheet: ${CFG.SHEETS.ORDER_ITEMS}`);
  if (!shM)  throw new Error(`Missing sheet: ${CFG.SHEETS.SKU_MATRIX}`);

  const scan = getOrderItemsScanWindow_(shOI, options);
  if (!scan || scan.numRows <= 0) {
    return { scanned: 0, changed: 0, exceptions: 0, checkpointSetToRow: scan ? scan.endRow : 0 };
  }

  const oiMap = headerMap_(shOI);
  const mMap  = headerMap_(shM);

  const cOI = CFG.COLS.ORDER_ITEMS;
  const iCreatedAt  = requireCol_(oiMap, cOI.CreatedAt);
  const iOrderName  = requireCol_(oiMap, cOI.OrderName);
  const iSKU        = requireCol_(oiMap, cOI.SKU);
  const iQty        = requireCol_(oiMap, cOI.Qty);
  const iUnits      = requireCol_(oiMap, cOI.PrintUnits);
  const iReady      = requireCol_(oiMap, cOI.ReadyForOrders);
  const iProfileKey = requireCol_(oiMap, cOI.PrintProfileKey);

  const iCategory =
    (oiMap[cOI.PrintCategory] !== undefined) ? oiMap[cOI.PrintCategory]
    : (oiMap["ProductType"] !== undefined ? oiMap["ProductType"] : -1);
  if (iCategory === -1) throw new Error(`Missing OrderItems column: "${cOI.PrintCategory}"`);

  const iLineItemID = (oiMap[cOI.LineItemID] !== undefined) ? oiMap[cOI.LineItemID] : -1;

  const cM = CFG.COLS.SKU_MATRIX;
  const mSKU      = requireCol_(mMap, cM.SKU);
  const mMode     = requireCol_(mMap, cM.PrintMode);
  const mKey      = requireCol_(mMap, cM.PrintProfileKey);
  const mCategory = requireCol_(mMap, cM.PrintCategory);

  // Format CreatedAt only to used rows
  const createdAtCol1 = iCreatedAt + 1;
  shOI.getRange(1, createdAtCol1, Math.max(shOI.getLastRow(), 1), 1)
      .setNumberFormat(CFG.FORMATS.DATETIME_UK);

  const skuMap = buildSkuMap_(shM, { mSKU, mMode, mKey, mCategory });

  const lastCol = shOI.getLastColumn();
  const values = shOI.getRange(scan.startRow, 1, scan.numRows, lastCol).getValues();

  const outCategory   = new Array(scan.numRows);
  const outProfileKey = new Array(scan.numRows);
  const outUnits      = new Array(scan.numRows);
  const outReady      = new Array(scan.numRows);
  const outCreatedAt  = new Array(scan.numRows);

  let writeCategory = false;
  let writeProfile  = false;
  let writeUnits    = false;
  let writeReady    = false;
  let writeCreated  = false;

  const blankishValues = getBlankishValues_();
  const exceptionRows = [];
  const logMissingSku = !!(CFG.DERIVE && CFG.DERIVE.ON_MISSING_SKU && CFG.DERIVE.ON_MISSING_SKU.LOG_EXCEPTION);

  let changedCount = 0;
  let removedDigitalCount = 0;
  const sheetRowsToDelete = [];

  for (let r = 0; r < scan.numRows; r++) {
    const row = values[r];

    // Step 1: normalize CreatedAt
    const vCreated = row[iCreatedAt];
    if (!(vCreated instanceof Date) || isNaN(vCreated.getTime())) {
      const d = parseDate_(vCreated);
      if (d) {
        outCreatedAt[r] = [d];
        writeCreated = true;
      } else {
        outCreatedAt[r] = [vCreated];
      }
    } else {
      outCreatedAt[r] = [vCreated];
    }

    // Step 2: enrich & gate
    const sku = String(row[iSKU] || "").trim();
    const qty = toInt_(row[iQty], 0);

    // Rule: digital items (BDD SKU suffix) are removed from OrderItems entirely.
    if (isDigitalSku_(sku)) {
      outCreatedAt[r] = [row[iCreatedAt]];
      outCategory[r] = [row[iCategory]];
      outProfileKey[r] = [row[iProfileKey]];
      outUnits[r] = [row[iUnits]];
      outReady[r] = [row[iReady]];

      sheetRowsToDelete.push(scan.startRow + r);
      removedDigitalCount++;
      continue;
    }

    const currentReady = isTrue_(row[iReady]);
    const catBlankish = isBlankish_(row[iCategory], blankishValues);
    const keyBlankish = isBlankish_(row[iProfileKey], blankishValues);

    if (currentReady && !catBlankish && !keyBlankish) {
      outCategory[r]   = [row[iCategory]];
      outProfileKey[r] = [row[iProfileKey]];
      outUnits[r]      = [row[iUnits]];
      outReady[r]      = [row[iReady]];
      continue;
    }

    // Missing SKU
    if (!sku) {
      const fb = (CFG.DERIVE && CFG.DERIVE.ON_MISSING_SKU) ? CFG.DERIVE.ON_MISSING_SKU : {};
      outCategory[r]   = [fb.PRINT_CATEGORY || "Unknown"];
      outProfileKey[r] = [""];
      outUnits[r]      = [toInt_(fb.PRINT_UNITS, 0)];
      outReady[r]      = [false];

      writeCategory = writeProfile = writeUnits = writeReady = true;
      changedCount++;

      if (logMissingSku && shX) {
        exceptionRows.push([
          new Date(),
          "MISSING_SKU",
          String(row[iOrderName] || ""),
          (iLineItemID >= 0 ? String(row[iLineItemID] || "") : ""),
          "",
          "SKU is blank; applied fallback values and set ReadyForOrders = FALSE."
        ]);
      }
      continue;
    }

    const info = skuMap.get(sku);

    // SKU not in matrix
    if (!info) {
      const fb = (CFG.DERIVE && CFG.DERIVE.ON_MISSING_SKU) ? CFG.DERIVE.ON_MISSING_SKU : {};
      outCategory[r]   = [fb.PRINT_CATEGORY || "Unknown"];
      outProfileKey[r] = [""];
      outUnits[r]      = [toInt_(fb.PRINT_UNITS, 0)];
      outReady[r]      = [false];

      writeCategory = writeProfile = writeUnits = writeReady = true;
      changedCount++;

      if (logMissingSku && shX) {
        exceptionRows.push([
          new Date(),
          "SKU_NOT_IN_MATRIX",
          String(row[iOrderName] || ""),
          (iLineItemID >= 0 ? String(row[iLineItemID] || "") : ""),
          sku,
          "SKU not found in SKU_Matrix; applied fallback values and set ReadyForOrders = FALSE."
        ]);
      }
      continue;
    }

    const nextCat = info.cat || deriveCategoryFallback_(info.mode, info.key);
    const nextKey = info.key || "";

    outCategory[r]   = [nextCat];
    outProfileKey[r] = [nextKey];
    writeCategory = true;
    writeProfile  = true;

    const desiredUnits = qty * (info.totalPerUnit || 0);
    const currentUnits = toInt_(row[iUnits], 0);

    const shouldFixUnits =
      (currentUnits <= 0) ||
      (currentUnits === qty && desiredUnits !== qty);

    const nextUnits = ((CFG.DERIVE && CFG.DERIVE.OVERWRITE_EXISTING) || shouldFixUnits) ? desiredUnits : currentUnits;
    outUnits[r] = [nextUnits];
    if (nextUnits !== currentUnits) writeUnits = true;

    const mode = String(info.mode || "").toUpperCase();
    const catOk = !!String(nextCat || "").trim() && !isBlankish_(nextCat, blankishValues);
    const keyOk = (mode === "NONE") ? true : !!String(nextKey || "").trim();
    const unitsOk = (mode === "NONE") ? true : (toInt_(nextUnits, 0) > 0 || desiredUnits > 0);

    outReady[r] = [(catOk && keyOk && unitsOk)];
    writeReady = true;

    changedCount++;
  }

  // Bulk writes
  if (writeCreated)  shOI.getRange(scan.startRow, iCreatedAt + 1, scan.numRows, 1).setValues(outCreatedAt);
  if (writeCategory) shOI.getRange(scan.startRow, iCategory + 1, scan.numRows, 1).setValues(outCategory);
  if (writeProfile)  shOI.getRange(scan.startRow, iProfileKey + 1, scan.numRows, 1).setValues(outProfileKey);
  if (writeUnits)    shOI.getRange(scan.startRow, iUnits + 1, scan.numRows, 1).setValues(outUnits);
  if (writeReady)    shOI.getRange(scan.startRow, iReady + 1, scan.numRows, 1).setValues(outReady);

  if (exceptionRows.length && shX) {
    shX.getRange(shX.getLastRow() + 1, 1, exceptionRows.length, exceptionRows[0].length).setValues(exceptionRows);
  }

  if (sheetRowsToDelete.length) {
    deleteRowsByIndices_(shOI, sheetRowsToDelete);
  }

  const checkpointRow = shOI.getLastRow();
  setOrderItemsCheckpoint_(checkpointRow);

  return {
    scanned: scan.numRows,
    changed: changedCount,
    removedDigital: removedDigitalCount,
    exceptions: exceptionRows.length,
    checkpointSetToRow: checkpointRow
  };
}

function isDigitalSku_(sku) {
  const s = String(sku || "").trim().toUpperCase();
  return !!s && s.endsWith("BDD");
}

function deleteRowsByIndices_(sh, sheetRows1) {
  const rows = Array.from(new Set((sheetRows1 || []).map(x => parseInt(x, 10)).filter(x => Number.isFinite(x) && x >= 2)))
    .sort((a, b) => b - a);
  if (!rows.length) return;

  let runStart = rows[0];
  let runLen = 1;

  for (let i = 1; i < rows.length; i++) {
    const cur = rows[i];
    if (cur === runStart - runLen) {
      runLen++;
    } else {
      sh.deleteRows(runStart - runLen + 1, runLen);
      runStart = cur;
      runLen = 1;
    }
  }

  sh.deleteRows(runStart - runLen + 1, runLen);
}

function buildSkuMap_(shM, idx) {
  const lastRow = shM.getLastRow();
  const lastCol = shM.getLastColumn();
  const values = (lastRow >= 2) ? shM.getRange(2, 1, lastRow - 1, lastCol).getValues() : [];

  const skuMap = new Map();
  for (const r of values) {
    const sku = String(r[idx.mSKU] || "").trim();
    if (!sku) continue;

    const cat  = String(r[idx.mCategory] || "").trim();
    const mode = String(r[idx.mMode] || "").trim().toUpperCase();
    const key  = String(r[idx.mKey] || "").trim();

    const totalPerUnit = (mode === "NONE") ? 0 : sumPrintCounts_(key);
    skuMap.set(sku, { cat, mode, key, totalPerUnit });
  }
  return skuMap;
}

/**
 * Checkpointed scan window: process only new/changed rows since last run,
 * with a small overlap safety net.
 */
function getOrderItemsScanWindow_(shOI, options) {
  const lastRow = shOI.getLastRow();
  if (lastRow < 2) {
    setOrderItemsCheckpoint_(1);
    return { startRow: 2, endRow: 1, numRows: 0 };
  }

  const props = PropertiesService.getDocumentProperties();
  const key = getOrderItemsCheckpointKey_();
  const saved = parseInt(props.getProperty(key) || "1", 10);
  const checkpointRow = Number.isFinite(saved) ? saved : 1;

  const overlap = (options.overlapRows !== undefined)
    ? Math.max(0, parseInt(options.overlapRows, 10) || 0)
    : getDefaultOrderItemsOverlap_();

  const startRow = Math.max(2, (checkpointRow - overlap));
  const endRow = lastRow;

  return (startRow > endRow)
    ? { startRow, endRow, numRows: 0 }
    : { startRow, endRow, numRows: (endRow - startRow + 1) };
}

function getDefaultOrderItemsOverlap_() {
  const v = (CFG.PERF && CFG.PERF.CHECKPOINT_OVERLAP !== undefined) ? CFG.PERF.CHECKPOINT_OVERLAP : 200;
  return Math.max(0, parseInt(v, 10) || 0);
}

function getOrderItemsCheckpointKey_() {
  return "ORDERITEMS_CHECKPOINT_LASTROW";
}

function setOrderItemsCheckpoint_(rowNum) {
  const props = PropertiesService.getDocumentProperties();
  props.setProperty(getOrderItemsCheckpointKey_(), String(Math.max(1, parseInt(rowNum, 10) || 1)));
}

function resetOrderItemsCheckpoint_() {
  setOrderItemsCheckpoint_(1);
}
