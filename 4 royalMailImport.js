/**
 * royalMailImport.js
 *
 * Watch-folder Royal Mail manifest import:
 * Watch Folder (.xls/.xlsx) -> convert -> parse -> upsert Shipments -> mirror Orders -> mirror BatchOrders -> update Batches shorthand -> archive file
 */

/**
 * One-time sheet setup (optional).
 */
function setupShipmentsSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const name = CFG.SHEETS.SHIPMENTS;

  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);

  const h = CFG.COLS.SHIPMENTS;
  const headers = [
    h.ShipmentID,
    h.OrderName,
    h.Postcode,
    h.RoyalMailTrackingNumber,
    h.RoyalMailManifestNo,
    h.RoyalMailBatchNumber,
    h.TrackingStatus,
    h.DespatchedAt,
    h.ShippingService,
    h.PackageSize,
    h.WeightKg,
    h.ImportedAt,
    h.SourceFileName,
  ];

  sh.getRange(1, 1, 1, headers.length).setValues([headers]);
  sh.setFrozenRows(1);
  sh.getRange(1, 1, 1, headers.length).setFontWeight("bold");
}

function installRoyalMailWatchTrigger() {
  removeRoyalMailWatchTrigger();

  const mins = (CFG.ROYAL_MAIL && CFG.ROYAL_MAIL.POLL_EVERY_MINUTES) ? CFG.ROYAL_MAIL.POLL_EVERY_MINUTES : 10;

  ScriptApp.newTrigger("pollRoyalMailWatchFolder")
    .timeBased()
    .everyMinutes(mins)
    .create();
}

function removeRoyalMailWatchTrigger() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === "pollRoyalMailWatchFolder") ScriptApp.deleteTrigger(t);
  });
}

/**
 * Poll watch folder for new .xls/.xlsx exports, import them, then move to archive.
 */
function pollRoyalMailWatchFolder() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);

  try {
    const watchId = CFG.ROYAL_MAIL && CFG.ROYAL_MAIL.WATCH_FOLDER_ID;
    const archiveId = CFG.ROYAL_MAIL && CFG.ROYAL_MAIL.ARCHIVE_FOLDER_ID;
    const maxFilesPerRun = (CFG.ROYAL_MAIL && CFG.ROYAL_MAIL.MAX_FILES_PER_RUN)
      ? Math.max(1, parseInt(CFG.ROYAL_MAIL.MAX_FILES_PER_RUN, 10) || 1)
      : 5;

    if (!watchId) throw new Error("CFG.ROYAL_MAIL.WATCH_FOLDER_ID is not set.");
    if (!archiveId) throw new Error("CFG.ROYAL_MAIL.ARCHIVE_FOLDER_ID is not set.");

    const props = PropertiesService.getDocumentProperties();
    const processedKey = "RM_PROCESSED_FILE_IDS";
    const processed = new Set(JSON.parse(props.getProperty(processedKey) || "[]"));

    const watch = DriveApp.getFolderById(watchId);
    const archive = DriveApp.getFolderById(archiveId);

    const files = watch.getFiles();
    const candidates = [];

    while (files.hasNext()) {
      const f = files.next();
      const name = f.getName();
      if (!/\.(xls|xlsx)$/i.test(name)) continue;
      if (processed.has(f.getId())) continue;
      candidates.push(f);
    }

    if (!candidates.length) return;

    candidates.sort((a, b) => a.getLastUpdated().getTime() - b.getLastUpdated().getTime());

    let filesDone = 0;
    for (const file of candidates) {
      if (filesDone >= maxFilesPerRun) break;

      const result = importRoyalMailManifestFile_(file);
      processed.add(file.getId());
      filesDone++;

      archive.addFile(file);
      watch.removeFile(file);

      ss.toast(`RM import: ${file.getName()} (shipments +${result.newShipments})`, "Royal Mail", 5);
    }

    props.setProperty(processedKey, JSON.stringify(Array.from(processed)));
    ss.toast(`Royal Mail: processed ${filesDone} file(s).`, "Royal Mail", 8);
  } finally {
    lock.releaseLock();
  }
}

/**
 * Convert, parse, upsert Shipments, mirror Orders/BatchOrders, update Batches shorthand.
 */
function importRoyalMailManifestFile_(file) {
  const sourceFileName = file.getName();

  const converted = convertToGoogleSheet_(file.getId(), sourceFileName);
  const convertedId = converted.id;

  try {
    const tmp = SpreadsheetApp.openById(convertedId);
    const sheet = tmp.getSheets()[0];
    const values = sheet.getDataRange().getValues();
    if (values.length < 2) return { newShipments: 0 };

    const headers = values[0].map(h => String(h || "").trim());
    const idx = (name) => headers.indexOf(name);

    const meta = {
      iOrderName: idx("Channel reference"),
      iRmBatch: idx("Batch number"),
      iPostcode: idx("Postcode"),
      iManifest: idx("Manifest number"),
      iDespatch: idx("Despatch date"),
      iTracking: idx("Tracking number"),
      iTrackingStatus: idx("Tracking status"),
      iService: idx("Shipping service"),
      iPkgSize: idx("Package size"),
      iWeightKg: idx("Weight (kg)"),
      sourceFileName
    };

    const required = [
      ["Channel reference", meta.iOrderName],
      ["Postcode", meta.iPostcode],
      ["Batch number", meta.iRmBatch],
      ["Manifest number", meta.iManifest],
      ["Despatch date", meta.iDespatch],
      ["Tracking number", meta.iTracking],
      ["Tracking status", meta.iTrackingStatus],
    ];

    const missing = required.filter(x => x[1] < 0).map(x => x[0]);
    if (missing.length) throw new Error("RM export missing columns: " + missing.join(", "));

    const rows = values.slice(1);
    const up = upsertShipmentsFromRMRows_(rows, meta);
    const touched = up.orderNamesTouched;

    mirrorShipmentsToOrders_(touched);
    mirrorOrdersToBatchOrders_(touched);
    updateBatchesRoyalMailShorthandFromBatchOrders_(touched);

    return { newShipments: up.appendedCount };
  } finally {
    try { DriveApp.getFileById(convertedId).setTrashed(true); } catch (e) {}
  }
}

function convertToGoogleSheet_(fileId, fileName) {
  const resource = {
    title: fileName.replace(/\.(xls|xlsx)$/i, "") + " (Converted)",
    mimeType: MimeType.GOOGLE_SHEETS
  };
  return Drive.Files.copy(resource, fileId, { convert: true });
}

/**
 * Upsert Shipments rows from RM export.
 * ShipmentID is stable: `${OrderName}|${TrackingNumber}`.
 */
function upsertShipmentsFromRMRows_(rows, meta) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(CFG.SHEETS.SHIPMENTS);
  if (!sh) throw new Error(`Missing sheet: ${CFG.SHEETS.SHIPMENTS}`);

  const map = headerMap_(sh);
  const c = CFG.COLS.SHIPMENTS;

  const iShipmentID = requireCol_(map, c.ShipmentID);
  const iOrderName  = requireCol_(map, c.OrderName);
  const iPostcode   = requireCol_(map, c.Postcode);
  const iTracking   = requireCol_(map, c.RoyalMailTrackingNumber);
  const iTStatus    = requireCol_(map, c.TrackingStatus);
  const iManifest   = requireCol_(map, c.RoyalMailManifestNo);
  const iRmBatch    = requireCol_(map, c.RoyalMailBatchNumber);
  const iDespatch   = requireCol_(map, c.DespatchedAt);
  const iService    = requireCol_(map, c.ShippingService);
  const iPkgSize    = requireCol_(map, c.PackageSize);
  const iWeightKg   = requireCol_(map, c.WeightKg);
  const iImportedAt = requireCol_(map, c.ImportedAt);
  const iSource     = requireCol_(map, c.SourceFileName);

  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  const existing = (lastRow >= 2) ? sh.getRange(2, 1, lastRow - 1, lastCol).getValues() : [];

  const byId = new Map();
  for (let r = 0; r < existing.length; r++) {
    const id = String(existing[r][iShipmentID] || "").trim();
    if (id) byId.set(id, r);
  }

  const headers = getHeaders_(sh);
  const now = new Date();

  let appendedCount = 0;
  const appendRows = [];
  const changedRows = [];
  const touched = new Set();
  const seenInFile = new Set();

  for (const r of rows) {
    const orderName = String(r[meta.iOrderName] || "").trim();
    const postcode = String(r[meta.iPostcode] || "").trim();
    const tracking = String(r[meta.iTracking] || "").trim();
    const trackingStatus = String(r[meta.iTrackingStatus] || "").trim();

    if (!orderName || !tracking) continue;

    const shipmentId = `${orderName}|${tracking}`;
    if (seenInFile.has(shipmentId)) continue;
    seenInFile.add(shipmentId);

    touched.add(orderName);

    const manifest = String(r[meta.iManifest] || "").trim();
    const rmBatch  = String(r[meta.iRmBatch] || "").trim();
    const despatchedAt = parseDate_(r[meta.iDespatch]) || "";
    const service = (meta.iService >= 0) ? String(r[meta.iService] || "").trim() : "";
    const pkgSize = (meta.iPkgSize >= 0) ? String(r[meta.iPkgSize] || "").trim() : "";
    const weight  = (meta.iWeightKg >= 0) ? r[meta.iWeightKg] : "";

    if (byId.has(shipmentId)) {
      const idx = byId.get(shipmentId);
      const row = existing[idx];

      row[iOrderName] = orderName;
      row[iPostcode] = postcode;
      row[iTracking] = tracking;
      row[iTStatus] = trackingStatus;
      row[iManifest] = manifest;
      row[iRmBatch] = rmBatch;
      row[iDespatch] = despatchedAt;
      row[iService] = service;
      row[iPkgSize] = pkgSize;
      row[iWeightKg] = weight;
      row[iImportedAt] = now;
      row[iSource] = meta.sourceFileName;

      changedRows.push(idx);
    } else {
      const row = new Array(headers.length).fill("");
      row[iShipmentID] = shipmentId;
      row[iOrderName] = orderName;
      row[iPostcode] = postcode;
      row[iTracking] = tracking;
      row[iTStatus] = trackingStatus;
      row[iManifest] = manifest;
      row[iRmBatch] = rmBatch;
      row[iDespatch] = despatchedAt;
      row[iService] = service;
      row[iPkgSize] = pkgSize;
      row[iWeightKg] = weight;
      row[iImportedAt] = now;
      row[iSource] = meta.sourceFileName;

      appendRows.push(row);
      appendedCount++;
    }
  }

  if (appendRows.length) {
    sh.getRange(sh.getLastRow() + 1, 1, appendRows.length, headers.length).setValues(appendRows);
  }

  if (changedRows.length) {
    writeRowsByRuns_(sh, existing, changedRows, headers.length);
  }

  return { appendedCount, orderNamesTouched: Array.from(touched) };
}

function mirrorShipmentsToOrders_(orderNamesTouched) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const shO = ss.getSheetByName(CFG.SHEETS.ORDERS);
  const shS = ss.getSheetByName(CFG.SHEETS.SHIPMENTS);
  if (!shO) throw new Error(`Missing sheet: ${CFG.SHEETS.ORDERS}`);
  if (!shS) throw new Error(`Missing sheet: ${CFG.SHEETS.SHIPMENTS}`);

  const touched = new Set(orderNamesTouched || []);
  if (!touched.size) return;

  const oMap = headerMap_(shO);
  const sMap = headerMap_(shS);

  const cO = CFG.COLS.ORDERS;
  const cS = CFG.COLS.SHIPMENTS;

  const iOName     = requireCol_(oMap, cO.OrderName);
  const iOStatus   = requireCol_(oMap, cO.Status);
  const iOPostcode = requireCol_(oMap, cO.Postcode);
  const iOBatch    = requireCol_(oMap, cO.RoyalMailBatchNumber);
  const iOTrack    = requireCol_(oMap, cO.RoyalMailTrackingNumber);
  const iOMani     = requireCol_(oMap, cO.RoyalMailManifestNo);

  const iSName     = requireCol_(sMap, cS.OrderName);
  const iSPostcode = requireCol_(sMap, cS.Postcode);
  const iSBatch    = requireCol_(sMap, cS.RoyalMailBatchNumber);
  const iSTrack    = requireCol_(sMap, cS.RoyalMailTrackingNumber);
  const iSMani     = requireCol_(sMap, cS.RoyalMailManifestNo);
  const iSStat     = requireCol_(sMap, cS.TrackingStatus);

  const deliveredToken = normTrackingStatus_(CFG.ROYAL_MAIL.TRACKING_STATUS_DELIVERED || "Delivered");

  const sLastRow = shS.getLastRow();
  const sLastCol = shS.getLastColumn();
  const sVals = (sLastRow >= 2) ? shS.getRange(2, 1, sLastRow - 1, sLastCol).getValues() : [];

  const agg = new Map();
  for (const r of sVals) {
    const on = String(r[iSName] || "").trim();
    if (!on || !touched.has(on)) continue;

    if (!agg.has(on)) {
      agg.set(on, {
        tracks: new Set(),
        manifests: new Set(),
        batches: new Set(),
        postcodes: new Set(),
        statuses: new Set(),
        shipmentCount: 0,
      });
    }

    const a = agg.get(on);

    const t = String(r[iSTrack] || "").trim();
    const m = String(r[iSMani] || "").trim();
    const b = String(r[iSBatch] || "").trim();
    const p = String(r[iSPostcode] || "").trim();
    const s = normTrackingStatus_(r[iSStat]);

    if (t) a.tracks.add(t);
    if (m) a.manifests.add(m);
    if (b) a.batches.add(b);
    if (p) a.postcodes.add(p);
    if (s) a.statuses.add(s);

    a.shipmentCount++;
  }

  const oLastRow = shO.getLastRow();
  const oLastCol = shO.getLastColumn();
  const oVals = (oLastRow >= 2) ? shO.getRange(2, 1, oLastRow - 1, oLastCol).getValues() : [];

  const changedRows = [];

  for (let i = 0; i < oVals.length; i++) {
    const row = oVals[i];
    const on = String(row[iOName] || "").trim();
    if (!on || !touched.has(on)) continue;

    const a = agg.get(on);
    if (!a) continue;

    const before = row.join("\u0001");

    const tracks = Array.from(a.tracks).sort().map(toRoyalMailTrackingUrl_).join("\n");
    const manifests = Array.from(a.manifests).sort().join("\n");
    const batches = Array.from(a.batches).sort().join("\n");

    row[iOTrack] = mergeNewlineList_(row[iOTrack], tracks);
    row[iOMani]  = mergeNewlineList_(row[iOMani], manifests);
    row[iOBatch] = mergeNewlineList_(row[iOBatch], batches);

    if (a.shipmentCount > 0 && a.postcodes.size > 0) {
      const pc = Array.from(a.postcodes).sort()[0];
      row[iOPostcode] = pc;
    }

    const current = String(row[iOStatus] || "").trim();
    if (current !== CFG.STATUS.HOLD && a.shipmentCount > 0) {
      const allDelivered =
        a.statuses.size > 0 &&
        Array.from(a.statuses).every(s => s === deliveredToken);

      row[iOStatus] = allDelivered ? CFG.STATUS.DELIVERED : CFG.STATUS.DESPATCHED;
    }

    const after = row.join("\u0001");
    if (after !== before) changedRows.push(i);
  }

  if (changedRows.length) writeRowsByRuns_(shO, oVals, changedRows, oLastCol);
}

function mirrorOrdersToBatchOrders_(orderNamesTouched) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const shBO = ss.getSheetByName(CFG.SHEETS.BATCH_ORDERS);
  const shO  = ss.getSheetByName(CFG.SHEETS.ORDERS);
  if (!shBO) throw new Error(`Missing sheet: ${CFG.SHEETS.BATCH_ORDERS}`);
  if (!shO)  throw new Error(`Missing sheet: ${CFG.SHEETS.ORDERS}`);

  const touched = new Set(orderNamesTouched || []);
  if (!touched.size) return;

  const boMap = headerMap_(shBO);
  const oMap  = headerMap_(shO);

  const cBO = CFG.COLS.BATCH_ORDERS;
  const cO  = CFG.COLS.ORDERS;

  const iBoOrder   = requireCol_(boMap, cBO.OrderName);
  const iBoStatus  = requireCol_(boMap, cBO.OrderStatus);
  const iBoRmBatch = requireCol_(boMap, cBO.RoyalMailBatchNumber);

  const iOOrder   = requireCol_(oMap, cO.OrderName);
  const iOStatus  = requireCol_(oMap, cO.Status);
  const iORmBatch = requireCol_(oMap, cO.RoyalMailBatchNumber);

  const oLastRow = shO.getLastRow();
  const oLastCol = shO.getLastColumn();
  const oVals = (oLastRow >= 2) ? shO.getRange(2, 1, oLastRow - 1, oLastCol).getValues() : [];

  const orderInfo = new Map();
  for (const r of oVals) {
    const on = String(r[iOOrder] || "").trim();
    if (!on || !touched.has(on)) continue;

    orderInfo.set(on, {
      status: String(r[iOStatus] || "").trim(),
      rmBatch: String(r[iORmBatch] || "").trim(),
    });
  }

  if (!orderInfo.size) return;

  const boLastRow = shBO.getLastRow();
  const boLastCol = shBO.getLastColumn();
  const boVals = (boLastRow >= 2) ? shBO.getRange(2, 1, boLastRow - 1, boLastCol).getValues() : [];

  const changedRows = [];
  for (let i = 0; i < boVals.length; i++) {
    const row = boVals[i];
    const on = String(row[iBoOrder] || "").trim();
    if (!on || !touched.has(on)) continue;

    const info = orderInfo.get(on);
    if (!info) continue;

    const beforeStatus = String(row[iBoStatus] || "");
    const beforeBatch = String(row[iBoRmBatch] || "");

    row[iBoStatus] = info.status;
    row[iBoRmBatch] = info.rmBatch;

    if (beforeStatus !== row[iBoStatus] || beforeBatch !== row[iBoRmBatch]) changedRows.push(i);
  }

  if (changedRows.length) writeRowsByRuns_(shBO, boVals, changedRows, boLastCol);
}

function updateBatchesRoyalMailShorthandFromBatchOrders_(orderNamesTouched) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const shBO = ss.getSheetByName(CFG.SHEETS.BATCH_ORDERS);
  const shB  = ss.getSheetByName(CFG.SHEETS.BATCHES);
  if (!shBO) throw new Error(`Missing sheet: ${CFG.SHEETS.BATCH_ORDERS}`);
  if (!shB)  throw new Error(`Missing sheet: ${CFG.SHEETS.BATCHES}`);

  const touched = new Set(orderNamesTouched || []);
  if (!touched.size) return;

  const boMap = headerMap_(shBO);
  const bMap  = headerMap_(shB);

  const cBO = CFG.COLS.BATCH_ORDERS;
  const cB  = CFG.COLS.BATCHES;

  const iBoOrder   = requireCol_(boMap, cBO.OrderName);
  const iBoBatch   = requireCol_(boMap, cBO.BatchID);
  const iBoRmBatch = requireCol_(boMap, cBO.RoyalMailBatchNumber);

  const iBBatch   = requireCol_(bMap, cB.BatchID);
  const iBRmBatch = requireCol_(bMap, cB.RoyalMailBatchNumber);

  const boLastRow = shBO.getLastRow();
  const boLastCol = shBO.getLastColumn();
  const boVals = (boLastRow >= 2) ? shBO.getRange(2, 1, boLastRow - 1, boLastCol).getValues() : [];

  const batchToRm = new Map();
  for (const r of boVals) {
    const on = String(r[iBoOrder] || "").trim();
    if (!on || !touched.has(on)) continue;

    const batchId = String(r[iBoBatch] || "").trim();
    if (!batchId) continue;

    const rmCell = String(r[iBoRmBatch] || "").trim();
    if (!batchToRm.has(batchId)) batchToRm.set(batchId, new Set());

    if (rmCell) {
      rmCell
        .split(/\r?\n/)
        .map(s => s.trim())
        .filter(Boolean)
        .forEach(x => batchToRm.get(batchId).add(x));
    }
  }

  if (!batchToRm.size) return;

  const bLastRow = shB.getLastRow();
  const bLastCol = shB.getLastColumn();
  const bVals = (bLastRow >= 2) ? shB.getRange(2, 1, bLastRow - 1, bLastCol).getValues() : [];

  const changedRows = [];
  for (let i = 0; i < bVals.length; i++) {
    const row = bVals[i];
    const batchId = String(row[iBBatch] || "").trim();
    if (!batchId || !batchToRm.has(batchId)) continue;

    const set = batchToRm.get(batchId);
    if (!set || set.size === 0) continue;

    let shorthand = "";
    if (set.size === 1) shorthand = Array.from(set)[0];
    else shorthand = `MULTI (${set.size})`;

    if (String(row[iBRmBatch] || "") !== shorthand) {
      row[iBRmBatch] = shorthand;
      changedRows.push(i);
    }
  }

  if (changedRows.length) writeRowsByRuns_(shB, bVals, changedRows, bLastCol);
}

function writeRowsByRuns_(sh, values, changedRowIndices0, width) {
  const sorted = Array.from(new Set(changedRowIndices0)).sort((a, b) => a - b);
  if (!sorted.length) return;

  let start = sorted[0];
  let prev = start;

  for (let i = 1; i < sorted.length; i++) {
    const cur = sorted[i];
    if (cur === prev + 1) {
      prev = cur;
    } else {
      const num = prev - start + 1;
      const block = new Array(num);
      for (let j = 0; j < num; j++) block[j] = values[start + j];
      sh.getRange(2 + start, 1, num, width).setValues(block);
      start = cur;
      prev = cur;
    }
  }

  const num = prev - start + 1;
  const block = new Array(num);
  for (let j = 0; j < num; j++) block[j] = values[start + j];
  sh.getRange(2 + start, 1, num, width).setValues(block);
}
