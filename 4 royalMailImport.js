/**
 * royalMailImport.js
 *
 * Watch-folder Royal Mail manifest import:
 * Watch Folder (.xls/.xlsx) -> convert -> parse -> upsert Shipments -> mirror Orders -> mirror BatchOrders -> update Batches shorthand -> archive file
 *
 * Assumes you already have:
 * - CFG (Config) with CFG.SHEETS + CFG.COLS mappings
 * - helpers: headerMap_(), requireCol_(), getHeaders_(), parseDate_(), mergeNewlineList_(), normTrackingStatus_()
 * - Advanced Drive Service enabled (Drive.Files.*)
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

/**
 * Install time-based trigger for pollRoyalMailWatchFolder().
 */
function installRoyalMailWatchTrigger() {
  removeRoyalMailWatchTrigger();

  const mins = (CFG.ROYAL_MAIL && CFG.ROYAL_MAIL.POLL_EVERY_MINUTES) ? CFG.ROYAL_MAIL.POLL_EVERY_MINUTES : 10;

  ScriptApp.newTrigger("pollRoyalMailWatchFolder")
    .timeBased()
    .everyMinutes(mins)
    .create();
}

/**
 * Remove any existing triggers pointing at pollRoyalMailWatchFolder().
 */
function removeRoyalMailWatchTrigger() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === "pollRoyalMailWatchFolder") ScriptApp.deleteTrigger(t);
  });
}

/**
 * Poll watch folder for new .xls/.xlsx exports, import them, then move to archive.
 * Uses DocumentProperties to avoid reprocessing the same file ID repeatedly.
 */
function pollRoyalMailWatchFolder() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);

  try {
    const watchId = CFG.ROYAL_MAIL && CFG.ROYAL_MAIL.WATCH_FOLDER_ID;
    const archiveId = CFG.ROYAL_MAIL && CFG.ROYAL_MAIL.ARCHIVE_FOLDER_ID;
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

    // Oldest first
    candidates.sort((a, b) => a.getLastUpdated().getTime() - b.getLastUpdated().getTime());

    let filesDone = 0;

    for (const file of candidates) {
      const result = importRoyalMailManifestFile_(file);

      // Mark processed
      processed.add(file.getId());
      filesDone++;

      // Move original to archive
      archive.addFile(file);
      watch.removeFile(file);

      // Toast for manual runs
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
 * Returns { newShipments }.
 */
function importRoyalMailManifestFile_(file) {
  const sourceFileName = file.getName();

  // Convert to Google Sheet (Advanced Drive Service)
  const converted = convertToGoogleSheet_(file.getId(), sourceFileName);
  const convertedId = converted.id;

  try {
    const tmp = SpreadsheetApp.openById(convertedId);
    const sheet = tmp.getSheets()[0];
    const values = sheet.getDataRange().getValues();
    if (values.length < 2) return { newShipments: 0 };

    const headers = values[0].map(h => String(h || "").trim());
    const idx = (name) => headers.indexOf(name);

    // Exact RM headers (as provided by you)
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
      ["Postcode", meta.iPostcode]
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

    // Mirror pipeline
    mirrorShipmentsToOrders_(touched);
    mirrorOrdersToBatchOrders_(touched);
    updateBatchesRoyalMailShorthandFromBatchOrders_(touched);

    return { newShipments: up.appendedCount };

  } finally {
    // Trash the converted temp sheet
    try { DriveApp.getFileById(convertedId).setTrashed(true); } catch (e) {}
  }
}

/**
 * Convert Excel -> Google Sheet.
 * Requires Advanced Drive Service enabled (Drive.Files.*).
 */
function convertToGoogleSheet_(fileId, fileName) {
  const resource = {
    title: fileName.replace(/\.(xls|xlsx)$/i, "") + " (Converted)",
    mimeType: MimeType.GOOGLE_SHEETS
  };
  return Drive.Files.copy(resource, fileId, { convert: true });
}

/**
 * Upsert Shipments rows from RM export.
 * ShipmentID is stable: `${OrderName}|${TrackingNumber}` and is never modified.
 */
function upsertShipmentsFromRMRows_(rows, meta) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(CFG.SHEETS.SHIPMENTS);
  if (!sh) throw new Error(`Missing sheet: ${CFG.SHEETS.SHIPMENTS}`);

  const map = headerMap_(sh);
  const c = CFG.COLS.SHIPMENTS;

  const iShipmentID = requireCol_(map, c.ShipmentID);
  const iOrderName  = requireCol_(map, c.OrderName);
  const iPostcode   = requireCol_(map, c.Postcode);              // NEW
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

  // ShipmentID -> index in `existing`
  const byId = new Map();
  for (let r = 0; r < existing.length; r++) {
    const id = String(existing[r][iShipmentID] || "").trim();
    if (id) byId.set(id, r);
  }

  const headers = getHeaders_(sh);
  const now = new Date();

  let updatedAny = false;
  let appendedCount = 0;
  const toAppend = [];
  const touched = new Set();

  // Dedup within this file for safety
  const seenInFile = new Set();

  for (const r of rows) {
    const orderName = String(r[meta.iOrderName] || "").trim();
    const postcode = String(r[meta.iPostcode] || "").trim();          // NEW
    const tracking = String(r[meta.iTracking] || "").trim();
    const trackingStatus = String(r[meta.iTrackingStatus] || "").trim();

    if (!orderName || !tracking) continue;

    const shipmentId = `${orderName}|${tracking}`;
    if (seenInFile.has(shipmentId)) continue;
    seenInFile.add(shipmentId);

    touched.add(orderName);

    const manifest = String(r[meta.iManifest] || "").trim();
    const rmBatch  = String(r[meta.iRmBatch] || "").trim();
    const despatchedAt = parseDate_(r[meta.iDespatch]) || ""; // true datetime
    const service = (meta.iService >= 0) ? String(r[meta.iService] || "").trim() : "";
    const pkgSize = (meta.iPkgSize >= 0) ? String(r[meta.iPkgSize] || "").trim() : "";
    const weight  = (meta.iWeightKg >= 0) ? r[meta.iWeightKg] : "";

    if (byId.has(shipmentId)) {
      const idx = byId.get(shipmentId);
      const row = existing[idx];

      // Do NOT modify ShipmentID (stable key)
      row[iOrderName] = orderName;
      row[iPostcode] = postcode;              // NEW
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

      updatedAny = true;
    } else {
      const row = new Array(headers.length).fill("");
      row[iShipmentID] = shipmentId;
      row[iOrderName] = orderName;
      row[iPostcode] = postcode;              // NEW
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

      toAppend.push(row);
      appendedCount++;
    }
  }

  if (toAppend.length) {
    sh.getRange(sh.getLastRow() + 1, 1, toAppend.length, headers.length).setValues(toAppend);
  }
  if (updatedAny && existing.length) {
    sh.getRange(2, 1, existing.length, headers.length).setValues(existing);
  }

  return { appendedCount, orderNamesTouched: Array.from(touched) };
}


/**
 * Mirror Shipments -> Orders:
 * - newline-unique: tracking, manifest, batch numbers
 * - Hold protected
 * - If shipments exist: all delivered => Delivered else Despatched
 */
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

  // Orders indices
  const iOName     = requireCol_(oMap, cO.OrderName);
  const iOStatus   = requireCol_(oMap, cO.Status);
  const iOPostcode = requireCol_(oMap, cO.Postcode); // NEW (push postcode)
  const iOBatch    = requireCol_(oMap, cO.RoyalMailBatchNumber);
  const iOTrack    = requireCol_(oMap, cO.RoyalMailTrackingNumber);
  const iOMani     = requireCol_(oMap, cO.RoyalMailManifestNo);

  // Shipments indices
  const iSName     = requireCol_(sMap, cS.OrderName);
  const iSPostcode = requireCol_(sMap, cS.Postcode); // NEW
  const iSBatch    = requireCol_(sMap, cS.RoyalMailBatchNumber);
  const iSTrack    = requireCol_(sMap, cS.RoyalMailTrackingNumber);
  const iSMani     = requireCol_(sMap, cS.RoyalMailManifestNo);
  const iSStat     = requireCol_(sMap, cS.TrackingStatus);

  const deliveredToken = normTrackingStatus_(CFG.ROYAL_MAIL.TRACKING_STATUS_DELIVERED || "Delivered");

  // Read Shipments (can be optimized later; OK for now)
  const sLastRow = shS.getLastRow();
  const sLastCol = shS.getLastColumn();
  const sVals = (sLastRow >= 2) ? shS.getRange(2, 1, sLastRow - 1, sLastCol).getValues() : [];

  // Aggregate per order:
  // - newline fields: tracking, manifest, batch numbers
  // - postcode: should be stable; if multiple, pick deterministic one (sorted first)
  // - status: all delivered -> Delivered else Despatched (Hold protected)
  const agg = new Map(); // orderName -> { tracks:Set, manifests:Set, batches:Set, postcodes:Set, statuses:Set, shipmentCount:number }

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

  // Read Orders
  const oLastRow = shO.getLastRow();
  const oLastCol = shO.getLastColumn();
  const oVals = (oLastRow >= 2) ? shO.getRange(2, 1, oLastRow - 1, oLastCol).getValues() : [];

  let changed = 0;

  for (const row of oVals) {
    const on = String(row[iOName] || "").trim();
    if (!on || !touched.has(on)) continue;

    const a = agg.get(on);
    if (!a) continue;

    // Merge RM fields (newline-separated, stable)
    const tracks = Array.from(a.tracks).sort().map(toRoyalMailTrackingUrl_).join("\n");
    const manifests = Array.from(a.manifests).sort().join("\n");
    const batches = Array.from(a.batches).sort().join("\n");

    row[iOTrack] = mergeNewlineList_(row[iOTrack], tracks);
    row[iOMani]  = mergeNewlineList_(row[iOMani], manifests);
    row[iOBatch] = mergeNewlineList_(row[iOBatch], batches);

    // Push postcode to Orders once shipments exist (authoritative shipping postcode)
    // If multiple postcodes exist (rare), pick a deterministic value (sorted first).
    if (a.shipmentCount > 0 && a.postcodes.size > 0) {
      const pc = Array.from(a.postcodes).sort()[0];
      row[iOPostcode] = pc;
    }

    const current = String(row[iOStatus] || "").trim();

    // Hold protected
    if (current === CFG.STATUS.HOLD) {
      changed++;
      continue;
    }

    // Once shipments exist, RM is authoritative for status:
    // all delivered => Delivered, else Despatched.
    if (a.shipmentCount > 0) {
      const allDelivered =
        a.statuses.size > 0 &&
        Array.from(a.statuses).every(s => s === deliveredToken);

      row[iOStatus] = allDelivered ? CFG.STATUS.DELIVERED : CFG.STATUS.DESPATCHED;
    }

    changed++;
  }

  if (changed) {
    shO.getRange(2, 1, oVals.length, oLastCol).setValues(oVals);
  }
}


/**
 * Mirror Orders -> BatchOrders (for touched OrderNames only):
 * - BatchOrders.OrderStatus = Orders.OrderStatus
 * - BatchOrders.RoyalMailBatchNumber = Orders.RoyalMailBatchNumber (newline list)
 */
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

  let changed = 0;

  for (const row of boVals) {
    const on = String(row[iBoOrder] || "").trim();
    if (!on || !touched.has(on)) continue;

    const info = orderInfo.get(on);
    if (!info) continue;

    row[iBoStatus] = info.status;
    row[iBoRmBatch] = info.rmBatch;
    changed++;
  }

  if (changed) {
    shBO.getRange(2, 1, boVals.length, boLastCol).setValues(boVals);
  }
}

/**
 * Update Batches.RoyalMailBatchNumber shorthand based on BatchOrders:
 * - If exactly 1 distinct RM batch number in batch => that number
 * - If >1 => MULTI (n)
 * - SAFEGUARD: if none exist => don't overwrite manual placeholder in Batches
 */
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

  let changed = 0;

  for (const row of bVals) {
    const batchId = String(row[iBBatch] || "").trim();
    if (!batchId || !batchToRm.has(batchId)) continue;

    const set = batchToRm.get(batchId);

    // SAFEGUARD: no RM-derived values => leave manual placeholder untouched
    if (!set || set.size === 0) continue;

    let shorthand = "";
    if (set.size === 1) shorthand = Array.from(set)[0];
    else shorthand = `MULTI (${set.size})`;

    row[iBRmBatch] = shorthand;
    changed++;
  }

  if (changed) {
    shB.getRange(2, 1, bVals.length, bLastCol).setValues(bVals);
  }
}

