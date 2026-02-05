/***************************************
 * sidebarOrders.js
 *
 * Google Sheets sidebar for quick order status management
 * Focus: New -> In Production -> Ready to Pack
 ***************************************/

function openOrdersSidebar() {
  const html = HtmlService.createTemplateFromFile('sidebarOrders')
    .evaluate()
    .setTitle('Order Queue')
    .setWidth(420);

  SpreadsheetApp.getUi().showSidebar(html);
}

function include_(name) {
  return HtmlService.createHtmlOutputFromFile(name).getContent();
}

function getOrderQueueData_(options) {
  const opts = options || {};
  const statusFilter = String(opts.status || CFG.STATUS.NEW).trim();
  const search = String(opts.search || '').trim().toLowerCase();
  const limit = Math.max(1, Math.min(500, parseInt(opts.limit, 10) || 200));

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const shO = ss.getSheetByName(CFG.SHEETS.ORDERS);
  if (!shO) throw new Error(`Missing sheet: ${CFG.SHEETS.ORDERS}`);

  const map = headerMap_(shO);
  const cO = CFG.COLS.ORDERS;

  const iOrder = requireCol_(map, cO.OrderName);
  const iCreated = requireCol_(map, cO.CreatedAt);
  const iStatus = requireCol_(map, cO.Status);
  const iPostcode = optionalCol_(map, cO.Postcode);

  const lastRow = shO.getLastRow();
  const lastCol = shO.getLastColumn();
  const values = (lastRow >= 2) ? shO.getRange(2, 1, lastRow - 1, lastCol).getValues() : [];

  const items = [];
  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    const orderName = String(row[iOrder] || '').trim();
    if (!orderName) continue;

    const status = String(row[iStatus] || '').trim();
    if (status !== statusFilter) continue;

    const postcode = (iPostcode >= 0) ? String(row[iPostcode] || '').trim() : '';
    const created = parseDate_(row[iCreated]);

    if (search) {
      const hay = `${orderName} ${postcode} ${status}`.toLowerCase();
      if (hay.indexOf(search) === -1) continue;
    }

    items.push({
      sheetRow: i + 2,
      orderName,
      status,
      postcode,
      createdAt: created ? created.toISOString() : '',
      createdAtDisplay: created
        ? Utilities.formatDate(created, CFG.TIMEZONE, CFG.FORMATS.DATETIME_UK)
        : ''
    });
  }

  items.sort((a, b) => {
    const at = a.createdAt ? Date.parse(a.createdAt) : Number.MAX_SAFE_INTEGER;
    const bt = b.createdAt ? Date.parse(b.createdAt) : Number.MAX_SAFE_INTEGER;
    return at - bt;
  });

  return {
    status: statusFilter,
    count: items.length,
    items: items.slice(0, limit),
    statuses: [CFG.STATUS.NEW, CFG.STATUS.IN_PROD, CFG.STATUS.READY]
  };
}

function updateOrderStatuses_(payload) {
  const p = payload || {};
  const targetStatus = String(p.targetStatus || '').trim();
  const orderNames = Array.isArray(p.orderNames) ? p.orderNames : [];

  if (!targetStatus) throw new Error('targetStatus is required');
  if (![CFG.STATUS.IN_PROD, CFG.STATUS.READY, CFG.STATUS.NEW].includes(targetStatus)) {
    throw new Error(`Unsupported target status: ${targetStatus}`);
  }

  const wanted = new Set(orderNames.map(x => String(x || '').trim()).filter(Boolean));
  if (!wanted.size) return { updated: 0, skipped: 0, reason: 'No orders selected' };

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const shO = ss.getSheetByName(CFG.SHEETS.ORDERS);
  if (!shO) throw new Error(`Missing sheet: ${CFG.SHEETS.ORDERS}`);

  const map = headerMap_(shO);
  const cO = CFG.COLS.ORDERS;
  const iOrder = requireCol_(map, cO.OrderName);
  const iStatus = requireCol_(map, cO.Status);

  const lastRow = shO.getLastRow();
  const lastCol = shO.getLastColumn();
  if (lastRow < 2) return { updated: 0, skipped: wanted.size, reason: 'No Orders rows found' };

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    const values = shO.getRange(2, 1, lastRow - 1, lastCol).getValues();

    const changed = [];
    let skipped = 0;

    for (let i = 0; i < values.length; i++) {
      const row = values[i];
      const orderName = String(row[iOrder] || '').trim();
      if (!wanted.has(orderName)) continue;

      const current = String(row[iStatus] || '').trim();

      // Protect manual/final statuses.
      if (isManualOrFinalOrderStatus_(current)) {
        skipped++;
        continue;
      }

      // Only allow monotonic progress within New/In Production/Ready to Pack.
      if (!canTransitionOrderStatus_(current, targetStatus)) {
        skipped++;
        continue;
      }

      if (current === targetStatus) continue;

      row[iStatus] = targetStatus;
      changed.push(i);
    }

    if (changed.length) {
      writeOrderRowsByRuns_(shO, values, changed, lastCol);
    }

    return { updated: changed.length, skipped };
  } finally {
    lock.releaseLock();
  }
}

function canTransitionOrderStatus_(fromStatus, toStatus) {
  const rank = {};
  rank[CFG.STATUS.NEW] = 1;
  rank[CFG.STATUS.IN_PROD] = 2;
  rank[CFG.STATUS.READY] = 3;

  const from = rank[String(fromStatus || '').trim()];
  const to = rank[String(toStatus || '').trim()];
  if (!from || !to) return false;
  return to >= from;
}

function writeOrderRowsByRuns_(sh, values, changedRowIndices0, width) {
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
