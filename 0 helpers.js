/***************************************
 * helpers.js
 * Shared utilities across the project
 ***************************************/

/***************
 * Header helpers
 ***************/
function getHeaders_(sh) {
  const lastCol = Math.max(sh.getLastColumn(), 1);
  return sh.getRange(1, 1, 1, lastCol).getValues()[0].map(h => String(h || "").trim());
}

function headerMap_(sh) {
  const headers = getHeaders_(sh);
  const map = {};
  headers.forEach((h, i) => { if (h) map[h] = i; });
  return map;
}

function requireCol_(map, headerName) {
  if (map[headerName] === undefined) {
    throw new Error(`Missing column: "${headerName}"`);
  }
  return map[headerName];
}

/***************
 * Column helpers
 ***************/

/**
 * Optional column lookup. Returns 0-based index or -1 if missing.
 */
function optionalCol_(colMap, headerName) {
  return (colMap && headerName && colMap[headerName] !== undefined) ? colMap[headerName] : -1;
}

/***************
 * List formatting
 ***************/

/**
 * Merge two newline-separated lists into a stable sorted unique newline list.
 */
function mergeNewlineList_(existing, incoming) {
  const e = String(existing || "").trim();
  const i = String(incoming || "").trim();
  if (!e) return i;
  if (!i) return e;

  const set = new Set(
    e.split(/\r?\n/).map(s => s.trim()).filter(Boolean)
  );
  for (const part of i.split(/\r?\n/).map(s => s.trim()).filter(Boolean)) set.add(part);

  return Array.from(set).sort().join("\n");
}

/***************
 * Parsing / coercion
 ***************/
function parseDate_(v) {
  if (!v) return null;
  if (v instanceof Date && !isNaN(v.getTime())) return v;
  if (typeof v === "string") {
    const t = Date.parse(v.trim());
    if (!isNaN(t)) return new Date(t);
  }
  return null;
}

function toInt_(v, fallback) {
  const n = (typeof v === "number") ? v : parseFloat(String(v || "").trim());
  return Number.isFinite(n) ? Math.round(n) : fallback;
}

function isTrue_(v) {
  if (v === true) return true;
  const s = String(v || "").trim().toLowerCase();
  return (s === "true" || s === "yes" || s === "y" || s === "1");
}

/***************
 * Blank-ish logic
 ***************/
function getBlankishValues_() {
  const deriveCfg = (CFG && CFG.DERIVE) ? CFG.DERIVE : {};
  return Array.isArray(deriveCfg.BLANKISH_VALUES)
    ? deriveCfg.BLANKISH_VALUES
    : ["(blank)", "(blanks)", "blank", "blanks", '""'];
}

function isBlankish_(v, blankishValues) {
  const s = String(v || "").trim().toLowerCase();
  if (!s) return true;
  const list = (blankishValues || []).map(x => String(x || "").trim().toLowerCase());
  return list.includes(s);
}

/***************
 * Print profile helpers
 ***************/
/**
 * Sums print counts from PrintProfileKey.
 * Example: "B86:1|B54:2" => 3
 */
function sumPrintCounts_(profileKey) {
  const key = String(profileKey || "").trim();
  if (!key) return 0;

  let sum = 0;
  const parts = key.split("|").map(s => s.trim()).filter(Boolean);
  for (const p of parts) {
    const m = p.match(/:(\d+)$/);
    sum += m ? parseInt(m[1], 10) : 1;
  }
  return sum;
}

function deriveCategoryFallback_(mode, profileKey) {
  const m = String(mode || "").toUpperCase();
  if (m === "NONE") {
    return (CFG.DERIVE && CFG.DERIVE.ON_NONE && CFG.DERIVE.ON_NONE.PRINT_CATEGORY)
      ? CFG.DERIVE.ON_NONE.PRINT_CATEGORY
      : "NONE";
  }

  const first = String(profileKey || "").split("|")[0].trim();
  const code = first.split(":")[0].trim();
  return code || ((CFG.DERIVE && CFG.DERIVE.ON_MISSING_SKU && CFG.DERIVE.ON_MISSING_SKU.PRINT_CATEGORY)
    ? CFG.DERIVE.ON_MISSING_SKU.PRINT_CATEGORY
    : "Unknown");
}

/***************
 * Status helpers
 ***************/

/**
 * Returns true if status should be treated as "manual/API controlled"
 * and must not be overwritten by automated derivation.
 */
function isManualOrFinalOrderStatus_(status) {
  const s = String(status || "").trim();
  return s === CFG.STATUS.HOLD ||
         s === CFG.STATUS.DESPATCHED ||
         s === CFG.STATUS.DELIVERED;
}

function normTrackingStatus_(s) {
  return String(s || "").trim().toLowerCase();
}

function toRoyalMailTrackingUrl_(value) {
  const v = String(value || "").trim();
  if (!v) return "";
  const prefix = "https://www.royalmail.com/track-your-item#/tracking-results/";
  // If it's already a full URL, leave it alone
  if (v.indexOf(prefix) === 0) return v;
  return prefix + encodeURIComponent(v);
}

function trackingListToRoyalMailUrls_(newlineList) {
  const s = String(newlineList || "").trim();
  if (!s) return "";
  return s
    .split(/\r?\n/)
    .map(x => x.trim())
    .filter(Boolean)
    .map(toRoyalMailTrackingUrl_)
    .join("\n");
}
