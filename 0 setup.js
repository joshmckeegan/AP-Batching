/***************************************
 * setup.js
 * Spreadsheet formatting / hygiene
 ***************************************/

function setup() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  if (typeof CFG === "undefined") {
    throw new Error("Missing CFG. Ensure config.js is loaded before running setup().");
  }
  if (!CFG.TIMEZONE) {
    throw new Error("CFG.TIMEZONE is missing.");
  }

  const setupCfg = (CFG.SETUP || {});
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const bandingTheme = resolveBandingTheme_(setupCfg.BANDING_THEME);

  ss.getSheets().forEach(sh => {
    // Skip hidden sheets (config-controlled)
    if (setupCfg.SKIP_HIDDEN_SHEETS !== false && sh.isSheetHidden && sh.isSheetHidden()) return;

    const lastCol = sh.getLastColumn();
    const lastRow = sh.getLastRow();
    if (lastCol < 1) return;

    // Read header row values (row 1)
    const headerVals = sh.getRange(1, 1, 1, lastCol).getValues()[0]
      .map(v => String(v || "").trim());

    // Only proceed if the sheet actually has headers
    if (!headerVals.some(h => !!h)) return;

    // Last headered column (resize col 1..last headered col)
    let lastHeaderCol0 = 0;
    for (let i = headerVals.length - 1; i >= 0; i--) {
      if (headerVals[i]) { lastHeaderCol0 = i; break; }
    }
    const lastHeaderCol1 = lastHeaderCol0 + 1;

    // Freeze header row
    if (setupCfg.FREEZE_HEADER_ROW && sh.getFrozenRows() < 1) {
      sh.setFrozenRows(1);
    }

    // Format header row across headered columns only
    const headerRange = sh.getRange(1, 1, 1, lastHeaderCol1);
    if (setupCfg.BOLD_HEADERS) headerRange.setFontWeight("bold");
    if (setupCfg.HEADER_BACKGROUND) headerRange.setBackground(setupCfg.HEADER_BACKGROUND);
    headerRange.setWrap(!!setupCfg.HEADER_WRAP);

    // Auto-resize columns 1..last headered column only
    if (setupCfg.AUTO_RESIZE_COLUMNS && lastHeaderCol1 > 0) {
      sh.autoResizeColumns(1, lastHeaderCol1);
    }

    // Alternating row colours (row banding) on used range only
    if (setupCfg.ROW_BANDING !== false) {
      const bandings = sh.getBandings();
      bandings.forEach(b => b.remove());

      const rowsToBand = Math.max(lastRow, 1);
      sh.getRange(1, 1, rowsToBand, lastHeaderCol1).applyRowBanding(bandingTheme);
    }
  });
}

function resolveBandingTheme_(themeName) {
  const t = String(themeName || "").trim().toUpperCase();
  const map = {
    LIGHT_BLUE: SpreadsheetApp.BandingTheme.LIGHT_BLUE,
    LIGHT_GREY: SpreadsheetApp.BandingTheme.LIGHT_GREY,
    LIGHT_GRAY: SpreadsheetApp.BandingTheme.LIGHT_GREY
  };
  return map[t] || SpreadsheetApp.BandingTheme.LIGHT_BLUE;
}
