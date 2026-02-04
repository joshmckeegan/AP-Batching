/*******************************************************
 * One-click pipeline to process waiting orders:
 * 1) Sync OrderItems → Orders
 * 2) Create/reuse Batches
 * 3) Rebuild BatchOrders
 *******************************************************/

function processWaitingOrders(options) {
  const opts = options || {};

  const stepDelayMs = (opts.stepDelayMs !== undefined) ? Number(opts.stepDelayMs) : 750;
  const includeToday = (opts.includeToday === true);

  // Renamed to avoid shadowing the rebuildBatchOrders() function
  const shouldRebuildBatchOrders = (opts.rebuildBatchOrders !== false);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);

  const started = new Date();
  try {
    ss.toast("Step 1/3: Syncing OrderItems → Orders...", "Pipeline", 5);
    syncOrdersFromOrderItems();

    if (stepDelayMs > 0) Utilities.sleep(stepDelayMs);

    ss.toast("Step 2/3: Creating/reusing batches...", "Pipeline", 5);
    if (includeToday) {
      createBatchesIncludeTodayOverride();
    } else {
      createBatchesAuto();
    }

    if (stepDelayMs > 0) Utilities.sleep(stepDelayMs);

    if (shouldRebuildBatchOrders) {
      ss.toast("Step 3/3: Rebuilding BatchOrders...", "Pipeline", 5);
      rebuildBatchOrders(); // calls your batchOrdersBuilder function now
    }

    const elapsedSec = Math.round((new Date().getTime() - started.getTime()) / 1000);
    ss.toast(`Pipeline complete in ~${elapsedSec}s`, "Pipeline", 8);

  } catch (err) {
    ss.toast(`Pipeline failed: ${err && err.message ? err.message : err}`, "Pipeline", 10);
    throw err;
  } finally {
    lock.releaseLock();
  }
}
