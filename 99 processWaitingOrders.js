/*******************************************************
 * One-click pipeline to process waiting orders:
 * 1) Sync OrderItems → Orders
 * 2) Create/reuse Batches
 * 3) Rebuild BatchOrders
 *
 * Large-volume friendly controls:
 * - optional staged execution
 * - configurable inter-step delay
 *******************************************************/

function processWaitingOrders(options) {
  const opts = options || {};

  const stepDelayMs = (opts.stepDelayMs !== undefined) ? Number(opts.stepDelayMs) : 500;
  const includeToday = (opts.includeToday === true);
  const shouldRebuildBatchOrders = (opts.rebuildBatchOrders !== false);
  const staged = (opts.staged === true);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.setSpreadsheetTimeZone(CFG.TIMEZONE);

  const started = new Date();

  try {
    if (staged) {
      runPipelineStage_(opts.stage || "sync");
      return;
    }

    ss.toast("Step 1/3: Syncing OrderItems → Orders...", "Pipeline", 5);
    syncOrdersFromOrderItems();

    if (stepDelayMs > 0) Utilities.sleep(stepDelayMs);

    ss.toast("Step 2/3: Creating/reusing batches...", "Pipeline", 5);
    if (includeToday) createBatchesIncludeTodayOverride();
    else createBatchesAuto();

    if (stepDelayMs > 0) Utilities.sleep(stepDelayMs);

    if (shouldRebuildBatchOrders) {
      ss.toast("Step 3/3: Rebuilding BatchOrders...", "Pipeline", 5);
      rebuildBatchOrders();
    }

    const elapsedSec = Math.round((new Date().getTime() - started.getTime()) / 1000);
    ss.toast(`Pipeline complete in ~${elapsedSec}s`, "Pipeline", 8);
  } catch (err) {
    ss.toast(`Pipeline failed: ${err && err.message ? err.message : err}`, "Pipeline", 10);
    throw err;
  }
}

function runPipelineStage_(stage) {
  const s = String(stage || "").trim().toLowerCase();
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  if (s === "sync") {
    ss.toast("Stage: sync", "Pipeline", 4);
    syncOrdersFromOrderItems();
    return;
  }

  if (s === "batch") {
    ss.toast("Stage: batch", "Pipeline", 4);
    createBatchesAuto();
    return;
  }

  if (s === "batchorders") {
    ss.toast("Stage: batchorders", "Pipeline", 4);
    rebuildBatchOrders();
    return;
  }

  throw new Error(`Unknown stage: ${stage}. Use sync | batch | batchorders`);
}
