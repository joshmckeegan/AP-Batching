/***************************************
 * menu.js
 * Single canonical onOpen() for menus
 ***************************************/

function onOpen() {
  const ui = SpreadsheetApp.getUi();

  const m = ui.createMenu("Batch Tracking");

  // Queue sidebar
  m.addItem("Open: Order Queue Sidebar", "openOrdersSidebar")

  // Order pipeline
  m.addItem("Run: Process Waiting Orders", "processWaitingOrders")
    .addItem("One-off: Repair Derived Fields + ReadyForOrders (full rescan)", "forceRepairReadyForOrdersOneOff")
    .addItem("Batches: Refresh PrintBatchName from RoyalMailBatchNumber", "refreshBatchNamesFromRoyalMailBatchNumber")
  // Batching
  m.addSeparator()
    .addItem("Create batches (full days only)", "createBatchesAuto")
    .addItem("OVERRIDE: Create batches incl. today", "createBatchesIncludeTodayOverride")
    .addItem("Rebuild BatchOrders", "rebuildBatchOrders")
  // Royal Mail
  .addSeparator()
  .addItem("Royal Mail: Run import now (watch folder)", "pollRoyalMailWatchFolder")
  .addItem("Royal Mail: Install watch trigger", "installRoyalMailWatchTrigger")
  .addItem("Royal Mail: Remove watch trigger", "removeRoyalMailWatchTrigger")
  // Setup
  m.addSeparator()
    .addItem("Setup: Format sheets (headers only)", "setup");
  // Admin / checkpoint tools
  m.addSeparator()
    .addItem("Admin: Reset OrderItems checkpoint (forces full rescan next run)", "adminResetOrderItemsCheckpoint")
    .addItem("Admin: Run full OrderItems rescan now", "adminRunFullOrderItemsRescanNow");
  m.addToUi();
}
