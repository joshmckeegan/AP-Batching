/*******************************
 * config.js
 * Single source of truth for sheet names, headers, formats, and behavior toggles.
 *******************************/

const CFG = {
  /***************
   * Spreadsheet
   ***************/
  TIMEZONE: "Europe/London",

  FORMATS: {
    DATETIME_UK: "dd/MM/yyyy HH:mm:ss",
    DATE_UK: "dd/MM/yyyy",
  },

  /***************
   * Sheet names
   ***************/
  SHEETS: {
    ORDER_ITEMS: "OrderItems",
    ORDERS: "Orders",
    SKU_MATRIX: "SKU_Matrix",
    EXCEPTIONS: "Exceptions",
    BATCHES: "Batches",
    BATCH_ORDERS: "BatchOrders",
    SHIPMENTS: "Shipments",
  },

  /***************
   * Column headers
   ***************/
  COLS: {
    ORDER_ITEMS: {
      CreatedAt: "CreatedAt",
      OrderName: "OrderName",
      ProductTitle: "ProductTitle",
      Qty: "Qty",
      PrintCategory: "PrintCategory",
      PrintProfileKey: "PrintProfileKey",
      LineItemID: "LineItemID",
      SKU: "SKU",
      PrintUnits: "PrintUnits",
      PrintBatchID: "PrintBatchID",
      PrintBatchName: "PrintBatchName",
      PrintedAt: "PrintedAt",
      PrintedBy: "PrintedBy",
      PackedAt: "PackedAt",
      PackedBy: "PackedBy",
      ReadyForOrders: "ReadyForOrders",

      // Optional future column
      PrintPlan: "PrintPlan",
    },

    ORDERS: {
      OrderName: "OrderName",
      Postcode: "Postcode",
      CreatedAt: "CreatedAt",
      Status: "OrderStatus",
      PackedAt: "PackedAt",
      PackedBy: "PackedBy",
      Notes: "Notes",
      RoyalMailBatchNumber: "RoyalMailBatchNumber",
      RoyalMailTrackingNumber: "RoyalMailTrackingNumber",
      RoyalMailManifestNo: "RoyalMailManifestNo",
    },

    SKU_MATRIX: {
      SKU: "SKU",
      PrintMode: "PrintMode",
      PrintProfileKey: "PrintProfileKey",
      PrintCategory: "PrintCategory",
    },

    EXCEPTIONS: {
      LoggedAt: "LoggedAt",
      Type: "Type",
      OrderName: "OrderName",
      LineItemID: "LineItemID",
      SKU: "SKU",
      Message: "Message",
    },

    BATCHES: {
      BatchID: "BatchID",
      PrintBatchName: "PrintBatchName",
      RoyalMailBatchNumber: "RoyalMailBatchNumber",
      BatchDate: "BatchDate",
      BatchType: "BatchType",
      PrintProfileKey: "PrintProfileKey",
      PrintCategory: "PrintCategory",
      Status: "OrderStatus",
      CreatedAt: "CreatedAt",
      CreatedBy: "CreatedBy",
      PrintedAt: "PrintedAt",
      PrintedBy: "PrintedBy",
      PackAssignedTo: "PackAssignedTo",
      PackStartAt: "PackStartAt",
      PackCompleteAt: "PackCompleteAt",
      TotalPrintUnits: "TotalPrintUnits",
      LineItemCount: "LineItemCount",
      OrderCount: "OrderCount",
      Notes: "Notes",
    },

    BATCH_ORDERS: {
      BatchOrderID: "BatchOrderID",
      BatchID: "BatchID",
      PrintBatchName: "PrintBatchName",
      RoyalMailBatchNumber: "RoyalMailBatchNumber",
      OrderName: "OrderName",
      OrderCreatedAt: "OrderCreatedAt",
      OrderStatus: "OrderStatus",
      OrderItemCount: "OrderItemCount",
      PrintUnits: "PrintUnits",
      LastUpdatedAt: "LastUpdatedAt",
    },

    SHIPMENTS: {
    ShipmentID: "ShipmentID",
    OrderName: "OrderName",
    Postcode: "Postcode",
    RoyalMailTrackingNumber: "RoyalMailTrackingNumber",
    RoyalMailManifestNo: "RoyalMailManifestNo",
    RoyalMailBatchNumber: "RoyalMailBatchNumber",
    DespatchedAt: "DespatchedAt",
    ShippingService: "ShippingService",
    PackageSize: "PackageSize",
    WeightKg: "WeightKg",
    ImportedAt: "ImportedAt",
    SourceFileName: "SourceFileName",
    TrackingStatus: "TrackingStatus",
    },
  },

  /***************
   * Orders status values
   ***************/
  STATUS: {
  HOLD: "Hold",
  NEW: "New",
  IN_PROD: "In Production",
  READY: "Ready to Pack",
  PACKED: "Packed",
  DESPATCHED: "Despatched",
  DELIVERED: "Delivered",
},

ORDER_STATUS_LIST: [
  "Hold",
  "New",
  "In Production",
  "Ready to Pack",
  "Packed",
  "Despatched",
  "Delivered",
],

ROYAL_MAIL: {
  WATCH_FOLDER_ID: "1H0-1gdXgvHvydmHEuMu4LHwCzPmQbHXZ",
  ARCHIVE_FOLDER_ID: "1bazV-1pvtQ2vSzXBDMBQVLJnhneWgncQ",
  POLL_EVERY_MINUTES: 30,
  TRACKING_STATUS_DELIVERED: "Delivered",
},

  /***************
   * Trigger settings
   ***************/
  TRIGGER: {
    FUNCTION_NAME: "syncOrdersFromOrderItems",
    EVERY_MINUTES: 2,
  },

  /***************
   * Setup formatting (setup.js)
   ***************/
  SETUP: {
  FREEZE_HEADER_ROW: true,
  BOLD_HEADERS: true,
  AUTO_RESIZE_COLUMNS: false,
  HEADER_BACKGROUND: "#f3f4f6",
  HEADER_WRAP: false,

  // New
  ROW_BANDING: true,
  BANDING_THEME: "LIGHT_BLUE",
  SKIP_HIDDEN_SHEETS: true,
  APPLY_STATUS_VALIDATION: true,
},


  /***************
   * Derivation + gating behaviour
   ***************/
  DERIVE: {
    OVERWRITE_EXISTING: false,

    // IMPORTANT: scripts expect BLANKISH_VALUES
    BLANKISH_VALUES: ["(blank)", "(blanks)", "blank", "blanks", '""'],

    ON_MISSING_SKU: {
      PRINT_CATEGORY: "Unknown",
      PRINT_UNITS: 0,
      LOG_EXCEPTION: true,
    },

    ON_NONE: {
      PRINT_CATEGORY: "NONE",
      PRINT_UNITS: 0,
    },
  },

  /***************
   * Batching controls
   ***************/
  BATCH: {
    // "ORDER_DATE" = bucket by DATE(CreatedAt)
    // "PRINT_DAY"  = bucket everything into today's date bucket
    DATE_MODE: "ORDER_DATE",
    FULL_DAYS_ONLY: true,
    FULL_DAYS_CUTOFF_DAYS_BACK: 1, // 1 means “exclude today only”

    LOOKBACK_DAYS: 28,

    MIN_LINEITEMS_FOR_AUTO: 2,
    MIN_PRINTUNITS_FOR_AUTO: 2,

    MAX_PRINTUNITS_PER_BATCH: 999,

    CREATE_MISC_PER_DATE: true,
    MISC_PROFILE_KEY: "MISC",
    MISC_CATEGORY: "MISC",

    TYPE_AUTO: "AUTO",
    TYPE_MISC: "MISC",

    STATUS_OPEN: "New",

    NAME_DATE_FORMAT_UK: "dd/MM/yyyy",

    CATEGORY_LABEL_MAP: {
      B108: "10x8",
      B108F: "10x8F",
      B1210: "12x10",
      B125: "12x5",
      B1612C: "16x12C",
      B1616C: "16x16C",
      B1620C: "16x20C",
      B176: "17x6",
      B54: "5x4",
      B64: "6x4",
      B75: "7x5",
      B86: "8x6",
      B86F: "8x6F",
      B96: "9x6",
      BDD: "Digital",
      BKEY: "Keyring",
      BMAG: "Magnet",
      BMUG: "Mug",
      BPP: "Passport",
      BNONP: "Non-Product",
      BNB: "Non-Batch",
      BSTF: "Staff",
      MISC: "MISC",
    },
  },

  /***************
   * Performance controls
   ***************/
  PERF: {
    CHUNK_SIZE: 5000,
    CHECKPOINT_OVERLAP: 200
  },
};
