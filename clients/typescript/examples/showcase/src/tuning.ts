// Every rate, count, bound and fixture of the demo, in one place. The values are the ones of the Rust showcase, so the
// same domain ids give the same faults.

/** The products of the seed catalog. */
export const CATALOG_SIZE = 60;

/** The units on hand of each product of the seed catalog, and the target of the nightly stocktake. */
export const CATALOG_STOCK_PER_SKU = 500;

/** The products of the seed catalog that are out of stock. */
export const DISCONTINUED_SKU_COUNT = 4;
export const MIN_LINES_PER_ORDER = 1;
export const MAX_LINES_PER_ORDER = 3;
export const MIN_QTY_PER_LINE = 1;
export const MAX_QTY_PER_LINE = 4;
export const MIN_PRICE_CENTS = 1_290;
export const MAX_PRICE_CENTS = 14_900;

export const STEADY_MIN_INTERARRIVAL_SECONDS = 4;
export const STEADY_MAX_INTERARRIVAL_SECONDS = 8;

/** The zone of the demand curve. */
export const STEADY_TIMEZONE = "Europe/Berlin";

/** The demand factor of each hour of the day in `STEADY_TIMEZONE`, 0 to 23. */
export const STEADY_HOURLY_DEMAND: readonly number[] = [
  0.25, 0.18, 0.14, 0.12, 0.12, 0.15, 0.25, 0.4, 0.55, 0.65, 0.7, 0.75, 0.85, 0.8, 0.7, 0.65, 0.7, 0.8, 0.9, 1.0, 1.0, 0.9,
  0.65, 0.4,
];

export const STEADY_RIPPLE_PERIOD_MINUTES = 45;
export const STEADY_RIPPLE_AMPLITUDE = 0.2;

/** The lowest demand factor. */
export const STEADY_MIN_DEMAND = 0.05;

/** The most orders that one `steady` run places: the run is bounded. */
export const STEADY_MAX_ORDERS = 10_000;

/** The largest pace divisor of `steady --pace`. */
export const STEADY_MAX_PACE = 1_000;

export const PSP_UNAVAILABLE_RATE = 0.2;

/** The attempts of a drawn provider outage that fail before the provider answers. */
export const PSP_FAILING_ATTEMPTS = 2;
export const CARD_DECLINE_RATE = 0.08;
export const STOCK_SHORTFALL_RATE = 0.05;
export const INVOICE_HANG_RATE = 0.03;
export const COURIER_FLAKE_RATE = 0.1;

/** The attempts of a drawn courier flake that fail before the courier answers. */
export const COURIER_FAILING_ATTEMPTS = 1;
export const PROMOTION_BUNDLE_BUG_RATE = 0.04;
export const PROMOTION_SIZE_CODE_BUG_RATE = 0.04;
export const LOYALTY_ENGINE_BUG_RATE = 0.02;
export const SUPPLIER_TIMEOUT_RATE = 0.25;

/** The most units that the nightly stocktake leaves on the shelf of one SKU. */
export const STOCKTAKE_CEILING_UNITS = 5_000;

export const PSP_RETRY_BASE_SECONDS = 5;
export const PSP_MAX_RETRIES = 4;
export const COURIER_RETRY_BASE_SECONDS = 3;
export const COURIER_MAX_RETRIES = 3;
export const SUPPLIER_RETRY_INTERVALS_SECONDS: readonly [number, ...number[]] = [10, 30, 60];
export const CRASH_RETRY_INTERVALS_SECONDS: readonly [number, ...number[]] = [3, 10];
export const CHAOS_EXPORT_RETRY_INTERVALS_SECONDS: readonly [number, ...number[]] = [30, 60];

/** The deadline of `generate_invoice`. */
export const INVOICE_TIMEOUT_MS = 8_000;

/** The deadline of each task that runs on a seat of a shared async child. */
export const SHARED_TASK_TIMEOUT_MS = 30_000;

/** `steady` starts a return workflow for every order whose ordinal is a multiple of this. */
export const RETURN_SPAWN_EVERY = 6;
export const RETURN_DAMAGE_RATE = 0.3;

/** `steady` starts a restock workflow for every order whose ordinal is a multiple of this. */
export const RESTOCK_SPAWN_EVERY = 20;

/** The supplier feeds that a restock needs. */
export const RESTOCK_MIN_SUCCESSFUL_FEEDS = 2;
export const RESTOCK_SKUS_PER_SUPPLIER = 5;

export const FLASH_SALE_DISCOUNT_PERCENT = 30;
export const EXPIRING_PRICE_SENDS = 80;
export const PRICE_GOOD_UNTIL_SECONDS = 45;

export const CATALOG_IMPORT_CHUNKS = 40;
export const CATALOG_IMPORT_ROWS_PER_CHUNK = 500;
export const ABANDONED_CART_AGE_MINUTES = 15;
export const CHAOS_EXPORT_CRASH_RATE = 0.5;
export const MARKETING_SEGMENT_SIZE = 2_500;

export const RUSH_ORDER_COUNT = 50;
export const RUSH_WINDOW_SECONDS = 30;
export const PROBLEM_CHILD_RETURNS = 10;
export const PROBLEM_CHILD_DECLINES = 8;
export const CHAOS_EXPORT_COUNT = 4;

export const COURIERS: readonly [string, ...string[]] = ["fleetline", "northgate", "palermo-express"];
export const EXPRESS_RATE = 0.3;
export const WAREHOUSES: readonly [string, ...string[]] = ["LEI-1", "ROT-2", "IST-3"];
export const PICK_STATIONS: readonly [string, ...string[]] = ["A1", "A2", "B1", "B2", "C1"];

/** The three suppliers: each one feeds its own parameter of `update_stock_levels`. */
export const SUPPLIERS: readonly [string, string, string] = ["atlas-textiles", "brera-knitwear", "coastline-denim"];

export const PRODUCT_LINES: readonly [string, ...string[]] = [
  "Oversized Tee",
  "Cropped Hoodie",
  "Wide-Leg Jean",
  "Ribbed Knit",
  "Poplin Shirt",
  "Cargo Skirt",
  "Puffer Vest",
  "Slip Dress",
];

export const PRODUCT_COLOURS: readonly [string, ...string[]] = ["Bone", "Ink", "Sage", "Rust", "Cobalt", "Ecru"];
export const PRODUCT_CATEGORIES: readonly [string, ...string[]] = ["tops", "bottoms", "outerwear", "dresses"];
export const SIZE_CODES: readonly [string, ...string[]] = ["XS", "S", "M", "L", "XL"];

/** The size code that the promotion story writes for a drawn data fault. */
export const CORRUPT_SIZE_CODE = "XXL";
export const PROMOTION_CODES: readonly [string, ...string[]] = ["SPRING10", "BUNDLE3", "LOYAL15", "FREESHIP"];
export const BUNDLE_MIN_QUANTITY = 2;
export const BUNDLE_PRICE_FLOOR_CENTS = 1_000;
export const BUNDLE_POT_DIVISOR = 10;
export const CLEARANCE_PRICE_CENTS = 690;
export const LOYALTY_POINTS_PER_EURO = 2;
export const LOYALTY_LIFETIME_MAX = 9_000;

/** How often each worker writes its state snapshot. */
export const WORKER_STATE_SNAPSHOT_INTERVAL_MS = 10_000;

/** How often the recovery pass runs. */
export const RECOVERY_CHECK_INTERVAL_MS = 10_000;

/** How long the history keeps a finished task or workflow. */
export const TERMINAL_RECORD_RETENTION_HOURS = 24;

/** The retention period of a rerun task, in days: a retention class that the app config registers. */
export const RERUN_RETENTION_DAYS = 7;
