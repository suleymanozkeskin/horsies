//! All rates, durations, queues, and deterministic fixtures for the demo.

use crate::simulate::WorkEnvelope;

pub const CATALOG_SIZE: usize = 60;
pub const CATALOG_STOCK_PER_SKU: i32 = 500;
pub const DISCONTINUED_SKU_COUNT: usize = 4;
pub const MIN_LINES_PER_ORDER: usize = 1;
pub const MAX_LINES_PER_ORDER: usize = 3;
pub const MIN_QTY_PER_LINE: i32 = 1;
pub const MAX_QTY_PER_LINE: i32 = 4;
pub const MIN_PRICE_CENTS: i32 = 1_290;
pub const MAX_PRICE_CENTS: i32 = 14_900;

pub const STEADY_MIN_INTERARRIVAL_SECONDS: u64 = 4;
pub const STEADY_MAX_INTERARRIVAL_SECONDS: u64 = 8;
pub const STEADY_TIMEZONE: &str = "Europe/Berlin";
pub const STEADY_HOURLY_DEMAND: [f64; 24] = [
    0.25, 0.18, 0.14, 0.12, 0.12, 0.15, 0.25, 0.40, 0.55, 0.65, 0.70, 0.75, 0.85, 0.80, 0.70, 0.65,
    0.70, 0.80, 0.90, 1.00, 1.00, 0.90, 0.65, 0.40,
];
pub const STEADY_RIPPLE_PERIOD_MINUTES: u64 = 45;
pub const STEADY_RIPPLE_AMPLITUDE: f64 = 0.20;

pub const PSP_UNAVAILABLE_RATE: f64 = 0.20;
pub const PSP_FAILING_ATTEMPTS: i32 = 2;
pub const CARD_DECLINE_RATE: f64 = 0.08;
pub const STOCK_SHORTFALL_RATE: f64 = 0.05;
pub const INVOICE_HANG_RATE: f64 = 0.03;
pub const COURIER_FLAKE_RATE: f64 = 0.10;
pub const COURIER_FAILING_ATTEMPTS: i32 = 1;
pub const PROMOTION_BUNDLE_BUG_RATE: f64 = 0.04;
pub const PROMOTION_SIZE_CODE_BUG_RATE: f64 = 0.04;
pub const LOYALTY_ENGINE_BUG_RATE: f64 = 0.02;
pub const SUPPLIER_TIMEOUT_RATE: f64 = 0.25;
pub const STOCKTAKE_CEILING_UNITS: i32 = 5_000;
pub const REPLENISH_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 4_000);

pub const PSP_RETRY_BASE_SECONDS: u32 = 5;
pub const PSP_MAX_RETRIES: u32 = 4;
pub const COURIER_RETRY_BASE_SECONDS: u32 = 3;
pub const COURIER_MAX_RETRIES: u32 = 3;
pub const SUPPLIER_RETRY_INTERVALS_SECONDS: &[u32] = &[10, 30, 60];
pub const CRASH_RETRY_INTERVALS_SECONDS: &[u32] = &[3, 10];

pub const VALIDATE_ORDER_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 3_500);
pub const RESERVE_STOCK_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 4_000);
pub const RELEASE_STOCK_WORK: WorkEnvelope = WorkEnvelope::new(1_500, 3_000);
pub const ALLOCATE_WAREHOUSE_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 4_000);
pub const AUTHORIZE_PAYMENT_WORK: WorkEnvelope = WorkEnvelope::new(2_500, 5_000);
pub const CAPTURE_PAYMENT_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 4_000);
pub const REFUND_PAYMENT_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 4_000);
pub const PICK_PACK_WORK: WorkEnvelope = WorkEnvelope::new(3_000, 6_000);
pub const GENERATE_INVOICE_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 5_000);
pub const BOOK_COURIER_WORK: WorkEnvelope = WorkEnvelope::new(2_500, 5_000);
pub const PRINT_LABEL_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 3_500);
pub const TRACKING_SEED_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 3_000);
pub const SEND_ORDER_EMAIL_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 3_500);
pub const APPLY_PROMOTIONS_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 4_000);
pub const LOYALTY_POINTS_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 3_500);
pub const SUPPLIER_FEED_WORK: WorkEnvelope = WorkEnvelope::new(3_000, 6_000);
pub const UPDATE_STOCK_LEVELS_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 4_000);
pub const INVOICE_TIMEOUT_MS: u32 = 8_000;
pub const INVOICE_HANG_MS: u64 = 20_000;

pub const RETURN_SPAWN_EVERY: usize = 6;
pub const RETURN_DAMAGE_RATE: f64 = 0.30;
pub const RECEIVE_RETURN_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 3_500);
pub const INSPECT_ITEM_WORK: WorkEnvelope = WorkEnvelope::new(2_500, 5_000);
pub const RESTOCK_OR_WRITEOFF_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 4_000);
pub const RESTOCK_SPAWN_EVERY: usize = 20;
pub const RESTOCK_MIN_SUCCESSFUL_FEEDS: usize = 2;
pub const RESTOCK_UNITS_PER_SUPPLIER: i32 = 40;
pub const RESTOCK_SKUS_PER_SUPPLIER: usize = 5;

pub const FLASH_SALE_SKUS: usize = 6;
pub const FLASH_SALE_DISCOUNT_PERCENT: i32 = 30;
pub const CDN_REJECT_RATE: f64 = 0.35;
pub const ORIGIN_REJECT_RATE: f64 = 0.35;
pub const SEARCH_PREWARM_FAIL_RATE: f64 = 0.50;
pub const EXPIRING_PRICE_SENDS: usize = 80;
pub const PRICE_GOOD_UNTIL_SECONDS: i64 = 45;
pub const PUBLISH_CDN_WORK: WorkEnvelope = WorkEnvelope::new(2_500, 4_500);
pub const PUBLISH_ORIGIN_WORK: WorkEnvelope = WorkEnvelope::new(2_500, 4_500);
pub const PREWARM_SEARCH_WORK: WorkEnvelope = WorkEnvelope::new(3_000, 5_000);
pub const WARM_CACHE_EDGE_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 3_500);
pub const UPDATE_PRICE_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 3_000);

pub const CATALOG_IMPORT_CHUNKS: usize = 40;
pub const CATALOG_IMPORT_CHUNK_WORK: WorkEnvelope = WorkEnvelope::new(7_000, 9_000);
pub const CATALOG_IMPORT_ROWS_PER_CHUNK: usize = 500;
pub const SALES_ROLLUP_WORK: WorkEnvelope = WorkEnvelope::new(3_000, 5_000);
pub const ABANDONED_CART_WORK: WorkEnvelope = WorkEnvelope::new(2_500, 4_000);
pub const RECONCILE_PAYMENTS_WORK: WorkEnvelope = WorkEnvelope::new(3_000, 5_000);
pub const FLAKY_EXPORT_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 4_000);
pub const ABANDONED_CART_AGE_MINUTES: i32 = 15;
pub const CHAOS_EXPORT_CRASH_RATE: f64 = 0.50;
pub const CHAOS_EXPORT_SPACING_SECONDS: u64 = 30;
pub const CHAOS_EXPORT_RETRY_INTERVALS_SECONDS: &[u32] = &[30, 60];
pub const SEND_SHIPPING_SMS_WORK: WorkEnvelope = WorkEnvelope::new(2_000, 3_000);
pub const MARKETING_BLAST_WORK: WorkEnvelope = WorkEnvelope::new(4_000, 7_000);
pub const MARKETING_BLAST_SEGMENTS: usize = 40;
pub const MARKETING_SEGMENT_SIZE: usize = 2_500;

pub const RUSH_ORDER_COUNT: usize = 50;
pub const RUSH_WINDOW_SECONDS: u64 = 30;
pub const PROBLEM_CHILD_RETURNS: usize = 10;
pub const PROBLEM_CHILD_DECLINES: usize = 8;
pub const CHAOS_EXPORT_COUNT: usize = 4;
pub const SUPPLIER_FEED_INTERVAL_SECONDS: u64 = 90;
pub const ABANDONED_CART_MINUTE: u8 = 5;
pub const SALES_ROLLUP_HOUR: u8 = 3;
pub const RECONCILE_HOUR_STEP: u8 = 4;
pub const RECONCILE_MINUTE: u8 = 15;
pub const REGIONS: &[&str] = &["eu-central", "uk", "turkiye", "nordics"];
pub const CACHE_WARM_INTERVAL_MINUTES: u64 = 5;
pub const SEARCH_PREWARM_INTERVAL_MINUTES: u64 = 10;
pub const RETENTION_AUDIT_DAYS: i32 = 30;
pub const PRICE_SYNC_MINUTE_STEP: u8 = 15;

pub const COURIERS: &[&str] = &["fleetline", "northgate", "palermo-express"];
pub const EXPRESS_RATE: f64 = 0.30;
pub const WAREHOUSES: &[&str] = &["LEI-1", "ROT-2", "IST-3"];
pub const PICK_STATIONS: &[&str] = &["A1", "A2", "B1", "B2", "C1"];
pub const SUPPLIERS: &[&str] = &["atlas-textiles", "brera-knitwear", "coastline-denim"];
pub const PRODUCT_LINES: &[&str] = &[
    "Oversized Tee",
    "Cropped Hoodie",
    "Wide-Leg Jean",
    "Ribbed Knit",
    "Poplin Shirt",
    "Cargo Skirt",
    "Puffer Vest",
    "Slip Dress",
];
pub const PRODUCT_COLOURS: &[&str] = &["Bone", "Ink", "Sage", "Rust", "Cobalt", "Ecru"];
pub const PRODUCT_CATEGORIES: &[&str] = &["tops", "bottoms", "outerwear", "dresses"];
pub const SIZE_CODES: &[&str] = &["XS", "S", "M", "L", "XL"];
pub const CORRUPT_SIZE_CODE: &str = "XXL";
pub const PROMOTION_CODES: &[&str] = &["SPRING10", "BUNDLE3", "LOYAL15", "FREESHIP"];
pub const BUNDLE_MIN_QUANTITY: i32 = 2;
pub const BUNDLE_PRICE_FLOOR_CENTS: i32 = 1_000;
pub const BUNDLE_POT_DIVISOR: i32 = 10;
pub const CLEARANCE_PRICE_CENTS: i32 = 690;
pub const LOYALTY_POINTS_PER_EURO: i32 = 2;
pub const LOYALTY_LIFETIME_MAX: i32 = 9_000;
pub const LOYALTY_LIFETIME_BUG_MIN: i32 = 10_000;
pub const LOYALTY_LIFETIME_BUG_MAX: i32 = 40_000;

// Demo application settings that are intentionally tuned beside the
// simulation constants.
pub const WORKER_STATE_SNAPSHOT_INTERVAL_MS: u64 = 10_000;
pub const RECOVERY_CHECK_INTERVAL_MS: u64 = 10_000;
pub const TERMINAL_RECORD_RETENTION_HOURS: u32 = 24;
