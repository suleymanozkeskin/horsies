//! Acme domain rows and task payloads.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const CARD_DECLINED: &str = "CARD_DECLINED";
pub const PSP_UNAVAILABLE: &str = "PSP_UNAVAILABLE";
pub const PAYMENT_ALREADY_CAPTURED: &str = "PAYMENT_ALREADY_CAPTURED";
pub const INSUFFICIENT_STOCK: &str = "INSUFFICIENT_STOCK";
pub const UNKNOWN_SKU: &str = "UNKNOWN_SKU";
pub const ORDER_NOT_FOUND: &str = "ORDER_NOT_FOUND";
pub const SHIPMENT_NOT_FOUND: &str = "SHIPMENT_NOT_FOUND";
pub const RETURN_NOT_FOUND: &str = "RETURN_NOT_FOUND";
pub const DAMAGED_ITEM: &str = "DAMAGED_ITEM";
pub const NO_WORKFLOW_CONTEXT: &str = "NO_WORKFLOW_CONTEXT";
pub const QUORUM_NOT_MET: &str = "QUORUM_NOT_MET";
pub const CDN_REJECTED: &str = "CDN_REJECTED";
pub const ORIGIN_REJECTED: &str = "ORIGIN_REJECTED";
pub const SEARCH_INDEX_STALE: &str = "SEARCH_INDEX_STALE";
pub const RECONCILIATION_MISMATCH: &str = "RECONCILIATION_MISMATCH";
pub const COURIER_UNAVAILABLE: &str = "COURIER_UNAVAILABLE";
pub const SUPPLIER_TIMEOUT: &str = "SUPPLIER_TIMEOUT";
pub const DATA_CORRUPTION: &str = "DATA_CORRUPTION";
pub const LOYALTY_ENGINE_BUG: &str = "LOYALTY_ENGINE_BUG";
pub const STORE_UNAVAILABLE: &str = "STORE_UNAVAILABLE";

pub type OrderStatus = String;
pub type PaymentKind = String;
pub type ReturnStatus = String;
pub type ItemCondition = String;

pub const ORDER_PLACED: &str = "placed";
pub const ORDER_VALIDATED: &str = "validated";
pub const ORDER_RESERVED: &str = "reserved";
pub const ORDER_AUTHORIZED: &str = "authorized";
pub const ORDER_PACKED: &str = "packed";
pub const ORDER_SHIPPED: &str = "shipped";
pub const ORDER_CAPTURED: &str = "captured";
pub const ORDER_FAILED: &str = "failed";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Product {
    pub sku: String,
    pub name: String,
    pub category: String,
    pub price_cents: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StockLevel {
    pub sku: String,
    pub on_hand: i32,
    pub reserved: i32,
}

impl StockLevel {
    pub fn available(&self) -> i32 {
        self.on_hand - self.reserved
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogEntry {
    pub product: Product,
    pub stock: StockLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderLine {
    pub line_no: i32,
    pub sku: String,
    pub size_code: String,
    pub quantity: i32,
    pub unit_price_cents: i32,
}

impl OrderLine {
    pub fn line_total_cents(&self) -> i32 {
        self.quantity * self.unit_price_cents
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Order {
    pub order_id: String,
    pub customer_id: String,
    pub status: OrderStatus,
    pub total_cents: i32,
    pub lines: Vec<OrderLine>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaymentIntent {
    pub payment_id: String,
    pub order_id: String,
    pub kind: PaymentKind,
    pub amount_cents: i32,
    pub psp_reference: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Shipment {
    pub shipment_id: String,
    pub order_id: String,
    pub courier: String,
    pub express: bool,
    pub attempts: i32,
    pub booking_reference: Option<String>,
    pub label_url: Option<String>,
    pub tracking_code: Option<String>,
}

macro_rules! payload {
    ($(#[$meta:meta])* $name:ident { $($(#[$field_meta:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        pub struct $name { $( $(#[$field_meta])* pub $field: $ty, )* }
    };
}

payload!(OrderValidation {
    order_id: String,
    line_count: i32,
    total_cents: i32
});
payload!(StockReservation {
    order_id: String,
    sku: String,
    quantity: i32,
    available_after: i32,
    replayed: bool
});
payload!(StockRelease {
    sku: String,
    quantity: i32,
    available_after: i32
});
payload!(StocktakeSummary {
    skus_topped_up: i32,
    reservations_cleared: i32,
    target_units: i32
});
payload!(WarehouseAllocation {
    order_id: String,
    warehouse_code: String,
    distance_km: i32
});
payload!(PaymentAuthorization {
    order_id: String,
    authorization_id: String,
    amount_cents: i32,
    psp_reference: String,
    attempt: i32,
    replayed: bool
});
payload!(PaymentCapture {
    order_id: String,
    capture_id: String,
    authorization_id: String,
    amount_cents: i32,
    replayed: bool
});
payload!(PaymentRefund {
    order_id: String,
    refund_id: String,
    amount_cents: i32
});
payload!(PickPack { order_id: String, station: String, units_picked: i32, workflow_id: Option<String>, task_index: Option<i32> });
payload!(Invoice {
    order_id: String,
    invoice_number: String,
    total_cents: i32,
    render_ms: i32
});
payload!(CourierBooking {
    order_id: String,
    courier: String,
    express: bool,
    booking_reference: String,
    attempt: i32,
    replayed: bool
});
payload!(ShippingLabel {
    order_id: String,
    label_url: String,
    label_format: String
});
payload!(TrackingSeed {
    order_id: String,
    courier: String,
    tracking_code: String,
    tracking_url: String
});
payload!(EmailReceipt {
    order_id: String,
    template: String,
    recipient: String
});
payload!(PromotionOutcome { order_id: String, discount_cents: i32, applied_codes: Vec<String> });
payload!(LoyaltyPoints {
    customer_id: String,
    order_id: String,
    points: i32,
    tier: String
});
payload!(SupplierFeed {
    supplier: String,
    sku_count: i32,
    changed_count: i32
});
payload!(StockUpdate {
    supplier: String,
    applied: i32,
    skipped: i32
});
payload!(ReturnCase { return_id: String, order_id: String, sku: String, quantity: i32, status: ReturnStatus, condition: Option<ItemCondition>, created_at: DateTime<Utc> });
payload!(ReturnReceipt {
    return_id: String,
    order_id: String,
    sku: String,
    quantity: i32
});
payload!(Inspection {
    return_id: String,
    sku: String,
    condition: ItemCondition,
    notes: String
});
payload!(RestockDecision { return_id: String, sku: String, quantity: i32, outcome: String, available_after: Option<i32> });
payload!(RestockPlan { suppliers_reporting: Vec<String>, suppliers_missing: Vec<String>, skus_adjusted: i32, units_added: i32 });
payload!(SalesRollup {
    orders_counted: i32,
    gross_cents: i32,
    captured_cents: i32
});
payload!(AbandonedCartSweep { swept: i32, oldest_order_id: Option<String> });
payload!(RegionalRollup {
    region: String,
    orders_counted: i32,
    gross_cents: i32
});
payload!(RetentionAudit {
    older_than_days: i32,
    orders_examined: i32,
    rows_prunable: i32
});
payload!(CatalogChunk {
    chunk_index: i32,
    rows: i32,
    checksum: String
});
payload!(PricePush {
    sku: String,
    price_cents: i32,
    target: String
});
payload!(CacheWarm {
    target: String,
    keys_warmed: i32
});
payload!(SearchPrewarm {
    documents: i32,
    index_name: String
});
payload!(PriceUpdate {
    sku: String,
    was_cents: i32,
    now_cents: i32
});
payload!(ReconciliationReport {
    authorizations: i32,
    captures: i32,
    unmatched: i32
});
payload!(ExportManifest {
    export_id: String,
    rows: i32
});
payload!(ShippingNotice {
    order_id: String,
    recipient: String,
    tracking_code: String
});
payload!(MarketingBlast {
    segment: String,
    recipients: i32
});
