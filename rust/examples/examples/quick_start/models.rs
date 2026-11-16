//! Domain models for the shipping example.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShippingMethod {
    Standard,
    Express,
    Overnight,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderItem {
    pub sku: String,
    pub quantity: u32,
    pub price_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    pub street: String,
    pub city: String,
    pub state: String,
    pub postal_code: String,
    pub country: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    pub customer_email: String,
    pub items: Vec<OrderItem>,
    pub shipping_address: Address,
    pub shipping_method: ShippingMethod,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatedOrder {
    pub order_id: String,
    pub customer_email: String,
    pub items: Vec<OrderItem>,
    pub shipping_address: Address,
    pub shipping_method: ShippingMethod,
    pub validated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryStatus {
    pub order_id: String,
    pub all_available: bool,
    pub item_availability: std::collections::HashMap<String, bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShippingCost {
    pub order_id: String,
    pub base_cost_cents: u64,
    pub total_weight_kg: f64,
    pub total_cost_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressValidation {
    pub order_id: String,
    pub is_valid: bool,
    pub normalized_address: Option<Address>,
    pub validation_notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reservation {
    pub order_id: String,
    pub reservation_id: String,
    pub reserved_items: Vec<OrderItem>,
    pub reserved_at: DateTime<Utc>,
    pub shipping_cost_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shipment {
    pub order_id: String,
    pub shipment_id: String,
    pub tracking_number: String,
    pub carrier: String,
    pub estimated_delivery: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationResult {
    pub order_id: String,
    pub email_sent: bool,
    pub sms_sent: bool,
    pub notification_id: String,
}
