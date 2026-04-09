//! Task definitions for the shipping example.

use std::collections::HashMap;

use chrono::Utc;
use uuid::Uuid;

use horsies::{Horsies, TaskError, TaskFunction, TaskResult, WorkflowInput};

use super::models::*;

// ---------------------------------------------------------------------------
// Args-from input structs
// ---------------------------------------------------------------------------

/// Input for tasks that receive the validated order via args_from.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, WorkflowInput)]
pub struct OrderArgsFrom {
    pub order: TaskResult<ValidatedOrder>,
}

/// Aggregated input for reserve_inventory (fan-in from three parallel checks).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, WorkflowInput)]
pub struct ReserveInput {
    pub inventory: TaskResult<InventoryStatus>,
    pub cost: TaskResult<ShippingCost>,
    pub address: TaskResult<AddressValidation>,
}

/// Input for create_shipment via args_from.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, WorkflowInput)]
pub struct ShipmentArgsFrom {
    pub reservation: TaskResult<Reservation>,
}

/// Input for send_notification via args_from.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, WorkflowInput)]
pub struct NotifyArgsFrom {
    pub shipment: TaskResult<Shipment>,
}

// ---------------------------------------------------------------------------
// Task functions — annotated with #[horsies::task]
// ---------------------------------------------------------------------------

#[horsies::task("validate_order", queue = "standard")]
pub async fn validate_order(order: Order) -> Result<ValidatedOrder, TaskError> {
    if order.order_id.is_empty() {
        return Err(TaskError::user("INVALID_ORDER_ID", "Order ID is required"));
    }
    if order.items.is_empty() {
        return Err(TaskError::user(
            "EMPTY_ORDER",
            "Order must contain at least one item",
        ));
    }
    if order.customer_email.is_empty() {
        return Err(TaskError::user(
            "MISSING_EMAIL",
            "Customer email is required",
        ));
    }

    Ok(ValidatedOrder {
        order_id: order.order_id,
        customer_email: order.customer_email,
        items: order.items,
        shipping_address: order.shipping_address,
        shipping_method: order.shipping_method,
        validated_at: Utc::now(),
    })
}

#[horsies::task("check_inventory", queue = "standard")]
pub async fn check_inventory(args: OrderArgsFrom) -> Result<InventoryStatus, TaskError> {
    let order = match args.order {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!(
                    "Cannot check inventory: upstream failed - {:?}",
                    e.error_code
                ),
            ))
        }
    };

    let availability: HashMap<String, bool> = order
        .items
        .iter()
        .map(|item| (item.sku.clone(), true))
        .collect();

    Ok(InventoryStatus {
        order_id: order.order_id,
        all_available: availability.values().all(|&v| v),
        item_availability: availability,
    })
}

#[horsies::task("calculate_shipping_cost", queue = "standard")]
pub async fn calculate_shipping_cost(args: OrderArgsFrom) -> Result<ShippingCost, TaskError> {
    let order = match args.order {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!(
                    "Cannot calculate cost: upstream failed - {:?}",
                    e.error_code
                ),
            ))
        }
    };

    let total_weight: f64 = order.items.iter().map(|i| i.quantity as f64 * 0.5).sum();

    let base_cost: u64 = match order.shipping_method {
        ShippingMethod::Standard => 500,
        ShippingMethod::Express => 1200,
        ShippingMethod::Overnight => 2500,
    };
    let weight_cost = (total_weight * 200.0) as u64;

    Ok(ShippingCost {
        order_id: order.order_id,
        base_cost_cents: base_cost,
        total_weight_kg: total_weight,
        total_cost_cents: base_cost + weight_cost,
    })
}

#[horsies::task("check_address", queue = "standard")]
pub async fn check_address(args: OrderArgsFrom) -> Result<AddressValidation, TaskError> {
    let order = match args.order {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!("Cannot check address: upstream failed - {:?}", e.error_code),
            ))
        }
    };

    let addr = &order.shipping_address;
    let is_valid = !addr.street.is_empty() && !addr.city.is_empty() && !addr.postal_code.is_empty();

    Ok(AddressValidation {
        order_id: order.order_id,
        is_valid,
        normalized_address: if is_valid { Some(addr.clone()) } else { None },
        validation_notes: if is_valid {
            None
        } else {
            Some("Incomplete address".into())
        },
    })
}

#[horsies::task("reserve_inventory", queue = "urgent")]
pub async fn reserve_inventory(input: ReserveInput) -> Result<Reservation, TaskError> {
    let inventory = match input.inventory {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!(
                    "Cannot reserve: inventory check failed - {:?}",
                    e.error_code
                ),
            ))
        }
    };
    let cost = match input.cost {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!(
                    "Cannot reserve: cost calculation failed - {:?}",
                    e.error_code
                ),
            ))
        }
    };
    let address = match input.address {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!("Cannot reserve: address check failed - {:?}", e.error_code),
            ))
        }
    };

    if !inventory.all_available {
        return Err(TaskError::user(
            "ITEMS_UNAVAILABLE",
            "Some items are not available",
        ));
    }
    if !address.is_valid {
        return Err(TaskError::user(
            "INVALID_ADDRESS",
            "Shipping address is invalid",
        ));
    }

    Ok(Reservation {
        order_id: inventory.order_id.clone(),
        reservation_id: Uuid::new_v4().to_string(),
        reserved_items: Vec::new(),
        reserved_at: Utc::now(),
        shipping_cost_cents: cost.total_cost_cents,
    })
}

#[horsies::task("create_shipment", queue = "urgent")]
pub async fn create_shipment(args: ShipmentArgsFrom) -> Result<Shipment, TaskError> {
    let reservation = match args.reservation {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!(
                    "Cannot create shipment: upstream failed - {:?}",
                    e.error_code
                ),
            ))
        }
    };

    Ok(Shipment {
        order_id: reservation.order_id,
        shipment_id: Uuid::new_v4().to_string(),
        tracking_number: format!("TRK{}", &Uuid::new_v4().to_string()[..12]).to_uppercase(),
        carrier: "FastShip".into(),
        estimated_delivery: Utc::now() + chrono::Duration::days(3),
        created_at: Utc::now(),
    })
}

#[horsies::task("send_notification", queue = "low")]
pub async fn send_notification(args: NotifyArgsFrom) -> Result<NotificationResult, TaskError> {
    let shipment = match args.shipment {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::user(
                "UPSTREAM_FAILED",
                format!("Cannot notify: upstream failed - {:?}", e.error_code),
            ))
        }
    };

    Ok(NotificationResult {
        order_id: shipment.order_id,
        email_sent: true,
        sms_sent: true,
        notification_id: Uuid::new_v4().to_string(),
    })
}

// ---------------------------------------------------------------------------
// Registration — one line per task via descriptors
// ---------------------------------------------------------------------------

pub struct Tasks {
    pub validate_order: TaskFunction<Order, ValidatedOrder>,
    pub check_inventory: TaskFunction<OrderArgsFrom, InventoryStatus>,
    pub calculate_shipping_cost: TaskFunction<OrderArgsFrom, ShippingCost>,
    pub check_address: TaskFunction<OrderArgsFrom, AddressValidation>,
    pub reserve_inventory: TaskFunction<ReserveInput, Reservation>,
    pub create_shipment: TaskFunction<ShipmentArgsFrom, Shipment>,
    pub send_notification: TaskFunction<NotifyArgsFrom, NotificationResult>,
}

pub fn register(app: &mut Horsies) -> Result<Tasks, Box<dyn std::error::Error>> {
    Ok(Tasks {
        validate_order: validate_order::register(app)?,
        check_inventory: check_inventory::register(app)?,
        calculate_shipping_cost: calculate_shipping_cost::register(app)?,
        check_address: check_address::register(app)?,
        reserve_inventory: reserve_inventory::register(app)?,
        create_shipment: create_shipment::register(app)?,
        send_notification: send_notification::register(app)?,
    })
}
