//! Database-backed adapters for the order workflow.
//!
//! The showcase keeps the task registry JSON-shaped so every workflow can be
//! built from one public handle type.  These adapters retain the source's
//! order, stock, payment, and courier effects at that boundary.

use horsies::TaskError;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::domain::{
    CARD_DECLINED, COURIER_UNAVAILABLE, INSUFFICIENT_STOCK, ORDER_CLOSED, ORDER_NOT_FOUND,
    SHIPMENT_NOT_FOUND, UNKNOWN_SKU,
};
use crate::settings::resolve_database_settings;
use crate::store::Store;
use crate::{simulate, tuning};

use super::promotions::{self, LoyaltyArgs, PromotionArgs};
use super::store_failure;

#[derive(Debug, Deserialize)]
struct ValidateArgs {
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct ReserveArgs {
    order_id: String,
    line_no: i32,
    sku: String,
    quantity: i32,
}

#[derive(Debug, Deserialize)]
struct AuthorizeArgs {
    order_id: String,
    amount_cents: i32,
}

#[derive(Debug, Deserialize)]
struct CaptureArgs {
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct OrderArgs {
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct CourierArgs {
    order_id: String,
    courier: String,
    express: bool,
}

#[derive(Debug, Deserialize)]
struct LabelArgs {
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct EmailArgs {
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct ReleaseArgs {
    sku: String,
    quantity: i32,
}

#[derive(Debug, Deserialize)]
struct StocktakeArgs {
    target_units: i32,
}

pub async fn apply_promotions(input: Value) -> Result<Value, TaskError> {
    let args: PromotionArgs = parse(input)?;
    let result = match promotions::apply_promotions(args.clone()).await {
        Ok(result) => result,
        Err(error) => {
            if let Ok(store) = store().await {
                compensate_failed_order(&store, &args.order_id).await;
            }
            return Err(error);
        }
    };
    serde_json::to_value(result).map_err(|error| invalid_input(error.to_string()))
}

pub async fn compute_loyalty_points(input: Value) -> Result<Value, TaskError> {
    let args: LoyaltyArgs = parse(input)?;
    let result = promotions::compute_loyalty_points(args).await?;
    serde_json::to_value(result).map_err(|error| invalid_input(error.to_string()))
}

fn invalid_input(message: impl Into<String>) -> TaskError {
    TaskError::new("TASK_ERROR", message)
}

async fn store() -> Result<Store, TaskError> {
    let settings = resolve_database_settings().map_err(|error| store_failure("settings", error))?;
    Store::connect(&settings)
        .await
        .map_err(|error| store_failure("connect", error))
}

fn parse<T: for<'de> Deserialize<'de>>(input: Value) -> Result<T, TaskError> {
    serde_json::from_value(input)
        .map_err(|error| invalid_input(format!("invalid task input: {error}")))
}

pub async fn validate_order(input: Value) -> Result<Value, TaskError> {
    let args: ValidateArgs = parse(input)?;
    let store = store().await?;
    let order = store
        .get_order(&args.order_id)
        .await
        .map_err(|error| store_failure("get_order", error))?
        .ok_or_else(|| TaskError::new(ORDER_NOT_FOUND, format!("no order {}", args.order_id)))?;
    if order.lines.is_empty() {
        return Err(TaskError::new(ORDER_NOT_FOUND, "order has no lines"));
    }
    store
        .set_order_status(&args.order_id, &"validated".to_owned())
        .await
        .map_err(|error| store_failure("set_order_status", error))?;
    Ok(json!({
        "order_id": args.order_id,
        "line_count": order.lines.len(),
        "total_cents": order.total_cents,
    }))
}

pub async fn reserve_stock(input: Value) -> Result<Value, TaskError> {
    let args: ReserveArgs = parse(input)?;
    let store = store().await?;
    let outcome = store
        .reserve_line(&args.order_id, args.line_no, &args.sku, args.quantity)
        .await
        .map_err(|error| store_failure("reserve_line", error))?;
    if !outcome.order_open {
        return Err(TaskError::new(
            ORDER_CLOSED,
            format!("order {} no longer accepts reservations", args.order_id),
        ));
    }
    if !outcome.known_sku {
        compensate_failed_order(&store, &args.order_id).await;
        return Err(TaskError::new(
            UNKNOWN_SKU,
            format!("{} is not in the catalog", args.sku),
        ));
    }
    if !outcome.reserved {
        compensate_failed_order(&store, &args.order_id).await;
        return Err(TaskError::new(
            INSUFFICIENT_STOCK,
            format!(
                "{} has {} available, needs {}",
                args.sku, outcome.available, args.quantity
            ),
        ));
    }
    Ok(json!({
        "order_id": args.order_id,
        "sku": args.sku,
        "quantity": args.quantity,
        "available_after": outcome.available,
        "replayed": outcome.replayed,
    }))
}

async fn compensate_failed_order(store: &Store, order_id: &str) {
    if let Err(error) = store.fail_order_and_release_reservations(order_id).await {
        eprintln!("reservation compensation failed for {order_id}: {error}");
    }
}

pub async fn release_stock(input: Value) -> Result<Value, TaskError> {
    let args: ReleaseArgs = parse(input)?;
    let store = store().await?;
    let available = store
        .release_line(&args.sku, args.quantity)
        .await
        .map_err(|error| store_failure("release_line", error))?
        .ok_or_else(|| {
            TaskError::new(UNKNOWN_SKU, format!("{} is not in the catalog", args.sku))
        })?;
    Ok(json!({
        "sku": args.sku,
        "quantity": args.quantity,
        "available_after": available,
    }))
}

pub async fn replenish_catalog(input: Value) -> Result<Value, TaskError> {
    let args: StocktakeArgs = parse(input)?;
    let store = store().await?;
    let (topped_up, reservations_cleared) = store
        .nightly_stocktake(args.target_units, tuning::STOCKTAKE_CEILING_UNITS)
        .await
        .map_err(|error| store_failure("nightly_stocktake", error))?;
    Ok(json!({
        "skus_topped_up": topped_up,
        "reservations_cleared": reservations_cleared,
        "target_units": args.target_units,
    }))
}

pub async fn authorize_payment(input: Value) -> Result<Value, TaskError> {
    let args: AuthorizeArgs = parse(input)?;
    let store = store().await?;
    let attempt = store
        .count_authorization_attempt(&args.order_id)
        .await
        .map_err(|error| store_failure("count_authorization_attempt", error))?
        .ok_or_else(|| TaskError::new(ORDER_NOT_FOUND, "order not found"))?;
    if let Some(payment) = store
        .find_payment(&args.order_id, &"authorization".to_owned())
        .await
        .map_err(|error| store_failure("find_payment", error))?
    {
        return Ok(json!({
            "order_id": args.order_id,
            "authorization_id": payment.payment_id,
            "amount_cents": payment.amount_cents,
            "psp_reference": payment.psp_reference,
            "attempt": attempt,
            "replayed": true,
        }));
    }
    if simulate::draw(tuning::CARD_DECLINE_RATE, &[&args.order_id, "card"]) {
        compensate_failed_order(&store, &args.order_id).await;
        return Err(TaskError::new(CARD_DECLINED, "issuer declined the card"));
    }
    if simulate::draw(tuning::PSP_UNAVAILABLE_RATE, &[&args.order_id, "psp"])
        && attempt <= tuning::PSP_FAILING_ATTEMPTS
    {
        return Err(TaskError::new(
            "PSP_UNAVAILABLE",
            format!("provider unavailable on attempt {attempt}"),
        ));
    }
    let psp_reference = format!("psp_{}", Uuid::new_v4().simple());
    let payment = store
        .record_payment(
            &args.order_id,
            &"authorization".to_owned(),
            args.amount_cents,
            &psp_reference,
        )
        .await
        .map_err(|error| store_failure("record_payment", error))?
        .ok_or_else(|| TaskError::new("PSP_UNAVAILABLE", "authorization insert raced"))?;
    Ok(json!({
        "order_id": args.order_id,
        "authorization_id": payment.payment_id,
        "amount_cents": payment.amount_cents,
        "psp_reference": payment.psp_reference,
        "attempt": attempt,
        "replayed": false,
    }))
}

pub async fn capture_payment(input: Value) -> Result<Value, TaskError> {
    let args: CaptureArgs = parse(input)?;
    let store = store().await?;
    let authorization = store
        .find_payment(&args.order_id, &"authorization".to_owned())
        .await
        .map_err(|error| store_failure("find_payment", error))?
        .ok_or_else(|| TaskError::new(ORDER_NOT_FOUND, "authorization is missing"))?;
    let capture = store
        .record_payment(
            &args.order_id,
            &"capture".to_owned(),
            authorization.amount_cents,
            &authorization.psp_reference,
        )
        .await
        .map_err(|error| store_failure("record_payment", error))?
        .ok_or_else(|| TaskError::new("PAYMENT_ALREADY_CAPTURED", "capture already exists"))?;
    store
        .set_order_status(&args.order_id, &"captured".to_owned())
        .await
        .map_err(|error| store_failure("set_order_status", error))?;
    Ok(json!({
        "order_id": args.order_id,
        "capture_id": capture.payment_id,
        "authorization_id": authorization.payment_id,
        "amount_cents": capture.amount_cents,
        "replayed": false,
    }))
}

pub async fn pick_pack(input: Value) -> Result<Value, TaskError> {
    let args: OrderArgs = parse(input)?;
    let store = store().await?;
    let order = store
        .get_order(&args.order_id)
        .await
        .map_err(|error| store_failure("get_order", error))?
        .ok_or_else(|| TaskError::new(ORDER_NOT_FOUND, "order not found"))?;
    for line in &order.lines {
        store
            .consume_line(&args.order_id, line.line_no, &line.sku, line.quantity)
            .await
            .map_err(|error| store_failure("consume_line", error))?;
    }
    store
        .set_order_status(&args.order_id, &"packed".to_owned())
        .await
        .map_err(|error| store_failure("set_order_status", error))?;
    Ok(
        json!({"order_id": args.order_id, "units_picked": order.lines.iter().map(|line| line.quantity).sum::<i32>()}),
    )
}

pub async fn generate_invoice(input: Value) -> Result<Value, TaskError> {
    let args: OrderArgs = parse(input)?;
    let store = store().await?;
    let order = store
        .get_order(&args.order_id)
        .await
        .map_err(|error| store_failure("get_order", error))?
        .ok_or_else(|| TaskError::new(ORDER_NOT_FOUND, "order not found"))?;
    Ok(
        json!({"order_id": args.order_id, "invoice_number": format!("INV-{}", order.order_id), "total_cents": order.total_cents, "render_ms": 0}),
    )
}

pub async fn book_courier(input: Value) -> Result<Value, TaskError> {
    let args: CourierArgs = parse(input)?;
    let store = store().await?;
    let attempt = store
        .count_courier_attempt(&args.order_id, &args.courier, args.express)
        .await
        .map_err(|error| store_failure("count_courier_attempt", error))?;
    if let Some(reference) = attempt.booking_reference {
        return Ok(
            json!({"order_id": args.order_id, "courier": args.courier, "express": args.express, "booking_reference": reference, "attempt": attempt.attempt, "replayed": true}),
        );
    }
    if simulate::draw(tuning::COURIER_FLAKE_RATE, &[&args.order_id, "courier"])
        && attempt.attempt <= tuning::COURIER_FAILING_ATTEMPTS
    {
        return Err(TaskError::new(
            COURIER_UNAVAILABLE,
            format!("{} refused attempt {}", args.courier, attempt.attempt),
        ));
    }
    let reference = format!(
        "{}-{}",
        args.courier
            .chars()
            .take(3)
            .collect::<String>()
            .to_uppercase(),
        &Uuid::new_v4().simple().to_string()[..10]
    );
    let stored = store
        .set_booking_reference(&args.order_id, &reference)
        .await
        .map_err(|error| store_failure("set_booking_reference", error))?;
    if !stored {
        return Err(TaskError::new(SHIPMENT_NOT_FOUND, "shipment row not found"));
    }
    Ok(
        json!({"order_id": args.order_id, "courier": args.courier, "express": args.express, "booking_reference": reference, "attempt": attempt.attempt, "replayed": false}),
    )
}

pub async fn print_label(input: Value) -> Result<Value, TaskError> {
    let args: LabelArgs = parse(input)?;
    let store = store().await?;
    let shipment = store
        .get_shipment(&args.order_id)
        .await
        .map_err(|error| store_failure("get_shipment", error))?
        .ok_or_else(|| TaskError::new(SHIPMENT_NOT_FOUND, "shipment row not found"))?;
    let reference = shipment
        .booking_reference
        .ok_or_else(|| TaskError::new(SHIPMENT_NOT_FOUND, "booking is missing"))?;
    let label_url = format!("https://labels.acme.invalid/{reference}.pdf");
    store
        .set_label_url(&args.order_id, &label_url)
        .await
        .map_err(|error| store_failure("set_label_url", error))?;
    Ok(
        json!({"order_id": args.order_id, "label_url": label_url, "label_format": if shipment.express {"A6"} else {"A5"}}),
    )
}

pub async fn tracking_seed(input: Value) -> Result<Value, TaskError> {
    let args: LabelArgs = parse(input)?;
    let store = store().await?;
    let shipment = store
        .get_shipment(&args.order_id)
        .await
        .map_err(|error| store_failure("get_shipment", error))?
        .ok_or_else(|| TaskError::new(SHIPMENT_NOT_FOUND, "shipment row not found"))?;
    let tracking_code = format!(
        "ACME{}",
        simulate::integer(10_000_000, 99_999_999, &[&args.order_id, "track"])
    );
    store
        .set_tracking_code(&args.order_id, &tracking_code)
        .await
        .map_err(|error| store_failure("set_tracking_code", error))?;
    store
        .set_order_status(&args.order_id, &"shipped".to_owned())
        .await
        .map_err(|error| store_failure("set_order_status", error))?;
    Ok(
        json!({"order_id": args.order_id, "courier": shipment.courier, "tracking_code": tracking_code, "tracking_url": format!("https://track.{}.invalid/{}", shipment.courier, tracking_code)}),
    )
}

pub async fn send_order_email(input: Value) -> Result<Value, TaskError> {
    let args: EmailArgs = parse(input)?;
    Ok(
        json!({"order_id": args.order_id, "template": "order-confirmation", "recipient": "customer@example.invalid"}),
    )
}
