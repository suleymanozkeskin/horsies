//! The store logic of the order, stock, payment and shipping tasks.
//!
//! The task functions call these with the store of their worker child; the
//! tests call them with a store of their own. Each function reads and writes
//! the `acme_*` tables only, and states its failures as `TaskError<AcmeCode>`.

use horsies::TaskError;
use uuid::Uuid;

use crate::domain::{
    AcmeCode, CourierBooking, Invoice, OrderValidation, PaymentAuthorization, PaymentCapture,
    PickPack, ShippingLabel, StockRelease, StockReservation, StocktakeSummary, TrackingSeed,
    ORDER_CAPTURED, ORDER_PACKED, ORDER_SHIPPED, ORDER_VALIDATED,
};
use crate::store::Store;
use crate::{simulate, tuning};

use super::{payload_count, payload_len, store_failure};

/// The payment kind of a card hold.
const AUTHORIZATION: &str = "authorization";
/// The payment kind of a settled hold.
const CAPTURE: &str = "capture";

/// Mark `order_id` validated.
///
/// Errors: `ORDER_NOT_FOUND` for an unknown order or one without lines;
/// `STORE_UNAVAILABLE`.
pub async fn validate(
    store: &Store,
    order_id: &str,
) -> Result<OrderValidation, TaskError<AcmeCode>> {
    let order = store
        .get_order(order_id)
        .await
        .map_err(|error| store_failure("get_order", error))?
        .ok_or_else(|| {
            TaskError::custom(AcmeCode::OrderNotFound, format!("no order {order_id}"))
        })?;
    if order.lines.is_empty() {
        return Err(TaskError::custom(
            AcmeCode::OrderNotFound,
            format!("order {order_id} has no lines"),
        ));
    }
    let _updated = store
        .set_order_status(order_id, &ORDER_VALIDATED.to_owned())
        .await
        .map_err(|error| store_failure("set_order_status", error))?;
    Ok(OrderValidation {
        order_id: order_id.to_owned(),
        line_count: payload_len(order.lines.len()),
        total_cents: order.total_cents,
    })
}

/// Reserve `quantity` units of `sku` for line `line_no` of `order_id`. A
/// refused reservation fails the order and releases its other reservations.
///
/// Errors: `ORDER_CLOSED`, `UNKNOWN_SKU`, `INSUFFICIENT_STOCK`,
/// `STORE_UNAVAILABLE`.
pub async fn reserve(
    store: &Store,
    order_id: &str,
    line_no: i32,
    sku: &str,
    quantity: i32,
) -> Result<StockReservation, TaskError<AcmeCode>> {
    let outcome = store
        .reserve_line(order_id, line_no, sku, quantity)
        .await
        .map_err(|error| store_failure("reserve_line", error))?;
    if !outcome.order_open {
        return Err(TaskError::custom(
            AcmeCode::OrderClosed,
            format!("order {order_id} no longer accepts reservations"),
        ));
    }
    if !outcome.known_sku {
        compensate_failed_order(store, order_id).await;
        return Err(TaskError::custom(
            AcmeCode::UnknownSku,
            format!("{sku} is not in the catalog"),
        ));
    }
    if !outcome.reserved {
        compensate_failed_order(store, order_id).await;
        return Err(TaskError::custom(
            AcmeCode::InsufficientStock,
            format!(
                "{sku} has {} available, needs {quantity}",
                outcome.available
            ),
        )
        .with_data(serde_json::json!({ "sku": sku, "available": outcome.available })));
    }
    Ok(StockReservation {
        order_id: order_id.to_owned(),
        sku: sku.to_owned(),
        quantity,
        available_after: outcome.available,
        replayed: outcome.replayed,
    })
}

/// Fail `order_id` and release its reservations. A store failure here is
/// logged: the nightly stocktake clears what stays reserved.
pub async fn compensate_failed_order(store: &Store, order_id: &str) {
    if let Err(error) = store.fail_order_and_release_reservations(order_id).await {
        eprintln!("reservation compensation failed for {order_id}: {error}");
    }
}

/// Put `quantity` units of `sku` back on the shelf.
///
/// Errors: `UNKNOWN_SKU`, `STORE_UNAVAILABLE`.
pub async fn release(
    store: &Store,
    sku: &str,
    quantity: i32,
) -> Result<StockRelease, TaskError<AcmeCode>> {
    let available_after = store
        .release_line(sku, quantity)
        .await
        .map_err(|error| store_failure("release_line", error))?
        .ok_or_else(|| {
            TaskError::custom(AcmeCode::UnknownSku, format!("{sku} is not in the catalog"))
        })?;
    Ok(StockRelease {
        sku: sku.to_owned(),
        quantity,
        available_after,
    })
}

/// Top every stocked SKU up to `target_units` and clear the reservations
/// that no live order holds.
///
/// Errors: `STORE_UNAVAILABLE`.
pub async fn replenish(
    store: &Store,
    target_units: i32,
) -> Result<StocktakeSummary, TaskError<AcmeCode>> {
    let (topped_up, reservations_cleared) = store
        .nightly_stocktake(target_units, tuning::STOCKTAKE_CEILING_UNITS)
        .await
        .map_err(|error| store_failure("nightly_stocktake", error))?;
    Ok(StocktakeSummary {
        skus_topped_up: payload_count(topped_up),
        reservations_cleared: payload_count(reservations_cleared),
        target_units,
    })
}

/// Hold `amount_cents` on the card of `order_id`. A hold that exists is
/// returned as a replay, so a second run does not charge twice.
///
/// Errors: `ORDER_NOT_FOUND`; `CARD_DECLINED` (the order fails and releases
/// its reservations); `PSP_UNAVAILABLE` on the first failing attempts of a
/// drawn outage, which the retry policy retries; `STORE_UNAVAILABLE`.
pub async fn authorize(
    store: &Store,
    order_id: &str,
    amount_cents: i32,
) -> Result<PaymentAuthorization, TaskError<AcmeCode>> {
    let attempt = store
        .count_authorization_attempt(order_id)
        .await
        .map_err(|error| store_failure("count_authorization_attempt", error))?
        .ok_or_else(|| {
            TaskError::custom(AcmeCode::OrderNotFound, format!("no order {order_id}"))
        })?;
    if let Some(payment) = store
        .find_payment(order_id, &AUTHORIZATION.to_owned())
        .await
        .map_err(|error| store_failure("find_payment", error))?
    {
        return Ok(PaymentAuthorization {
            order_id: order_id.to_owned(),
            authorization_id: payment.payment_id,
            amount_cents: payment.amount_cents,
            psp_reference: payment.psp_reference,
            attempt,
            replayed: true,
        });
    }
    if simulate::draw(tuning::CARD_DECLINE_RATE, &[order_id, "card"]) {
        compensate_failed_order(store, order_id).await;
        return Err(TaskError::custom(
            AcmeCode::CardDeclined,
            "issuer declined the card",
        ));
    }
    if simulate::draw(tuning::PSP_UNAVAILABLE_RATE, &[order_id, "psp"])
        && attempt <= tuning::PSP_FAILING_ATTEMPTS
    {
        return Err(TaskError::custom(
            AcmeCode::PspUnavailable,
            format!("provider unavailable on attempt {attempt}"),
        ));
    }
    let psp_reference = format!("psp_{}", Uuid::new_v4().simple());
    let payment = store
        .record_payment(
            order_id,
            &AUTHORIZATION.to_owned(),
            amount_cents,
            &psp_reference,
        )
        .await
        .map_err(|error| store_failure("record_payment", error))?
        .ok_or_else(|| {
            TaskError::custom(
                AcmeCode::PspUnavailable,
                "another attempt recorded the authorization first",
            )
        })?;
    Ok(PaymentAuthorization {
        order_id: order_id.to_owned(),
        authorization_id: payment.payment_id,
        amount_cents: payment.amount_cents,
        psp_reference: payment.psp_reference,
        attempt,
        replayed: false,
    })
}

/// Settle `hold`, the authorization of `order_id`, and mark the order
/// captured.
///
/// Errors: `PAYMENT_ALREADY_CAPTURED` when the order has a capture;
/// `STORE_UNAVAILABLE`.
pub async fn capture(
    store: &Store,
    order_id: &str,
    hold: &PaymentAuthorization,
) -> Result<PaymentCapture, TaskError<AcmeCode>> {
    let capture = store
        .record_payment(
            order_id,
            &CAPTURE.to_owned(),
            hold.amount_cents,
            &hold.psp_reference,
        )
        .await
        .map_err(|error| store_failure("record_payment", error))?
        .ok_or_else(|| {
            TaskError::custom(
                AcmeCode::PaymentAlreadyCaptured,
                format!("order {order_id} is already captured"),
            )
        })?;
    let _updated = store
        .set_order_status(order_id, &ORDER_CAPTURED.to_owned())
        .await
        .map_err(|error| store_failure("set_order_status", error))?;
    Ok(PaymentCapture {
        order_id: order_id.to_owned(),
        capture_id: capture.payment_id,
        authorization_id: hold.authorization_id.clone(),
        amount_cents: capture.amount_cents,
        replayed: false,
    })
}

/// Consume the reserved units of each line of `order_id` and mark it packed.
///
/// Errors: `ORDER_NOT_FOUND`, `STORE_UNAVAILABLE`.
pub async fn pick_and_pack(store: &Store, order_id: &str) -> Result<PickPack, TaskError<AcmeCode>> {
    let order = store
        .get_order(order_id)
        .await
        .map_err(|error| store_failure("get_order", error))?
        .ok_or_else(|| {
            TaskError::custom(AcmeCode::OrderNotFound, format!("no order {order_id}"))
        })?;
    for line in &order.lines {
        let _consumed = store
            .consume_line(order_id, line.line_no, &line.sku, line.quantity)
            .await
            .map_err(|error| store_failure("consume_line", error))?;
    }
    let _updated = store
        .set_order_status(order_id, &ORDER_PACKED.to_owned())
        .await
        .map_err(|error| store_failure("set_order_status", error))?;
    Ok(PickPack {
        order_id: order_id.to_owned(),
        station: simulate::choice(tuning::PICK_STATIONS, &[order_id, "station"]).to_owned(),
        units_picked: order.lines.iter().map(|line| line.quantity).sum(),
        workflow_id: None,
        task_index: None,
    })
}

/// The invoice of `order_id`.
///
/// Errors: `ORDER_NOT_FOUND`, `STORE_UNAVAILABLE`.
pub async fn invoice(store: &Store, order_id: &str) -> Result<Invoice, TaskError<AcmeCode>> {
    let order = store
        .get_order(order_id)
        .await
        .map_err(|error| store_failure("get_order", error))?
        .ok_or_else(|| {
            TaskError::custom(AcmeCode::OrderNotFound, format!("no order {order_id}"))
        })?;
    Ok(Invoice {
        order_id: order_id.to_owned(),
        invoice_number: format!("INV-{}", order.order_id),
        total_cents: order.total_cents,
        render_ms: 0,
    })
}

/// Book `courier` for `order_id`. A booking that exists is returned as a
/// replay.
///
/// Errors: `COURIER_UNAVAILABLE` on the first failing attempts of a drawn
/// flake, which the retry policy retries; `SHIPMENT_NOT_FOUND`;
/// `STORE_UNAVAILABLE`.
pub async fn book(
    store: &Store,
    order_id: &str,
    courier: &str,
    express: bool,
) -> Result<CourierBooking, TaskError<AcmeCode>> {
    let attempt = store
        .count_courier_attempt(order_id, courier, express)
        .await
        .map_err(|error| store_failure("count_courier_attempt", error))?;
    if let Some(booking_reference) = attempt.booking_reference {
        return Ok(CourierBooking {
            order_id: order_id.to_owned(),
            courier: courier.to_owned(),
            express,
            booking_reference,
            attempt: attempt.attempt,
            replayed: true,
        });
    }
    if simulate::draw(tuning::COURIER_FLAKE_RATE, &[order_id, "courier"])
        && attempt.attempt <= tuning::COURIER_FAILING_ATTEMPTS
    {
        return Err(TaskError::custom(
            AcmeCode::CourierUnavailable,
            format!("{courier} refused attempt {}", attempt.attempt),
        ));
    }
    let courier_prefix = courier.chars().take(3).collect::<String>().to_uppercase();
    let booking_token = Uuid::new_v4().simple().to_string();
    let booking_reference = format!("{courier_prefix}-{}", &booking_token[..10]);
    let stored = store
        .set_booking_reference(order_id, &booking_reference)
        .await
        .map_err(|error| store_failure("set_booking_reference", error))?;
    if !stored {
        return Err(TaskError::custom(
            AcmeCode::ShipmentNotFound,
            format!("order {order_id} has no shipment row"),
        ));
    }
    Ok(CourierBooking {
        order_id: order_id.to_owned(),
        courier: courier.to_owned(),
        express,
        booking_reference,
        attempt: attempt.attempt,
        replayed: false,
    })
}

/// Print the label of the booked shipment of `order_id`.
///
/// Errors: `SHIPMENT_NOT_FOUND` for no shipment or no booking;
/// `STORE_UNAVAILABLE`.
pub async fn label(store: &Store, order_id: &str) -> Result<ShippingLabel, TaskError<AcmeCode>> {
    let shipment = store
        .get_shipment(order_id)
        .await
        .map_err(|error| store_failure("get_shipment", error))?
        .ok_or_else(|| {
            TaskError::custom(
                AcmeCode::ShipmentNotFound,
                format!("order {order_id} has no shipment row"),
            )
        })?;
    let reference = shipment.booking_reference.ok_or_else(|| {
        TaskError::custom(
            AcmeCode::ShipmentNotFound,
            format!("the shipment of order {order_id} has no booking"),
        )
    })?;
    let label_url = format!("https://labels.acme.invalid/{reference}.pdf");
    let _updated = store
        .set_label_url(order_id, &label_url)
        .await
        .map_err(|error| store_failure("set_label_url", error))?;
    let label_format = match shipment.express {
        true => "A6",
        false => "A5",
    };
    Ok(ShippingLabel {
        order_id: order_id.to_owned(),
        label_url,
        label_format: label_format.to_owned(),
    })
}

/// Seed the tracking code of the shipment of `order_id` and mark the order
/// shipped.
///
/// Errors: `SHIPMENT_NOT_FOUND`, `STORE_UNAVAILABLE`.
pub async fn track(store: &Store, order_id: &str) -> Result<TrackingSeed, TaskError<AcmeCode>> {
    let shipment = store
        .get_shipment(order_id)
        .await
        .map_err(|error| store_failure("get_shipment", error))?
        .ok_or_else(|| {
            TaskError::custom(
                AcmeCode::ShipmentNotFound,
                format!("order {order_id} has no shipment row"),
            )
        })?;
    let tracking_code = format!(
        "ACME{}",
        simulate::integer(10_000_000, 99_999_999, &[order_id, "track"])
    );
    let _updated = store
        .set_tracking_code(order_id, &tracking_code)
        .await
        .map_err(|error| store_failure("set_tracking_code", error))?;
    let _updated = store
        .set_order_status(order_id, &ORDER_SHIPPED.to_owned())
        .await
        .map_err(|error| store_failure("set_order_status", error))?;
    Ok(TrackingSeed {
        order_id: order_id.to_owned(),
        tracking_url: format!("https://track.{}.invalid/{tracking_code}", shipment.courier),
        courier: shipment.courier,
        tracking_code,
    })
}
