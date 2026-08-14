use chrono::{TimeZone, Utc};
use horsies::{HorsiesError, OnError, WorkflowSpec, WorkflowSpecBuilder};
use serde_json::json;

use crate::domain::{Order, OrderLine};
use crate::{simulate, tuning};

use super::{finish, WorkflowTasks};

pub fn check_order() -> Order {
    Order {
        order_id: "ACME-CHECK-0001".to_owned(),
        customer_id: "CUS-CHECK".to_owned(),
        status: "placed".to_owned(),
        total_cents: 9_900,
        lines: (1..=tuning::MAX_LINES_PER_ORDER)
            .map(|line_no| OrderLine {
                line_no: line_no as i32,
                sku: format!("ACME-SKU-{line_no:04}"),
                size_code: tuning::SIZE_CODES[0].to_owned(),
                quantity: 1,
                unit_price_cents: 3_300,
            })
            .collect(),
        created_at: Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("valid check date"),
    }
}

pub fn build(tasks: &WorkflowTasks, order: &Order) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("order_fulfillment");
    builder
        .definition_key("acme.order_fulfillment.v1")
        .on_error(OnError::Fail);
    let validate = builder.task(tasks.node(
        "validate_order",
        "validate_order",
        json!({"order_id": order.order_id}),
    )?);
    let mut reservations = Vec::new();
    for line in &order.lines {
        reservations.push(
            builder.task(
                tasks
                    .node(
                        "reserve_stock",
                        &format!("reserve_stock_{}", line.line_no),
                        json!({
                            "order_id": order.order_id,
                            "line_no": line.line_no,
                            "sku": line.sku,
                            "quantity": line.quantity,
                        }),
                    )?
                    .waits_for(validate),
            ),
        );
    }
    let authorize = builder.task(
        tasks
            .node(
                "authorize_payment",
                "authorize_payment",
                json!({"order_id": order.order_id, "amount_cents": order.total_cents}),
            )?
            .waits_for_all(reservations.iter().copied()),
    );
    let pick = builder.task(
        tasks
            .node(
                "pick_pack",
                "pick_pack",
                json!({"order_id": order.order_id}),
            )?
            .waits_for(authorize),
    );
    let invoice = builder.task(
        tasks
            .node(
                "generate_invoice",
                "generate_invoice",
                json!({"order_id": order.order_id}),
            )?
            .waits_for(authorize),
    );
    let courier = simulate::choice(tuning::COURIERS, &[&order.order_id, "courier"]);
    let express = simulate::draw(tuning::EXPRESS_RATE, &[&order.order_id, "express"]);
    let shipping = builder.sub_workflow(
        tasks
            .child(
                "shipping",
                "shipping",
                json!({"order_id": order.order_id, "courier": courier, "express": express}),
            )
            .waits_for(pick)
            .waits_for(invoice),
    );
    let capture = builder.task(
        tasks
            .node(
                "capture_payment",
                "capture_payment",
                json!({"order_id": order.order_id}),
            )?
            .waits_for(shipping)
            .waits_for(authorize),
    );
    builder.task(
        tasks
            .node(
                "send_order_email",
                "send_order_email",
                json!({"order_id": order.order_id}),
            )?
            .waits_for(capture)
            .allow_failed_deps(true),
    );
    finish(
        builder,
        Some("capture_payment"),
        &[
            (
                "capture_payment".into(),
                "authorization".into(),
                "authorize_payment".into(),
            ),
            (
                "send_order_email".into(),
                "capture".into(),
                "capture_payment".into(),
            ),
        ],
        &[],
        None,
    )
}
