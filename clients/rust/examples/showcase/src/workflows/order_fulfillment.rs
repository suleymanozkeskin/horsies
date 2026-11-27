//! The order workflow: validate, reserve each line, authorize, pick and
//! invoice, ship (a sub-workflow), capture, and email the customer.

use chrono::{TimeZone, Utc};
use horsies::{
    HorsiesError, OnError, SubWorkflowParams, WorkflowDefinition, WorkflowSpec, WorkflowSpecBuilder,
};

use crate::domain::{Order, OrderLine, PaymentCapture};
use crate::tasks::inventory::reserve_stock;
use crate::tasks::notify::send_order_email;
use crate::tasks::orders::{generate_invoice, pick_pack, validate_order};
use crate::tasks::payments::{authorize_payment, capture_payment};
use crate::{simulate, tuning};

use super::shipping::{Shipping, ShippingParams};

/// The order workflow. Its params are the order; its result is the capture.
pub struct OrderFulfillment;

impl WorkflowDefinition for OrderFulfillment {
    type Output = PaymentCapture;
    type Params = Order;

    fn name() -> &'static str {
        "order_fulfillment"
    }

    fn definition_key() -> &'static str {
        "acme.order_fulfillment.v1"
    }

    fn build_with(
        mut builder: WorkflowSpecBuilder,
        order: Order,
    ) -> Result<WorkflowSpec, HorsiesError> {
        builder
            .definition_key(Self::definition_key())
            .on_error(OnError::Fail);
        let order_id = order.order_id.clone();

        let validate = builder.task(
            validate_order::node()
                .node_id("validate_order")
                .set(validate_order::fields::order_id(), order_id.clone())?,
        )?;
        let mut reservations = Vec::new();
        for line in &order.lines {
            reservations.push(
                builder.task(
                    reserve_stock::node()
                        .node_id(format!("reserve_stock_{}", line.line_no))
                        .set(reserve_stock::fields::order_id(), order_id.clone())?
                        .set(reserve_stock::fields::line_no(), line.line_no)?
                        .set(reserve_stock::fields::sku(), line.sku.clone())?
                        .set(reserve_stock::fields::quantity(), line.quantity)?
                        .waits_for(validate),
                )?,
            );
        }
        let authorize = builder.task(
            authorize_payment::node()
                .node_id("authorize_payment")
                .set(authorize_payment::fields::order_id(), order_id.clone())?
                .set(authorize_payment::fields::amount_cents(), order.total_cents)?
                .waits_for_all(reservations.iter().copied()),
        )?;
        let pick = builder.task(
            pick_pack::node()
                .node_id("pick_pack")
                .set(pick_pack::fields::order_id(), order_id.clone())?
                .waits_for(authorize),
        )?;
        let invoice = builder.task(
            generate_invoice::node()
                .node_id("generate_invoice")
                .set(generate_invoice::fields::order_id(), order_id.clone())?
                .waits_for(authorize),
        )?;
        let shipment = builder.sub_workflow(
            Shipping::node(SubWorkflowParams::Given(ShippingParams {
                order_id: order_id.clone(),
                courier: simulate::choice(tuning::COURIERS, &[&order_id, "courier"]).to_owned(),
                express: simulate::draw(tuning::EXPRESS_RATE, &[&order_id, "express"]),
            }))?
            .node_id("shipping")
            .waits_for(pick)
            .waits_for(invoice),
        );
        let capture = builder.task(
            capture_payment::node()
                .node_id("capture_payment")
                .set(capture_payment::fields::order_id(), order_id.clone())?
                .arg_from(capture_payment::fields::authorization(), authorize)
                .waits_for(shipment),
        )?;
        let _email = builder.task(
            send_order_email::node()
                .node_id("send_order_email")
                .set(send_order_email::fields::order_id(), order_id)?
                .arg_from(send_order_email::fields::capture(), capture)
                .allow_failed_dependencies(true),
        )?;
        builder.output(capture);
        builder.build()
    }
}

/// The check case of the order template: an order of the most lines.
pub fn check_order() -> Order {
    Order {
        order_id: "ACME-CHECK-0001".to_owned(),
        customer_id: "CUS-CHECK".to_owned(),
        status: "placed".to_owned(),
        total_cents: 9_900,
        lines: (1..=tuning::MAX_LINES_PER_ORDER)
            .map(|line_no| OrderLine {
                line_no: i32::try_from(line_no).unwrap_or(i32::MAX),
                sku: format!("ACME-SKU-{line_no:04}"),
                size_code: tuning::SIZE_CODES[0].to_owned(),
                quantity: 1,
                unit_price_cents: 3_300,
            })
            .collect(),
        created_at: Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("2026-01-01T00:00:00Z is one instant"),
    }
}
