use horsies::{HorsiesError, OnError, WorkflowSpec};
use serde_json::json;

use super::{finish, WorkflowTasks};

pub fn build(
    tasks: &WorkflowTasks,
    order_id: &str,
    amount_cents: i32,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = horsies::WorkflowSpecBuilder::new("fraud_review");
    builder
        .definition_key("acme.fraud_review.v1")
        .on_error(OnError::Pause);
    let reconcile = builder.task(tasks.node(
        "reconcile_payments",
        "reconcile_payments",
        json!({"window": format!("dispute-{order_id}")}),
    )?);
    builder.task(
        tasks
            .node(
                "refund_payment",
                "refund_payment",
                json!({"order_id": order_id, "amount_cents": amount_cents}),
            )?
            .waits_for(reconcile),
    );
    finish(builder, Some("refund_payment"), &[], &[], None)
}
