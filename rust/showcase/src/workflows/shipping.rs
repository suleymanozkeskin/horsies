use horsies::{HorsiesError, WorkflowSpec};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{builder, finish, WorkflowTasks};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShippingParams {
    pub order_id: String,
    pub courier: String,
    pub express: bool,
}

pub fn build(
    tasks: &WorkflowTasks,
    order_id: &str,
    courier: &str,
    express: bool,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = builder("shipping", "acme.shipping.v1");
    let book = builder.task(tasks.node(
        "book_courier",
        "book_courier",
        json!({"order_id": order_id, "courier": courier, "express": express}),
    )?);
    let label = builder.task(
        tasks
            .node("print_label", "print_label", json!({"order_id": order_id}))?
            .waits_for(book),
    );
    builder.task(
        tasks
            .node(
                "tracking_seed",
                "tracking_seed",
                json!({"order_id": order_id}),
            )?
            .waits_for(label),
    );
    finish(
        builder,
        Some("tracking_seed"),
        &[
            (
                "print_label".into(),
                "booking".into(),
                "book_courier".into(),
            ),
            ("tracking_seed".into(), "label".into(), "print_label".into()),
        ],
        &[],
        None,
    )
}
