use horsies::{HorsiesError, OnError, WorkflowSpec};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{builder, finish, WorkflowTasks};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReturnsParams {
    pub return_id: String,
    pub order_id: String,
    pub sku: String,
    pub quantity: i32,
}

pub fn build(
    tasks: &WorkflowTasks,
    return_id: &str,
    order_id: &str,
    sku: &str,
    quantity: i32,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = builder("returns_review", "acme.returns_review.v1");
    builder.on_error(OnError::Pause);
    let receive = builder.task(tasks.node(
        "receive_return",
        "receive_return",
        json!({"return_id": return_id, "order_id": order_id, "sku": sku, "quantity": quantity}),
    )?);
    let inspect = builder.task(
        tasks
            .node(
                "inspect_item",
                "inspect_item",
                json!({"return_id": return_id}),
            )?
            .waits_for(receive),
    );
    builder.task(
        tasks
            .node(
                "restock_or_writeoff",
                "restock_or_writeoff",
                json!({"return_id": return_id}),
            )?
            .waits_for(inspect)
            .allow_failed_deps(true),
    );
    finish(
        builder,
        Some("restock_or_writeoff"),
        &[(
            "restock_or_writeoff".into(),
            "inspection".into(),
            "inspect_item".into(),
        )],
        &[],
        None,
    )
}
