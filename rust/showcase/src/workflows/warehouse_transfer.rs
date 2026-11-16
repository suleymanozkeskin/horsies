use horsies::{HorsiesError, WorkflowSpec};
use serde_json::json;

use super::{builder, finish, WorkflowTasks};

pub fn build(
    tasks: &WorkflowTasks,
    sku: &str,
    quantity: i32,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = builder("warehouse_transfer", "acme.warehouse_transfer.v1");
    let allocate = builder.task(tasks.node(
        "allocate_warehouse",
        "allocate_warehouse",
        json!({"sku": sku, "quantity": quantity}),
    )?);
    builder.task(
        tasks
            .node(
                "release_stock",
                "release_stock",
                json!({"sku": sku, "quantity": quantity}),
            )?
            .waits_for(allocate),
    );
    finish(builder, Some("release_stock"), &[], &[], None)
}
