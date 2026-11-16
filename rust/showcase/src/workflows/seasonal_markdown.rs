use horsies::{HorsiesError, WorkflowSpec};
use serde_json::json;

use super::{builder, finish, WorkflowTasks};

pub fn build(
    tasks: &WorkflowTasks,
    campaign_id: &str,
    skus: Vec<String>,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = builder("seasonal_markdown", "acme.seasonal_markdown.v1");
    let mut refs = Vec::new();
    for (index, sku) in skus.iter().enumerate() {
        refs.push(builder.task(tasks.node(
            "update_price",
            &format!("update_price_{index:02}"),
            json!({"sku": sku, "campaign_id": campaign_id}),
        )?));
    }
    builder.task(
        tasks
            .node(
                "sales_rollup",
                "sales_rollup",
                json!({"window": format!("markdown-{campaign_id}")}),
            )?
            .waits_for_all(refs),
    );
    finish(builder, Some("sales_rollup"), &[], &[], None)
}
