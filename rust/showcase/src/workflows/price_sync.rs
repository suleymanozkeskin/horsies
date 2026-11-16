use horsies::{HorsiesError, WorkflowSpec};
use serde_json::json;

use super::{builder, finish, WorkflowTasks};

pub fn build(
    tasks: &WorkflowTasks,
    campaign_id: &str,
    sku: &str,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = builder("price_sync", "acme.price_sync.v1");
    let cdn = builder.task(tasks.node(
        "publish_cdn",
        "publish_cdn",
        json!({"campaign_id": campaign_id, "sku": sku}),
    )?);
    let origin = builder.task(tasks.node(
        "publish_origin",
        "publish_origin",
        json!({"campaign_id": campaign_id, "sku": sku}),
    )?);
    builder.task(
        tasks
            .node(
                "warm_cache_edge",
                "warm_cache_edge",
                json!({"campaign_id": campaign_id}),
            )?
            .waits_for(cdn)
            .waits_for(origin),
    );
    finish(builder, Some("warm_cache_edge"), &[], &[], None)
}
