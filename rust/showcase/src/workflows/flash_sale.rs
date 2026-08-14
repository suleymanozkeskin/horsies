use horsies::{HorsiesError, WorkflowSpec};
use serde_json::json;

use super::{builder, finish, success_policy, WorkflowTasks};

pub fn build(
    tasks: &WorkflowTasks,
    campaign_id: &str,
    sku: &str,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = builder("flash_sale", "acme.flash_sale.v1");
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
    let search = builder.task(tasks.node(
        "prewarm_search",
        "prewarm_search",
        json!({"campaign_id": campaign_id}),
    )?);
    let _warm = builder.task(
        tasks
            .node(
                "warm_cache_edge",
                "warm_cache_edge",
                json!({"campaign_id": campaign_id}),
            )?
            .waits_for(cdn)
            .waits_for(origin)
            .join_any(),
    );
    let mut spec = finish(builder, Some("warm_cache_edge"), &[], &[], None)?;
    spec.success_policy = Some(success_policy(
        &["publish_cdn"],
        &["prewarm_search"],
        &spec.tasks,
    )?);
    // The second publish target is an alternate success case.
    spec.success_policy
        .as_mut()
        .expect("policy set")
        .cases
        .push(horsies::SuccessCase {
            required_indices: vec![origin.index],
            name: None,
        });
    let _ = search;
    Ok(spec)
}
