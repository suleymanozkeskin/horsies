use horsies::{HorsiesError, WorkflowSpec};
use serde_json::json;

use super::{builder, finish, success_policy, WorkflowTasks};

pub fn build(tasks: &WorkflowTasks, suppliers: Vec<String>) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = builder("restock", "acme.restock.v1");
    let mut refs = Vec::new();
    let mut ids = Vec::new();
    for supplier in &suppliers {
        let id = format!("feed_{}", supplier.replace('-', "_"));
        ids.push(id.clone());
        refs.push(builder.task(tasks.node(
            "sync_supplier_feed",
            &id,
            json!({"supplier": supplier}),
        )?));
    }
    let aggregate = builder.task(
        tasks
            .node(
                "update_stock_levels",
                "update_stock_levels",
                json!({"feed_node_ids": ids}),
            )?
            .waits_for_all(refs.iter().copied())
            .workflow_ctx_from(refs.iter().copied())
            .join_quorum(crate::tuning::RESTOCK_MIN_SUCCESSFUL_FEEDS.min(suppliers.len()) as i32),
    );
    let mut spec = finish(builder, Some("update_stock_levels"), &[], &[], None)?;
    spec.success_policy = Some(success_policy(
        &["update_stock_levels"],
        &ids.iter().map(String::as_str).collect::<Vec<_>>(),
        &spec.tasks,
    )?);
    let _ = aggregate;
    Ok(spec)
}
