use horsies::{HorsiesError, WorkflowSpec};
use serde_json::json;

use super::{builder, finish, WorkflowTasks};

pub fn build(
    tasks: &WorkflowTasks,
    segment: &str,
    older_than_minutes: i32,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = builder("customer_winback", "acme.customer_winback.v1");
    let sweep = builder.task(tasks.node(
        "abandoned_cart_sweep",
        "abandoned_cart_sweep",
        json!({"older_than_minutes": older_than_minutes}),
    )?);
    builder.task(
        tasks
            .node(
                "winback_blast",
                "winback_blast",
                json!({"segment": segment}),
            )?
            .waits_for(sweep),
    );
    finish(
        builder,
        Some("winback_blast"),
        &[(
            "winback_blast".into(),
            "sweep".into(),
            "abandoned_cart_sweep".into(),
        )],
        &[],
        None,
    )
}
