use horsies::{HorsiesError, WorkflowSpec};
use serde_json::json;

use super::{builder, finish, WorkflowTasks};

pub fn build(
    tasks: &WorkflowTasks,
    window: &str,
    older_than_minutes: i32,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = builder("daily_report", "acme.daily_report.v1");
    let rollup =
        builder.task(tasks.node("sales_rollup", "sales_rollup", json!({"window": window}))?);
    let sweep = builder.task(
        tasks
            .node(
                "abandoned_cart_sweep",
                "abandoned_cart_sweep",
                json!({"older_than_minutes": older_than_minutes}),
            )?
            .waits_for(rollup),
    );
    builder.task(
        tasks
            .node(
                "winback_blast",
                "winback_blast",
                json!({"segment": format!("winback-{window}")}),
            )?
            .waits_for(sweep),
    );
    finish(
        builder,
        Some("abandoned_cart_sweep"),
        &[(
            "winback_blast".into(),
            "sweep".into(),
            "abandoned_cart_sweep".into(),
        )],
        &[("abandoned_cart_sweep".into(), vec!["sales_rollup".into()])],
        None,
    )
}
