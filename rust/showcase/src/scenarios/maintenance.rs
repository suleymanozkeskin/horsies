//! Maintenance workflow scenario.

use super::{bullet, heading, prepare_database, register_runtime, say, ScenarioResult};

const WORKFLOWS: &[&str] = &[
    "price_sync",
    "customer_winback",
    "warehouse_transfer",
    "seasonal_markdown",
    "fraud_review",
];

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — maintenance");
    let (settings, store) = prepare_database().await?;
    let (app, _handles, workflows) = register_runtime(settings.sqlx_url())?;
    for name in WORKFLOWS {
        let workflow = workflows
            .static_specs
            .get(*name)
            .ok_or_else(|| format!("{name} workflow is not registered"))?;
        let handle = workflow.start().await.map_err(|error| error.to_string())?;
        bullet(format!("{name}: {}", handle.workflow_id()));
    }
    say("maintenance workflows submitted");
    drop(app);
    store.close().await;
    Ok(())
}
