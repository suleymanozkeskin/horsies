//! Maintenance workflow scenario.

use horsies::WorkflowFunction;
use serde::de::DeserializeOwned;

use super::{bullet, heading, prepare_database, register_runtime, say, ScenarioResult};

/// Start `workflow` and print its id.
async fn start<T>(workflow: &WorkflowFunction<T>) -> ScenarioResult<()>
where
    T: DeserializeOwned + 'static,
{
    let handle = workflow.start().await.map_err(|error| error.to_string())?;
    bullet(format!("{}: {}", workflow.name(), handle.workflow_id()));
    Ok(())
}

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — maintenance");
    let (settings, store) = prepare_database().await?;
    let showcase = register_runtime(settings.sqlx_url())?;
    let workflows = &showcase.workflows;
    start(&workflows.price_sync).await?;
    start(&workflows.customer_winback).await?;
    start(&workflows.warehouse_transfer).await?;
    start(&workflows.seasonal_markdown).await?;
    start(&workflows.fraud_review).await?;
    say("maintenance workflows submitted");
    store.close().await;
    Ok(())
}
