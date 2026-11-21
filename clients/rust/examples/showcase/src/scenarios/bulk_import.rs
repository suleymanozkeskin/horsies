//! Catalog import workflow scenario.

use super::{bullet, heading, prepare_database, register_runtime, say, ScenarioResult};

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — bulk import");
    let (settings, store) = prepare_database().await?;
    let showcase = register_runtime(settings.sqlx_url())?;
    let handle = showcase
        .workflows
        .catalog_import
        .start()
        .await
        .map_err(|error| error.to_string())?;
    bullet(format!("catalog import workflow: {}", handle.workflow_id()));
    say(format!(
        "catalog rows available: {}",
        store
            .list_catalog()
            .await
            .map_err(|error| error.to_string())?
            .len()
    ));
    store.close().await;
    Ok(())
}
