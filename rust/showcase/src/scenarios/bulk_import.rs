//! Catalog import workflow scenario.

use super::{bullet, heading, prepare_database, register_runtime, say, ScenarioResult};

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — bulk import");
    let (settings, store) = prepare_database().await?;
    let (app, _handles, workflows) = register_runtime(settings.sqlx_url())?;
    let import = workflows
        .static_specs
        .get("catalog_import")
        .ok_or_else(|| "catalog_import workflow is not registered".to_owned())?;
    let handle = import.start().await.map_err(|error| error.to_string())?;
    bullet(format!("catalog import workflow: {}", handle.workflow_id()));
    say(format!(
        "catalog rows available: {}",
        store
            .list_catalog()
            .await
            .map_err(|error| error.to_string())?
            .len()
    ));
    drop(app);
    store.close().await;
    Ok(())
}
