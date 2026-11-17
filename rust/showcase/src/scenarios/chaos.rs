//! Failure and recovery scenario.

use crate::tasks::analytics::flaky_export;
use crate::tuning;

use super::{bullet, heading, prepare_database, register_runtime, say, ScenarioResult};

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — chaos");
    let (settings, store) = prepare_database().await?;
    let showcase = register_runtime(settings.sqlx_url())?;
    for index in 0..tuning::CHAOS_EXPORT_COUNT {
        let _export = showcase
            .tasks
            .flaky_export
            .send(flaky_export::Input {
                export_id: format!("chaos-export-{index}"),
            })
            .await
            .map_err(|error| error.to_string())?;
    }
    bullet(format!(
        "submitted {} deterministic export drills",
        tuning::CHAOS_EXPORT_COUNT
    ));
    say("restart the worker during this bounded run to exercise recovery");
    store.close().await;
    Ok(())
}
