//! Failure and recovery scenario.

use serde_json::json;

use super::{
    bullet, heading, prepare_database, register_runtime, say, send_json_task, ScenarioResult,
};

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — chaos");
    let (settings, store) = prepare_database().await?;
    let (app, handles, _workflows) = register_runtime(settings.sqlx_url())?;
    let mut submitted = 0usize;
    for index in 0..6 {
        let id = format!("chaos-export-{index}");
        send_json_task(&handles, "flaky_export", json!({"export_id": id})).await?;
        submitted += 1;
    }
    bullet(format!("submitted {submitted} deterministic export drills"));
    say("restart the worker during this bounded run to exercise recovery");
    drop(app);
    store.close().await;
    Ok(())
}
