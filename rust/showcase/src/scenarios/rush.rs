//! High-volume order scenario.

use std::time::Duration;

use super::{
    bullet, heading, load_catalog, next_order, register_runtime, say, send_standalone, start_order,
    ScenarioResult,
};
use crate::settings::resolve_database_settings;
use crate::store::Store;
use crate::tuning;

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — rush");
    let settings = resolve_database_settings().map_err(|error| error.to_string())?;
    let store = Store::connect(&settings)
        .await
        .map_err(|error| error.to_string())?;
    let catalog = load_catalog(&store).await?;
    let (app, handles, workflows) = register_runtime(settings.sqlx_url())?;
    bullet(format!(
        "placing {} orders over {} seconds",
        tuning::RUSH_ORDER_COUNT,
        tuning::RUSH_WINDOW_SECONDS
    ));
    for _ in 0..tuning::RUSH_ORDER_COUNT {
        let order = next_order(&store, &catalog).await?;
        start_order(&workflows, order.clone()).await?;
        send_standalone(&handles, &order).await?;
        tokio::time::sleep(Duration::from_secs_f64(
            tuning::RUSH_WINDOW_SECONDS as f64 / tuning::RUSH_ORDER_COUNT as f64,
        ))
        .await;
    }
    say("rush orders submitted");
    drop(app);
    store.close().await;
    Ok(())
}
