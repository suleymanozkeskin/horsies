//! Flash-sale campaign scenario.

use chrono::{Duration, Utc};
use serde_json::json;

use super::{
    bullet, heading, load_catalog, prepare_database, register_runtime, say, send_json_task,
    ScenarioResult,
};

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — flash sale");
    let (settings, store) = prepare_database().await?;
    let catalog = load_catalog(&store).await?;
    let (app, handles, workflows) = register_runtime(settings.sqlx_url())?;
    let flash = workflows
        .static_specs
        .get("flash_sale")
        .ok_or_else(|| "flash_sale workflow is not registered".to_owned())?;
    for campaign in ["flash-a", "flash-b"] {
        let handle = flash.start().await.map_err(|error| error.to_string())?;
        bullet(format!("{campaign}: {}", handle.workflow_id()));
    }
    let good_until = Utc::now() + Duration::seconds(crate::tuning::PRICE_GOOD_UNTIL_SECONDS);
    for index in 0..crate::tuning::EXPIRING_PRICE_SENDS {
        let entry = &catalog[index % catalog.len()];
        send_json_task(&handles, "update_price", json!({
            "sku": entry.product.sku,
            "price_cents": entry.product.price_cents - crate::tuning::FLASH_SALE_DISCOUNT_PERCENT,
            "good_until": good_until,
        })).await?;
    }
    say(format!(
        "submitted {} expiring price updates",
        crate::tuning::EXPIRING_PRICE_SENDS
    ));
    drop(app);
    store.close().await;
    Ok(())
}
