//! Flash-sale campaign scenario.

use chrono::{Duration, Utc};
use horsies::TaskSendOptions;

use crate::tasks::promotions::update_price;
use crate::tuning;

use super::{
    bullet, heading, load_catalog, prepare_database, register_runtime, say, ScenarioResult,
};

/// The campaign of the expiring price updates.
const PRICE_CAMPAIGN: &str = "flash-sale";

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — flash sale");
    let (settings, store) = prepare_database().await?;
    let catalog = load_catalog(&store).await?;
    let showcase = register_runtime(settings.sqlx_url())?;
    for campaign in ["flash-a", "flash-b"] {
        let handle = showcase
            .workflows
            .flash_sale
            .start()
            .await
            .map_err(|error| error.to_string())?;
        bullet(format!("{campaign}: {}", handle.workflow_id()));
    }
    let good_until = Utc::now() + Duration::seconds(tuning::PRICE_GOOD_UNTIL_SECONDS);
    let expiring = showcase
        .tasks
        .update_price
        .with_options(TaskSendOptions::new().good_until(good_until));
    for index in 0..tuning::EXPIRING_PRICE_SENDS {
        let entry = &catalog[index % catalog.len()];
        let _update = expiring
            .send(update_price::Input {
                sku: entry.product.sku.clone(),
                campaign_id: PRICE_CAMPAIGN.to_owned(),
            })
            .await
            .map_err(|error| error.to_string())?;
    }
    say(format!(
        "submitted {} expiring price updates",
        tuning::EXPIRING_PRICE_SENDS
    ));
    store.close().await;
    Ok(())
}
