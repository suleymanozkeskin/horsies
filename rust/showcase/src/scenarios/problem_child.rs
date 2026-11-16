//! Deliberate business failures and return flow.

use chrono::Utc;

use super::{
    bullet, heading, load_catalog, register_runtime, reserve_order_id, start_order, store_order,
    ScenarioResult,
};
use crate::settings::resolve_database_settings;
use crate::store::Store;
use crate::{simulate, tuning};

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — problem child");
    let settings = resolve_database_settings().map_err(|error| error.to_string())?;
    let store = Store::connect(&settings)
        .await
        .map_err(|error| error.to_string())?;
    let catalog = load_catalog(&store).await?;
    let (app, _handles, workflows) = register_runtime(settings.sqlx_url())?;
    for index in 0..tuning::PROBLEM_CHILD_DECLINES {
        let declined = reserve_order_id(&store, |id| {
            simulate::draw(tuning::CARD_DECLINE_RATE, &[id, "card"])
        })
        .await?
        .ok_or_else(|| "no card-decline identity found".to_owned())?;
        let order = store_order(&store, &declined, &catalog).await?;
        start_order(&workflows, order).await?;
        bullet(format!(
            "declined order {}/{}: {declined}",
            index + 1,
            tuning::PROBLEM_CHILD_DECLINES
        ));
    }
    let shortfall = reserve_order_id(&store, |id| {
        simulate::draw(tuning::STOCK_SHORTFALL_RATE, &[id, "shortfall"])
    })
    .await?
    .ok_or_else(|| "no stock-shortfall identity found".to_owned())?;
    let order = store_order(&store, &shortfall, &catalog).await?;
    start_order(&workflows, order).await?;
    bullet(format!("stock-shortfall order: {shortfall}"));
    let returns = store
        .list_returnable_orders(tuning::PROBLEM_CHILD_RETURNS as i64)
        .await
        .map_err(|error| error.to_string())?;
    if returns.is_empty() {
        bullet("no captured orders available for returns; run steady first");
    }
    for (order_id, sku, quantity) in returns {
        let order_label = order_id.clone();
        let return_id = format!(
            "RET-{}",
            store
                .next_return_number()
                .await
                .map_err(|error| error.to_string())?
        );
        store
            .open_return(&crate::domain::ReturnCase {
                return_id: return_id.clone(),
                order_id,
                sku,
                quantity,
                status: "opened".to_owned(),
                condition: None,
                created_at: Utc::now(),
            })
            .await
            .map_err(|error| error.to_string())?;
        bullet(format!("return opened: {return_id} for {order_label}"));
    }
    drop(app);
    store.close().await;
    Ok(())
}
