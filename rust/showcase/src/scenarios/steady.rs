//! Order flow used by the demo and by the bounded end-to-end gate.

use std::time::Duration;

use serde_json::json;

use super::{
    bullet, heading, load_catalog, next_order, register_runtime, reserve_order_id, say,
    send_json_task, send_standalone, start_order, store_order, web_base_url, ScenarioResult,
};
use crate::domain::Order;
use crate::store::Store;
use crate::{simulate, tuning};

pub fn start_fulfillment_hint(order: &Order) {
    say(format!(
        "{}  {} line(s), {:.2} EUR",
        order.order_id,
        order.lines.len(),
        order.total_cents as f64 / 100.0
    ));
}

async fn place_and_start(
    store: &Store,
    catalog: &[crate::domain::CatalogEntry],
    handles: &crate::tasks::TaskHandles,
    workflows: &crate::workflows::RegisteredWorkflows,
) -> ScenarioResult<Order> {
    let order = next_order(store, catalog).await?;
    start_fulfillment_hint(&order);
    let handle = start_order(workflows, order.clone()).await?;
    say(format!(
        "  workflow: {}/workflows?run={}",
        web_base_url(),
        handle.workflow_id()
    ));
    send_standalone(handles, &order).await?;
    Ok(order)
}

fn id_for_draw(rate: f64, label: &str, predicate: impl Fn(&str) -> bool) -> Option<String> {
    (0..100_000)
        .map(|index| format!("S4-{label}-{index:05}"))
        .find(|id| simulate::draw(rate, &[id, label]) && predicate(id))
}

async fn cover_failure_table(
    store: &Store,
    catalog: &[crate::domain::CatalogEntry],
    handles: &crate::tasks::TaskHandles,
    workflows: &crate::workflows::RegisteredWorkflows,
) -> ScenarioResult<()> {
    let bundle = id_for_draw(tuning::PROMOTION_BUNDLE_BUG_RATE, "bundle-bug", |_| true)
        .ok_or_else(|| "could not find a bundle-bug identity".to_owned())?;
    send_json_task(handles, "apply_promotions", json!({"order_id": bundle})).await?;

    let size = id_for_draw(tuning::PROMOTION_SIZE_CODE_BUG_RATE, "size-code", |id| {
        !simulate::draw(tuning::PROMOTION_BUNDLE_BUG_RATE, &[id, "bundle-bug"])
    })
    .ok_or_else(|| "could not find a size-code identity".to_owned())?;
    send_json_task(handles, "apply_promotions", json!({"order_id": size})).await?;

    let loyalty = id_for_draw(tuning::LOYALTY_ENGINE_BUG_RATE, "lifetime-bug", |_| true)
        .ok_or_else(|| "could not find a loyalty identity".to_owned())?;
    send_json_task(
        handles,
        "compute_loyalty_points",
        json!({"customer_id": loyalty, "order_id": "S4-LOYALTY"}),
    )
    .await?;

    let card_id = reserve_order_id(store, |id| {
        simulate::draw(tuning::CARD_DECLINE_RATE, &[id, "card"])
            && !simulate::draw(tuning::STOCK_SHORTFALL_RATE, &[id, "shortfall"])
    })
    .await?
    .ok_or_else(|| "could not find a card-decline identity".to_owned())?;
    let card_order = store_order(store, &card_id, catalog).await?;
    start_order(workflows, card_order)
        .await
        .map(|_| ())
        .map_err(|error| format!("card-decline workflow: {error}"))?;

    let stock_id = reserve_order_id(store, |id| {
        simulate::draw(tuning::STOCK_SHORTFALL_RATE, &[id, "shortfall"])
    })
    .await?
    .ok_or_else(|| "could not find a stock-shortfall identity".to_owned())?;
    let stock_order = store_order(store, &stock_id, catalog).await?;
    start_order(workflows, stock_order).await?;

    let psp_id = reserve_order_id(store, |id| {
        simulate::draw(tuning::PSP_UNAVAILABLE_RATE, &[id, "psp"])
            && !simulate::draw(tuning::CARD_DECLINE_RATE, &[id, "card"])
            && !simulate::draw(tuning::STOCK_SHORTFALL_RATE, &[id, "shortfall"])
    })
    .await?
    .ok_or_else(|| "could not find a PSP-unavailable identity".to_owned())?;
    let psp_order = store_order(store, &psp_id, catalog).await?;
    start_order(workflows, psp_order).await?;

    let courier_id = reserve_order_id(store, |id| {
        simulate::draw(tuning::COURIER_FLAKE_RATE, &[id, "courier"])
            && !simulate::draw(tuning::CARD_DECLINE_RATE, &[id, "card"])
            && !simulate::draw(tuning::STOCK_SHORTFALL_RATE, &[id, "shortfall"])
    })
    .await?
    .ok_or_else(|| "could not find a courier-flake identity".to_owned())?;
    let courier_order = store_order(store, &courier_id, catalog).await?;
    start_order(workflows, courier_order).await?;
    Ok(())
}

pub async fn run(max_orders: Option<usize>, cover_errors: bool, pace: f64) -> ScenarioResult<()> {
    heading("Acme Clothing — steady");
    let (settings, store) = super::prepare_database().await?;
    let catalog = load_catalog(&store).await?;
    let (app, handles, workflows) = register_runtime(settings.sqlx_url())?;
    say(format!(
        "database: {} (resolved from {})",
        settings.database_name, settings.source
    ));
    bullet(format!("live dashboard: {}", web_base_url()));
    bullet(format!(
        "placing {}",
        max_orders
            .map(|count| format!("{count} bounded orders"))
            .unwrap_or_else(|| "orders until Ctrl-C".to_owned())
    ));
    if cover_errors {
        cover_failure_table(&store, &catalog, &handles, &workflows).await?;
    }

    let mut placed = 0usize;
    loop {
        if max_orders.is_some_and(|limit| placed >= limit) {
            break;
        }
        let order = place_and_start(&store, &catalog, &handles, &workflows).await?;
        placed += 1;
        if max_orders.is_none() {
            let delay = simulate::integer(
                tuning::STEADY_MIN_INTERARRIVAL_SECONDS as i64,
                tuning::STEADY_MAX_INTERARRIVAL_SECONDS as i64,
                &[&order.order_id, "interarrival"],
            ) as f64
                / simulate::demand_factor(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|error| error.to_string())?
                        .as_secs_f64(),
                );
            let pace = if pace.is_finite() && pace >= 1.0 { pace } else { 1.0 };
            tokio::time::sleep(Duration::from_secs_f64(delay / pace)).await;
        }
    }
    say(format!("placed {placed} orders"));
    drop(app);
    store.close().await;
    Ok(())
}
