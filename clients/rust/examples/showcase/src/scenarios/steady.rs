//! Order flow used by the demo and by the bounded end-to-end gate.

use std::time::Duration;

use super::{
    bullet, heading, load_catalog, next_order, register_runtime, reserve_order_id, say,
    send_standalone, start_order, store_order, web_base_url, ScenarioResult,
};
use crate::domain::{CatalogEntry, Order, ReturnCase};
use crate::store::Store;
use crate::tasks::promotions::{apply_promotions, compute_loyalty_points};
use crate::tasks::AcmeTasks;
use crate::workflows::returns_review::ReturnsParams;
use crate::workflows::RegisteredWorkflows;
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
    catalog: &[CatalogEntry],
    tasks: &AcmeTasks,
    workflows: &RegisteredWorkflows,
) -> ScenarioResult<Order> {
    let order = next_order(store, catalog).await?;
    start_fulfillment_hint(&order);
    let handle = start_order(workflows, order.clone()).await?;
    say(format!(
        "  workflow: {}/workflows?run={}",
        web_base_url(),
        handle.workflow_id()
    ));
    send_standalone(tasks, &order).await?;
    Ok(order)
}

async fn spawn_return(
    store: &Store,
    workflows: &RegisteredWorkflows,
    order: &Order,
) -> ScenarioResult<()> {
    let line = order
        .lines
        .first()
        .ok_or_else(|| format!("order {} has no returnable line", order.order_id))?;
    let return_number = store
        .next_return_number()
        .await
        .map_err(|error| error.to_string())?;
    let return_id = format!("RET-{return_number:05}");
    store
        .open_return(&ReturnCase {
            return_id: return_id.clone(),
            order_id: order.order_id.clone(),
            sku: line.sku.clone(),
            quantity: line.quantity,
            status: "opened".to_owned(),
            condition: None,
            created_at: chrono::Utc::now(),
        })
        .await
        .map_err(|error| error.to_string())?;
    let handle = workflows
        .returns_review
        .start(ReturnsParams {
            return_id: return_id.clone(),
            order_id: order.order_id.clone(),
            sku: line.sku.clone(),
            quantity: line.quantity,
        })
        .await
        .map_err(|error| error.to_string())?;
    say(format!(
        "  return {return_id}  ->  {}/workflows?run={}",
        web_base_url(),
        handle.workflow_id()
    ));
    Ok(())
}

async fn spawn_restock(workflows: &RegisteredWorkflows) -> ScenarioResult<()> {
    let handle = workflows
        .restock
        .start()
        .await
        .map_err(|error| error.to_string())?;
    say(format!(
        "  restock  ->  {}/workflows?run={}",
        web_base_url(),
        handle.workflow_id()
    ));
    Ok(())
}

fn what_to_watch() {
    heading("what to watch");
    bullet(format!(
        "{}/workflows              every run, live",
        web_base_url()
    ));
    bullet(format!(
        "{}/?retried=true          authorizations that survived a PSP outage",
        web_base_url()
    ));
    bullet(format!(
        "{}/?error_code=CARD_DECLINED       declines — retry one, it declines again",
        web_base_url()
    ));
    bullet(format!(
        "{}/?error_code=INSUFFICIENT_STOCK  the skip cascade in the graph view",
        web_base_url()
    ));
    bullet(format!(
        "{}/?error_code=UNHANDLED_EXCEPTION  the bundle-pricing crash, as data",
        web_base_url()
    ));
    bullet(format!(
        "{}/?error_code=DATA_CORRUPTION      the size-code failure",
        web_base_url()
    ));
    bullet(format!(
        "{}/?error_code=LOYALTY_ENGINE_BUG   the task-local panic code",
        web_base_url()
    ));
    bullet(format!(
        "{}/?error_code=TASK_TIMEOUT          a stalled invoice render",
        web_base_url()
    ));
    bullet(format!(
        "{}/workers                CPU and memory",
        web_base_url()
    ));
    say("");
    bullet("pause a RUNNING workflow from its run page, then resume it");
    bullet("Ctrl-C stops placing orders; orders already running finish");
}

fn id_for_draw(rate: f64, label: &str, predicate: impl Fn(&str) -> bool) -> Option<String> {
    (0..100_000)
        .map(|index| format!("S4-{label}-{index:05}"))
        .find(|id| simulate::draw(rate, &[id, label]) && predicate(id))
}

async fn cover_failure_table(
    store: &Store,
    catalog: &[CatalogEntry],
    tasks: &AcmeTasks,
    workflows: &RegisteredWorkflows,
) -> ScenarioResult<()> {
    let bundle = id_for_draw(tuning::PROMOTION_BUNDLE_BUG_RATE, "bundle-bug", |_| true)
        .ok_or_else(|| "could not find a bundle-bug identity".to_owned())?;
    let _bundle = tasks
        .apply_promotions
        .send(apply_promotions::Input { order_id: bundle })
        .await
        .map_err(|error| error.to_string())?;

    let size = id_for_draw(tuning::PROMOTION_SIZE_CODE_BUG_RATE, "size-code", |id| {
        !simulate::draw(tuning::PROMOTION_BUNDLE_BUG_RATE, &[id, "bundle-bug"])
    })
    .ok_or_else(|| "could not find a size-code identity".to_owned())?;
    let _size = tasks
        .apply_promotions
        .send(apply_promotions::Input { order_id: size })
        .await
        .map_err(|error| error.to_string())?;

    let loyalty = id_for_draw(tuning::LOYALTY_ENGINE_BUG_RATE, "lifetime-bug", |_| true)
        .ok_or_else(|| "could not find a loyalty identity".to_owned())?;
    let _loyalty = tasks
        .compute_loyalty_points
        .send(compute_loyalty_points::Input {
            customer_id: loyalty,
            order_id: "S4-LOYALTY".to_owned(),
        })
        .await
        .map_err(|error| error.to_string())?;

    let card_id = reserve_order_id(store, |id| {
        simulate::draw(tuning::CARD_DECLINE_RATE, &[id, "card"])
            && !simulate::draw(tuning::STOCK_SHORTFALL_RATE, &[id, "shortfall"])
    })
    .await?
    .ok_or_else(|| "could not find a card-decline identity".to_owned())?;
    let card_order = store_order(store, &card_id, catalog).await?;
    let _card = start_order(workflows, card_order)
        .await
        .map_err(|error| format!("card-decline workflow: {error}"))?;

    let stock_id = reserve_order_id(store, |id| {
        simulate::draw(tuning::STOCK_SHORTFALL_RATE, &[id, "shortfall"])
    })
    .await?
    .ok_or_else(|| "could not find a stock-shortfall identity".to_owned())?;
    let stock_order = store_order(store, &stock_id, catalog).await?;
    let _stock = start_order(workflows, stock_order).await?;

    let psp_id = reserve_order_id(store, |id| {
        simulate::draw(tuning::PSP_UNAVAILABLE_RATE, &[id, "psp"])
            && !simulate::draw(tuning::CARD_DECLINE_RATE, &[id, "card"])
            && !simulate::draw(tuning::STOCK_SHORTFALL_RATE, &[id, "shortfall"])
    })
    .await?
    .ok_or_else(|| "could not find a PSP-unavailable identity".to_owned())?;
    let psp_order = store_order(store, &psp_id, catalog).await?;
    let _psp = start_order(workflows, psp_order).await?;

    let courier_id = reserve_order_id(store, |id| {
        simulate::draw(tuning::COURIER_FLAKE_RATE, &[id, "courier"])
            && !simulate::draw(tuning::CARD_DECLINE_RATE, &[id, "card"])
            && !simulate::draw(tuning::STOCK_SHORTFALL_RATE, &[id, "shortfall"])
    })
    .await?
    .ok_or_else(|| "could not find a courier-flake identity".to_owned())?;
    let courier_order = store_order(store, &courier_id, catalog).await?;
    let _courier = start_order(workflows, courier_order).await?;
    Ok(())
}

pub async fn run(max_orders: Option<usize>, cover_errors: bool, pace: f64) -> ScenarioResult<()> {
    heading("Acme Clothing — steady");
    let (settings, store) = super::prepare_database().await?;
    let catalog = load_catalog(&store).await?;
    let showcase = register_runtime(settings.sqlx_url())?;
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
    what_to_watch();
    if cover_errors {
        cover_failure_table(&store, &catalog, &showcase.tasks, &showcase.workflows).await?;
    }

    let mut placed = 0usize;
    let mut interrupted = false;
    loop {
        if max_orders.is_some_and(|limit| placed >= limit) {
            break;
        }
        let order = place_and_start(&store, &catalog, &showcase.tasks, &showcase.workflows).await?;
        placed += 1;
        if placed.is_multiple_of(tuning::RETURN_SPAWN_EVERY) {
            spawn_return(&store, &showcase.workflows, &order).await?;
        }
        if placed.is_multiple_of(tuning::RESTOCK_SPAWN_EVERY) {
            spawn_restock(&showcase.workflows).await?;
        }
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
            let pace = if pace.is_finite() && pace >= 1.0 {
                pace
            } else {
                1.0
            };
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs_f64(delay / pace)) => {}
                signal = tokio::signal::ctrl_c() => {
                    signal.map_err(|error| error.to_string())?;
                    interrupted = true;
                    break;
                }
            }
        }
    }
    if interrupted {
        say(format!("\nstopped after placing {placed} orders"));
    } else {
        say(format!("placed {placed} orders"));
    }
    store.close().await;
    Ok(())
}
