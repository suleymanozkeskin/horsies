//! Operator-facing Acme scenarios.

use chrono::Utc;
use horsies::TaskHandle;
use serde_json::{json, Value};

use crate::app::build_app_with_handles_for_url;
use crate::domain::{CatalogEntry, Order, OrderLine, Product, StockLevel};
use crate::settings::{resolve_database_settings, DatabaseSettings};
use crate::store::{ensure_database, Store};
use crate::tasks::TaskHandles;
use crate::{simulate, tuning};

pub mod bulk_import;
pub mod chaos;
pub mod flash_sale;
pub mod maintenance;
pub mod problem_child;
pub mod rush;
pub mod seed;
pub mod steady;

pub const WEB_BASE_URL: &str = "http://127.0.0.1:8600";

pub type ScenarioResult<T> = Result<T, String>;

pub fn web_base_url() -> String {
    std::env::var("ACME_WEB_URL").unwrap_or_else(|_| WEB_BASE_URL.to_owned())
}

pub fn say(text: impl AsRef<str>) {
    println!("{}", text.as_ref());
}

pub fn heading(text: &str) {
    say(format!("\n{text}\n{}", "-".repeat(text.len())));
}

pub fn bullet(text: impl AsRef<str>) {
    say(format!("  {}", text.as_ref()));
}

pub fn settings() -> ScenarioResult<DatabaseSettings> {
    resolve_database_settings().map_err(|error| error.to_string())
}

pub async fn open_store(settings: &DatabaseSettings) -> ScenarioResult<Store> {
    Store::connect(settings)
        .await
        .map_err(|error| error.to_string())
}

pub async fn prepare_database() -> ScenarioResult<(DatabaseSettings, Store)> {
    let settings = settings()?;
    ensure_database(&settings)
        .await
        .map_err(|error| error.to_string())?;
    let store = open_store(&settings).await?;
    store
        .ensure_schema()
        .await
        .map_err(|error| error.to_string())?;
    Ok((settings, store))
}

pub fn product(index: usize) -> Product {
    let sku = format!("ACME-SKU-{index:04}");
    Product {
        name: format!(
            "{} {}",
            simulate::choice(tuning::PRODUCT_COLOURS, &[&sku, "colour"]),
            simulate::choice(tuning::PRODUCT_LINES, &[&sku, "line"])
        ),
        category: simulate::choice(tuning::PRODUCT_CATEGORIES, &[&sku, "category"]).to_owned(),
        price_cents: simulate::integer(
            tuning::MIN_PRICE_CENTS as i64,
            tuning::MAX_PRICE_CENTS as i64,
            &[&sku, "price"],
        ) as i32,
        sku,
    }
}

pub fn catalog_seed() -> (Vec<Product>, Vec<StockLevel>) {
    let products = (1..=tuning::CATALOG_SIZE).map(product).collect::<Vec<_>>();
    let first_discontinued = tuning::CATALOG_SIZE - tuning::DISCONTINUED_SKU_COUNT;
    let stock = products
        .iter()
        .enumerate()
        .map(|(position, product)| StockLevel {
            sku: product.sku.clone(),
            on_hand: if position >= first_discontinued {
                0
            } else {
                tuning::CATALOG_STOCK_PER_SKU
            },
            reserved: 0,
        })
        .collect();
    (products, stock)
}

pub async fn load_seed_catalog(store: &Store) -> ScenarioResult<usize> {
    let (products, stock) = catalog_seed();
    store
        .load_catalog(&products, &stock)
        .await
        .map_err(|error| error.to_string())
}

pub async fn load_catalog(store: &Store) -> ScenarioResult<Vec<CatalogEntry>> {
    let catalog = store
        .list_catalog()
        .await
        .map_err(|error| error.to_string())?;
    if catalog.is_empty() {
        return Err("the catalog is empty; run acme seed first".to_owned());
    }
    Ok(catalog)
}

pub fn build_order(order_id: &str, catalog: &[CatalogEntry]) -> ScenarioResult<Order> {
    let in_stock = catalog
        .iter()
        .filter(|entry| entry.stock.available() > 0)
        .collect::<Vec<_>>();
    let discontinued = catalog
        .iter()
        .filter(|entry| entry.stock.on_hand == 0)
        .collect::<Vec<_>>();
    let Some(_) = in_stock.first() else {
        return Err("catalog has no sellable stock".to_owned());
    };
    let line_count = (simulate::integer(
        tuning::MIN_LINES_PER_ORDER as i64,
        tuning::MAX_LINES_PER_ORDER as i64,
        &[order_id, "lines"],
    ) as usize)
        .min(in_stock.len());
    let mut picked = simulate::sample(&in_stock, line_count, &[order_id, "skus"]);
    if !discontinued.is_empty()
        && simulate::draw(tuning::STOCK_SHORTFALL_RATE, &[order_id, "shortfall"])
    {
        picked[0] = simulate::choice(&discontinued, &[order_id, "discontinued"]);
    }
    let clearance = simulate::draw(tuning::PROMOTION_BUNDLE_BUG_RATE, &[order_id, "bundle-bug"]);
    let corrupt_size = simulate::draw(
        tuning::PROMOTION_SIZE_CODE_BUG_RATE,
        &[order_id, "size-bug"],
    );
    let lines = picked
        .into_iter()
        .enumerate()
        .map(|(index, entry)| {
            let line_no = index + 1;
            let line_key = line_no.to_string();
            let quantity = if clearance {
                tuning::BUNDLE_MIN_QUANTITY
            } else {
                simulate::integer(
                    tuning::MIN_QTY_PER_LINE as i64,
                    tuning::MAX_QTY_PER_LINE as i64,
                    &[order_id, "qty", &line_key],
                ) as i32
            };
            let size_code = if corrupt_size && line_no == 1 {
                tuning::CORRUPT_SIZE_CODE.to_owned()
            } else {
                simulate::choice(tuning::SIZE_CODES, &[order_id, "size", &line_key]).to_owned()
            };
            OrderLine {
                line_no: line_no as i32,
                sku: entry.product.sku.clone(),
                size_code,
                quantity,
                unit_price_cents: if clearance {
                    tuning::CLEARANCE_PRICE_CENTS
                } else {
                    entry.product.price_cents
                },
            }
        })
        .collect::<Vec<_>>();
    let customer_id = format!(
        "CUS-{:04}",
        simulate::integer(1, 400, &[order_id, "customer"])
    );
    let total_cents = lines.iter().map(OrderLine::line_total_cents).sum();
    Ok(Order {
        order_id: order_id.to_owned(),
        customer_id,
        status: "placed".to_owned(),
        total_cents,
        lines,
        created_at: Utc::now(),
    })
}

pub async fn next_order(store: &Store, catalog: &[CatalogEntry]) -> ScenarioResult<Order> {
    let number = store
        .next_order_number()
        .await
        .map_err(|error| error.to_string())?;
    let order = build_order(&format!("ACME-{number:05}"), catalog)?;
    store
        .insert_order(&order)
        .await
        .map_err(|error| error.to_string())?;
    Ok(order)
}

pub async fn reserve_order_id<F>(store: &Store, mut wanted: F) -> ScenarioResult<Option<String>>
where
    F: FnMut(&str) -> bool,
{
    for _ in 0..500 {
        let number = store
            .next_order_number()
            .await
            .map_err(|error| error.to_string())?;
        let id = format!("ACME-{number:05}");
        if wanted(&id) {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

pub async fn store_order(
    store: &Store,
    order_id: &str,
    catalog: &[CatalogEntry],
) -> ScenarioResult<Order> {
    let order = build_order(order_id, catalog)?;
    store
        .insert_order(&order)
        .await
        .map_err(|error| error.to_string())?;
    Ok(order)
}

pub fn register_runtime(
    url: &str,
) -> ScenarioResult<(
    horsies::Horsies,
    TaskHandles,
    crate::workflows::RegisteredWorkflows,
)> {
    build_app_with_handles_for_url(url).map_err(|error| error.to_string())
}

pub async fn send_json_task(
    handles: &TaskHandles,
    task_name: &str,
    args: serde_json::Value,
) -> ScenarioResult<TaskHandle<Value>> {
    let task = handles
        .get(task_name)
        .ok_or_else(|| format!("task {task_name} is not registered"))?;
    task.send(args).await.map_err(|error| error.to_string())
}

pub async fn start_order(
    workflows: &crate::workflows::RegisteredWorkflows,
    order: Order,
) -> ScenarioResult<horsies::WorkflowHandle<Value>> {
    workflows
        .order_fulfillment
        .start(order)
        .await
        .map_err(|error| error.to_string())
}

pub async fn send_standalone(handles: &TaskHandles, order: &Order) -> ScenarioResult<()> {
    send_json_task(
        handles,
        "apply_promotions",
        json!({"order_id": order.order_id}),
    )
    .await?;
    send_json_task(
        handles,
        "compute_loyalty_points",
        json!({"customer_id": order.customer_id, "order_id": order.order_id}),
    )
    .await?;
    Ok(())
}

pub fn task_name(task: &crate::tasks::JsonTask) -> &str {
    task.task_name()
}
