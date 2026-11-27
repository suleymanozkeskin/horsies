//! Every failure path of an order releases its reservations: at once for a
//! decline or a short line, through the nightly stocktake for a panic, a
//! cancel, an abandoned order and a return.

mod support;

use acme_showcase::domain::{
    AcmeCode, Order, OrderLine, Product, ReturnCase, StockLevel, StocktakeSummary, ORDER_PLACED,
};
use acme_showcase::store::Store;
use acme_showcase::tasks::{promotions, runtime};
use acme_showcase::{simulate, tuning};
use chrono::Utc;
use horsies::TaskErrorCode;
use serial_test::serial;
use uuid::Uuid;

async fn seed_order_with_id(
    store: &Store,
    label: &str,
    order_id: String,
    status: &str,
) -> (Order, String) {
    let token = Uuid::new_v4().simple().to_string();
    let sku = format!("reservation-{label}-{}", &token[..12]);
    store
        .load_catalog(
            &[Product {
                sku: sku.clone(),
                name: "Reservation regression item".into(),
                category: "tests".into(),
                price_cents: 1_000,
            }],
            &[StockLevel {
                sku: sku.clone(),
                on_hand: 8,
                reserved: 0,
            }],
        )
        .await
        .expect("catalog");
    let order = Order {
        order_id,
        customer_id: format!("customer-{label}-{token}"),
        status: status.to_owned(),
        total_cents: 1_000,
        lines: vec![OrderLine {
            line_no: 1,
            sku: sku.clone(),
            size_code: "M".into(),
            quantity: 2,
            unit_price_cents: 1_000,
        }],
        created_at: Utc::now(),
    };
    store.insert_order(&order).await.expect("order");
    (order, sku)
}

async fn seed_order(store: &Store, label: &str, status: &str) -> (Order, String) {
    let token = Uuid::new_v4().simple().to_string();
    seed_order_with_id(store, label, format!("reservation-{label}-{token}"), status).await
}

async fn reserved(store: &Store, sku: &str) -> i32 {
    sqlx::query_scalar("SELECT reserved FROM acme_stock WHERE sku=$1")
        .bind(sku)
        .fetch_one(store.pool())
        .await
        .expect("reserved count")
}

async fn run_nightly_cleanup(store: &Store) -> StocktakeSummary {
    runtime::replenish(store, tuning::CATALOG_STOCK_PER_SKU)
        .await
        .expect("nightly reservation cleanup")
}

async fn reserve_one(store: &Store, order: &Order, sku: &str) {
    let outcome = store
        .reserve_line(&order.order_id, 1, sku, 2)
        .await
        .expect("reservation");
    assert!(outcome.reserved, "reservation must be held before failure");
    assert!(outcome.order_open);
    assert_eq!(reserved(store, sku).await, 2);
}

#[tokio::test]
#[serial]
async fn every_failure_path_releases_or_cleans_its_reservations() {
    let Some((_settings, store)) = support::database().await else {
        return;
    };

    // Declined authorization compensates immediately.
    let card_token = Uuid::new_v4().simple().to_string();
    let card_id = (0..100_000)
        .map(|index| format!("reservation-card-{card_token}-{index}"))
        .find(|id| simulate::draw(tuning::CARD_DECLINE_RATE, &[id, "card"]))
        .expect("card-decline identity");
    let (card_order, card_sku) = seed_order_with_id(&store, "card", card_id, ORDER_PLACED).await;
    reserve_one(&store, &card_order, &card_sku).await;
    let card_error = runtime::authorize(&store, &card_order.order_id, card_order.total_cents)
        .await
        .expect_err("card decline");
    assert_eq!(
        card_error.code(),
        &TaskErrorCode::Custom(AcmeCode::CardDeclined)
    );
    assert_eq!(reserved(&store, &card_sku).await, 0);

    // A later insufficient line compensates a line that reserved earlier.
    let token = Uuid::new_v4().simple().to_string();
    let partial_sku = format!("reservation-partial-{token}");
    let missing_sku = format!("reservation-missing-{token}");
    let partial_order_id = format!("reservation-partial-{token}");
    store
        .load_catalog(
            &[
                Product {
                    sku: partial_sku.clone(),
                    name: "Partial reservation".into(),
                    category: "tests".into(),
                    price_cents: 1_000,
                },
                Product {
                    sku: missing_sku.clone(),
                    name: "Empty reservation".into(),
                    category: "tests".into(),
                    price_cents: 1_000,
                },
            ],
            &[
                StockLevel {
                    sku: partial_sku.clone(),
                    on_hand: 2,
                    reserved: 0,
                },
                StockLevel {
                    sku: missing_sku.clone(),
                    on_hand: 0,
                    reserved: 0,
                },
            ],
        )
        .await
        .expect("partial catalog");
    let partial_order = Order {
        order_id: partial_order_id,
        customer_id: format!("customer-{token}"),
        status: ORDER_PLACED.into(),
        total_cents: 2_000,
        lines: vec![
            OrderLine {
                line_no: 1,
                sku: partial_sku.clone(),
                size_code: "M".into(),
                quantity: 2,
                unit_price_cents: 1_000,
            },
            OrderLine {
                line_no: 2,
                sku: missing_sku.clone(),
                size_code: "M".into(),
                quantity: 1,
                unit_price_cents: 1_000,
            },
        ],
        created_at: Utc::now(),
    };
    store
        .insert_order(&partial_order)
        .await
        .expect("partial order");
    reserve_one(&store, &partial_order, &partial_sku).await;
    let insufficient = runtime::reserve(&store, &partial_order.order_id, 2, &missing_sku, 1)
        .await
        .expect_err("insufficient stock");
    assert_eq!(
        insufficient.code(),
        &TaskErrorCode::Custom(AcmeCode::InsufficientStock)
    );
    assert_eq!(reserved(&store, &partial_sku).await, 0);

    // Promotion panic, cancellation, abandonment, and returns use the
    // pinned Python nightly cleanup path. Each case starts with a held line.
    let bundle_token = Uuid::new_v4().simple().to_string();
    let bundle_id = (0..100_000)
        .map(|index| format!("reservation-bundle-{bundle_token}-{index}"))
        .find(|id| simulate::draw(tuning::PROMOTION_BUNDLE_BUG_RATE, &[id, "bundle-bug"]))
        .expect("bundle panic identity");
    let (bundle_order, bundle_sku) =
        seed_order_with_id(&store, "bundle", bundle_id, ORDER_PLACED).await;
    reserve_one(&store, &bundle_order, &bundle_sku).await;
    let bundle_order_id = bundle_order.order_id.clone();
    let panicked =
        std::panic::catch_unwind(move || promotions::promotion_outcome(&bundle_order_id));
    assert!(panicked.is_err(), "bundle pricing must remain a real panic");
    let cleanup = run_nightly_cleanup(&store).await;
    assert!(cleanup.reservations_cleared >= 1);
    assert_eq!(reserved(&store, &bundle_sku).await, 0);

    let (cancelled_order, cancelled_sku) = seed_order(&store, "cancelled", ORDER_PLACED).await;
    reserve_one(&store, &cancelled_order, &cancelled_sku).await;
    store
        .set_order_status(&cancelled_order.order_id, &"cancelled".into())
        .await
        .expect("cancel order");
    run_nightly_cleanup(&store).await;
    assert_eq!(reserved(&store, &cancelled_sku).await, 0);

    let (abandoned_order, abandoned_sku) = seed_order(&store, "abandoned", ORDER_PLACED).await;
    reserve_one(&store, &abandoned_order, &abandoned_sku).await;
    store
        .set_order_status(&abandoned_order.order_id, &"abandoned".into())
        .await
        .expect("abandon order");
    run_nightly_cleanup(&store).await;
    assert_eq!(reserved(&store, &abandoned_sku).await, 0);

    let (return_order, return_sku) = seed_order(&store, "return", ORDER_PLACED).await;
    reserve_one(&store, &return_order, &return_sku).await;
    store
        .open_return(&ReturnCase {
            return_id: format!("return-{}", Uuid::new_v4().simple()),
            order_id: return_order.order_id,
            sku: return_sku.clone(),
            quantity: 2,
            status: "received".into(),
            condition: None,
            created_at: Utc::now(),
        })
        .await
        .expect("return");
    run_nightly_cleanup(&store).await;
    assert_eq!(reserved(&store, &return_sku).await, 0);
}
