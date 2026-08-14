use std::env;

use acme_showcase::domain::{Order, OrderLine, Product, ReturnCase, StockLevel, ORDER_PLACED};
use acme_showcase::{ensure_database, resolve_database_settings, Store};
use chrono::Utc;

#[tokio::test]
async fn schema_and_store_round_trip() {
    if env::var("ACME_DATABASE_URL").is_err() {
        // The database gate is opt-in. CI's unit lane has no PostgreSQL
        // authority; the disposable gate supplies this variable explicitly.
        return;
    }
    let settings = resolve_database_settings().expect("settings");
    ensure_database(&settings).await.expect("database");
    let store = Store::connect(&settings).await.expect("connect");
    store.ensure_schema().await.expect("schema");

    let relations = sqlx::query(
        "SELECT relname FROM pg_class WHERE relname LIKE 'acme_%' AND relkind IN ('r','S') ORDER BY relname",
    )
    .fetch_all(store.pool())
    .await
    .expect("relations");
    assert_eq!(
        relations.len(),
        9,
        "all Acme tables and sequences must exist"
    );

    let products = vec![Product {
        sku: "sku-s1".into(),
        name: "S1 Jacket".into(),
        category: "outerwear".into(),
        price_cents: 12_500,
    }];
    let stock = vec![StockLevel {
        sku: "sku-s1".into(),
        on_hand: 4,
        reserved: 0,
    }];
    assert_eq!(
        store
            .load_catalog(&products, &stock)
            .await
            .expect("catalog"),
        1
    );
    assert_eq!(store.count_products().await.expect("count"), 1);
    assert_eq!(
        store.list_catalog().await.expect("list")[0]
            .stock
            .available(),
        4
    );

    let order = Order {
        order_id: "order-s1".into(),
        customer_id: "customer-s1".into(),
        status: ORDER_PLACED.into(),
        total_cents: 12_500,
        lines: vec![OrderLine {
            line_no: 1,
            sku: "sku-s1".into(),
            size_code: "M".into(),
            quantity: 2,
            unit_price_cents: 12_500,
        }],
        created_at: Utc::now(),
    };
    store.insert_order(&order).await.expect("order insert");
    assert_eq!(
        store.get_order("order-s1").await.expect("order get"),
        Some(order)
    );
    let first = store
        .reserve_line("order-s1", 1, "sku-s1", 2)
        .await
        .expect("reserve");
    assert_eq!(
        (first.reserved, first.replayed, first.available),
        (true, false, 2)
    );
    let replay = store
        .reserve_line("order-s1", 1, "sku-s1", 2)
        .await
        .expect("reserve replay");
    assert_eq!(
        (replay.reserved, replay.replayed, replay.available),
        (true, true, 2)
    );
    assert_eq!(
        store
            .count_authorization_attempt("order-s1")
            .await
            .expect("attempt"),
        Some(1)
    );
    assert!(store
        .record_payment("order-s1", &"authorization".into(), 12_500, "psp-s1")
        .await
        .expect("payment")
        .is_some());
    assert_eq!(
        store
            .find_payment("order-s1", &"authorization".into())
            .await
            .expect("payment read")
            .unwrap()
            .psp_reference,
        "psp-s1"
    );
    let shipment = store
        .count_courier_attempt("order-s1", "acme-courier", false)
        .await
        .expect("shipment");
    assert_eq!(shipment.attempt, 1);
    assert!(store
        .set_booking_reference("order-s1", "book-s1")
        .await
        .expect("booking"));
    assert_eq!(
        store
            .get_shipment("order-s1")
            .await
            .expect("shipment read")
            .unwrap()
            .booking_reference
            .as_deref(),
        Some("book-s1")
    );
    let case = ReturnCase {
        return_id: "return-s1".into(),
        order_id: "order-s1".into(),
        sku: "sku-s1".into(),
        quantity: 1,
        status: "received".into(),
        condition: None,
        created_at: Utc::now(),
    };
    store.open_return(&case).await.expect("return insert");
    assert_eq!(
        store
            .get_return("return-s1")
            .await
            .expect("return read")
            .unwrap(),
        case
    );
    assert!(store
        .record_inspection("return-s1", &"resellable".into())
        .await
        .expect("inspect"));
    assert!(store
        .close_return("return-s1", &"restocked".into())
        .await
        .expect("close"));
    assert!(
        store
            .consume_line("order-s1", 1, "sku-s1", 2)
            .await
            .expect("consume")
            .consumed
    );
}
