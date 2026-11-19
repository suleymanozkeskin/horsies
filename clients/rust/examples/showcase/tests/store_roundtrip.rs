//! The store round trip on a database of its own.
//!
//! The other test binaries of the crate share the database of
//! `ACME_DATABASE_URL` and load the Acme catalog into it. This test counts
//! and reads every Acme table, so it runs on a new database on the same
//! server: the configured database name with [`OWN_DATABASE_SUFFIX`].

use std::collections::BTreeMap;
use std::env;

use acme_showcase::domain::{Order, OrderLine, Product, ReturnCase, StockLevel, ORDER_PLACED};
use acme_showcase::settings::{resolve_database_settings_from, ACME_URL_VARIABLE};
use acme_showcase::{ensure_database, resolve_database_settings, DatabaseSettings, Store};
use chrono::Utc;
use sqlx::{Connection, PgConnection};
use url::Url;

/// The suffix of the database name of this test, after the configured
/// database name.
const OWN_DATABASE_SUFFIX: &str = "_store_roundtrip";

/// The longest database name that PostgreSQL keeps: `NAMEDATALEN - 1` bytes.
/// PostgreSQL cuts a longer name without an error, so the test refuses it.
const DATABASE_NAME_BOUND_BYTES: usize = 63;

/// The settings of the database of this test: the server, the credentials
/// and the URL form of `configured`, with the database name of
/// [`OWN_DATABASE_SUFFIX`]. Panics when the name is above
/// [`DATABASE_NAME_BOUND_BYTES`] or the URL does not parse.
fn own_database_settings(configured: &DatabaseSettings) -> DatabaseSettings {
    let name = format!("{}{OWN_DATABASE_SUFFIX}", configured.database_name);
    assert!(
        name.len() <= DATABASE_NAME_BOUND_BYTES,
        "the database name {name:?} of the test has more than {DATABASE_NAME_BOUND_BYTES} bytes; \
         set a shorter database name in {ACME_URL_VARIABLE}"
    );
    let mut url = Url::parse(&configured.url).expect("the configured database URL parses");
    url.set_path(&format!("/{name}"));
    let environment = BTreeMap::from([(ACME_URL_VARIABLE.to_owned(), url.to_string())]);
    resolve_database_settings_from(&environment, &BTreeMap::new())
        .expect("the settings of the database of the test")
}

/// Drops the database of `settings` and its open sessions, when it exists.
/// A run that panics leaves its database; the next run drops it before the
/// create. Panics when the maintenance database refuses the connection or
/// the drop.
async fn drop_own_database(settings: &DatabaseSettings) {
    let mut maintenance = PgConnection::connect(&settings.maintenance_dsn)
        .await
        .expect("connect to the maintenance database");
    let statement = format!(
        "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
        settings.database_name.replace('"', "\"\"")
    );
    sqlx::query(&statement)
        .execute(&mut maintenance)
        .await
        .expect("drop the database of the test");
    maintenance
        .close()
        .await
        .expect("close the maintenance connection");
}

#[tokio::test]
async fn schema_and_store_round_trip() {
    if env::var("ACME_DATABASE_URL").is_err() {
        // The database gate is opt-in. CI's unit lane has no PostgreSQL
        // authority; the disposable gate supplies this variable explicitly.
        return;
    }
    let configured = resolve_database_settings().expect("settings");
    let settings = own_database_settings(&configured);
    drop_own_database(&settings).await;
    assert!(
        ensure_database(&settings).await.expect("database"),
        "the database of the test is new"
    );
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
            .expect("the authorization payment of order-s1 is stored")
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
            .expect("the shipment of order-s1 is stored")
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
            .expect("the return return-s1 is stored"),
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
    store.close().await;
    drop_own_database(&settings).await;
}
