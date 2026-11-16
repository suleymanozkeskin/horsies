use std::env;

use acme_showcase::scenarios::{load_seed_catalog, steady};
use acme_showcase::settings::{resolve_database_settings, DatabaseSettings};
use acme_showcase::store::{ensure_database, Store};
use acme_showcase::tuning;
use serial_test::serial;

async fn database() -> Option<(DatabaseSettings, Store)> {
    env::var("ACME_DATABASE_URL").ok()?;
    let settings = resolve_database_settings().expect("showcase settings");
    ensure_database(&settings).await.expect("showcase database");
    let store = Store::connect(&settings).await.expect("showcase store");
    store.ensure_schema().await.expect("showcase schema");
    Some((settings, store))
}

#[tokio::test]
#[serial]
async fn steady_starts_return_and_restock_on_the_pinned_ordinals() {
    let Some((settings, store)) = database().await else {
        return;
    };
    load_seed_catalog(&store).await.expect("seed catalog");
    store.close().await;

    steady::run(Some(tuning::RESTOCK_SPAWN_EVERY), false, 1.0)
        .await
        .expect("steady scenario");

    let store = Store::connect(&settings)
        .await
        .expect("reconnect showcase store");
    let returns: i64 =
        sqlx::query_scalar("SELECT count(*) FROM horsies_workflows WHERE name = 'returns_review'")
            .fetch_one(store.pool())
            .await
            .expect("return workflow count");
    let restocks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM horsies_workflows WHERE name = 'restock'")
            .fetch_one(store.pool())
            .await
            .expect("restock workflow count");
    assert_eq!(
        returns,
        (tuning::RESTOCK_SPAWN_EVERY / tuning::RETURN_SPAWN_EVERY) as i64
    );
    assert_eq!(restocks, 1);
}
