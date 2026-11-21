//! The steady scenario starts a return and a restock workflow on its pinned
//! order counts.
//!
//! All test binaries of the crate share the database of
//! `ACME_DATABASE_URL`, and a second `cargo test` run (the `web` feature)
//! finds the workflows of the first. The test counts the workflows that
//! its own run of the scenario added.

mod support;

use acme_showcase::scenarios::{load_seed_catalog, steady};
use acme_showcase::store::Store;
use acme_showcase::tuning;
use serial_test::serial;

/// The workflow name of the return that the scenario starts.
const RETURN_WORKFLOW: &str = "returns_review";

/// The workflow name of the restock that the scenario starts.
const RESTOCK_WORKFLOW: &str = "restock";

/// The count of the workflows named `name` in the showcase database. A
/// database where no app has started yet has no `horsies_workflows` table,
/// and so no workflow: the count is 0.
async fn workflow_count(store: &Store, name: &str) -> i64 {
    let table: Option<String> = sqlx::query_scalar("SELECT to_regclass('horsies_workflows')::text")
        .fetch_one(store.pool())
        .await
        .unwrap_or_else(|error| panic!("the lookup of horsies_workflows: {error}"));
    match table {
        None => 0,
        Some(_) => sqlx::query_scalar("SELECT count(*) FROM horsies_workflows WHERE name = $1")
            .bind(name)
            .fetch_one(store.pool())
            .await
            .unwrap_or_else(|error| panic!("the count of the {name} workflows: {error}")),
    }
}

#[tokio::test]
#[serial]
async fn steady_starts_return_and_restock_on_the_pinned_ordinals() {
    let Some((settings, store)) = support::database().await else {
        return;
    };
    load_seed_catalog(&store).await.expect("seed catalog");
    let returns_before = workflow_count(&store, RETURN_WORKFLOW).await;
    let restocks_before = workflow_count(&store, RESTOCK_WORKFLOW).await;
    store.close().await;

    steady::run(Some(tuning::RESTOCK_SPAWN_EVERY), false, 1.0)
        .await
        .expect("steady scenario");

    let store = Store::connect(&settings)
        .await
        .expect("reconnect showcase store");
    let returns = workflow_count(&store, RETURN_WORKFLOW).await - returns_before;
    let restocks = workflow_count(&store, RESTOCK_WORKFLOW).await - restocks_before;
    assert_eq!(
        returns,
        (tuning::RESTOCK_SPAWN_EVERY / tuning::RETURN_SPAWN_EVERY) as i64
    );
    assert_eq!(restocks, 1);
}
