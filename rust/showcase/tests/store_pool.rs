use acme_showcase::settings::resolve_database_settings;
use acme_showcase::store::{ensure_database, TaskStore};

#[tokio::test]
async fn concurrent_task_adapters_share_one_pool_and_separate_apps_stay_independent() {
    if std::env::var("ACME_DATABASE_URL").is_err() {
        return;
    }
    let settings = resolve_database_settings().expect("database settings");
    ensure_database(&settings).await.expect("database");
    let shared = TaskStore::new(settings.sqlx_url());
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let adapter = shared.clone();
        tasks.spawn(async move { adapter.get().await.expect("shared Store") });
    }
    let mut stores = Vec::new();
    while let Some(store) = tasks.join_next().await {
        stores.push(store.expect("adapter task"));
    }
    let independent = TaskStore::new(settings.sqlx_url())
        .get()
        .await
        .expect("separate app Store");
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(stores[0].pool())
        .await
        .expect("explicit app database");
    assert_eq!(database, settings.database_name);
    stores[0].close().await;
    assert!(stores.iter().all(|store| store.pool().is_closed()));
    assert!(!independent.pool().is_closed());
    independent.close().await;
}
