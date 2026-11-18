//! Ports of the Python pruning driver tests: the declared period decides the
//! drop, and a leaf inside its period stays.
//!
//! Python specification: `tests/integration/test_partition_pruning_driver.py`.

use chrono::DateTime;

use super::*;
use crate::core::history::maintenance::pruning::PrunePass;
use crate::test_support::sql::{self, TestPool};

async fn database_today(pool: &TestPool) -> DateTime<Utc> {
    let mut connection = pool.acquire().await.expect("acquire clock");
    utc_day_start(database_now(&mut connection).await)
}

async fn relation_exists(pool: &TestPool, relation: &str) -> bool {
    sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(pool)
        .await
        .expect("read relation presence")
}

async fn catalog_dropped(pool: &TestPool, leaf_name: &str) -> bool {
    sql::query_scalar(&format!(
        "SELECT dropped_at IS NOT NULL FROM {LEAF_CATALOG} WHERE leaf_name = $1"
    ))
    .bind(leaf_name)
    .fetch_one(pool)
    .await
    .expect("read catalog drop stamp")
}

/// A daily leaf of `class_key` that starts `days_ago` days before today.
async fn leaf_days_ago(pool: &TestPool, parent: &str, class_key: &str, days_ago: i64) -> LeafRef {
    let leaf = leaf_ref(
        parent,
        class_key,
        database_today(pool).await - Duration::days(days_ago),
    );
    assert!(matches!(
        create_leaf(pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    leaf
}

fn swept_names(pass: &PrunePass) -> Vec<&str> {
    pass.history_swept
        .iter()
        .map(|entry| entry.leaf_name.as_str())
        .collect()
}

/// `TestDeclaredDurationGovernsTheDrop.test_same_age_leaves_diverge_on_declared_duration`:
/// two leaves of the same age, one in a 7-day class and one in a 30-day class.
/// Only the period can explain why one drops and the other stays.
#[tokio::test]
#[serial]
async fn same_age_leaves_diverge_on_the_declared_period() {
    let database = TestDatabase::create_with_connections(3).await;
    let short_parent = register_class(&database.pool, "standard_7d", 7).await;
    let long_parent = register_class(&database.pool, "standard_30d", 30).await;
    let short_leaf = leaf_days_ago(&database.pool, &short_parent, "standard_7d", 10).await;
    let long_leaf = leaf_days_ago(&database.pool, &long_parent, "standard_30d", 10).await;

    let pass = prune_expired_partitions(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
    )
    .await
    .expect("a pruning pass");

    assert!(pass.errors.is_empty(), "{:?}", pass.errors);
    assert_eq!(swept_names(&pass), vec![short_leaf.leaf_name()]);
    assert!(!relation_exists(&database.pool, short_leaf.leaf_name()).await);
    assert!(catalog_dropped(&database.pool, short_leaf.leaf_name()).await);
    assert!(relation_exists(&database.pool, long_leaf.leaf_name()).await);
    assert!(!catalog_dropped(&database.pool, long_leaf.leaf_name()).await);
    database.drop().await;
}

/// `TestDeclaredDurationGovernsTheDrop.test_a_sub_day_duration_still_retains_its_whole_leaf`,
/// at the smallest period the baseline allows (one day): retention is a
/// minimum. Today's and yesterday's leaves stay, because a leaf leaves only
/// one period after its whole day ended; a leaf three days old drops.
#[tokio::test]
#[serial]
async fn the_smallest_period_still_retains_each_whole_day() {
    let database = TestDatabase::create_with_connections(3).await;
    let parent = register_class(&database.pool, "standard_1d", 1).await;
    let today = leaf_days_ago(&database.pool, &parent, "standard_1d", 0).await;
    let yesterday = leaf_days_ago(&database.pool, &parent, "standard_1d", 1).await;
    let old = leaf_days_ago(&database.pool, &parent, "standard_1d", 3).await;

    let pass = prune_expired_partitions(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
    )
    .await
    .expect("a pruning pass");

    assert!(pass.errors.is_empty(), "{:?}", pass.errors);
    assert_eq!(swept_names(&pass), vec![old.leaf_name()]);
    assert!(relation_exists(&database.pool, today.leaf_name()).await);
    assert!(relation_exists(&database.pool, yesterday.leaf_name()).await);
    assert!(!relation_exists(&database.pool, old.leaf_name()).await);
    database.drop().await;
}

/// `TestHistoryPruning.test_overdue_leaf_is_dropped_and_unexpired_leaf_untouched`:
/// an overdue leaf drops; a past leaf inside its period and today's leaf stay,
/// with no catalog drop stamp.
#[tokio::test]
#[serial]
async fn an_overdue_leaf_drops_and_leaves_inside_their_period_stay() {
    let database = TestDatabase::create_with_connections(3).await;
    let parent = register_class(&database.pool, "standard_30d", 30).await;
    let expired = leaf_days_ago(&database.pool, &parent, "standard_30d", 40).await;
    let inside = leaf_days_ago(&database.pool, &parent, "standard_30d", 10).await;
    let fresh = leaf_days_ago(&database.pool, &parent, "standard_30d", 0).await;

    let pass = prune_expired_partitions(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
    )
    .await
    .expect("a pruning pass");

    assert!(pass.errors.is_empty(), "{:?}", pass.errors);
    assert_eq!(pass.dropped_count(), 1);
    assert_eq!(swept_names(&pass), vec![expired.leaf_name()]);
    assert!(!relation_exists(&database.pool, expired.leaf_name()).await);
    assert!(catalog_dropped(&database.pool, expired.leaf_name()).await);
    for kept in [&inside, &fresh] {
        assert!(relation_exists(&database.pool, kept.leaf_name()).await);
        assert!(!catalog_dropped(&database.pool, kept.leaf_name()).await);
    }
    database.drop().await;
}
