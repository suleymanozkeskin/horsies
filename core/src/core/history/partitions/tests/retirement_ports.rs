//! Ports of the Python tests of the retention path through the 0006
//! functions: the detach, the finalize and the drop of a PERIOD leaf call
//! `horsies_begin_leaf_detach`, `horsies_record_leaf_detached` and
//! `horsies_drop_detached_leaf`.
//!
//! The two rows that replace the record with
//! `MISSING` and `STILL_ATTACHED` are the pure test
//! `retirement::tests::the_record_of_a_completed_detach_maps_to_its_state`.

use super::*;
use crate::core::history::commands::FinalizeInterruptedLeafDetach;
use crate::core::history::errors::HistoryError;
use crate::core::history::partitions::manager::{
    finalize_interrupted_detach, FinalizeInterruptedLeafOutcome,
};
use crate::test_support::sql::{self, TestPool};

/// A detachable leaf of `class_key` (1 day) that starts `days_ago` days
/// before today.
async fn expired_leaf(pool: &TestPool, parent: &str, class_key: &str, days_ago: i64) -> LeafRef {
    let mut clock = pool.acquire().await.expect("acquire clock");
    let today = utc_day_start(database_now(&mut clock).await);
    drop(clock);
    let leaf = leaf_ref(parent, class_key, today - Duration::days(days_ago));
    assert!(matches!(
        create_leaf(pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    leaf
}

async fn detached_at_is_set(pool: &TestPool, leaf: &LeafRef) -> bool {
    sql::query_scalar(&format!(
        "SELECT detached_at IS NOT NULL FROM {LEAF_CATALOG} WHERE leaf_name = $1"
    ))
    .bind(leaf.leaf_name())
    .fetch_one(pool)
    .await
    .expect("read the detach stamp")
}

async fn relation_exists(pool: &TestPool, relation: &str) -> bool {
    sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(pool)
        .await
        .expect("read relation presence")
}

/// `test_a_drop_of_a_ref_that_differs_from_the_catalog_rolls_back`: a drop
/// whose ref names another class than the catalog row is a contract error,
/// and the caller's rollback keeps the relation.
#[tokio::test]
async fn a_drop_of_a_ref_that_differs_from_the_catalog_rolls_back() {
    let database = TestDatabase::create_with_connections(2).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let leaf = expired_leaf(&database.pool, &parent, class_key, 5).await;
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), None).expect("detach command");
    assert!(matches!(
        detach_expired_leaf(
            &core_pool_for_test_url(database.pool.url()),
            StatementLog::Off,
            &command
        )
        .await
        .expect("detach the leaf"),
        DetachExpiredLeafOutcome::Detached { .. }
    ));
    let stale = LeafRef::new(leaf.leaf_name(), "standard_7d", leaf.bounds().clone())
        .expect("stale leaf ref");

    let error = drop_detached_leaf_in_pool(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        &DropDetachedHistoryLeaf::new(stale),
    )
    .await
    .expect_err("a ref that differs from the catalog is a caller defect");

    assert!(
        matches!(&error, HistoryError::Contract(detail) if detail.contains("catalog row has class")),
        "{error:?}"
    );
    assert!(relation_exists(&database.pool, leaf.leaf_name()).await);
    database.drop().await;
}

/// `test_finalize_records_a_detach_made_out_of_band`: a leaf detached
/// outside maintenance has no detach stamp; the finalize's
/// `horsies_begin_leaf_detach` records it, no detach statement runs, and the
/// leaf is reported detached.
#[tokio::test]
async fn finalize_records_a_detach_made_out_of_band() {
    let database = TestDatabase::create_with_connections(2).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let leaf = expired_leaf(&database.pool, &parent, class_key, 5).await;
    sql::query(&format!(
        "ALTER TABLE {parent} DETACH PARTITION {}",
        leaf.leaf_name()
    ))
    .execute(&database.pool)
    .await
    .expect("detach out of band");
    assert!(!detached_at_is_set(&database.pool, &leaf).await);

    let command = FinalizeInterruptedLeafDetach::new(leaf.clone(), None).expect("finalize");
    let outcome = finalize_interrupted_detach(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        &command,
    )
    .await
    .expect("finalize outcome");

    assert!(
        matches!(
            outcome,
            FinalizeInterruptedLeafOutcome::Inspection(LeafInspection::Detached { .. })
        ),
        "{outcome:?}"
    );
    assert!(detached_at_is_set(&database.pool, &leaf).await);
    database.drop().await;
}

/// `test_a_held_leaf_lock_makes_the_detach_and_the_drop_busy`: while another
/// transaction holds the leaf locks, the detach of one leaf and the drop of
/// another (already detached) are `Busy`, and neither waits.
#[tokio::test]
async fn a_held_leaf_lock_makes_the_detach_and_the_drop_busy() {
    let database = TestDatabase::create_with_connections(3).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let leaf = expired_leaf(&database.pool, &parent, class_key, 5).await;
    let other = expired_leaf(&database.pool, &parent, class_key, 6).await;
    let detach_other = DetachExpiredHistoryLeaf::new(other.clone(), None).expect("detach other");
    assert!(matches!(
        detach_expired_leaf(
            &core_pool_for_test_url(database.pool.url()),
            StatementLog::Off,
            &detach_other
        )
        .await
        .expect("detach the other leaf"),
        DetachExpiredLeafOutcome::Detached { .. }
    ));

    let mut holder = database.pool.begin().await.expect("begin lock holder");
    for held in [&leaf, &other] {
        sql::query(&format!(
            "SELECT pg_advisory_xact_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))"
        ))
        .bind(held.class_key())
        .bind(held.bounds().lower())
        .execute(&mut *holder)
        .await
        .expect("hold the leaf lock");
    }
    let detach = DetachExpiredHistoryLeaf::new(leaf.clone(), None).expect("detach command");
    let busy_detach = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        detach_expired_leaf(
            &core_pool_for_test_url(database.pool.url()),
            StatementLog::Off,
            &detach,
        ),
    )
    .await
    .expect("a busy detach does not wait")
    .expect("busy detach outcome");
    let busy_drop = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        drop_detached_leaf_in_pool(
            &core_pool_for_test_url(database.pool.url()),
            StatementLog::Off,
            &DropDetachedHistoryLeaf::new(other.clone()),
        ),
    )
    .await
    .expect("a busy drop does not wait")
    .expect("busy drop outcome");
    holder.rollback().await.expect("release the leaf locks");

    assert_eq!(
        busy_detach,
        DetachExpiredLeafOutcome::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        }
    );
    assert_eq!(
        busy_drop,
        LeafDrop::Busy {
            leaf_name: other.leaf_name().to_owned(),
        }
    );
    assert!(relation_exists(&database.pool, other.leaf_name()).await);
    database.drop().await;
}
