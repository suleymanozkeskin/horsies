//! Ports of Python `tests/e2e/test_heartbeat_prune_arm.py`.
//!
//! The pruning driver has tests that call it directly. These enter where a
//! worker does: a heartbeat leaf is planted overdue, the reaper loop that
//! the worker starts is spawned, and the leaf must be gone without anything
//! in the test asking for it.
//!
//! The leaf is overdue because its bounds are backdated, not because a clock
//! was moved: the sweep compares the upper bound plus the heartbeat horizon
//! with the database clock, so a leaf anchored well before the horizon is
//! expired by the same arithmetic production uses.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serial_test::serial;
use tokio_util::sync::CancellationToken;

use super::{new_reaper_health, spawn_reaper};
use crate::core::config::recovery::RecoveryConfig;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::commands::LeafRef;
use crate::core::history::heartbeats::partitioning::hourly_leaf_ref;
use crate::core::history::maintenance::coverage::ensure_startup_coverage_in_pool;
use crate::core::history::names::HEARTBEAT_CLASS_KEY;
use crate::test_support::sql::{self, TestPool};

/// Far enough back that no configured heartbeat horizon (2 to 48 hours)
/// still counts the leaf as live (Python `_MINIMUM_AGE`).
const MINIMUM_AGE_HOURS: i32 = 72;

/// The most hours the leaf search steps back to find an hour the catalog
/// has never held (Python `_unremembered_hour`).
const UNREMEMBERED_HOUR_SEARCH_LIMIT: i32 = 240;

/// How long the test waits for the reaper's first pass to drop the leaf.
const DROP_WAIT_BOUND: Duration = Duration::from_secs(90);

/// How often the test reads whether the leaf still exists.
const DROP_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// The pool with the heartbeat class registered at the default horizon, as
/// at worker start.
async fn pool_at_worker_start() -> TestPool {
    let pool = crate::test_support::migrated_pool().await;
    let retention = RetentionConfig::default();
    ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        retention.history_leaf_horizon_days,
        retention.heartbeat_leaf_horizon_hours,
        &[],
    )
    .await
    .expect("startup coverage");
    pool
}

/// An hour at least [`MINIMUM_AGE_HOURS`] old whose leaf name the catalog
/// has never held. The catalog keeps the rows of dropped leaves, so an hour
/// it remembers would collide with that row instead of testing the drop.
async fn unremembered_hour(pool: &TestPool) -> LeafRef {
    for step in 0..UNREMEMBERED_HOUR_SEARCH_LIMIT {
        let lower: DateTime<Utc> =
            sql::query_scalar("SELECT date_trunc('hour', now()) - make_interval(hours => $1)")
                .bind(MINIMUM_AGE_HOURS + step)
                .fetch_one(pool)
                .await
                .expect("database clock");
        let leaf = hourly_leaf_ref(lower).expect("hourly leaf reference");
        let remembered: bool = sql::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM horsies_task_history_leaf_catalog WHERE leaf_name = $1
             )",
        )
        .bind(leaf.leaf_name())
        .fetch_one(pool)
        .await
        .expect("read the leaf catalog");
        if !remembered {
            return leaf;
        }
    }
    panic!(
        "no unremembered hour within {UNREMEMBERED_HOUR_SEARCH_LIMIT} hours of \
         {MINIMUM_AGE_HOURS} hours ago"
    );
}

/// Create one heartbeat leaf through the shipped creation path
/// (`horsies_create_heartbeat_leaf`, as the coverage pass calls it).
async fn plant_leaf(pool: &TestPool, leaf: &LeafRef) {
    let (outcome, leaf_name): (String, Option<String>) =
        sql::query_as("SELECT outcome, leaf_name FROM horsies_create_heartbeat_leaf($1)")
            .bind(leaf.bounds().lower())
            .fetch_one(pool)
            .await
            .expect("create the leaf");
    assert!(
        matches!(outcome.as_str(), "CREATED" | "ALREADY_CONFORMANT"),
        "could not plant the leaf: {outcome}"
    );
    assert_eq!(leaf_name.as_deref(), Some(leaf.leaf_name()));
}

async fn leaf_exists(pool: &TestPool, leaf: &LeafRef) -> bool {
    sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(leaf.leaf_name())
        .fetch_one(pool)
        .await
        .expect("read the relation")
}

/// Remove the leaf and its catalog row (Python `_forget_leaf`).
async fn forget_leaf(pool: &TestPool, leaf: &LeafRef) {
    sql::query(&format!("DROP TABLE IF EXISTS {}", leaf.leaf_name()))
        .execute(pool)
        .await
        .expect("drop the leaf");
    sql::query("DELETE FROM horsies_task_history_leaf_catalog WHERE leaf_name = $1")
        .bind(leaf.leaf_name())
        .execute(pool)
        .await
        .expect("forget the catalog row");
}

/// A recovery config whose passes touch no task row: only the maintenance
/// steps of the pass act.
fn maintenance_only_recovery() -> RecoveryConfig {
    RecoveryConfig {
        auto_requeue_stale_claimed: false,
        auto_fail_stale_running: false,
        auto_terminate_orphaned_workflow_tasks: false,
        ..RecoveryConfig::default()
    }
}

/// Python `test_a_booted_worker_drops_an_overdue_leaf`: the reaper loop's
/// first pass runs at start and drops a heartbeat leaf past its horizon,
/// with no direct call of the maintenance step.
#[tokio::test]
#[serial]
async fn a_started_reaper_drops_an_overdue_heartbeat_leaf() {
    let pool = pool_at_worker_start().await;
    let leaf = unremembered_hour(&pool).await;
    plant_leaf(&pool, &leaf).await;
    assert!(
        leaf_exists(&pool, &leaf).await,
        "setup failed: the overdue leaf was not created"
    );
    let cancel = CancellationToken::new();
    // The reaper's broker has no sqlx connection: the drop runs on the core
    // pools.
    let reaper = spawn_reaper(
        Arc::new(crate::test_support::broker_with_closed_sqlx_pools_on(pool.url()).await),
        crate::test_support::child_specs::no_child_specs(),
        maintenance_only_recovery(),
        RetentionConfig::default(),
        new_reaper_health(),
        cancel.clone(),
    );

    let dropped = tokio::time::timeout(DROP_WAIT_BOUND, async {
        while leaf_exists(&pool, &leaf).await {
            tokio::time::sleep(DROP_POLL_INTERVAL).await;
        }
    })
    .await;
    cancel.cancel();
    reaper.await.expect("the reaper loop ends");
    forget_leaf(&pool, &leaf).await;

    dropped.unwrap_or_else(|_elapsed| {
        panic!(
            "{} was still present after the reaper ran its first pass; nothing \
             drops expired heartbeat leaves",
            leaf.leaf_name()
        )
    });
}

/// Python `test_the_planted_leaf_is_genuinely_expired`: guard the premise of
/// the test above. A leaf younger than the heartbeat horizon would prove
/// nothing.
#[tokio::test]
#[serial]
async fn the_planted_heartbeat_leaf_is_past_the_heartbeat_horizon() {
    let pool = pool_at_worker_start().await;
    let horizon_hours: i16 = sql::query_scalar(
        "SELECT heartbeat_horizon_hours FROM horsies_retention_classes WHERE class_key = $1",
    )
    .bind(HEARTBEAT_CLASS_KEY)
    .fetch_one(&pool)
    .await
    .expect("the heartbeat class is registered at worker start");
    let leaf = unremembered_hour(&pool).await;

    let due: bool =
        sql::query_scalar("SELECT $1 + make_interval(hours => $2) <= statement_timestamp()")
            .bind(leaf.bounds().upper())
            .bind(i32::from(horizon_hours))
            .fetch_one(&pool)
            .await
            .expect("compare with the database clock");

    assert!(
        MINIMUM_AGE_HOURS > i32::from(horizon_hours),
        "the planted leaf is {MINIMUM_AGE_HOURS} h old against a {horizon_hours} h horizon"
    );
    assert!(
        due,
        "{} ends at {} and is not past the {horizon_hours} h horizon",
        leaf.leaf_name(),
        leaf.bounds().upper()
    );
}
