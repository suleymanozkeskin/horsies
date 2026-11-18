//! Ports of the Python coverage tests: publication per pass, containment of a
//! refusing class, horizon retune, index repair, manifest repair, coverage
//! refusals and the missing-leaf rules.
//!
//! Python specification: `tests/integration/test_coverage_publication_batching.py`,
//! `test_partition_coverage_containment.py`, `test_partition_coverage_horizon.py`,
//! `test_partition_maintenance_fast_path.py` and
//! `test_task_history_missing_leaf_resilience.py`. The core keeps the pool
//! coverage path only (history ledger row 33), so every pass here runs through
//! `ensure_partition_coverage_in_pool` or `ensure_startup_coverage_in_pool`.

use chrono::DateTime;

use super::*;
use crate::core::history::ddl::runtime_names::daily_leaf_name as history_leaf_name;
use crate::core::history::maintenance::coverage::{CoverageEnsureFailed, CoverageEnsured};
use crate::test_support::sql::{self, CallError, TestPool};

fn period(days: i64) -> RetentionDays {
    RetentionDays::parse_period(Duration::days(days)).expect("retention period")
}

async fn database_today(pool: &TestPool) -> DateTime<Utc> {
    let mut connection = pool.acquire().await.expect("acquire clock");
    utc_day_start(database_now(&mut connection).await)
}

async fn database_hour(pool: &TestPool) -> DateTime<Utc> {
    let mut connection = pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut connection).await;
    now.with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate to the hour")
}

async fn class_leaf_count(pool: &TestPool, class_key: &str) -> i64 {
    sql::query_scalar(&format!(
        "SELECT count(*) FROM {LEAF_CATALOG} WHERE class_key = $1"
    ))
    .bind(class_key)
    .fetch_one(pool)
    .await
    .expect("count class leaves")
}

async fn relation_exists(pool: &TestPool, relation: &str) -> bool {
    sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(pool)
        .await
        .expect("read relation presence")
}

async fn index_definition(pool: &TestPool, index_name: &str) -> String {
    sql::query_scalar("SELECT pg_get_indexdef(to_regclass($1))")
        .bind(index_name)
        .fetch_one(pool)
        .await
        .expect("read index definition")
}

async fn needs_republication(pool: &TestPool) -> bool {
    let mut connection = pool.acquire().await.expect("acquire publisher");
    StagedLoaderPublisher
        .needs_republication(&mut connection)
        .await
        .expect("compare manifest and readers")
}

async fn settled_pool_pass(pool: &TestPool, history_days: u32, heartbeat_hours: u32) {
    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        history_days,
        heartbeat_hours,
        &[],
    )
    .await
    .expect("settling coverage pass");
    assert!(
        matches!(outcome, CoverageOutcome::Ensured(_)),
        "{outcome:?}"
    );
}

fn failed(outcome: CoverageOutcome) -> CoverageEnsureFailed {
    match outcome {
        CoverageOutcome::Failed(failed) => failed,
        CoverageOutcome::Ensured(ensured) => panic!("expected a failed pass, got {ensured:?}"),
    }
}

fn ensured(outcome: CoverageOutcome) -> CoverageEnsured {
    match outcome {
        CoverageOutcome::Ensured(ensured) => ensured,
        CoverageOutcome::Failed(failed) => panic!("expected an ensured pass, got {failed:?}"),
    }
}

/// `test_a_created_leaf_is_published_by_its_own_creation`: a pass that
/// creates the leaves of two classes publishes each created leaf in its own
/// creation call, so the readers are current at the end and the last step
/// does not publish; the manifest rows share one timestamp and have
/// contiguous positions.
#[tokio::test]
#[serial]
async fn coverage_creating_leaves_in_two_classes_leaves_the_readers_current() {
    let database = TestDatabase::create_with_connections(4).await;
    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        3,
        3,
        &[period(7)],
    )
    .await
    .expect("coverage pass");
    let report = ensured(outcome);
    assert_eq!(report.created_history_leaves, 8);
    // The last step did not publish: the probe found the readers current.
    assert!(!report.republished);
    assert_eq!(class_leaf_count(&database.pool, "standard_30d").await, 4);
    assert_eq!(class_leaf_count(&database.pool, "standard_7d").await, 4);

    let manifest: Vec<(i32, chrono::DateTime<Utc>)> = sql::query_as(&format!(
        "SELECT probe_position, published_at FROM {TASK_LOOKUP_MANIFEST} ORDER BY probe_position"
    ))
    .fetch_all(&database.pool)
    .await
    .expect("read manifest");
    let positions: Vec<i32> = manifest.iter().map(|(position, _)| *position).collect();
    let expected: Vec<i32> = (0..i32::try_from(manifest.len()).expect("manifest size")).collect();
    assert_eq!(positions, expected);
    let mut published: Vec<chrono::DateTime<Utc>> = manifest
        .iter()
        .map(|(_, published_at)| *published_at)
        .collect();
    published.dedup();
    assert_eq!(published.len(), 1);
    assert!(!needs_republication(&database.pool).await);
    database.drop().await;
}

/// `test_a_returned_refusal_is_contained_like_a_raise`: a class whose current
/// leaf name is taken by a foreign relation is refused and named, and a class
/// that sorts after it still gets every leaf it is owed.
#[tokio::test]
#[serial]
async fn a_returned_refusal_is_contained_like_a_raise() {
    let database = TestDatabase::create_with_connections(4).await;
    // `standard_10d` sorts before `standard_7d`.
    let refusing_parent = register_class(&database.pool, "standard_10d", 10).await;
    let squatter = history_leaf_name(&refusing_parent, database_today(&database.pool).await)
        .expect("squatted leaf name");
    sql::query(&format!("CREATE TABLE {squatter} (x integer)"))
        .execute(&database.pool)
        .await
        .expect("take the leaf name");
    let horizon = 5_u32;
    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        horizon,
        3,
        &[period(10), period(7)],
    )
    .await
    .expect("contained coverage pass");
    let failure = failed(outcome);
    assert!(failure.refusal.contains("standard_10d"), "{failure:?}");
    assert_eq!(
        class_leaf_count(&database.pool, "standard_7d").await,
        i64::from(horizon) + 1,
        "a class after the refusing one gets every leaf it is owed"
    );
    database.drop().await;
}

/// `test_a_refusal_keeps_the_leaves_created_before_it`: the second pass
/// creates day+3 of the class and then refuses day+4; the day it created stays.
#[tokio::test]
#[serial]
async fn a_refusal_keeps_the_leaves_created_before_it() {
    let database = TestDatabase::create_with_connections(4).await;
    let class_key = "standard_7d";
    let first = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        3,
        &[period(7)],
    )
    .await
    .expect("first coverage pass");
    let _ = ensured(first);
    assert_eq!(class_leaf_count(&database.pool, class_key).await, 3);

    let parent = finite_class_parent_name(class_key).expect("class parent");
    let squatter = history_leaf_name(
        &parent,
        database_today(&database.pool).await + Duration::days(4),
    )
    .expect("squatted leaf name");
    sql::query(&format!("CREATE TABLE {squatter} (x integer)"))
        .execute(&database.pool)
        .await
        .expect("take the day+4 leaf name");

    let second = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        4,
        3,
        &[period(7)],
    )
    .await
    .expect("second coverage pass");
    let failure = failed(second);
    assert!(failure.refusal.contains(class_key), "{failure:?}");
    assert_eq!(
        class_leaf_count(&database.pool, class_key).await,
        4,
        "the day created before the refusal stays"
    );
    database.drop().await;
}

/// `test_horizon_retune_is_a_success_not_a_failure`: a pass with a new
/// heartbeat horizon is a coverage success and stores the new horizon.
#[tokio::test]
#[serial]
async fn a_heartbeat_horizon_retune_is_a_success_and_is_stored() {
    let database = TestDatabase::create_with_connections(4).await;
    settled_pool_pass(&database.pool, 2, 3).await;
    let retuned = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        6,
        &[],
    )
    .await
    .expect("retuned coverage pass");
    let _ = ensured(retuned);
    let stored: i16 = sql::query_scalar(&format!(
        "SELECT heartbeat_horizon_hours FROM {RETENTION_CLASSES} WHERE class_key = $1"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .fetch_one(&database.pool)
    .await
    .expect("read stored horizon");
    assert_eq!(stored, 6);
    database.drop().await;
}

/// `test_complete_probe_uses_24_hour_heartbeat_leaf_names`: a 48-hour horizon
/// spans every hour of a day, and every hourly leaf the pass creates is
/// named on the 24-hour clock, so the next pass finds them all conformant.
#[tokio::test]
#[serial]
async fn the_probe_names_heartbeat_leaves_on_the_24_hour_clock() {
    let database = TestDatabase::create_with_connections(4).await;
    let first = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        48,
        &[],
    )
    .await
    .expect("first coverage pass");
    assert_eq!(ensured(first).created_heartbeat_leaves, 49);
    let leaves: Vec<(String, DateTime<Utc>)> = sql::query_as(&format!(
        "SELECT leaf_name, lower_anchor FROM {LEAF_CATALOG} WHERE class_key = $1"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .fetch_all(&database.pool)
    .await
    .expect("read heartbeat leaves");
    assert!(leaves.iter().any(|(_, lower)| lower.hour() >= 13));
    for (leaf_name, lower) in &leaves {
        assert_eq!(
            leaf_name,
            &hourly_leaf_name(*lower).expect("hourly leaf name")
        );
    }
    let second = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        48,
        &[],
    )
    .await
    .expect("second coverage pass");
    let report = ensured(second);
    assert_eq!(report.created_heartbeat_leaves, 0);
    assert_eq!(report.created_history_leaves, 0);
    database.drop().await;
}

/// `test_coverage_repairs_noncanonical_index_metadata`, history half: a
/// `task_id DESC` identity index and an `enqueued_at DESC` ordering index are
/// replaced by the canonical ones. The heartbeat half is
/// `coverage_repairs_a_nondefault_heartbeat_operator_class`.
#[tokio::test]
#[serial]
async fn coverage_repairs_descending_history_index_shapes() {
    let database = TestDatabase::create_with_connections(4).await;
    settled_pool_pass(&database.pool, 2, 2).await;
    let parent = finite_class_parent_name("standard_30d").expect("default parent");
    let leaf = history_leaf_name(&parent, database_today(&database.pool).await).expect("leaf");
    let id_index = leaf_id_index_name(&leaf);
    let ordering_index = leaf_enqueued_index_name(&leaf);
    for statement in [
        format!("DROP INDEX {id_index}"),
        format!("DROP INDEX {ordering_index}"),
        format!("CREATE INDEX {id_index} ON {leaf} (task_id DESC)"),
        format!("CREATE INDEX {ordering_index} ON {leaf} (enqueued_at DESC)"),
    ] {
        sql::query(&statement)
            .execute(&database.pool)
            .await
            .expect("plant a descending index");
    }
    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("repairing coverage pass");
    let _ = ensured(outcome);
    assert!(index_definition(&database.pool, &id_index)
        .await
        .ends_with("USING btree (task_id)"));
    assert!(ordering_index_exists(&database.pool, &leaf).await);
    database.drop().await;
}

/// `test_stale_manifest_metadata_is_republished`: manifest rows that differ
/// from the catalog are written again by a pass that creates nothing.
#[tokio::test]
#[serial]
async fn stale_manifest_rows_are_republished() {
    let database = TestDatabase::create_with_connections(4).await;
    settled_pool_pass(&database.pool, 2, 2).await;
    sql::query(&format!(
        "UPDATE {TASK_LOOKUP_MANIFEST} SET probe_position = probe_position + 100"
    ))
    .execute(&database.pool)
    .await
    .expect("move every manifest position");
    assert!(needs_republication(&database.pool).await);
    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("republishing coverage pass");
    let report = ensured(outcome);
    assert_eq!(report.created_history_leaves, 0);
    assert!(report.republished);
    assert!(!needs_republication(&database.pool).await);
    let lowest: i32 = sql::query_scalar(&format!(
        "SELECT min(probe_position) FROM {TASK_LOOKUP_MANIFEST}"
    ))
    .fetch_one(&database.pool)
    .await
    .expect("read lowest position");
    assert_eq!(lowest, 0);
    database.drop().await;
}

/// `test_fast_path_rejects_stale_index_schema_version`: the current heartbeat
/// leaf cataloged with another index schema version fails the pass for the
/// heartbeat class.
#[tokio::test]
#[serial]
async fn a_stale_index_schema_version_fails_the_heartbeat_class() {
    let database = TestDatabase::create_with_connections(4).await;
    settled_pool_pass(&database.pool, 3, 6).await;
    let current = hourly_leaf_name(database_hour(&database.pool).await).expect("current hour");
    let updated = sql::query(&format!(
        "UPDATE {LEAF_CATALOG} SET index_schema_version = 0
         WHERE class_key = $1
           AND lower_anchor <= statement_timestamp()
           AND upper_anchor > statement_timestamp()"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .execute(&database.pool)
    .await
    .expect("stale the current heartbeat leaf");
    assert_eq!(updated.rows_affected(), 1);
    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        3,
        6,
        &[],
    )
    .await
    .expect("coverage pass");
    let failure = failed(outcome);
    assert_eq!(failure.class_key.as_deref(), Some(HEARTBEAT_CLASS_KEY));
    assert!(failure.refusal.contains(&current), "{failure:?}");
    database.drop().await;
}

/// `test_fast_path_rejects_null_heartbeat_registration`: on the baseline the
/// database refuses a heartbeat class row with no horizon, so coverage never
/// reads one.
#[tokio::test]
#[serial]
async fn a_heartbeat_class_row_without_a_horizon_is_refused() {
    let database = TestDatabase::create_with_connections(4).await;
    settled_pool_pass(&database.pool, 3, 6).await;
    let refused = sql::query(&format!(
        "UPDATE {RETENTION_CLASSES} SET heartbeat_horizon_hours = NULL WHERE class_key = $1"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .execute(&database.pool)
    .await
    .expect_err("a heartbeat class needs its horizon");
    assert_eq!(
        refused
            .as_database_error()
            .and_then(|error| error.constraint()),
        Some("horsies_retention_classes_kind_shape")
    );
    database.drop().await;
}

/// `test_history_error_does_not_stop_heartbeat_repair`: a history leaf repair
/// that fails (its leaf lock is held) does not stop the repair of the current
/// heartbeat leaf in the same pass.
#[tokio::test]
#[serial]
async fn a_history_failure_does_not_stop_heartbeat_repair() {
    let database = TestDatabase::create_with_connections(4).await;
    settled_pool_pass(&database.pool, 3, 6).await;
    let today = database_today(&database.pool).await;
    let parent = finite_class_parent_name("standard_30d").expect("default parent");
    let history_leaf = history_leaf_name(&parent, today).expect("history leaf");
    let heartbeat_leaf = hourly_leaf_name(database_hour(&database.pool).await).expect("hour");
    let heartbeat_index = probe_index_name(&heartbeat_leaf).expect("probe index");
    for statement in [
        format!("DROP INDEX {}", leaf_id_index_name(&history_leaf)),
        format!("DROP INDEX {heartbeat_index}"),
    ] {
        sql::query(&statement)
            .execute(&database.pool)
            .await
            .expect("damage a current leaf");
    }
    let mut holder = database.pool.acquire().await.expect("acquire lock holder");
    sql::query(&format!(
        "SELECT pg_advisory_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))"
    ))
    .bind("standard_30d")
    .bind(today)
    .execute(&mut *holder)
    .await
    .expect("hold the history leaf lock");

    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        3,
        6,
        &[],
    )
    .await
    .expect("coverage pass");
    let released: bool = sql::query_scalar(&format!(
        "SELECT pg_advisory_unlock({LEAF_LOCK_KEY_FUNCTION}($1, $2))"
    ))
    .bind("standard_30d")
    .bind(today)
    .fetch_one(&mut *holder)
    .await
    .expect("release the history leaf lock");
    assert!(released);
    drop(holder);

    let failure = failed(outcome);
    assert!(failure.refusal.contains("standard_30d"), "{failure:?}");
    assert!(failure.heartbeat_covered_now);
    assert!(relation_exists(&database.pool, &heartbeat_index).await);
    database.drop().await;
}

/// `test_startup_refuses_when_current_heartbeat_leaf_is_busy`: startup on a
/// fresh database refuses while another session holds the lock of the
/// current heartbeat leaf.
#[tokio::test]
#[serial]
async fn startup_refuses_while_the_current_heartbeat_leaf_is_busy() {
    let database = TestDatabase::create_with_connections(4).await;
    let hour = database_hour(&database.pool).await;
    let mut holder = database.pool.begin().await.expect("begin lock holder");
    sql::query(&format!(
        "SELECT pg_advisory_xact_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))"
    ))
    .bind(HEARTBEAT_CLASS_KEY)
    .bind(hour)
    .execute(&mut *holder)
    .await
    .expect("hold the current heartbeat leaf lock");
    let outcome = ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        3,
        6,
        &[],
    )
    .await;
    holder.rollback().await.expect("release the lock");
    match outcome.expect("startup coverage") {
        StartupCoverageOutcome::Refused(CoverageOutcome::Failed(failure)) => {
            assert!(!failure.heartbeat_covered_now);
            assert!(failure.refusal.contains("Busy"), "{failure:?}");
        }
        other => panic!("expected a refused startup, got {other:?}"),
    }
    database.drop().await;
}

/// `TestMissingLeafBreaksAndHeals.test_maintenance_pass_republishes_on_the_divergence_alone`:
/// a leaf older than the horizon removed out of band makes a pass that creates
/// nothing republish, and the pass names the leaf.
#[tokio::test]
#[serial]
async fn a_pass_republishes_on_a_vanished_leaf_alone() {
    let database = TestDatabase::create_with_connections(4).await;
    let parent = register_class(&database.pool, "standard_30d", 30).await;
    let gone = leaf_ref(
        &parent,
        "standard_30d",
        database_today(&database.pool).await - Duration::days(10),
    );
    assert!(matches!(
        create_leaf(&database.pool, &gone).await,
        LeafCreation::Created { .. }
    ));
    settled_pool_pass(&database.pool, 2, 2).await;
    sql::query(&format!("DROP TABLE {}", gone.leaf_name()))
        .execute(&database.pool)
        .await
        .expect("drop the old leaf out of band");
    assert!(needs_republication(&database.pool).await);

    let outcome = ensure_partition_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("coverage pass");
    let report = ensured(outcome);
    assert_eq!(report.created_history_leaves, 0);
    assert!(report.republished);
    assert_eq!(report.absent_leaves, vec![gone.leaf_name().to_owned()]);
    assert!(!needs_republication(&database.pool).await);
    database.drop().await;
}

/// `TestMissingLeafBreaksAndHeals.test_in_horizon_loss_does_not_veto_worker_startup`:
/// the current leaf of a class removed out of band fails that class and is
/// reported, but startup stays ready because heartbeats are covered.
#[tokio::test]
#[serial]
async fn an_in_horizon_loss_does_not_refuse_worker_startup() {
    let database = TestDatabase::create_with_connections(4).await;
    settled_pool_pass(&database.pool, 2, 2).await;
    let parent = finite_class_parent_name("standard_30d").expect("default parent");
    let gone = history_leaf_name(&parent, database_today(&database.pool).await).expect("leaf");
    sql::query(&format!("DROP TABLE {gone}"))
        .execute(&database.pool)
        .await
        .expect("drop the current leaf out of band");

    let outcome = ensure_startup_coverage_in_pool(
        &crate::broker::pools::core_pool_for_test_url(database.pool.url()),
        crate::broker::core_rows::StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("startup coverage");
    match outcome {
        StartupCoverageOutcome::Ready(CoverageOutcome::Failed(failure)) => {
            assert!(failure.heartbeat_covered_now);
            assert_eq!(failure.absent_leaves, vec![gone.clone()]);
            assert!(failure.refusal.contains(&gone), "{failure:?}");
        }
        other => panic!("expected a ready startup with a failed class, got {other:?}"),
    }
    assert!(!needs_republication(&database.pool).await);
    database.drop().await;
}

/// `TestMissingLeafBreaksAndHeals.test_no_parent_resolving_at_all_raises`: the
/// publication fails closed (`42P01`) when no history parent resolves.
#[tokio::test]
#[serial]
async fn the_manifest_read_fails_closed_when_no_history_parent_resolves() {
    let database = TestDatabase::create().await;
    let parent = register_class(&database.pool, "standard_30d", 30).await;
    let leaf = leaf_ref(
        &parent,
        "standard_30d",
        database_today(&database.pool).await,
    );
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    sql::query(&format!(
        "DROP TABLE {parent}, {TASK_HISTORY_FOREVER} CASCADE"
    ))
    .execute(&database.pool)
    .await
    .expect("drop every history parent");
    let mut transaction = database.pool.begin().await.expect("begin publication");
    let refused = StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect_err("no parent resolves");
    transaction.rollback().await.expect("rollback publication");
    assert!(
        matches!(
            refused,
            CallError::Database(ref error)
                if error.sqlstate() == Some("42P01")
                    && error
                        .as_database_error()
                        .is_some_and(|database_error| database_error.message().contains("cannot see"))
        ),
        "{refused:?}"
    );
    database.drop().await;
}

/// `TestMissingLeafBreaksAndHeals.test_one_history_parent_gone_is_reported_not_raised`.
#[tokio::test]
#[serial]
async fn one_history_parent_gone_is_reported_not_raised() {
    let database = TestDatabase::create().await;
    let today = database_today(&database.pool).await;
    let kept_parent = register_class(&database.pool, "standard_30d", 30).await;
    let lost_parent = register_class(&database.pool, "standard_7d", 7).await;
    let kept = leaf_ref(&kept_parent, "standard_30d", today);
    let lost = leaf_ref(&lost_parent, "standard_7d", today);
    for leaf in [&kept, &lost] {
        assert!(matches!(
            create_leaf(&database.pool, leaf).await,
            LeafCreation::Created { .. }
        ));
    }
    sql::query(&format!("DROP TABLE {lost_parent} CASCADE"))
        .execute(&database.pool)
        .await
        .expect("drop one class parent");

    let mut transaction = database.pool.begin().await.expect("begin publication");
    let absent: Vec<String> = sql::query_scalar("SELECT horsies_history_absent_leaves()")
        .fetch_one(&mut *transaction)
        .await
        .expect("read absent history leaves");
    assert_eq!(absent, vec![lost.leaf_name().to_owned()]);
    let attached: Vec<String> = sql::query_scalar(&format!(
        "SELECT leaf_name FROM {LEAF_CATALOG}
         WHERE detached_at IS NULL AND dropped_at IS NULL ORDER BY leaf_name"
    ))
    .fetch_all(&mut *transaction)
    .await
    .expect("read attached catalog leaves");
    assert!(attached.iter().any(|name| name == kept.leaf_name()));
    let republished = StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("republish with one class lost");
    transaction.commit().await.expect("commit publication");
    assert_eq!(republished.absent_leaves, vec![lost.leaf_name().to_owned()]);
    database.drop().await;
}
