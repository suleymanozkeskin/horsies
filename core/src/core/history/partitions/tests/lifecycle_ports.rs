//! Ports of the Python partition lifecycle tests: class registration, leaf
//! inspection, lock contention, session time zones, DST folds and the
//! ordering index.

use std::time::Instant;

use chrono::{DateTime, TimeZone};

use super::*;
use crate::core::history::outcomes::LeafAttachment;
use crate::core::history::partitions::catalog::AT_LOWER_BOUND_KIND;
use crate::test_support::history::try_create_history_leaf;
use crate::test_support::sql::{self, TestConnection, TestPool, TestPoolOptions};

const WEST_OF_UTC: &str = "Etc/GMT+12";
const EAST_OF_UTC: &str = "Etc/GMT-12";
const DST_ZONE: &str = "Europe/Berlin";
const LEAF_ATTACHED_SQL: &str =
    "SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass($1))";

/// The first pass of the repeated hour in Europe/Berlin in 2026: 02:00+02.
fn coming_fold() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 25, 0, 0, 0)
        .single()
        .expect("fold instant")
}

/// The same instant in 2025.
fn past_fold() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2025, 10, 26, 0, 0, 0)
        .single()
        .expect("fold instant")
}

/// A pool on the test database whose sessions run in `zone`.
async fn pool_in_zone(database: &TestDatabase, zone: &'static str, max: u32) -> TestPool {
    let pool = TestPoolOptions::new()
        .max_connections(max)
        .session_setup(format!("SELECT set_config('timezone', '{zone}', false)"))
        .connect(database.url())
        .await
        .expect("connect zoned pool");
    let session_zone: String = sql::query_scalar("SHOW TimeZone")
        .fetch_one(&pool)
        .await
        .expect("read session time zone");
    assert_eq!(session_zone, zone, "the pool's sessions run in the zone");
    pool
}

/// A one-connection core session pool whose session runs in `zone`.
async fn core_pool_in_zone(database: &TestDatabase, zone: &str) -> CorePool {
    let core = crate::broker::pools::session_core_pool_for_test_url(database.url(), 1);
    {
        let mut connection = core
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("check out the zoned core connection");
        let _rows = connection
            .query(
                &SET_SESSION_TIMEZONE.bind().value(&zone).finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await
            .expect("set the core session time zone");
    }
    assert_eq!(core_session_timezone(&core).await, zone);
    core
}

async fn set_local_zone(connection: &mut TestConnection, zone: &str) {
    sql::query("SELECT set_config('timezone', $1, true)")
        .bind(zone)
        .execute(connection)
        .await
        .expect("set the transaction time zone");
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

async fn is_attached(pool: &TestPool, leaf_name: &str) -> bool {
    sql::query_scalar(LEAF_ATTACHED_SQL)
        .bind(leaf_name)
        .fetch_one(pool)
        .await
        .expect("read leaf attachment")
}

async fn relation_exists(pool: &TestPool, relation: &str) -> bool {
    sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(pool)
        .await
        .expect("read relation presence")
}

async fn catalog_row_count(pool: &TestPool, leaf_name: &str) -> i64 {
    sql::query_scalar(&format!(
        "SELECT count(*) FROM {LEAF_CATALOG} WHERE leaf_name = $1"
    ))
    .bind(leaf_name)
    .fetch_one(pool)
    .await
    .expect("count catalog rows")
}

/// A RUNNING workflow, one node and one HISTORY pending row whose anchor is
/// inside the leaf that starts at `leaf_lower`. Every baseline column is named.
async fn seed_history_pending_row(pool: &TestPool, class_key: &str, leaf_lower: DateTime<Utc>) {
    let workflow_id = Uuid::new_v4();
    let node_row_id = Uuid::new_v4();
    let mut transaction = pool.begin().await.expect("begin pending seed");
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'port pending', NULL, 'RUNNING', 'fail', 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .execute(&mut *transaction)
    .await
    .expect("insert pending workflow");
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow,
             id, workflow_id, task_index, task_name, input, queue_name, priority,
             dependencies, allow_failed_deps, join_type, max_retries, execution,
             retention_class_key, status, is_subworkflow, created_at
         ) VALUES ('NONE', $1, $2, 0, 'port pending node', '{}', 'default', 100,
                   '{}', FALSE, 'all', 0, 'ISOLATED',
                   $3, 'RUNNING', FALSE, NOW())",
    )
    .bind(node_row_id)
    .bind(workflow_id)
    .bind(class_key)
    .execute(&mut *transaction)
    .await
    .expect("insert pending node");
    sql::query(
        "INSERT INTO horsies_workflow_phase2_pending (
             task_id, workflow_id, workflow_node_row_id, terminalization_kind,
             recovery_source, history_class, history_anchor, history_schema_version,
             result_digest, quarantine_task_id, created_at, attempt_count,
             last_attempt_at, last_failure_class
         ) VALUES ($1, $2, $3, 'COMPLETE_LOCKED', 'HISTORY',
                   $4, $5, 1, $6, NULL, statement_timestamp(), 0, NULL, NULL)",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(node_row_id)
    .bind(class_key)
    .bind(leaf_lower + Duration::hours(1))
    .bind(vec![7_u8; 32])
    .execute(&mut *transaction)
    .await
    .expect("insert pending row");
    transaction.commit().await.expect("commit pending seed");
}

/// `TestClassRegistration.test_register_verify_and_conflict`: a second
/// registration of a period (`horsies_register_retention_classes`) is
/// `VERIFIED`. The conflict half cannot be planted on the baseline: the key
/// fixes the period, and the database refuses a row whose key and days
/// disagree.
#[tokio::test]
async fn a_registered_period_registers_again_as_already_registered() {
    let database = TestDatabase::create().await;
    let period = RetentionDays::parse_period(Duration::days(30)).expect("thirty days");
    let mut transaction = database.pool.begin().await.expect("begin registration");
    let first = register_period_class(&mut transaction, period).await;
    let second = register_period_class(&mut transaction, period).await;
    transaction.commit().await.expect("commit registration");
    assert_eq!(
        first, "REGISTERED",
        "a fresh database has no standard_30d row before the first coverage pass"
    );
    assert_eq!(second, "VERIFIED");
    assert!(relation_exists(&database.pool, "horsies_task_history_standard_30d").await);
    let stored: i16 = sql::query_scalar(&format!(
        "SELECT retention_days FROM {RETENTION_CLASSES} WHERE class_key = 'standard_30d'"
    ))
    .fetch_one(&database.pool)
    .await
    .expect("read stored period");
    assert_eq!(stored, 30);

    let refused = sql::query(&format!(
        "INSERT INTO {RETENTION_CLASSES} (
             class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at
         ) VALUES ('standard_31d', 'PERIOD', 7, NULL, statement_timestamp())"
    ))
    .execute(&database.pool)
    .await
    .expect_err("a key that does not name its period is refused");
    assert_eq!(
        refused
            .as_database_error()
            .and_then(|error| error.constraint()),
        Some("horsies_retention_classes_kind_shape")
    );
    database.drop().await;
}

/// `TestCreateAndCoverage.test_mismatched_request_is_a_catalog_conflict`: a
/// request for a cataloged leaf whose catalog row holds bounds one day
/// earlier is a metadata conflict, and neither the catalog row nor the leaf
/// changes. `horsies_create_history_leaf` names the leaf from the requested
/// day, so the test shifts the catalog row, not the request.
#[tokio::test]
async fn a_request_whose_bounds_differ_from_the_catalog_is_a_catalog_conflict() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
    let lower = database_today(&database.pool).await - Duration::days(40);
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let shifted_lower = lower - Duration::days(1);
    sql::query(&format!(
        "UPDATE {LEAF_CATALOG} SET lower_anchor = $2, upper_anchor = $3 WHERE leaf_name = $1"
    ))
    .bind(leaf.leaf_name())
    .bind(shifted_lower)
    .bind(lower)
    .execute(&database.pool)
    .await
    .expect("shift the catalog row by one day");
    let outcome = create_leaf(&database.pool, &leaf).await;
    assert!(
        matches!(
            &outcome,
            LeafCreation::CatalogConflict {
                leaf_name,
                kind: CatalogConflictKind::MetadataMismatch,
                ..
            } if leaf_name == leaf.leaf_name()
        ),
        "{outcome:?}"
    );
    let row: (String, String, String, Option<DateTime<Utc>>, DateTime<Utc>) =
        sql::query_as(&format!(
            "SELECT leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor
             FROM {LEAF_CATALOG} WHERE leaf_name = $1"
        ))
        .bind(leaf.leaf_name())
        .fetch_one(&database.pool)
        .await
        .expect("read the catalog row");
    assert_eq!(
        row,
        (
            leaf.leaf_name().to_owned(),
            class_key.to_owned(),
            AT_LOWER_BOUND_KIND.to_owned(),
            Some(shifted_lower),
            lower,
        )
    );
    assert!(is_attached(&database.pool, leaf.leaf_name()).await);
    database.drop().await;
}

/// `TestRetentionFunctionsPath.test_a_ref_that_differs_from_the_catalog_raises_and_releases_the_lock`
/// and `test_catalog_rejects_caller_supplied_bounds_that_do_not_match_leaf`
/// (prototype): a detach whose bounds differ from the cataloged leaf is a
/// contract error, the leaf lock that `horsies_begin_leaf_detach` took is
/// free again, and the leaf stays attached and detachable.
#[tokio::test]
async fn a_detach_whose_bounds_differ_from_the_catalog_raises_and_frees_the_lock() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
    let lower = database_today(&database.pool).await - Duration::days(40);
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let shifted = LeafRef::new(
        leaf.leaf_name(),
        class_key,
        LeafBounds::new(lower + Duration::days(1), lower + Duration::days(2))
            .expect("shifted bounds"),
    )
    .expect("shifted leaf ref");
    let command = DetachExpiredHistoryLeaf::new(shifted, Some(5_000)).expect("detach");
    let error = detach_expired_leaf(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        &command,
    )
    .await
    .expect_err("a ref that differs from the catalog is a caller defect");
    assert!(
        matches!(
            &error,
            crate::core::history::errors::HistoryError::Contract(detail)
                if detail.contains("catalog row has class")
        ),
        "{error:?}"
    );
    let try_xact = format!("SELECT pg_try_advisory_xact_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let mut probe = database.pool.begin().await.expect("begin lock probe");
    let free: bool = sql::query_scalar(&try_xact)
        .bind(class_key)
        .bind(lower)
        .fetch_one(&mut *probe)
        .await
        .expect("probe the leaf lock");
    probe.rollback().await.expect("end lock probe");
    assert!(free, "the leaf lock is free after the refused detach");
    assert!(is_attached(&database.pool, leaf.leaf_name()).await);
    let inspector = core_pool_for_test_url(database.pool.url());
    assert!(matches!(
        inspect_leaf_in_pool(
            &inspector,
            StatementLog::Off,
            &InspectHistoryLeaf::new(leaf.clone())
        )
        .await
        .expect("inspect the leaf"),
        LeafInspection::Detachable { .. }
    ));
    inspector.close();
    database.drop().await;
}

/// `TestInspection.test_unknown_class_and_forever_class`, on
/// `horsies_inspect_history_leaf`, which inspects one leaf by its name: a
/// name that no catalog row holds (here of a class that does not exist) is
/// missing and not cataloged, and the static forever leaf is refused, since
/// retention never inspects a leaf of the forever class.
#[tokio::test]
async fn inspection_of_an_unknown_class_and_of_the_forever_class() {
    let database = TestDatabase::create().await;
    let today = database_today(&database.pool).await;
    let bounds = LeafBounds::new(today, today + Duration::days(1)).expect("one day");
    let inspector = core_pool_for_test_url(database.pool.url());
    let ghost_name = "horsies_task_history_ghost_2026_01_01";
    let ghost = LeafRef::new(ghost_name, "ghost", bounds.clone()).expect("ghost leaf ref");
    assert_eq!(
        inspect_leaf_in_pool(
            &inspector,
            StatementLog::Off,
            &InspectHistoryLeaf::new(ghost)
        )
        .await
        .expect("inspect an unknown class"),
        LeafInspection::Missing {
            leaf_name: ghost_name.to_owned(),
            cataloged: false,
            eligible_at: None,
        }
    );
    let forever_leaf: String = sql::query_scalar(&format!(
        "SELECT leaf_name FROM {LEAF_CATALOG} WHERE class_key = $1"
    ))
    .bind(FOREVER_CLASS_KEY)
    .fetch_one(&database.pool)
    .await
    .expect("read the static forever leaf");
    let forever =
        LeafRef::new(forever_leaf.as_str(), FOREVER_CLASS_KEY, bounds).expect("forever leaf ref");
    let refused = inspect_leaf_in_pool(
        &inspector,
        StatementLog::Off,
        &InspectHistoryLeaf::new(forever),
    )
    .await
    .expect_err("retention never inspects a leaf of the forever class");
    let expected_message =
        format!("leaf {forever_leaf} belongs to the forever class, which retention never detaches");
    assert!(
        matches!(
            &refused,
            crate::core::history::errors::HistoryError::Statement(
                crate::db::StatementFailure::StatementRefused { cause, .. }
            ) if cause.server_error().message() == expected_message
        ),
        "{refused:?}"
    );
    inspector.close();
    database.drop().await;
}

/// `TestInspection.test_fresh_leaf_is_not_expired` and
/// `test_unexpired_leaf_is_not_detachable` (prototype): today's leaf is not
/// expired until its upper bound plus the class period, and a detach keeps it.
#[tokio::test]
async fn a_fresh_leaf_is_not_expired_and_a_detach_keeps_it() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
    let today = database_today(&database.pool).await;
    let leaf = leaf_ref(&parent, class_key, today);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let inspector = core_pool_for_test_url(database.pool.url());
    let inspection = inspect_leaf_in_pool(
        &inspector,
        StatementLog::Off,
        &InspectHistoryLeaf::new(leaf.clone()),
    )
    .await
    .expect("inspect today's leaf");
    inspector.close();
    assert_eq!(
        inspection,
        LeafInspection::NotExpired {
            leaf_name: leaf.leaf_name().to_owned(),
            eligible_at: today + Duration::days(31),
        }
    );
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).expect("detach");
    let outcome = detach_expired_leaf(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        &command,
    )
    .await
    .expect("detach outcome");
    assert!(
        matches!(
            outcome,
            DetachExpiredLeafOutcome::Inspection(LeafInspection::NotExpired { .. })
        ),
        "{outcome:?}"
    );
    assert!(is_attached(&database.pool, leaf.leaf_name()).await);
    database.drop().await;
}

/// `TestLockContention.test_detach_relation_wait_is_bounded_and_restores_timeouts`:
/// a parent held in SHARE mode makes the detach busy within the 2 s DDL lock
/// timeout, and the session's statement and lock timeouts are the ones it had
/// before the call.
#[tokio::test]
#[serial(timing)]
async fn a_busy_detach_is_bounded_and_restores_both_timeouts() {
    let database = TestDatabase::create_with_connections(2).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let lower = database_today(&database.pool).await - Duration::days(5);
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    // One connection: the detach takes the session whose timeouts are set here.
    let detach_pool = crate::broker::pools::session_core_pool_for_test_url(database.pool.url(), 1);
    set_core_session_timeouts(&detach_pool, "13s", "17s").await;

    let mut holder = database.pool.begin().await.expect("begin parent holder");
    sql::query(&format!("LOCK TABLE {parent} IN SHARE MODE"))
        .execute(&mut *holder)
        .await
        .expect("hold the parent in SHARE mode");
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(9_000)).expect("detach");
    let started = Instant::now();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        detach_expired_leaf(&detach_pool, StatementLog::Off, &command),
    )
    .await
    .expect("the detach returns")
    .expect("detach outcome");
    let elapsed = started.elapsed();
    holder.rollback().await.expect("release the parent");

    assert_eq!(
        outcome,
        DetachExpiredLeafOutcome::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        }
    );
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "the detach waited {elapsed:?}"
    );
    assert_eq!(
        core_session_timeouts(&detach_pool).await,
        ("13s".to_owned(), "17s".to_owned())
    );
    assert!(is_attached(&database.pool, leaf.leaf_name()).await);
    detach_pool.close();
    database.drop().await;
}

/// `TestLockContention.test_cancelled_lock_acquisition_invalidates_session`:
/// a detach cancelled right after it took the leaf's session lock closes its
/// connection, so the lock is free again.
#[tokio::test]
#[serial(timing)]
async fn a_detach_cancelled_right_after_the_lock_frees_the_lock() {
    let database = TestDatabase::create_with_connections(2).await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let lower = database_today(&database.pool).await - Duration::days(5);
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    // `horsies_begin_leaf_detach` takes the session lock inside its call. The
    // read of the session's timeouts comes after it; the proxy holds that
    // statement.
    let (proxy, proxied, pause) = pausing_proxied_pool(&database, "AS lock_timeout").await;
    let detach_pool = crate::broker::pools::session_core_pool_for_test_url(proxied.url(), 1);
    proxied.close().await;
    let command = DetachExpiredHistoryLeaf::new(leaf.clone(), Some(5_000)).expect("detach");
    let detach = tokio::spawn(async move {
        detach_expired_leaf(&detach_pool, StatementLog::Off, &command).await
    });
    pause.wait_until_entered().await;

    let try_xact = format!("SELECT pg_try_advisory_xact_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))");
    let mut probe = database.pool.begin().await.expect("begin lock probe");
    let free_while_held: bool = sql::query_scalar(&try_xact)
        .bind(class_key)
        .bind(lower)
        .fetch_one(&mut *probe)
        .await
        .expect("probe the held lock");
    probe.rollback().await.expect("end lock probe");
    assert!(
        !free_while_held,
        "the detach holds the leaf lock at the pause"
    );

    detach.abort();
    assert!(detach
        .await
        .expect_err("the detach is cancelled")
        .is_cancelled());
    let mut acquired = false;
    for _ in 0..40 {
        let mut probe = database.pool.begin().await.expect("begin lock probe");
        acquired = sql::query_scalar(&try_xact)
            .bind(class_key)
            .bind(lower)
            .fetch_one(&mut *probe)
            .await
            .expect("probe the released lock");
        probe.rollback().await.expect("end lock probe");
        if acquired {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(acquired, "the cancelled detach released its session lock");
    proxy.stop().await;
    database.drop().await;
}

/// `test_drop_refuses_out_of_band_detach_with_pending_locator` (prototype): a
/// leaf detached outside maintenance that a HISTORY pending row still points
/// into is not dropped.
#[tokio::test]
async fn a_drop_keeps_an_out_of_band_detached_leaf_with_a_pending_row() {
    let database = TestDatabase::create().await;
    let class_key = "standard_1d";
    let parent = register_class(&database.pool, class_key, 1).await;
    let lower = database_today(&database.pool).await - Duration::days(5);
    let leaf = leaf_ref(&parent, class_key, lower);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    seed_history_pending_row(&database.pool, class_key, lower).await;
    sql::query(&format!(
        "ALTER TABLE {parent} DETACH PARTITION {}",
        leaf.leaf_name()
    ))
    .execute(&database.pool)
    .await
    .expect("detach out of band");

    let dropper = core_pool_for_test_url(database.pool.url());
    let outcome = drop_detached_leaf_in_pool(
        &dropper,
        StatementLog::Off,
        &DropDetachedHistoryLeaf::new(leaf.clone()),
    )
    .await
    .expect("drop outcome");
    dropper.close();
    assert!(
        matches!(
            outcome,
            LeafDrop::Inspection(LeafInspection::PendingBlocked {
                blocker_count: 1,
                attachment: LeafAttachment::Detached,
                ..
            })
        ),
        "{outcome:?}"
    );
    assert!(relation_exists(&database.pool, leaf.leaf_name()).await);
    database.drop().await;
}

/// `test_worker_and_operator_partition_privilege_modes_are_distinct`
/// (prototype), worker half: a role without CREATE on the schema cannot
/// create a leaf, and no catalog row is written. The privilege report of the
/// operator half belonged to the partition health survey, which the core
/// leaves out.
#[tokio::test]
async fn a_role_without_create_privilege_cannot_create_a_leaf() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
    let leaf = leaf_ref(
        &parent,
        class_key,
        database_today(&database.pool).await + Duration::days(3),
    );
    let role = format!("port_worker_{}", Uuid::new_v4().simple());
    sql::query(&format!("CREATE ROLE {role} NOLOGIN"))
        .execute(&database.pool)
        .await
        .expect("create worker role");
    sql::query(&format!("GRANT USAGE ON SCHEMA public TO {role}"))
        .execute(&database.pool)
        .await
        .expect("grant schema usage");
    sql::query(&format!(
        "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO {role}"
    ))
    .execute(&database.pool)
    .await
    .expect("grant row privileges only");

    let mut transaction = database.pool.begin().await.expect("begin worker session");
    sql::query(&format!("SET LOCAL ROLE {role}"))
        .execute(&mut *transaction)
        .await
        .expect("become the worker role");
    let refused = try_create_history_leaf(&mut transaction, class_key, Some(leaf.bounds().lower()))
        .await
        .expect_err("a worker role cannot run partition DDL");
    transaction.rollback().await.expect("end worker session");
    assert_eq!(refused.sqlstate(), Some("42501"), "{refused:?}");
    assert!(!relation_exists(&database.pool, leaf.leaf_name()).await);
    assert_eq!(catalog_row_count(&database.pool, leaf.leaf_name()).await, 0);

    sql::query(&format!("DROP OWNED BY {role}"))
        .execute(&database.pool)
        .await
        .expect("drop the role's grants");
    sql::query(&format!("DROP ROLE {role}"))
        .execute(&database.pool)
        .await
        .expect("drop worker role");
    database.drop().await;
}

/// `TestRenderingDivergesWithoutThePin.test_raw_rendering_differs_across_the_probe_timezones`:
/// the premise of the time-zone tests. PostgreSQL renders a partition bound in
/// the session time zone.
#[tokio::test]
async fn raw_bound_rendering_differs_across_the_probe_time_zones() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
    let leaf = leaf_ref(&parent, class_key, database_today(&database.pool).await);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    let mut renderings = Vec::new();
    for zone in [WEST_OF_UTC, EAST_OF_UTC] {
        let mut transaction = database.pool.begin().await.expect("begin rendering");
        set_local_zone(&mut transaction, zone).await;
        let rendering: String = sql::query_scalar(
            "SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class AS c
             WHERE c.oid = to_regclass($1)",
        )
        .bind(leaf.leaf_name())
        .fetch_one(&mut *transaction)
        .await
        .expect("render the bound");
        transaction.rollback().await.expect("end rendering");
        renderings.push(rendering);
    }
    assert_ne!(renderings[0], renderings[1]);
    database.drop().await;
}

/// `TestConformanceIsSessionIndependent.test_leaf_created_on_one_side_is_conformant_on_the_other`,
/// both directions: a leaf created from one side of UTC is conformant and
/// inspectable from the other side. One class per direction.
#[tokio::test]
async fn a_leaf_created_on_one_side_of_utc_is_conformant_on_the_other() {
    let database = TestDatabase::create().await;
    for (class_key, days, creating, inspecting) in [
        ("standard_30d", 30, WEST_OF_UTC, EAST_OF_UTC),
        ("standard_31d", 31, EAST_OF_UTC, WEST_OF_UTC),
    ] {
        let parent = register_class(&database.pool, class_key, days).await;
        let today = database_today(&database.pool).await;
        let mut transaction = database.pool.begin().await.expect("begin creation");
        set_local_zone(&mut transaction, creating).await;
        let mut created = Vec::new();
        for offset in 0..4 {
            let leaf = leaf_ref(&parent, class_key, today + Duration::days(offset));
            created.push(
                create_history_leaf_of(&mut transaction, &leaf)
                    .await
                    .expect("create from one side"),
            );
        }
        transaction.commit().await.expect("commit creation");
        assert_eq!(created.len(), 4);
        assert!(created
            .iter()
            .all(|outcome| matches!(outcome, LeafCreation::Created { .. })));

        let leaf = leaf_ref(&parent, class_key, today);
        let mut transaction = database.pool.begin().await.expect("begin verification");
        set_local_zone(&mut transaction, inspecting).await;
        let verified = create_history_leaf_of(&mut transaction, &leaf)
            .await
            .expect("verify from the other side");
        transaction.rollback().await.expect("end verification");
        assert_eq!(
            verified,
            LeafCreation::AlreadyConformant {
                leaf_name: leaf.leaf_name().to_owned(),
            },
            "{creating} -> {inspecting}"
        );
        let inspector = core_pool_in_zone(&database, inspecting).await;
        let inspection = inspect_leaf_in_pool(
            &inspector,
            StatementLog::Off,
            &InspectHistoryLeaf::new(leaf),
        )
        .await
        .expect("inspect from the other side");
        inspector.close();
        assert!(
            matches!(
                inspection,
                LeafInspection::NotExpired { .. } | LeafInspection::Detachable { .. }
            ),
            "{creating} -> {inspecting}: {inspection:?}"
        );
    }
    database.drop().await;
}

/// `TestConformanceIsSessionIndependent.test_heartbeat_leaf_conformance_crosses_utc_both_ways`.
#[tokio::test]
async fn a_heartbeat_leaf_is_conformant_across_utc_both_ways() {
    let database = TestDatabase::create().await;
    let hour = database_hour(&database.pool).await;
    for (offset_hours, creating, inspecting) in
        [(0, WEST_OF_UTC, EAST_OF_UTC), (3, EAST_OF_UTC, WEST_OF_UTC)]
    {
        let leaf = hourly_leaf_ref(hour + Duration::hours(offset_hours)).expect("hourly leaf");
        let mut transaction = database.pool.begin().await.expect("begin creation");
        set_local_zone(&mut transaction, creating).await;
        let _registration = register_heartbeat_class(&mut transaction, 2).await;
        let created = create_heartbeat_leaf_of(&mut transaction, &leaf)
            .await
            .expect("create from one side");
        transaction.commit().await.expect("commit creation");
        assert!(
            matches!(created, LeafCreation::Created { .. }),
            "{created:?}"
        );

        let mut transaction = database.pool.begin().await.expect("begin verification");
        set_local_zone(&mut transaction, inspecting).await;
        let verified = create_heartbeat_leaf_of(&mut transaction, &leaf)
            .await
            .expect("verify from the other side");
        transaction.rollback().await.expect("end verification");
        assert_eq!(
            verified,
            LeafCreation::AlreadyConformant {
                leaf_name: leaf.leaf_name().to_owned(),
            },
            "{creating} -> {inspecting}"
        );
    }
    database.drop().await;
}

/// `test_daily_leaves_at_the_fold_verify_as_conformant`: from Europe/Berlin
/// sessions, the daily leaves on both sides of the 2026 fold are created,
/// then verify as conformant and inspect as not expired or detachable.
#[tokio::test]
async fn daily_leaves_at_the_fold_verify_as_conformant() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
    let berlin = pool_in_zone(&database, DST_ZONE, 1).await;
    let leaves = [
        leaf_ref(&parent, class_key, coming_fold() - Duration::days(1)),
        leaf_ref(&parent, class_key, coming_fold()),
    ];
    for leaf in &leaves {
        assert!(matches!(
            create_leaf(&berlin, leaf).await,
            LeafCreation::Created { .. }
        ));
    }
    let berlin_core = core_pool_in_zone(&database, DST_ZONE).await;
    for leaf in &leaves {
        assert_eq!(
            create_leaf(&berlin, leaf).await,
            LeafCreation::AlreadyConformant {
                leaf_name: leaf.leaf_name().to_owned(),
            }
        );
        let inspection = inspect_leaf_in_pool(
            &berlin_core,
            StatementLog::Off,
            &InspectHistoryLeaf::new(leaf.clone()),
        )
        .await
        .expect("inspect a fold leaf");
        assert!(
            matches!(
                inspection,
                LeafInspection::NotExpired { .. } | LeafInspection::Detachable { .. }
            ),
            "{inspection:?}"
        );
    }
    berlin_core.close();
    berlin.close().await;
    database.drop().await;
}

async fn create_hourly(pool: &TestPool, leaf: &LeafRef) -> LeafCreation {
    create_heartbeat_leaf_of(pool, leaf)
        .await
        .expect("create hourly leaf")
}

async fn register_heartbeats(pool: &TestPool) {
    let _registration = register_heartbeat_class(pool, 2).await;
}

/// `test_hourly_leaves_at_the_fold_verify_as_conformant`.
#[tokio::test]
async fn hourly_leaves_at_the_fold_verify_as_conformant() {
    let database = TestDatabase::create().await;
    let berlin = pool_in_zone(&database, DST_ZONE, 1).await;
    register_heartbeats(&berlin).await;
    let leaves = [
        hourly_leaf_ref(coming_fold()).expect("fold hour"),
        hourly_leaf_ref(coming_fold() + Duration::hours(1)).expect("repeated hour"),
    ];
    for leaf in &leaves {
        let created = create_hourly(&berlin, leaf).await;
        assert!(
            matches!(created, LeafCreation::Created { .. }),
            "{created:?}"
        );
    }
    for leaf in &leaves {
        assert_eq!(
            create_hourly(&berlin, leaf).await,
            LeafCreation::AlreadyConformant {
                leaf_name: leaf.leaf_name().to_owned(),
            }
        );
    }
    berlin.close().await;
    database.drop().await;
}

/// `test_the_heartbeat_sweep_drops_leaves_across_a_fold`: the sweep, run from
/// Europe/Berlin sessions, detaches and drops the two hourly leaves of the
/// 2025 fold.
#[tokio::test]
async fn the_heartbeat_sweep_drops_leaves_across_a_fold() {
    let database = TestDatabase::create().await;
    let berlin = pool_in_zone(&database, DST_ZONE, 2).await;
    register_heartbeats(&berlin).await;
    let leaves = [
        hourly_leaf_ref(past_fold()).expect("fold hour"),
        hourly_leaf_ref(past_fold() + Duration::hours(1)).expect("repeated hour"),
    ];
    for leaf in &leaves {
        let created = create_hourly(&berlin, leaf).await;
        assert!(
            matches!(created, LeafCreation::Created { .. }),
            "{created:?}"
        );
    }
    // One connection, whose session runs in Europe/Berlin.
    let berlin_core = crate::broker::pools::session_core_pool_for_test_url(berlin.url(), 1);
    {
        let mut connection = berlin_core
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("check out the Berlin core connection");
        let _rows = connection
            .query(
                &SET_SESSION_TIMEZONE.bind().value(&DST_ZONE).finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await
            .expect("set the Berlin session time zone");
    }
    assert_eq!(core_session_timezone(&berlin_core).await, DST_ZONE);
    let sweep = sweep_expired_heartbeat_leaves(&berlin_core, StatementLog::Off, 64)
        .await
        .expect("sweep expired heartbeat leaves");
    assert!(sweep.errors.is_empty(), "{:?}", sweep.errors);
    assert_eq!(
        sweep
            .swept
            .iter()
            .map(|entry| entry.leaf_name.as_str())
            .collect::<Vec<_>>(),
        leaves
            .iter()
            .map(|leaf| leaf.leaf_name())
            .collect::<Vec<_>>()
    );
    for entry in &sweep.swept {
        assert_eq!(
            entry.detach,
            DetachExpiredLeafOutcome::Detached {
                leaf_name: entry.leaf_name.clone(),
            }
        );
        assert_eq!(
            entry.drop,
            Some(LeafDrop::Dropped {
                leaf_name: entry.leaf_name.clone(),
            })
        );
    }
    berlin.close().await;
    database.drop().await;
}

/// `TestCoverageAndWrites.test_heartbeats_route_through_the_parent_into_leaves`:
/// a beat written to the partitioned parent is stored in the leaf of its hour.
#[tokio::test]
async fn a_heartbeat_written_to_the_parent_lands_in_its_hour_leaf() {
    let database = TestDatabase::create().await;
    let coverage = ensure_partition_coverage_in_pool(
        &core_pool_for_test_url(database.pool.url()),
        StatementLog::Off,
        2,
        2,
        &[],
    )
    .await
    .expect("heartbeat coverage");
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
    let task_id = Uuid::new_v4();
    sql::query(&format!(
        "INSERT INTO {HEARTBEATS_TABLE} (task_id, role, sent_at, sender_id, hostname, pid)
         VALUES ($1, 'runner', statement_timestamp(), 'port-worker', 'port-host', 1)"
    ))
    .bind(task_id)
    .execute(&database.pool)
    .await
    .expect("write a heartbeat through the parent");
    let (stored_in, sent_at): (String, DateTime<Utc>) = sql::query_as(&format!(
        "SELECT tableoid::regclass::text, sent_at FROM {HEARTBEATS_TABLE} WHERE task_id = $1"
    ))
    .bind(task_id)
    .fetch_one(&database.pool)
    .await
    .expect("read the beat's leaf");
    let hour = sent_at
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncate to the hour");
    assert_eq!(stored_in, hourly_leaf_name(hour).expect("hourly leaf name"));
    database.drop().await;
}

/// `TestLeafBirth.test_forever_leaf_carries_the_ordering_index`.
#[tokio::test]
async fn the_static_forever_leaf_carries_the_ordering_index() {
    let database = TestDatabase::create().await;
    assert!(ordering_index_exists(&database.pool, TASK_HISTORY_FOREVER_LEAF).await);
    database.drop().await;
}

/// `TestLeafRepair.test_repair_recreates_a_dropped_ordering_index`.
#[tokio::test]
async fn leaf_repair_recreates_a_dropped_ordering_index() {
    let database = TestDatabase::create().await;
    let class_key = "standard_30d";
    let parent = register_class(&database.pool, class_key, 30).await;
    let leaf = leaf_ref(&parent, class_key, database_today(&database.pool).await);
    assert!(matches!(
        create_leaf(&database.pool, &leaf).await,
        LeafCreation::Created { .. }
    ));
    sql::query(&format!(
        "DROP INDEX {}",
        leaf_enqueued_index_name(leaf.leaf_name())
    ))
    .execute(&database.pool)
    .await
    .expect("drop the ordering index");
    assert!(!ordering_index_exists(&database.pool, leaf.leaf_name()).await);

    let repaired = create_leaf(&database.pool, &leaf).await;
    assert!(
        matches!(repaired, LeafCreation::IndexRepaired { .. }),
        "{repaired:?}"
    );
    assert!(ordering_index_exists(&database.pool, leaf.leaf_name()).await);
    database.drop().await;
}
