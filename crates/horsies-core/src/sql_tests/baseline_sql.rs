//! The baseline migration text on a fresh database: the tables, the
//! functions, the seed rows, the checks of the leaf catalog, the manifest
//! and the workflow nodes, and the wakes of a task insert.
//!
//! Each test creates its own database ([`BaselineDatabase::create`]), with
//! the baseline migration and nothing else. A case that writes runs in a
//! transaction that is rolled back, except the wake cases: a notification
//! needs a commit, so each wake case has a database of its own.

use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_postgres::error::SqlState;
use tokio_postgres::Client;
use uuid::Uuid;

use super::harness::{describe, BaselineDatabase};

const FUNCTION_CALLS_SQL: &str = include_str!("cases/baseline_function_calls.sql");

const EXPECTED_TABLES: [&str; 23] = [
    "horsies_client_family",
    "horsies_heartbeats",
    "horsies_key_reservations",
    "horsies_migrations",
    "horsies_recovery_scan_cursors",
    "horsies_retention_classes",
    "horsies_schedule_state",
    "horsies_schema_version",
    "horsies_task_attempts",
    "horsies_task_history",
    "horsies_task_history_clock_exceptions",
    "horsies_task_history_forever",
    "horsies_task_history_forever_open",
    "horsies_task_history_leaf_catalog",
    "horsies_task_lookup_manifest",
    "horsies_tasks",
    "horsies_worker_liveness",
    "horsies_worker_states",
    "horsies_workflow_phase2_pending",
    "horsies_workflow_phase2_quarantine",
    "horsies_workflow_recovery_failures",
    "horsies_workflow_tasks",
    "horsies_workflows",
];

const EXPECTED_FUNCTIONS: [&str; 168] = [
    // 0001_tables.sql: the UUIDv7 helper and the trigger functions.
    "horsies_uuidv7",
    "horsies_notify_task_changes",
    "horsies_notify_task_status_change",
    "horsies_notify_worker_state_change",
    "horsies_notify_workflow_status_change",
    // 0003_interim_functions.sql.
    "horsies_abandon_nodes_of_paused_workflows",
    "horsies_abandon_owned_node",
    "horsies_abandon_owned_nodes",
    "horsies_cancel_nodes_of_cancelled_workflow",
    "horsies_cancel_owned_node",
    "horsies_cancel_owned_nodes",
    "horsies_claim_plan",
    "horsies_encode_rerun_input",
    "horsies_encode_task_attempts",
    "horsies_find_non_runnable_workflow_tasks",
    "horsies_key_reservation_claim",
    "horsies_key_reservation_cleanup",
    "horsies_key_reservation_terminalize",
    "horsies_key_reservation_terminalize_batch",
    "horsies_move_task_to_history",
    "horsies_phase2_consume",
    "horsies_phase2_quarantine_one",
    "horsies_task_history_leaf_lock_key",
    "horsies_terminalization_miss",
    "horsies_uuid7_birth",
    // 0004_lifecycle_functions.sql.
    "horsies_cancel_orphaned_tasks",
    "horsies_cancel_owned_orphan",
    "horsies_cancel_task",
    "horsies_claim",
    "horsies_claim_candidates",
    "horsies_claim_in_flight",
    "horsies_complete_task_fused",
    "horsies_complete_tasks_fused",
    "horsies_enqueue_task",
    "horsies_expire_owned_claim",
    "horsies_expire_pending_tasks",
    "horsies_finish_failed_attempt",
    "horsies_finish_stale_attempt",
    "horsies_finish_workflow_success",
    "horsies_forget_ended_worker_liveness",
    "horsies_insert_task_rows",
    "horsies_mark_finalizing",
    "horsies_node_has_dependents",
    "horsies_node_downstream_subworkflow",
    "horsies_release_claims",
    "horsies_requeue_stale_claims",
    "horsies_stale_running_scan",
    "horsies_start_attempt",
    "horsies_take_worker_liveness",
    "horsies_unheld_worker_owners",
    "horsies_worker_liveness_generations",
    "horsies_worker_liveness_key",
    // 0005_workflow_functions.sql.
    "horsies_task_error_envelope",
    "horsies_task_error_object",
    "horsies_workflow_apply_recovery_limit",
    "horsies_workflow_args_from_entries",
    "horsies_workflow_args_from_index",
    "horsies_workflow_cancel",
    "horsies_workflow_child_finish",
    "horsies_workflow_child_node_result",
    "horsies_workflow_child_start_rows",
    "horsies_workflow_err_slot",
    "horsies_workflow_explicit_output",
    "horsies_workflow_fail_unbuildable",
    "horsies_workflow_failure_error",
    "horsies_workflow_json_or_null",
    "horsies_workflow_move_descendants",
    "horsies_workflow_node_apply",
    "horsies_workflow_node_finish",
    "horsies_workflow_node_tasks_insert",
    "horsies_workflow_outcome_row",
    "horsies_workflow_output_error_data",
    "horsies_workflow_outputless_result",
    "horsies_workflow_pause",
    "horsies_workflow_phase2_record_raise",
    "horsies_workflow_phase2_recover",
    "horsies_workflow_phase2_source",
    "horsies_workflow_policy_indices_cause",
    "horsies_workflow_progress",
    "horsies_workflow_promote",
    "horsies_workflow_promote_and_complete",
    "horsies_workflow_promote_level",
    "horsies_workflow_python_str_list",
    "horsies_workflow_record_failure",
    "horsies_workflow_require",
    "horsies_workflow_result_kind",
    "horsies_workflow_resume",
    "horsies_workflow_source_value",
    "horsies_workflow_start",
    "horsies_workflow_start_advance",
    "horsies_workflow_start_check",
    "horsies_workflow_start_child_row",
    "horsies_workflow_start_failed_node",
    "horsies_workflow_start_nodes",
    "horsies_workflow_start_top_row",
    "horsies_workflow_start_tree",
    "horsies_workflow_success_cases",
    "horsies_workflow_task_input",
    "horsies_workflow_task_node_refusal",
    "horsies_workflow_try_complete",
    "horsies_workflow_unbuildable_nodes",
    "horsies_workflow_unreadable_result_cause",
    "horsies_workflow_tree_check",
    "horsies_workflow_child_start",
    "horsies_workflow_claimed_tasks_settle",
    "horsies_workflow_task_stop_before_start",
    "horsies_workflow_expire_paused",
    "horsies_workflow_node_reevaluate",
    "horsies_workflow_nodes_enqueue_ready",
    "horsies_workflow_recovery_audit_page",
    "horsies_workflow_audit_claim_renew",
    "horsies_workflow_recover_completion",
    "horsies_workflow_fail_orphaned",
    "horsies_workflow_recovery_record_raise",
    "horsies_workflow_json_type_name",
    "horsies_workflow_json_int",
    "horsies_workflow_json_breaks_strict_rules",
    "horsies_workflow_options_json",
    "horsies_workflow_retry_interval_entries",
    "horsies_workflow_retry_codes_plain",
    "horsies_workflow_retry_code_entries",
    "horsies_workflow_retry_policy_entries",
    "horsies_workflow_retry_policy_value_error",
    "horsies_workflow_retry_policy_refusal",
    "horsies_workflow_execution_refusal",
    "horsies_workflow_task_options_refusal",
    // 0006_history_functions.sql.
    "horsies_begin_leaf_detach",
    "horsies_drop_detached_leaf",
    "horsies_inspect_history_leaf",
    "horsies_move_tasks_to_history",
    "horsies_prune_candidates",
    "horsies_record_leaf_detached",
    "horsies_require_history_leaf",
    "horsies_rerun_source_read",
    "horsies_task_absence_reason",
    // 0007_scheduler_monitoring_functions.sql.
    "horsies_fire_schedule",
    "horsies_list_expired_tasks",
    "horsies_list_stale_tasks",
    "horsies_monitoring_check_scope",
    "horsies_monitoring_schedules",
    "horsies_monitoring_task_breakdown",
    "horsies_monitoring_task_detail",
    "horsies_monitoring_task_facets",
    "horsies_monitoring_task_list",
    "horsies_monitoring_task_stats",
    "horsies_monitoring_workflow_names",
    "horsies_monitoring_workflow_node",
    "horsies_monitoring_workflow_run",
    "horsies_monitoring_workflow_runs",
    "horsies_schedule_slot_task_id",
    // 0008_history_maintenance_functions.sql. horsies_terminalization_miss
    // gains an overload there; its name is listed above.
    "horsies_coverage_probe",
    "horsies_create_heartbeat_leaf",
    "horsies_create_history_leaf",
    "horsies_create_partition_leaf",
    "horsies_heartbeat_leaf_name",
    "horsies_history_absent_leaves",
    "horsies_history_leaf_indexes",
    "horsies_history_leaf_name",
    "horsies_history_leaf_state",
    "horsies_history_reader_manifest",
    "horsies_history_reader_signatures",
    "horsies_history_reader_stamp",
    "horsies_history_readers_current",
    "horsies_publish_history_readers",
    "horsies_register_retention_classes",
    "horsies_render_history_reader",
    "horsies_retention_period_days",
    "horsies_terminalization_family",
];

const TABLES_SQL: &str = r"
    SELECT c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema()
      AND c.relkind IN ('r', 'p')
      AND c.relname LIKE 'horsies\_%'
";

const FUNCTIONS_SQL: &str = r"
    SELECT p.proname
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = current_schema()
      AND p.proname LIKE 'horsies\_%'
";

const MOVE_SIGNATURES_SQL: &str = "
    SELECT p.oid::regprocedure::text
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = current_schema()
      AND p.proname = 'horsies_move_task_to_history'
";

const SEED_ROWS_SQL: &str = "
    SELECT
        (SELECT array_agg(version) FROM horsies_schema_version),
        (SELECT array_agg(class_key || ':' || class_kind) FROM horsies_retention_classes),
        (SELECT array_agg(leaf_name) FROM horsies_task_history_leaf_catalog),
        (SELECT array_agg(scan_name ORDER BY scan_name) FROM horsies_recovery_scan_cursors),
        (SELECT count(*) FROM horsies_migrations),
        (SELECT count(*) FROM horsies_client_family)
";

const LEAF_ROW_SQL: &str = "
    INSERT INTO horsies_task_history_leaf_catalog (
        leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor, index_schema_version,
        created_at, detached_at, dropped_at
    ) VALUES ($1, 'forever', $2, $3, $4, 1, now(), NULL, NULL)
";

const MANIFEST_ROW_SQL: &str = "
    INSERT INTO horsies_task_lookup_manifest (
        leaf_name, probe_position, lower_bound_kind, lower_anchor, upper_anchor, published_at
    ) VALUES ($1, $2, $3, $4, $5, now())
";

const UPPER: &str = "2030-01-01T00:00:00+00:00";
const LOWER: &str = "2029-12-31T00:00:00+00:00";

const WORKFLOW_ROW_SQL: &str = "
    INSERT INTO horsies_workflows (
        id, name, status, on_error, depth, root_workflow_id, sent_at, created_at, updated_at
    ) VALUES (
        '0193e5c4-0000-7000-8000-000000000001', 'params_case', 'RUNNING', 'fail', 0,
        '0193e5c4-0000-7000-8000-000000000001', now(), now(), now()
    )
";

/// Parameters: `$1` max_retries, `$2` execution, `$3` retention_class_key,
/// `$4` is_subworkflow, `$5` sub_workflow_params.
const NODE_ROW_SQL: &str = "
    INSERT INTO horsies_workflow_tasks (downstream_subworkflow,
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies,
        allow_failed_deps, join_type, max_retries, execution, retention_class_key, status,
        is_subworkflow, sub_workflow_params, created_at
    ) VALUES ('NONE',
        '0193e5c4-0000-7000-8000-000000000002', '0193e5c4-0000-7000-8000-000000000001', 0, 'node_0',
        'params.case', '{}', 'default', 100, '{}', FALSE, 'all', $1, $2,
        $3, 'PENDING', $4, $5, now()
    )
";

/// Parameter: `$1` downstream_subworkflow.
const DOWNSTREAM_NODE_SQL: &str = "
    INSERT INTO horsies_workflow_tasks (
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies,
        allow_failed_deps, join_type, max_retries, execution, retention_class_key, downstream_subworkflow,
        status, is_subworkflow, created_at
    ) VALUES (
        '0193e5c4-0000-7000-8000-000000000002', '0193e5c4-0000-7000-8000-000000000001', 0, 'node_0',
        'downstream.case', '{}', 'default', 100, '{}', FALSE, 'all', 0, 'ISOLATED', 'forever',
        $1, 'PENDING', FALSE, now()
    )
";

const FOREVER_LEAF_SQL: &str = "
    SELECT pg_get_expr(c.relpartbound, c.oid), l.lower_bound_kind, l.lower_anchor IS NULL
    FROM pg_class c
    JOIN horsies_task_history_leaf_catalog l ON l.leaf_name = c.relname
    WHERE c.relname = 'horsies_task_history_forever_open'
";

/// Parameters: `$1` id, `$2` status.
const TASK_ROW_SQL: &str = "
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key,
        sent_at, created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest,
        status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
        finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name,
        retry_count, max_retries, next_retry_at, good_until, is_workflow_task, updated_at,
        retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        $1, 'notify.task', 'default', 100, '{}', NULL, 'ISOLATED', 'forever',
        now(), now(), 1, sha256('x'::bytea), NULL,
        CAST($2 AS varchar), now(),
        CASE WHEN CAST($2 AS varchar) = 'PENDING' THEN NULL ELSE now() END,
        CASE WHEN CAST($2 AS varchar) = 'PENDING' THEN NULL ELSE 'w1' END,
        CASE WHEN CAST($2 AS varchar) = 'CLAIMED' THEN now() + interval '1 minute' END,
        CASE WHEN CAST($2 AS varchar) = 'RUNNING' THEN now() END,
        NULL, NULL,
        CASE WHEN CAST($2 AS varchar) = 'RUNNING' THEN 1 END,
        CASE WHEN CAST($2 AS varchar) = 'RUNNING' THEN 'host' END,
        CASE WHEN CAST($2 AS varchar) = 'RUNNING' THEN 'proc' END,
        0, 0, NULL, NULL, FALSE, now(), FALSE, NULL, NULL
    )
";

const INSERT_CHANNELS: [&str; 3] = ["task_new", "task_queue_default", "horsies_task_status"];

/// The time the session waits for the notifications of the insert.
const NOTIFY_WAIT: Duration = Duration::from_millis(500);

const TASK_ID: &str = "00000000-0000-7000-8000-0000000000c1";

/// The first column of each row of `query`, as a set.
async fn names(client: &Client, query: &str) -> HashSet<String> {
    client
        .query(query, &[])
        .await
        .unwrap_or_else(|error| panic!("{query}: {}", describe(&error)))
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect()
}

fn expected(names: &[&str]) -> HashSet<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

fn timestamp(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .expect("a fixed RFC 3339 time")
        .with_timezone(&Utc)
}

async fn run(client: &Client, statement: &str) {
    client
        .batch_execute(statement)
        .await
        .unwrap_or_else(|error| panic!("{statement}: {}", describe(&error)));
}

/// Asserts that `outcome` failed with `state` and, when one is named, on
/// `constraint`.
fn assert_refused(
    outcome: Result<u64, tokio_postgres::Error>,
    state: &SqlState,
    constraint: Option<&str>,
    case: &str,
) {
    let error = outcome.expect_err(case);
    let database_error = error
        .as_db_error()
        .unwrap_or_else(|| panic!("{case}: not a server error: {error}"));
    assert_eq!(database_error.code(), state, "{case}: {}", describe(&error));
    match constraint {
        Some(name) => assert_eq!(database_error.constraint(), Some(name), "{case}"),
        None => {}
    }
}

#[tokio::test]
async fn migration_creates_the_baseline_tables() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    assert_eq!(
        names(session.client(), TABLES_SQL).await,
        expected(&EXPECTED_TABLES)
    );
}

#[tokio::test]
async fn migration_creates_the_baseline_functions() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    assert_eq!(
        names(session.client(), FUNCTIONS_SQL).await,
        expected(&EXPECTED_FUNCTIONS)
    );
}

#[tokio::test]
async fn the_single_move_has_one_signature_that_takes_the_locked_row() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    assert_eq!(
        names(session.client(), MOVE_SIGNATURES_SQL).await,
        expected(&["horsies_move_task_to_history(horsies_tasks,text,text,\
                    timestamp with time zone,text,text,text)"])
    );
}

#[tokio::test]
async fn migration_writes_the_seed_rows() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    let row = session
        .client()
        .query_one(SEED_ROWS_SQL, &[])
        .await
        .unwrap_or_else(|error| panic!("read the seed rows: {}", describe(&error)));
    assert_eq!(row.get::<_, Vec<i32>>(0), vec![1000]);
    assert_eq!(row.get::<_, Vec<String>>(1), vec!["forever:FOREVER"]);
    assert_eq!(
        row.get::<_, Vec<String>>(2),
        vec!["horsies_task_history_forever_open"]
    );
    assert_eq!(
        row.get::<_, Vec<String>>(3),
        vec!["orphan_workflow_tasks", "running_workflows"]
    );
    assert_eq!(row.get::<_, i64>(4), 0);
    assert_eq!(row.get::<_, i64>(5), 0);
}

#[tokio::test]
async fn every_interim_function_runs_on_the_baseline_tables() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    session
        .run_script_rolled_back("baseline_function_calls.sql", FUNCTION_CALLS_SQL)
        .await;
}

/// One insert of a lower bound: its kind, its anchor, and the check it
/// violates, or `None` when the row is accepted.
struct LowerBoundCase {
    kind: &'static str,
    lower: Option<&'static str>,
    violated: Option<&'static str>,
}

/// The lower bound cases of a table whose pair check is `pair` and whose
/// order check is `ordered`.
fn lower_bound_cases(pair: &'static str, ordered: &'static str) -> [LowerBoundCase; 6] {
    [
        LowerBoundCase {
            kind: "OPEN",
            lower: None,
            violated: None,
        },
        LowerBoundCase {
            kind: "AT",
            lower: Some(LOWER),
            violated: None,
        },
        LowerBoundCase {
            kind: "OPEN",
            lower: Some(LOWER),
            violated: Some(pair),
        },
        LowerBoundCase {
            kind: "AT",
            lower: None,
            violated: Some(pair),
        },
        LowerBoundCase {
            kind: "ON",
            lower: None,
            violated: Some(pair),
        },
        LowerBoundCase {
            kind: "AT",
            lower: Some(UPPER),
            violated: Some(ordered),
        },
    ]
}

#[tokio::test]
async fn leaf_catalog_lower_bound_is_typed() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    let client = session.client();
    let upper = timestamp(UPPER);
    for case in lower_bound_cases(
        "horsies_task_history_leaf_catalog_lower_bound_pair",
        "horsies_task_history_leaf_catalog_bounds_ordered",
    ) {
        let label = format!("{} {:?}", case.kind, case.lower);
        let lower = case.lower.map(timestamp);
        run(client, "BEGIN").await;
        let inserted = client
            .execute(LEAF_ROW_SQL, &[&"leaf_case", &case.kind, &lower, &upper])
            .await;
        match case.violated {
            None => {
                let _rows =
                    inserted.unwrap_or_else(|error| panic!("{label}: {}", describe(&error)));
            }
            Some(constraint) => assert_refused(
                inserted,
                &SqlState::CHECK_VIOLATION,
                Some(constraint),
                &label,
            ),
        }
        run(client, "ROLLBACK").await;
    }
}

#[tokio::test]
async fn leaf_catalog_holds_one_open_leaf_per_class_and_upper_anchor() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    let client = session.client();
    let upper = timestamp(UPPER);
    let no_lower: Option<DateTime<Utc>> = None;
    run(client, "BEGIN").await;
    let _rows = client
        .execute(LEAF_ROW_SQL, &[&"leaf_first", &"OPEN", &no_lower, &upper])
        .await
        .unwrap_or_else(|error| panic!("insert the first leaf: {}", describe(&error)));
    let second = client
        .execute(LEAF_ROW_SQL, &[&"leaf_second", &"OPEN", &no_lower, &upper])
        .await;
    assert_refused(
        second,
        &SqlState::UNIQUE_VIOLATION,
        Some("horsies_task_history_leaf_catalog_open_interval_unique"),
        "a second open leaf",
    );
    run(client, "ROLLBACK").await;
}

#[tokio::test]
async fn manifest_lower_bound_is_typed() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    let client = session.client();
    let upper = timestamp(UPPER);
    for case in lower_bound_cases(
        "horsies_task_lookup_manifest_lower_bound_pair",
        "horsies_task_lookup_manifest_bounds_ordered",
    ) {
        let label = format!("{} {:?}", case.kind, case.lower);
        let lower = case.lower.map(timestamp);
        run(client, "BEGIN").await;
        let inserted = client
            .execute(
                MANIFEST_ROW_SQL,
                &[&"leaf_case", &0_i32, &case.kind, &lower, &upper],
            )
            .await;
        match case.violated {
            None => {
                let _rows =
                    inserted.unwrap_or_else(|error| panic!("{label}: {}", describe(&error)));
            }
            Some(constraint) => assert_refused(
                inserted,
                &SqlState::CHECK_VIOLATION,
                Some(constraint),
                &label,
            ),
        }
        run(client, "ROLLBACK").await;
    }
}

#[tokio::test]
async fn sub_workflow_params_are_set_exactly_on_sub_workflow_nodes() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    let client = session.client();
    let violated = Some("horsies_workflow_tasks_sub_workflow_params_by_kind");
    let cases: [(bool, Option<&str>, Option<&str>); 7] = [
        (false, None, None),
        (true, Some("NOT_GIVEN"), None),
        (true, Some("GIVEN"), None),
        (true, None, violated),
        (true, Some("SOME"), violated),
        (false, Some("NOT_GIVEN"), violated),
        (false, Some("GIVEN"), violated),
    ];
    for (is_subworkflow, sub_workflow_params, violated) in cases {
        let label = format!("{is_subworkflow} {sub_workflow_params:?}");
        // The task facts follow the node kind, so only the params CHECK decides.
        let (max_retries, execution, retention_class_key) = match is_subworkflow {
            true => (None, None, None),
            false => (Some(0_i32), Some("ISOLATED"), Some("forever")),
        };
        run(client, "BEGIN").await;
        run(client, WORKFLOW_ROW_SQL).await;
        let inserted = client
            .execute(
                NODE_ROW_SQL,
                &[
                    &max_retries,
                    &execution,
                    &retention_class_key,
                    &is_subworkflow,
                    &sub_workflow_params,
                ],
            )
            .await;
        match violated {
            None => {
                let _rows =
                    inserted.unwrap_or_else(|error| panic!("{label}: {}", describe(&error)));
            }
            Some(constraint) => assert_refused(
                inserted,
                &SqlState::CHECK_VIOLATION,
                Some(constraint),
                &label,
            ),
        }
        run(client, "ROLLBACK").await;
    }
}

#[tokio::test]
async fn downstream_subworkflow_is_one_of_two_named_values() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    let client = session.client();
    let cases: [(Option<&str>, Option<SqlState>); 4] = [
        (Some("NONE"), None),
        (Some("REACHABLE"), None),
        (Some("SOME"), Some(SqlState::CHECK_VIOLATION)),
        (None, Some(SqlState::NOT_NULL_VIOLATION)),
    ];
    for (downstream_subworkflow, violation) in cases {
        let label = format!("{downstream_subworkflow:?}");
        run(client, "BEGIN").await;
        run(client, WORKFLOW_ROW_SQL).await;
        let inserted = client
            .execute(DOWNSTREAM_NODE_SQL, &[&downstream_subworkflow])
            .await;
        match violation {
            None => {
                let _rows =
                    inserted.unwrap_or_else(|error| panic!("{label}: {}", describe(&error)));
            }
            Some(state) => assert_refused(inserted, &state, None, &label),
        }
        run(client, "ROLLBACK").await;
    }
}

#[tokio::test]
async fn forever_leaf_starts_at_minvalue_with_an_open_catalog_bound() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    let client = session.client();
    run(client, "SET TimeZone = 'UTC'").await;
    let row = client
        .query_one(FOREVER_LEAF_SQL, &[])
        .await
        .unwrap_or_else(|error| panic!("read the forever leaf: {}", describe(&error)));
    assert_eq!(
        row.get::<_, String>(0),
        "FOR VALUES FROM (MINVALUE) TO ('9999-01-01 00:00:00+00')"
    );
    assert_eq!(row.get::<_, String>(1), "OPEN");
    assert!(row.get::<_, bool>(2));
}

/// Every insert sends the status event; only a PENDING insert sends the
/// task_new and task_queue_<queue> wake.
#[tokio::test]
async fn a_task_insert_wakes_its_queue_only_when_pending() {
    let pending = vec![
        ("task_new", TASK_ID),
        ("task_queue_default", "new"),
        ("horsies_task_status", TASK_ID),
    ];
    let status_only = vec![("horsies_task_status", TASK_ID)];
    let cases = [
        ("PENDING", pending),
        ("CLAIMED", status_only.clone()),
        ("RUNNING", status_only),
    ];
    let task_id = Uuid::parse_str(TASK_ID).expect("a fixed task id");
    for (status, expected) in cases {
        let database = BaselineDatabase::create(&[]).await;
        let session = database.session().await;
        let client = session.client();
        for channel in INSERT_CHANNELS {
            run(client, &format!("LISTEN {channel}")).await;
        }
        let _rows = client
            .execute(TASK_ROW_SQL, &[&task_id, &status])
            .await
            .unwrap_or_else(|error| panic!("{status}: {}", describe(&error)));
        tokio::time::sleep(NOTIFY_WAIT).await;
        let expected: Vec<(String, String)> = expected
            .into_iter()
            .map(|(channel, payload)| (channel.to_owned(), payload.to_owned()))
            .collect();
        assert_eq!(session.take_notifications(), expected, "{status}");
    }
}
