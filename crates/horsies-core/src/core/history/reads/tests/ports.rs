//! Ports of the Python history reader tests: the clock-exception record of a
//! move, the detail read of a moved task, one republish against a stale
//! reader body, and the reader settings and failure rules.
//!
//! Python specification: `tests/integration/test_duplicate_identity_guard.py`,
//! `test_task_history_detail_reads.py`, `test_task_history_staged_lookup_function.py`,
//! `test_reader_publication_stamps.py` and `test_task_result_history_fallback.py`.

use sha2::{Digest, Sha256};

use super::*;
use crate::broker::history_reads::{read_history_detail, read_task_result, ResultReaderSeen};
use crate::broker::pools::unpinged_single_connection_core_pool;
use crate::core::history::names::{TASK_DETAIL_FUNCTION, TASK_RESULT_FUNCTION};
use crate::core::history::reads::publisher::rendered_reader_signatures;
use crate::core::lifecycle::TerminalizationOutcome;
use crate::test_support::sqlx_terminal_writes::{self as terminal_writes, TerminalFailure};

const PORT_WORKER: &str = "port-reads-worker";

/// Publish the readers over the default class leaves and commit.
async fn publish_readers(pool: &PgPool) {
    let mut transaction = pool.begin().await.expect("begin publication");
    let coverage = ensure_partition_coverage(&mut transaction, 2, 2, &[], &StagedLoaderPublisher)
        .await
        .expect("publish the readers");
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "{coverage:?}"
    );
    transaction.commit().await.expect("commit publication");
}

/// A RUNNING plain task claimed by `PORT_WORKER`. Every baseline column is
/// named.
async fn seed_running_task(
    pool: &PgPool,
    task_id: Uuid,
    class_key: &str,
    input: &str,
    retain_rerun_input: bool,
) {
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at,
             command_fingerprint_version, command_fingerprint, idempotency_key_digest,
             status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
             started_at, finalizing_at, finalizing_by_worker_id,
             worker_pid, worker_hostname, worker_process_name,
             retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
             updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
         ) VALUES (
             $1, 'port.reads', 'default', 100, $3, NULL, 'ISOLATED',
             $2, NOW(), NOW(),
             1, decode(repeat('0b', 32), 'hex'), NULL,
             'RUNNING', NOW(), NOW(), $4, NULL,
             NOW(), NULL, NULL,
             7, 'port-host', 'port-process',
             0, 0, NULL, NULL, FALSE,
             NOW(), $5, NULL, NULL
         )",
    )
    .bind(task_id)
    .bind(class_key)
    .bind(input)
    .bind(PORT_WORKER)
    .bind(retain_rerun_input)
    .execute(pool)
    .await
    .expect("insert running task");
}

async fn complete(pool: &PgPool, task_id: Uuid) {
    let outcomes =
        terminal_writes::complete_plain_task(pool, task_id, PORT_WORKER, "{\"Ok\":true}")
            .await
            .expect("complete the task");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
}

async fn clock_exception_count(pool: &PgPool, task_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM horsies_task_history_clock_exceptions WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("count clock exceptions")
}

/// `test_duplicate_identity_guard.py::test_a_clock_ahead_move_is_recorded_and_the_guard_finds_it`:
/// a move of an id born after its terminal time records the id, and the
/// duplicate guard finds it through that record. An id born before its
/// terminal time is not recorded.
#[tokio::test]
#[serial]
async fn a_clock_ahead_move_is_recorded_and_the_guard_finds_it() {
    let database = TestDatabase::create().await;
    publish_readers(&database.pool).await;
    let mut connection = database.pool.acquire().await.expect("acquire clock");
    let now = database_now(&mut connection).await.expect("database now");
    drop(connection);
    let ahead = v7_with_birth(now + Duration::hours(1));
    let normal = v7_with_birth(now - Duration::minutes(1));
    for task_id in [ahead, normal] {
        seed_running_task(&database.pool, task_id, "standard_30d", "{}", false).await;
        complete(&database.pool, task_id).await;
    }
    assert_eq!(clock_exception_count(&database.pool, ahead).await, 1);
    assert_eq!(clock_exception_count(&database.pool, normal).await, 0);
    let mut connection = database.pool.acquire().await.expect("acquire guard");
    assert!(duplicate_guard(&mut connection, ahead).await);
    drop(connection);
    database.drop().await;
}

/// `TestDetail.test_inline_prepared_envelope_rides_the_detail`: a failed task
/// that kept its input carries the rerun envelope the move built, with its
/// version and digest, on the history detail.
#[tokio::test]
#[serial]
async fn the_detail_carries_the_archived_inline_rerun_input() {
    let database = TestDatabase::create().await;
    publish_readers(&database.pool).await;
    let task_id = v7_with_birth(Utc::now() - Duration::minutes(1));
    seed_running_task(&database.pool, task_id, "standard_30d", "{\"x\":1}", true).await;
    let outcomes = terminal_writes::fail_running_task(
        &database.pool,
        task_id,
        PORT_WORKER,
        &TerminalFailure::of_task("{\"Err\":{}}", Some("TASK_ERROR"), Some("port failure")),
    )
    .await
    .expect("fail the task");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "{outcomes:?}"
    );
    let envelope = br#"{"input":{"x":1},"options":null}"#.to_vec();
    let mut connection = database.pool.acquire().await.expect("acquire reader");
    let detail = read_task_detail(&mut connection, task_id)
        .await
        .expect("read detail");
    drop(connection);
    let TaskDetailResult::History(detail) = detail else {
        panic!("expected a history detail, got {detail:?}");
    };
    assert_eq!(detail.rerun_input_disposition, "INLINE");
    assert_eq!(detail.rerun_input_version, Some(1));
    assert_eq!(
        detail.rerun_input_inline.as_deref(),
        Some(envelope.as_slice())
    );
    assert_eq!(
        detail.rerun_input_digest,
        Some(Sha256::digest(&envelope).to_vec())
    );
    database.drop().await;
}

/// `TestDetail.test_unknown_task_with_no_published_floor_is_unclassified`: an
/// id that is not a UUIDv7 and that no table holds is absent and not
/// classified.
#[tokio::test]
#[serial]
async fn an_absent_id_that_is_not_a_uuid7_is_not_classified() {
    let database = TestDatabase::create().await;
    publish_readers(&database.pool).await;
    let task_id = Uuid::new_v4();
    let mut connection = database.pool.acquire().await.expect("acquire reader");
    let detail = read_task_detail(&mut connection, task_id)
        .await
        .expect("read detail");
    drop(connection);
    assert_eq!(
        detail,
        TaskDetailResult::Absent {
            task_id,
            reason: TaskAbsenceReason::NotClassified,
        }
    );
    database.drop().await;
}

/// Two period classes, one history row in each, in yesterday's leaves. The
/// readers are published over both. Returns the row of each class.
async fn seed_two_classes(pool: &PgPool) -> (Uuid, Uuid) {
    let mut transaction = pool.begin().await.expect("begin two-class seed");
    let now = database_now(&mut transaction).await.expect("database now");
    let yesterday = utc_day(now - Duration::days(1));
    let mut rows = Vec::new();
    for (sequence, (class_key, days)) in [("standard_30d", 30), ("standard_7d", 7)]
        .into_iter()
        .enumerate()
    {
        let parent = register_test_class(&mut transaction, class_key, days).await;
        create_test_leaf(&mut transaction, &parent, class_key, yesterday).await;
        // Each class row has its own id: the same birth, another value in
        // the low bits. With one id, a body that probes one class finds
        // that class's copy of the other row.
        let task_id = Uuid::from_u128(
            v7_with_birth(yesterday + Duration::hours(1)).as_u128() + sequence as u128,
        );
        seed_history_row(
            &mut transaction,
            task_id,
            class_key,
            yesterday + Duration::hours(2),
            "COMPLETED",
            "COMPLETE_FUSED",
        )
        .await;
        rows.push(task_id);
    }
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish over both classes");
    transaction.commit().await.expect("commit two-class seed");
    (rows[0], rows[1])
}

/// Marks one reader as written by another renderer: its comment is not the
/// stamp of the published manifest, so `horsies_history_readers_current()`
/// answers FALSE and the next publication replaces it.
async fn mark_reader_stale(connection: &mut PgConnection, signature: &str) {
    sqlx::query(&format!("COMMENT ON FUNCTION {signature} IS 'stale'"))
        .execute(connection)
        .await
        .expect("mark the reader stale");
}

/// `TestPublicationAtomicity.test_publisher_keeps_both_functions_in_agreement`:
/// a provenance body that probes the `standard_30d` class only (a publication
/// that missed the other class) misses the other class's row; one republish
/// renders it again and it finds the row.
#[tokio::test]
#[serial]
async fn one_republish_heals_a_stale_provenance_body() {
    let database = TestDatabase::create().await;
    let (row_a, row_b) = seed_two_classes(&database.pool).await;
    let mut transaction = database.pool.begin().await.expect("begin divergence");
    assert!(provenance_found(&mut transaction, row_b).await);
    sqlx::query(
        "CREATE OR REPLACE FUNCTION horsies_task_provenance_staged(p_task_id uuid)
         RETURNS TABLE(task_id uuid, status text, terminal_at timestamp with time zone,
                       terminalization_kind text, is_workflow_task boolean,
                       command_fingerprint_version smallint, command_fingerprint bytea,
                       sent_at timestamp with time zone)
         LANGUAGE sql STABLE STRICT AS $$
             SELECT h.task_id, h.status::text, h.terminal_at, h.terminalization_kind::text,
                 h.is_workflow_task, h.command_fingerprint_version, h.command_fingerprint, h.sent_at
             FROM horsies_task_history AS h
             WHERE h.retention_class_key = 'standard_30d' AND h.task_id = p_task_id
         $$",
    )
    .execute(&mut *transaction)
    .await
    .expect("install a stale provenance body");
    mark_reader_stale(&mut transaction, "horsies_task_provenance_staged(uuid)").await;
    assert!(!provenance_found(&mut transaction, row_b).await);

    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("republish");
    assert!(provenance_found(&mut transaction, row_b).await);
    assert!(provenance_found(&mut transaction, row_a).await);
    transaction.commit().await.expect("commit divergence");
    database.drop().await;
}

/// `TestPublicationAtomicity.test_publisher_keeps_the_detail_function_in_agreement`.
#[tokio::test]
#[serial]
async fn one_republish_heals_a_stale_detail_body() {
    let database = TestDatabase::create().await;
    let (_, row_b) = seed_two_classes(&database.pool).await;
    let mut transaction = database.pool.begin().await.expect("begin divergence");
    sqlx::query(
        "CREATE OR REPLACE FUNCTION horsies_task_detail_staged(p_task_id uuid)
         RETURNS TABLE(location text, task_row horsies_task_history)
         LANGUAGE sql STABLE STRICT AS $$
             SELECT 'HISTORY'::text, h
             FROM horsies_task_history AS h
             WHERE h.retention_class_key = 'standard_30d' AND h.task_id = p_task_id
         $$",
    )
    .execute(&mut *transaction)
    .await
    .expect("install a stale detail body");
    mark_reader_stale(&mut transaction, "horsies_task_detail_staged(uuid)").await;
    let diverged = read_task_detail(&mut transaction, row_b)
        .await
        .expect("read detail through the stale body");
    assert!(
        !matches!(diverged, TaskDetailResult::History(_)),
        "{diverged:?}"
    );

    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("republish");
    let restored = read_task_detail(&mut transaction, row_b)
        .await
        .expect("read detail after the republish");
    assert!(
        matches!(&restored, TaskDetailResult::History(detail) if detail.task_id == row_b),
        "{restored:?}"
    );
    transaction.commit().await.expect("commit divergence");
    database.drop().await;
}

/// `test_reader_publication_stamps.py::test_every_rendered_reader_runs_without_sequential_scans`:
/// every rendered reader carries `enable_seqscan = off`, so a probe of a new,
/// unanalyzed leaf keeps an index plan.
#[tokio::test]
#[serial]
async fn every_rendered_reader_runs_without_sequential_scans() {
    let database = TestDatabase::create().await;
    publish_readers(&database.pool).await;
    for signature in rendered_reader_signatures() {
        let settings: Option<Vec<String>> =
            sqlx::query_scalar("SELECT proconfig FROM pg_proc WHERE oid = to_regprocedure($1)")
                .bind(&signature)
                .fetch_one(&database.pool)
                .await
                .expect("read the reader settings");
        assert!(
            settings
                .unwrap_or_default()
                .iter()
                .any(|setting| setting == "enable_seqscan=off"),
            "{signature} runs with sequential scans on"
        );
    }
    database.drop().await;
}

/// `TestStagedResultReader.test_a_reader_dropped_after_it_was_seen_fails_once`:
/// a result reader removed out of band after a read saw it fails the next
/// call once with the `42883` refusal that names the function; the call
/// after it probes and answers `NotPublished`, so the broker reads the live
/// and detail readers. The reads share one reader state, as the calls of one
/// broker do, and run on one connection of a core pool, so the second read
/// meets the statement that the first read prepared.
#[tokio::test]
#[serial]
async fn a_result_reader_dropped_after_it_was_seen_fails_once() {
    let database = TestDatabase::create().await;
    publish_readers(&database.pool).await;
    let task_id = v7_with_birth(Utc::now() - Duration::minutes(1));
    seed_live_row(
        &mut database.pool.acquire().await.expect("acquire seed"),
        task_id,
        "standard_30d",
        Utc::now(),
    )
    .await;
    let reader = unpinged_single_connection_core_pool(&database.pool.connect_options());
    let reader_state = ResultReaderSeen::default();
    let seen = read_task_result(&reader, StatementLog::Off, &reader_state, task_id)
        .await
        .expect("read through the published reader");
    assert!(matches!(seen, TaskResultRead::Live(_)), "{seen:?}");
    sqlx::query("DROP FUNCTION horsies_task_result_staged(uuid)")
        .execute(&database.pool)
        .await
        .expect("drop the result reader out of band");

    let first = read_task_result(&reader, StatementLog::Off, &reader_state, task_id)
        .await
        .expect_err("the first call after the drop fails");
    assert!(
        matches!(
            &first,
            BrokerError::Statement(StatementFailure::StatementRefused { cause, .. })
                if cause.server_error().code().code() == "42883"
        ),
        "{first:?}"
    );
    assert!(first.to_string().contains(TASK_RESULT_FUNCTION), "{first}");

    let second = read_task_result(&reader, StatementLog::Off, &reader_state, task_id)
        .await
        .expect("the next call answers");
    assert!(matches!(second, TaskResultRead::NotPublished), "{second:?}");
    reader.close();
    database.drop().await;
}

/// A detail reader removed out of band (#838). The broker detail read sends
/// no publication probe, because the install and every coverage pass publish
/// the readers. The call is refused with `42883`, and the error names
/// `horsies_task_detail_staged`. After the next coverage pass publishes the
/// readers, the next call answers.
#[tokio::test]
#[serial]
async fn a_dropped_detail_reader_fails_the_call_until_the_readers_are_published() {
    let database = TestDatabase::create().await;
    publish_readers(&database.pool).await;
    let task_id = v7_with_birth(Utc::now() - Duration::minutes(1));
    seed_live_row(
        &mut database.pool.acquire().await.expect("acquire seed"),
        task_id,
        "standard_30d",
        Utc::now(),
    )
    .await;
    let reader = unpinged_single_connection_core_pool(&database.pool.connect_options());
    let seen = read_history_detail(&reader, StatementLog::Off, task_id)
        .await
        .expect("read through the published reader");
    assert!(matches!(seen, TaskDetailResult::Live { .. }), "{seen:?}");
    sqlx::query("DROP FUNCTION horsies_task_detail_staged(uuid)")
        .execute(&database.pool)
        .await
        .expect("drop the detail reader out of band");

    let refused = read_history_detail(&reader, StatementLog::Off, task_id)
        .await
        .expect_err("a missing detail reader refuses the call");
    assert!(
        matches!(
            &refused,
            BrokerError::Statement(StatementFailure::StatementRefused { cause, .. })
                if cause.server_error().code().code() == "42883"
        ),
        "{refused:?}"
    );
    assert!(
        refused.to_string().contains(TASK_DETAIL_FUNCTION),
        "{refused}"
    );

    publish_readers(&database.pool).await;
    let recovered = read_history_detail(&reader, StatementLog::Off, task_id)
        .await
        .expect("the call after the publication answers");
    assert!(
        matches!(recovered, TaskDetailResult::Live { .. }),
        "{recovered:?}"
    );
    reader.close();
    database.drop().await;
}
