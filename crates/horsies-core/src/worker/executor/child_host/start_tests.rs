//! The start of a run in a Rust child on the test database: one
//! `horsies_start_attempt` statement, and the stop of each miss (Python
//! `python/horsies/core/worker/child_runner.py`, `_confirm_ownership_and_set_running`,
//! `_after_missed_start`).

use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::host::{ChildApp, ChildLogSetup, ChildStartHook};
use super::link::FrameWriter;
use super::run::{mark_finalizing, start_attempt, HostContext, RunFacts, Started};
use crate::broker::child_writes::{
    WriteDeadline, CHILD_STATEMENT_BOUND, FINALIZING_MARKER_BOUND, FINALIZING_MARKER_END,
    START_ATTEMPT_END,
};
use crate::broker::terminalization_matrix::{migrated_database_url, migrated_pool};
use crate::broker::{OrphanSelfHeal, PostgresBroker, PrestartExpiry};
use crate::core::config::PostgresConfig;
use crate::core::registry::task::TaskRegistry;
use crate::db::{CheckoutClass, CorePool, KeptOpen};
use crate::worker::executor::protocol::StopReason;

const WORKER_ID: &str = "start-attempt-worker";
const OTHER_WORKER_ID: &str = "start-attempt-other-worker";
const PID: i32 = 4242;

/// Polls of a wait for a state that another session reaches.
const WAIT_POLLS: u32 = 600;
/// The time between two polls.
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// The bound of a start or a marker on a database that cannot be reached:
/// each checkout fails at once, and the release after a failed start is
/// sent three times with waits of 0.5 s and 1 s.
const UNREACHABLE_BOUND: Duration = Duration::from_secs(30);
/// The abandon of a claimed task whose workflow is paused.
const ABANDON_FUNCTION: &str = "horsies_abandon_owned_node";
/// The expiry of an owned claim whose deadline passed.
const EXPIRY_FUNCTION: &str = "horsies_expire_owned_claim";

/// A child context on `broker`; its frames go to a socket no one reads.
fn context_on(broker: PostgresBroker) -> (HostContext, UnixStream) {
    let (ours, theirs) = UnixStream::pair().expect("a socket pair");
    let writer = FrameWriter::start(ours, 16).expect("the frame writer starts");
    let context = HostContext {
        broker: Arc::new(broker),
        app: Arc::new(ChildApp {
            registry: Arc::new(TaskRegistry::new()),
            hostname: "start-host".to_owned(),
            start_hook: ChildStartHook::NoHook,
            producer_manifest: super::producer::test_producer_manifest(),
            log_setup: ChildLogSetup::NoSetup,
        }),
        pid: PID,
        writer,
    };
    (context, theirs)
}

/// A child context on `pool`; its frames go to a socket no one reads.
fn context(pool: PgPool) -> (HostContext, UnixStream) {
    context_on(PostgresBroker::from_pool(pool))
}

/// A child context on a port where no server listens: every checkout fails
/// at once.
fn unreachable_context() -> (HostContext, UnixStream) {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
        .expect("a lazy pool opens no connection");
    context(pool)
}

/// The broker of a child whose core pool holds one connection, on the
/// database of this test.
async fn one_connection_child_broker() -> PostgresBroker {
    let mut config = PostgresConfig::from_url(migrated_database_url().await);
    config.pool_size = 1;
    config.max_overflow = 0;
    PostgresBroker::connect_child(&config, KeptOpen::NoConnections, false)
        .await
        .expect("connect the broker of a child")
}

/// A CLAIMED plain row held by [`WORKER_ID`]; `good_until` passed when
/// `expired`. Returns the id and the claim generation.
async fn seed_claimed(pool: &PgPool, expired: bool) -> (Uuid, DateTime<Utc>) {
    let id = Uuid::new_v4();
    let generation = sqlx::query_scalar(
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
            $1, 'app.tasks.start', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW(), NOW(),
            1, decode(repeat('07', 32), 'hex'), NULL,
            'CLAIMED', NOW(), date_trunc('milliseconds', NOW()), $2,
            NOW() + interval '1 minute',
            NULL, NULL, NULL,
            NULL, NULL, NULL,
            0, 0, NULL,
            CASE WHEN $3 THEN NOW() - interval '1 second' ELSE NULL END, FALSE,
            NOW(), FALSE, NULL, NULL
         ) RETURNING claimed_at",
    )
    .bind(id)
    .bind(WORKER_ID)
    .bind(expired)
    .fetch_one(pool)
    .await
    .expect("seed a CLAIMED row");
    (id, generation)
}

/// The facts of a run of the seeded row, with the orphan self-heal off:
/// a stop before start writes nothing for an unlinked task.
fn facts(task_id: Uuid, claimed_at: DateTime<Utc>) -> RunFacts {
    RunFacts {
        task_id,
        task_name: "app.tasks.start".to_owned(),
        worker_id: WORKER_ID.to_owned(),
        claimed_at,
        orphan_self_heal: OrphanSelfHeal::Off,
    }
}

async fn live_row(pool: &PgPool, task_id: Uuid) -> Option<serde_json::Value> {
    sqlx::query_scalar("SELECT to_jsonb(t) FROM horsies_tasks t WHERE id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the live row")
}

async fn remove_task(pool: &PgPool, task_id: Uuid) {
    for statement in [
        "DELETE FROM horsies_heartbeats WHERE task_id = $1",
        "DELETE FROM horsies_tasks WHERE id = $1",
        "DELETE FROM horsies_task_history WHERE task_id = $1",
    ] {
        sqlx::query(statement)
            .bind(task_id)
            .execute(pool)
            .await
            .expect("remove the test task");
    }
}

/// The facts of the run of a workflow task. The run frames carry no task
/// kind; the workflow linkage of the seeded row makes it a workflow task.
fn workflow_facts(task_id: Uuid, claimed_at: DateTime<Utc>) -> RunFacts {
    facts(task_id, claimed_at)
}

/// Make the seeded row of `task_id` a workflow task. A workflow task has no
/// command fingerprint (baseline `horsies_tasks_command_fingerprint_by_origin`).
async fn make_workflow_task(pool: &PgPool, task_id: Uuid) {
    sqlx::query(
        "UPDATE horsies_tasks
         SET is_workflow_task = TRUE,
             command_fingerprint = NULL,
             command_fingerprint_version = NULL
         WHERE id = $1",
    )
    .bind(task_id)
    .execute(pool)
    .await
    .expect("make the row a workflow task");
}

/// A workflow in `workflow_status` with one ENQUEUED task node linked to
/// `task_id`, every baseline column named that has no NULL. Returns the
/// workflow id.
async fn link_to_workflow(pool: &PgPool, task_id: Uuid, workflow_status: &str) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, depth, root_workflow_id,
             sent_at, created_at, started_at, updated_at
         ) VALUES ($1, 'start_workflow', NULL, $2, 'fail', 0, $1,
                   NOW(), NOW(), NOW(), NOW())",
    )
    .bind(workflow_id)
    .bind(workflow_status)
    .execute(pool)
    .await
    .expect("seed the workflow");
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
             queue_name, priority, dependencies, allow_failed_deps, join_type, max_retries,
             execution, retention_class_key, status, task_id, is_subworkflow, created_at
         ) VALUES ('NONE', $1, $2, 0, 'only', 'app.tasks.start', '{}',
                   'default', 100, '{}', FALSE, 'all', 0,
                   'ISOLATED', 'forever', 'ENQUEUED', $3, FALSE, NOW())",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed the workflow node");
    workflow_id
}

/// Set the status of `workflow_id` on `connection`; in an open transaction
/// the write holds the workflow row until the transaction ends.
async fn set_workflow_status(connection: &mut PgConnection, workflow_id: Uuid, status: &str) {
    sqlx::query("UPDATE horsies_workflows SET status = $2, updated_at = NOW() WHERE id = $1")
        .bind(workflow_id)
        .bind(status)
        .execute(connection)
        .await
        .expect("set the workflow status");
}

async fn node_status(pool: &PgPool, workflow_id: Uuid) -> String {
    sqlx::query_scalar("SELECT status::text FROM horsies_workflow_tasks WHERE workflow_id = $1")
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .expect("read the node status")
}

async fn remove_workflow(pool: &PgPool, workflow_id: Uuid) {
    sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("remove the test workflow");
}

/// Lock the row of `task_id` on `connection` until its transaction ends.
async fn lock_the_row(connection: &mut PgConnection, task_id: Uuid) {
    sqlx::query("SELECT 1 FROM horsies_tasks WHERE id = $1 FOR UPDATE")
        .bind(task_id)
        .execute(connection)
        .await
        .expect("lock the task row");
}

/// The status and the finalizing marker of the row of `task_id`, read under
/// its row lock: a write that waited on the lock before this read ends
/// first, so the read sees it.
async fn row_after_lock_waiters(pool: &PgPool, task_id: Uuid) -> (String, Option<DateTime<Utc>>) {
    sqlx::query_as("SELECT status, finalizing_at FROM horsies_tasks WHERE id = $1 FOR UPDATE")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the row under its lock")
}

/// Return once a backend whose statement names `function` waits on a lock.
async fn until_a_backend_waits_in(pool: &PgPool, function: &str) {
    for _ in 0..WAIT_POLLS {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock'
                   AND query LIKE '%' || $1 || '%'
                   AND pid <> pg_backend_pid()
             )",
        )
        .bind(function)
        .fetch_one(pool)
        .await
        .expect("read the lock waits");
        match waiting {
            true => return,
            false => tokio::time::sleep(POLL_INTERVAL).await,
        }
    }
    panic!("no backend waited on a lock in {function}");
}

/// Return once a checkout waits on `pool`.
async fn until_a_checkout_waits(pool: &CorePool) {
    for _ in 0..WAIT_POLLS {
        match pool.status().waiting {
            0 => tokio::time::sleep(POLL_INTERVAL).await,
            _ => return,
        }
    }
    panic!("no checkout waited on the core pool");
}

/// Python `TestConfirmOwnership` (the start): a held claim is set RUNNING in
/// one statement, with the process of the child: its pid and the name
/// `worker-<pid>`.
#[tokio::test]
#[serial]
async fn a_held_claim_starts_running_with_the_process_of_its_child() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let (context, _peer) = context(pool.clone());

    let started = start_attempt(&context, &facts(task_id, generation)).await;

    assert!(matches!(started, Started::Running));
    let row = live_row(&pool, task_id).await.expect("the row stays live");
    assert_eq!(row["status"], "RUNNING");
    assert_eq!(row["worker_pid"], PID);
    assert_eq!(row["worker_hostname"], "start-host");
    assert_eq!(row["worker_process_name"], format!("worker-{PID}"));
    remove_task(&pool, task_id).await;
}

/// Python `_after_missed_start`, `NOT_HELD`: a start of an older claim
/// generation writes nothing and stops with `CLAIM_LOST`.
#[tokio::test]
#[serial]
async fn a_start_of_another_claim_generation_is_a_lost_claim() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let (context, _peer) = context(pool.clone());
    let before = live_row(&pool, task_id).await;

    let started = start_attempt(
        &context,
        &facts(task_id, generation - chrono::Duration::seconds(5)),
    )
    .await;

    assert!(matches!(started, Started::Stopped(StopReason::ClaimLost)));
    assert_eq!(live_row(&pool, task_id).await, before);
    remove_task(&pool, task_id).await;
}

/// Python `_after_missed_start`, `DEADLINE_PASSED`: the claim of a row whose
/// `good_until` passed is expired, and the run stops with `TASK_EXPIRED`.
#[tokio::test]
#[serial]
async fn a_passed_deadline_expires_the_claim_before_the_start() {
    let pool = crate::broker::terminalization_matrix::migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, true).await;
    let (context, _peer) = context(pool.clone());

    let started = start_attempt(&context, &facts(task_id, generation)).await;

    assert!(matches!(started, Started::Stopped(StopReason::TaskExpired)));
    assert_eq!(live_row(&pool, task_id).await, None);
    let status: Option<String> =
        sqlx::query_scalar("SELECT status::text FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .fetch_optional(&pool)
            .await
            .expect("read the history row");
    assert_eq!(status.as_deref(), Some("EXPIRED"));
    remove_task(&pool, task_id).await;
}

/// Python `test_shared_lifecycle.py::TestSetRunning::test_a_node_that_is_not_linked_fails_the_workflow_check`:
/// the start of a workflow task whose node is not linked to it, with the
/// orphan self-heal off, writes nothing and stops with `NODE_NOT_LINKED`
/// (Python's `WORKFLOW_CHECK_FAILED`). With the self-heal on, the stop
/// before start cancels the orphan (`start_node_tests`).
#[tokio::test]
#[serial]
async fn a_workflow_task_whose_node_is_not_linked_stops_and_writes_nothing() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    make_workflow_task(&pool, task_id).await;
    let (context, _peer) = context(pool.clone());
    let before = live_row(&pool, task_id).await;

    let started = start_attempt(&context, &workflow_facts(task_id, generation)).await;

    assert!(matches!(
        started,
        Started::Stopped(StopReason::NodeNotLinked)
    ));
    assert_eq!(live_row(&pool, task_id).await, before);
    remove_task(&pool, task_id).await;
}

/// Python `TestSetRunning::test_a_pool_error_is_ownership_unconfirmed`: a
/// start whose connection cannot be had ends `OWNERSHIP_UNCONFIRMED` before
/// any task code, within a bound.
#[tokio::test]
async fn a_start_that_cannot_reach_the_database_is_ownership_unconfirmed() {
    let (context, _peer) = unreachable_context();
    let run = facts(Uuid::new_v4(), Utc::now());

    let started = tokio::time::timeout(UNREACHABLE_BOUND, start_attempt(&context, &run))
        .await
        .expect("the start ends although the database cannot be reached");

    assert!(matches!(
        started,
        Started::Stopped(StopReason::OwnershipUnconfirmed)
    ));
}

/// Python `TestMissedStart::test_a_claim_that_is_not_held_or_expired_is_lost_with_no_write`,
/// the `LEASE_EXPIRED` case (`NOT_HELD`:
/// `a_start_of_another_claim_generation_is_a_lost_claim`): a start after the
/// claim lease ended writes nothing and stops with `CLAIM_LOST`.
#[tokio::test]
#[serial]
async fn a_start_after_the_claim_lease_ended_is_a_lost_claim_with_no_write() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    sqlx::query(
        "UPDATE horsies_tasks SET claim_expires_at = NOW() - interval '1 second'
         WHERE id = $1",
    )
    .bind(task_id)
    .execute(&pool)
    .await
    .expect("end the claim lease");
    let (context, _peer) = context(pool.clone());
    let before = live_row(&pool, task_id).await;

    let started = start_attempt(&context, &facts(task_id, generation)).await;

    assert!(matches!(started, Started::Stopped(StopReason::ClaimLost)));
    assert_eq!(live_row(&pool, task_id).await, before);
    remove_task(&pool, task_id).await;
}

/// Python `TestMissedStart::test_a_refused_expiry_is_a_lost_claim`: the start
/// finds the deadline passed, and the claim moves to another worker before
/// the expiry runs, so the expiry is refused and the run stops with
/// `CLAIM_LOST`. The test holds the advisory lock of the task, which the
/// expiry takes first and the start does not take, and moves the claim while
/// the expiry waits for it.
#[tokio::test]
#[serial]
async fn a_refused_expiry_before_the_start_is_a_lost_claim() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, true).await;
    let (context, _peer) = context(pool.clone());
    let run = facts(task_id, generation);
    let mut lock_holder = pool.begin().await.expect("begin the lock holder");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 731))")
        .bind(task_id)
        .execute(&mut *lock_holder)
        .await
        .expect("take the advisory lock of the task");
    let driver = async {
        until_a_backend_waits_in(&pool, EXPIRY_FUNCTION).await;
        sqlx::query("UPDATE horsies_tasks SET claimed_by_worker_id = $2 WHERE id = $1")
            .bind(task_id)
            .bind(OTHER_WORKER_ID)
            .execute(&pool)
            .await
            .expect("move the claim to another worker");
        lock_holder.commit().await.expect("end the advisory lock");
    };

    let (started, ()) = tokio::join!(start_attempt(&context, &run), driver);

    assert!(matches!(started, Started::Stopped(StopReason::ClaimLost)));
    let row = live_row(&pool, task_id).await.expect("the row stays live");
    assert_eq!(row["status"], "CLAIMED");
    assert_eq!(row["claimed_by_worker_id"], OTHER_WORKER_ID);
    remove_task(&pool, task_id).await;
}

/// Python `TestMissedStart::test_a_resume_before_the_abandon_starts_the_task_on_the_next_attempt`:
/// the start finds the workflow paused, and the workflow is resumed before
/// the abandon: the abandon is refused, and the next start attempt sets the
/// row and its node RUNNING. The test holds the workflow row with an
/// uncommitted resume, which the abandon waits for and the start does not.
#[tokio::test]
#[serial]
async fn a_resume_before_the_abandon_starts_the_task_on_the_next_attempt() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    make_workflow_task(&pool, task_id).await;
    let workflow_id = link_to_workflow(&pool, task_id, "PAUSED").await;
    let (context, _peer) = context(pool.clone());
    let run = workflow_facts(task_id, generation);
    let mut resume = pool.begin().await.expect("begin the resume");
    set_workflow_status(&mut resume, workflow_id, "RUNNING").await;
    let driver = async {
        until_a_backend_waits_in(&pool, ABANDON_FUNCTION).await;
        resume.commit().await.expect("commit the resume");
    };

    let (started, ()) = tokio::join!(start_attempt(&context, &run), driver);

    assert!(matches!(started, Started::Running));
    let row = live_row(&pool, task_id).await.expect("the row stays live");
    assert_eq!(row["status"], "RUNNING");
    assert_eq!(node_status(&pool, workflow_id).await, "RUNNING");
    remove_task(&pool, task_id).await;
    remove_workflow(&pool, workflow_id).await;
}

/// Python `TestMissedStart::test_a_resume_on_every_attempt_leaves_the_claim_to_its_lease`:
/// a resume before the abandon on each of the two start attempts ends the
/// run `OWNERSHIP_UNCONFIRMED` with nothing written; the worker's outcome
/// writer releases the claim. The child's core pool holds one connection:
/// the test holds it while it pauses the workflow again for the second
/// attempt and opens the second resume.
#[tokio::test]
#[serial]
async fn a_resume_on_every_attempt_leaves_the_claim_to_its_lease() {
    let (context, _peer) = context_on(one_connection_child_broker().await);
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    make_workflow_task(&pool, task_id).await;
    let workflow_id = link_to_workflow(&pool, task_id, "PAUSED").await;
    let (pools, _in_flight) = context
        .broker
        .core_pools_for_write()
        .expect("the core pools of the child");
    let run = workflow_facts(task_id, generation);
    let mut first_resume = pool.begin().await.expect("begin the first resume");
    set_workflow_status(&mut first_resume, workflow_id, "RUNNING").await;
    let driver = async {
        until_a_backend_waits_in(&pool, ABANDON_FUNCTION).await;
        let held = pools
            .main()
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("hold the connection of the child's core pool");
        first_resume
            .commit()
            .await
            .expect("commit the first resume");
        until_a_checkout_waits(pools.main()).await;
        let mut pause = pool.acquire().await.expect("acquire a connection");
        set_workflow_status(&mut pause, workflow_id, "PAUSED").await;
        drop(pause);
        let mut second_resume = pool.begin().await.expect("begin the second resume");
        set_workflow_status(&mut second_resume, workflow_id, "RUNNING").await;
        drop(held);
        until_a_backend_waits_in(&pool, ABANDON_FUNCTION).await;
        second_resume
            .commit()
            .await
            .expect("commit the second resume");
    };

    let (started, ()) = tokio::join!(start_attempt(&context, &run), driver);

    assert!(matches!(
        started,
        Started::Stopped(StopReason::OwnershipUnconfirmed)
    ));
    let row = live_row(&pool, task_id).await.expect("the row stays live");
    assert_eq!(row["status"], "CLAIMED");
    assert_eq!(row["claimed_by_worker_id"], WORKER_ID);
    assert_eq!(node_status(&pool, workflow_id).await, "ENQUEUED");
    remove_task(&pool, task_id).await;
    remove_workflow(&pool, workflow_id).await;
}

/// Python `TestFinalizingMarker::test_a_failed_marker_is_logged_and_does_not_raise`:
/// a marker that cannot reach the database ends the call, and the run goes
/// on to its result frame.
#[tokio::test]
async fn a_marker_that_cannot_reach_the_database_lets_the_run_go_on() {
    let (context, _peer) = unreachable_context();
    let run = facts(Uuid::new_v4(), Utc::now());

    tokio::time::timeout(UNREACHABLE_BOUND, mark_finalizing(&context, &run))
        .await
        .expect("the marker ends although the database cannot be reached");
}

/// Python `test_task_row_statements.py::test_the_start_names_the_claim_generation_and_this_process`
/// (the claim generation: `a_start_of_another_claim_generation_is_a_lost_claim`;
/// the pid and process name:
/// `a_held_claim_starts_running_with_the_process_of_its_child`): the first
/// runner beat names the sender `<worker>:<pid>`, the host and the pid of
/// the child.
#[tokio::test]
#[serial]
async fn the_first_runner_beat_names_the_worker_and_the_process_of_the_child() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let (context, _peer) = context(pool.clone());

    let started = start_attempt(&context, &facts(task_id, generation)).await;

    assert!(matches!(started, Started::Running));
    let beats: Vec<(String, String, String, i32)> = sqlx::query_as(
        "SELECT role, sender_id, hostname, pid FROM horsies_heartbeats WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_all(&pool)
    .await
    .expect("read the beats");
    let [(role, sender_id, hostname, pid)] = beats.as_slice() else {
        panic!("one beat, got {beats:?}");
    };
    assert_eq!(role, "runner");
    assert_eq!(sender_id, &format!("{WORKER_ID}:{PID}"));
    assert_eq!(hostname, "start-host");
    assert_eq!(*pid, PID);
    remove_task(&pool, task_id).await;
}

/// Python `test_worker_child_runner.py::TestExpireClaimedBeforeStart::test_applied_or_replayed_expiry_commits_and_returns_expired`:
/// the expiry before the start records `TASK_EXPIRED` as the error code and
/// in the stored result, and its replay also reads as expired.
#[tokio::test]
#[serial]
async fn the_expiry_before_the_start_records_task_expired_and_a_replay_reads_as_expired() {
    let pool = migrated_pool().await;
    let (task_id, _generation) = seed_claimed(&pool, true).await;
    let broker = PostgresBroker::from_pool(pool.clone());

    let applied = broker
        .expire_claimed_task_before_start(task_id, WORKER_ID, WriteDeadline::start_attempt())
        .await
        .expect("the expiry");
    let replayed = broker
        .expire_claimed_task_before_start(task_id, WORKER_ID, WriteDeadline::start_attempt())
        .await
        .expect("the replay of the expiry");

    assert_eq!(applied, PrestartExpiry::Expired);
    assert_eq!(replayed, PrestartExpiry::Expired);
    let (error_code, result): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT error_code, convert_from(result_payload, 'UTF8')
         FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .expect("read the history row");
    assert_eq!(error_code.as_deref(), Some("TASK_EXPIRED"));
    let result = result.unwrap_or_default();
    assert!(result.contains("TASK_EXPIRED"), "{result}");
    remove_task(&pool, task_id).await;
}

/// Python `TestExpireClaimedBeforeStart::test_refusal_or_absence_falls_through_to_existing_classification`:
/// an expiry refused for a claim of another worker (`LOST_CLAIM`) or for a
/// deadline that has not passed (`SOURCE_STATE_CONFLICT`), and the expiry of
/// an absent task (`TASK_ABSENT`), are not expiries and write nothing.
#[tokio::test]
#[serial]
async fn a_refused_or_absent_expiry_is_not_an_expiry() {
    let pool = migrated_pool().await;
    let broker = PostgresBroker::from_pool(pool.clone());
    let (of_another_worker, _) = seed_claimed(&pool, true).await;
    sqlx::query("UPDATE horsies_tasks SET claimed_by_worker_id = $2 WHERE id = $1")
        .bind(of_another_worker)
        .bind(OTHER_WORKER_ID)
        .execute(&pool)
        .await
        .expect("move the claim to another worker");
    let (not_due, _) = seed_claimed(&pool, false).await;
    let before = [
        live_row(&pool, of_another_worker).await,
        live_row(&pool, not_due).await,
    ];

    for (task_id, case) in [
        (of_another_worker, "a claim of another worker"),
        (not_due, "a deadline that has not passed"),
        (Uuid::new_v4(), "an absent task"),
    ] {
        let expiry = broker
            .expire_claimed_task_before_start(task_id, WORKER_ID, WriteDeadline::start_attempt())
            .await
            .expect(case);
        assert_eq!(expiry, PrestartExpiry::NotExpired, "{case}");
    }

    let after = [
        live_row(&pool, of_another_worker).await,
        live_row(&pool, not_due).await,
    ];
    assert_eq!(after, before);
    remove_task(&pool, of_another_worker).await;
    remove_task(&pool, not_due).await;
}

/// Python `TestSetRunning::test_a_transition_that_hangs_ends_at_its_bound`
/// and `TestRefusalBound::*`: a start that waits on a row lock gets its
/// cancel request at the statement bound and ends `OWNERSHIP_UNCONFIRMED`
/// within the bound of one attempt (the release after the failed start
/// waits on the same lock, in the time left of the attempt), and its write
/// does not commit after the lock ends.
#[tokio::test]
#[serial]
async fn a_start_that_waits_on_a_row_lock_ends_at_its_bound() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let (context, _peer) = context(pool.clone());
    let run = facts(task_id, generation);
    let mut lock_holder = pool.begin().await.expect("begin the lock holder");
    lock_the_row(&mut lock_holder, task_id).await;
    let began = std::time::Instant::now();

    let started = tokio::time::timeout(START_ATTEMPT_END, start_attempt(&context, &run))
        .await
        .expect("the start ends at its bound while the row lock is held");
    let elapsed = began.elapsed();
    lock_holder.rollback().await.expect("end the row lock");

    assert!(matches!(
        started,
        Started::Stopped(StopReason::OwnershipUnconfirmed)
    ));
    assert!(elapsed >= CHILD_STATEMENT_BOUND, "{elapsed:?}");
    let (status, _finalizing_at) = row_after_lock_waiters(&pool, task_id).await;
    assert_ne!(status, "RUNNING");
    remove_task(&pool, task_id).await;
}

/// Python `TestFinalizingMarker::test_a_marker_that_hangs_ends_at_its_bound`:
/// a marker that waits on a row lock ends at its bound, and it is not
/// written after the lock ends.
#[tokio::test]
#[serial]
async fn a_marker_that_waits_on_a_row_lock_ends_at_its_bound() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let (context, _peer) = context(pool.clone());
    let run = facts(task_id, generation);
    assert!(matches!(
        start_attempt(&context, &run).await,
        Started::Running
    ));
    let mut lock_holder = pool.begin().await.expect("begin the lock holder");
    lock_the_row(&mut lock_holder, task_id).await;
    let began = std::time::Instant::now();

    tokio::time::timeout(FINALIZING_MARKER_END, mark_finalizing(&context, &run))
        .await
        .expect("the marker ends at its bound while the row lock is held");
    let elapsed = began.elapsed();
    lock_holder.rollback().await.expect("end the row lock");
    assert!(elapsed >= FINALIZING_MARKER_BOUND, "{elapsed:?}");

    let (status, finalizing_at) = row_after_lock_waiters(&pool, task_id).await;
    assert_eq!(status, "RUNNING");
    assert_eq!(finalizing_at, None);
    remove_task(&pool, task_id).await;
}
