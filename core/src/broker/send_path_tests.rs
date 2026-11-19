//! The send path against PostgreSQL: the task options check at enqueue, a key
//! that another command holds, a failed enqueue statement, a pooled
//! connection that the server closed, and the session settings of the
//! broker's connections.
//!
//! Python specification (baseline versions on `program/0.8.0`):
//! `clients/python/tests/integration/test_enqueue_retry_limit.py`,
//! `test_send_idempotency_conflict.py`,
//! `test_statement_error_hides_parameters.py` and
//! `test_sync_send_caller_thread.py`.
//!
//! Every test sends under a task name of its own, so the rows of one test
//! never meet the counts of another on the shared database.

use std::time::Duration;

use chrono::Utc;
use serial_test::serial;
use uuid::Uuid;

use crate::broker::{BrokerError, PostgresBroker, SendKind};
use crate::core::config::{EnqueueRetention, PostgresConfig};
use crate::core::history::identity::uuid7::mint_task_id;
use crate::core::task::send_types::{
    send_error_from_enqueue, TaskSendError, TaskSendErrorCode, TaskSendPayload,
};
use crate::test_support::sql::{self, TestPool};
use crate::test_support::{migrated_database_url, migrated_pool};

/// The upper bound of `retry_policy.max_retries` (Python `MAX_RETRY_ATTEMPTS`).
const MAX_RETRY_ATTEMPTS: i32 = 20;

/// A value that must never show in an error text.
const ARGUMENT_MARKER: &str = "bound-value-marker-5d1e";

/// Polls of the wait for a backend state, and the pause between two polls:
/// a bound of 5 s.
const STATE_POLL_LIMIT: u32 = 200;
const STATE_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// A task name that no other test sends under.
fn unique_task_name(label: &str) -> String {
    format!("send_path_{label}_{}", Uuid::new_v4().simple())
}

/// The URL with one more query parameter.
fn with_query_parameter(url: &str, parameter: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}{parameter}")
}

/// A send of `input` under `task_name` with a fresh task id, as the send
/// path builds it: no key, no deadline, no delay, kept forever.
fn plain_payload(task_name: &str, input: &str, task_options: Option<&str>) -> TaskSendPayload {
    TaskSendPayload {
        task_name: task_name.to_owned(),
        queue_name: "default".to_owned(),
        priority: 50,
        input_json: input.to_owned(),
        sent_at: Utc::now(),
        good_until: None,
        enqueue_delay_seconds: None,
        task_options: task_options.map(str::to_owned),
        idempotency_key: None,
        retention: EnqueueRetention::Forever,
    }
}

/// Enqueue `payload` under `task_id`, as a send and a resend of the send
/// path do.
async fn enqueue_payload(
    broker: &PostgresBroker,
    task_id: Uuid,
    payload: &TaskSendPayload,
) -> Result<Uuid, BrokerError> {
    broker
        .enqueue(
            &payload.task_name,
            &payload.input_json,
            &payload.queue_name,
            payload.priority,
            Some(payload.sent_at),
            None,
            payload.good_until,
            payload.task_options.as_deref(),
            Some(task_id),
            payload.enqueue_delay_seconds,
            payload.idempotency_key.as_deref(),
            payload.retention,
            None,
            SendKind::First,
        )
        .await
}

fn new_task_id() -> Uuid {
    mint_task_id().expect("mint a task id")
}

async fn rows_of_task_name(pool: &TestPool, task_name: &str) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name = $1")
        .bind(task_name)
        .fetch_one(pool)
        .await
        .expect("count the task rows")
}

async fn rows_of_task_id(pool: &TestPool, task_id: Uuid) -> i64 {
    sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count the task row")
}

async fn stored_max_retries(pool: &TestPool, task_id: Uuid) -> i32 {
    sql::query_scalar("SELECT max_retries FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the stored retry limit")
}

async fn remove_task_name(pool: &TestPool, task_name: &str) {
    sql::query(
        "DELETE FROM horsies_key_reservations
         WHERE task_id IN (SELECT id FROM horsies_tasks WHERE task_name = $1)",
    )
    .bind(task_name)
    .execute(pool)
    .await
    .expect("remove the key reservations of the test");
    sql::query("DELETE FROM horsies_tasks WHERE task_name = $1")
        .bind(task_name)
        .execute(pool)
        .await
        .expect("remove the task rows of the test");
}

/// Task options with a valid exponential policy whose `max_retries` is
/// `max_retries` (any JSON value), Python's `_options(max_retries=...)`.
fn options_with_max_retries(max_retries: &str) -> String {
    policy_options(&format!(
        "\"max_retries\": {max_retries}, \"intervals\": [5], \
         \"backoff_strategy\": \"exponential\", \"jitter\": true, \
         \"auto_retry_for\": [\"TRANSIENT\"]"
    ))
}

/// Task options whose `retry_policy` object has exactly `fields`.
fn policy_options(fields: &str) -> String {
    format!("{{\"retry_policy\": {{{fields}}}}}")
}

/// The policies that Python refuses with `INVALID_TASK_OPTIONS`, with the
/// ids of its parametrization.
fn malformed_policies() -> Vec<(&'static str, String)> {
    let valid_rest = "\"intervals\": [5], \"backoff_strategy\": \"exponential\", \
                      \"jitter\": true, \"auto_retry_for\": [\"TRANSIENT\"]";
    vec![
        ("negative", options_with_max_retries("-1")),
        (
            "above_bound",
            options_with_max_retries(&(MAX_RETRY_ATTEMPTS + 1).to_string()),
        ),
        ("boolean", options_with_max_retries("true")),
        ("string", options_with_max_retries("\"3\"")),
        (
            "exponential_with_two_intervals",
            policy_options(
                "\"max_retries\": 3, \"intervals\": [5, 10], \
                 \"backoff_strategy\": \"exponential\", \"jitter\": true, \
                 \"auto_retry_for\": [\"TRANSIENT\"]",
            ),
        ),
        (
            "no_retry_codes",
            policy_options(
                "\"max_retries\": 3, \"intervals\": [5], \
                 \"backoff_strategy\": \"exponential\", \"jitter\": true, \
                 \"auto_retry_for\": []",
            ),
        ),
        (
            "unknown_field",
            policy_options(&format!(
                "\"max_retries\": 3, {valid_rest}, \"surprise\": 1"
            )),
        ),
        (
            "partial_policy",
            "{\"retry_policy\": {\"max_retries\": 3}}".to_owned(),
        ),
        ("not_an_object", "{\"retry_policy\": [3]}".to_owned()),
    ]
}

/// Python `test_enqueue_retry_limit.py::test_a_malformed_policy_is_refused_without_a_row`:
/// the enqueue refuses a retry policy that does not fit Python's
/// `RetryPolicy` before any statement. The refusal is not retryable, the send
/// path reports it as `VALIDATION_FAILED` (Python's `INVALID_TASK_OPTIONS`),
/// and no row is written.
#[tokio::test]
#[serial]
async fn a_malformed_retry_policy_is_refused_before_any_row() {
    let pool = migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task_name = unique_task_name("malformed_policy");

    for (case, task_options) in malformed_policies() {
        let payload = plain_payload(&task_name, "{}", Some(&task_options));
        let task_id = new_task_id();

        let refused = enqueue_payload(&broker, task_id, &payload)
            .await
            .expect_err(case);

        assert!(!refused.is_retryable(), "{case}: {refused}");
        let send_error = send_error_from_enqueue(task_id, payload, &refused);
        assert_eq!(
            send_error.code(),
            TaskSendErrorCode::ValidationFailed,
            "{case}: {refused}"
        );
        assert!(send_error.resend_target().is_none(), "{case}");
    }

    assert_eq!(rows_of_task_name(&pool, &task_name).await, 0);
}

/// Python `test_enqueue_retry_limit.py::test_valid_policies_store_their_limits`,
/// the policies with a limit: the row stores the limit as sent, up to the
/// bound of 20.
#[tokio::test]
#[serial]
async fn valid_retry_policies_store_their_limits() {
    let pool = migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task_name = unique_task_name("valid_policy");

    for limit in [1, MAX_RETRY_ATTEMPTS] {
        let task_options = options_with_max_retries(&limit.to_string());
        let payload = plain_payload(&task_name, "{}", Some(&task_options));
        let task_id = enqueue_payload(&broker, new_task_id(), &payload)
            .await
            .expect("a valid policy is enqueued");

        assert_eq!(stored_max_retries(&pool, task_id).await, limit);
    }

    remove_task_name(&pool, &task_name).await;
}

/// Python `test_enqueue_retry_limit.py::test_valid_policies_store_their_limits`,
/// the `null` policy: a task with no policy has no retry, so the row stores
/// 0.
#[tokio::test]
#[serial]
async fn a_null_retry_policy_stores_zero_retries() {
    let pool = migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task_name = unique_task_name("null_policy");
    let payload = plain_payload(&task_name, "{}", Some("{\"retry_policy\": null}"));

    let task_id = enqueue_payload(&broker, new_task_id(), &payload)
        .await
        .expect("a null policy is enqueued");

    assert_eq!(stored_max_retries(&pool, task_id).await, 0);
    remove_task_name(&pool, &task_name).await;
}

/// Python `test_send_idempotency_conflict.py::TestKeyReservedByAnotherTask::
/// test_conflict_names_the_owner_and_retry_is_accepted`: a key that another
/// command holds is `IDEMPOTENCY_KEY_CONFLICT` naming the owner task. The
/// resend methods accept it; a resend while the reservation is active meets
/// the same conflict with the same owner. Nothing more is written.
#[tokio::test]
#[serial]
async fn a_key_held_by_another_command_names_its_owner_and_a_resend_meets_the_same_conflict() {
    let pool = migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task_name = unique_task_name("key_owner");
    let key = format!("conflict-key-{}", Uuid::new_v4());
    let mut first = plain_payload(&task_name, "{\"value\":1}", None);
    first.idempotency_key = Some(key.clone());
    let owner = enqueue_payload(&broker, new_task_id(), &first)
        .await
        .expect("the first keyed send is applied");
    let mut second = plain_payload(&task_name, "{\"value\":2}", None);
    second.idempotency_key = Some(key);
    let second_task_id = new_task_id();

    let conflict = enqueue_payload(&broker, second_task_id, &second)
        .await
        .expect_err("another command under the same key conflicts");

    match &conflict {
        BrokerError::IdempotencyKeyConflict { task_id, .. } => assert_eq!(*task_id, owner),
        other => panic!("expected a key conflict, got {other:?}"),
    }
    let send_error = send_error_from_enqueue(second_task_id, second, &conflict);
    assert_eq!(send_error.code(), TaskSendErrorCode::IdempotencyKeyConflict);
    match &send_error {
        TaskSendError::IdempotencyKeyConflict {
            task_id,
            owner_task_id,
            ..
        } => {
            assert_eq!(*task_id, second_task_id);
            assert_eq!(*owner_task_id, owner);
        }
        other => panic!("expected a key conflict send error, got {other:?}"),
    }
    let resend = send_error
        .resend_target()
        .expect("the resend methods accept a key conflict");

    let resent = enqueue_payload(&broker, resend.task_id, resend.payload)
        .await
        .expect_err("the reservation is still active");

    match resent {
        BrokerError::IdempotencyKeyConflict { task_id, .. } => assert_eq!(task_id, owner),
        other => panic!("expected the same key conflict, got {other:?}"),
    }
    assert_eq!(rows_of_task_name(&pool, &task_name).await, 1);
    remove_task_name(&pool, &task_name).await;
}

/// Cancel the one backend that waits for a lock on `horsies_tasks`, and
/// return its pid. Bounded by [`STATE_POLL_LIMIT`] polls.
async fn cancel_the_insert_that_waits_for_the_table(pool: &TestPool) -> i32 {
    for _ in 0..STATE_POLL_LIMIT {
        let waiting: Option<i32> = sql::query_scalar(
            "SELECT pid FROM pg_locks
             WHERE relation = 'horsies_tasks'::regclass AND NOT granted
             LIMIT 1",
        )
        .fetch_optional(pool)
        .await
        .expect("read the lock waits");
        if let Some(pid) = waiting {
            let cancelled: bool = sql::query_scalar("SELECT pg_cancel_backend($1)")
                .bind(pid)
                .fetch_one(pool)
                .await
                .expect("cancel the waiting statement");
            assert!(cancelled, "backend {pid} was not cancelled");
            return pid;
        }
        tokio::time::sleep(STATE_POLL_INTERVAL).await;
    }
    panic!("no statement waited for the task table lock within the poll bound");
}

/// Python `test_statement_error_hides_parameters.py::test_failed_enqueue_hides_the_task_arguments`:
/// an enqueue statement that the server cancels (Python: a lock timeout;
/// here a cancel of the statement that waits for the same table lock) fails
/// as a retryable `ENQUEUE_FAILED` whose message names the cause, and
/// neither the message nor the error text holds a bound input value.
#[tokio::test]
#[serial]
async fn a_failed_enqueue_statement_names_its_cause_and_holds_no_input_value() {
    let pool = migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task_name = unique_task_name("hidden_arguments");
    let input = format!("{{\"secret\":\"{ARGUMENT_MARKER}\"}}");
    let payload = plain_payload(&task_name, &input, None);
    let task_id = new_task_id();
    let mut lock_holder = pool.begin().await.expect("open the lock holder");
    sql::query("LOCK TABLE horsies_tasks IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock_holder)
        .await
        .expect("lock the task table");

    let (sent, _cancelled_pid) = tokio::join!(
        enqueue_payload(&broker, task_id, &payload),
        cancel_the_insert_that_waits_for_the_table(&pool),
    );
    lock_holder
        .rollback()
        .await
        .expect("release the task table lock");

    let failure = sent.expect_err("the cancelled statement fails the enqueue");
    assert!(failure.is_retryable(), "{failure}");
    assert!(
        !format!("{failure:?}").contains(ARGUMENT_MARKER),
        "the error holds a bound value: {failure:?}"
    );
    let send_error = send_error_from_enqueue(task_id, payload, &failure);
    match &send_error {
        TaskSendError::EnqueueFailed {
            retryable, message, ..
        } => {
            assert!(*retryable);
            assert!(
                message.contains("canceling statement"),
                "the message names no cause: {message}"
            );
            assert!(!message.contains(ARGUMENT_MARKER), "{message}");
        }
        other => panic!("expected ENQUEUE_FAILED, got {other:?}"),
    }
    assert!(!send_error.to_string().contains(ARGUMENT_MARKER));
    assert_eq!(rows_of_task_id(&pool, task_id).await, 0);
}

/// Terminate every backend of `application_name` and wait until none is
/// left. Bounded by [`STATE_POLL_LIMIT`] polls.
async fn terminate_application(pool: &TestPool, application_name: &str) {
    let terminated: i64 = sql::query_scalar(
        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
         WHERE application_name = $1",
    )
    .bind(application_name)
    .fetch_one(pool)
    .await
    .expect("terminate the broker's backends");
    assert!(
        terminated > 0,
        "the broker holds no connection to terminate"
    );
    for _ in 0..STATE_POLL_LIMIT {
        let left: i64 =
            sql::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1")
                .bind(application_name)
                .fetch_one(pool)
                .await
                .expect("count the broker's backends");
        if left == 0 {
            return;
        }
        tokio::time::sleep(STATE_POLL_INTERVAL).await;
    }
    panic!("the terminated backends did not end within the poll bound");
}

/// Python `test_sync_send_caller_thread.py::
/// test_a_dead_connection_fails_its_send_as_retryable_then_the_next_send_reconnects`:
/// with `pool_pre_ping` off, nothing checks a pooled connection at
/// checkout. A send on a connection that the server closed fails as a
/// retryable `ENQUEUE_FAILED` and writes nothing; the next send writes its
/// row on a new connection.
///
/// A driver that sees the close before the checkout replaces the connection
/// and the first send writes its row; that answer is also accepted. A send
/// never fails as not retryable, and the second send always writes.
#[tokio::test]
#[serial]
async fn a_send_on_a_closed_pooled_connection_is_retryable_and_the_next_send_writes() {
    let pool = migrated_pool().await;
    let url = migrated_database_url().await;
    let application_name = format!("dead_send_{}", Uuid::new_v4().simple());
    let mut config = PostgresConfig::from_url(with_query_parameter(
        &url,
        &format!("application_name={application_name}"),
    ));
    config.pool_pre_ping = false;
    config.pool_size = 1;
    config.max_overflow = 0;
    let broker = PostgresBroker::connect_with(&config, crate::ClientFamily::Rust)
        .await
        .expect("connect the broker");
    let task_name = unique_task_name("dead_connection");
    let _first = enqueue_payload(
        &broker,
        new_task_id(),
        &plain_payload(&task_name, "{\"value\":5}", None),
    )
    .await
    .expect("the first send opens the connection");
    terminate_application(&pool, &application_name).await;

    let after_close_id = new_task_id();
    let after_close_payload = plain_payload(&task_name, "{\"value\":6}", None);
    let after_close = enqueue_payload(&broker, after_close_id, &after_close_payload).await;
    let next_id = enqueue_payload(
        &broker,
        new_task_id(),
        &plain_payload(&task_name, "{\"value\":7}", None),
    )
    .await
    .expect("the next send reconnects");

    match after_close {
        Ok(task_id) => assert_eq!(rows_of_task_id(&pool, task_id).await, 1),
        Err(failure) => {
            assert!(failure.is_retryable(), "{failure}");
            let send_error = send_error_from_enqueue(after_close_id, after_close_payload, &failure);
            assert_eq!(send_error.code(), TaskSendErrorCode::EnqueueFailed);
            assert!(send_error.retryable());
            assert_eq!(rows_of_task_id(&pool, after_close_id).await, 0);
        }
    }
    assert_eq!(rows_of_task_id(&pool, next_id).await, 1);
    broker.close().await;
    remove_task_name(&pool, &task_name).await;
}

/// Python `test_broker_postgres.py::TestEnqueueAcceptedInput::test_the_row_stores_the_kwargs_text_as_its_input`
/// (earlier `test_envelope_bytes_do_not_change`): the row stores the sent
/// input text byte for byte, with its spacing and key order, and the rerun
/// input that the move to history builds reads that text. Each text is a
/// value the send accepts: an object, `null` and a list.
#[tokio::test]
#[serial]
async fn the_row_stores_the_sent_input_text_byte_for_byte() {
    let pool = migrated_pool().await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task_name = unique_task_name("accepted_input");

    for input in ["{}", "null", "{ \"z\": 1, \"a\": 2 }", "[ ]"] {
        let task_id = broker
            .enqueue(
                &task_name,
                input,
                "default",
                50,
                Some(Utc::now()),
                None,
                None,
                None,
                Some(new_task_id()),
                None,
                None,
                EnqueueRetention::Forever,
                Some(true),
                SendKind::First,
            )
            .await
            .expect("the send writes its row");

        let stored: String = sql::query_scalar("SELECT input FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read the stored input");
        assert_eq!(stored, input);
    }
    remove_task_name(&pool, &task_name).await;
}
