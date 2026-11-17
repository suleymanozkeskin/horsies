//! The refusals of the terminalization functions that the database decides
//! before it changes a row, on the installed schema.
//!
//! Each case has a database of its own, a copy of the installed schema
//! ([`TestDatabase::create`]), and seeds one task row. The function call
//! fails with its error, and the task row keeps its status.
//!
//! The other tests of `tests/integration/test_terminalization_functions.py`
//! stay in Python: they read each outcome through the Python lifecycle
//! (`decode_outcome_row`, `apply_async`, `apply_batch_async`, `apply_sync`,
//! `call_for`, `batch_call_for`).

use chrono::{DateTime, TimeZone, Utc};
use tokio_postgres::error::SqlState;
use uuid::Uuid;

use super::harness::{describe, Session};
use crate::test_support::TestDatabase;

const WORKER: &str = "terminalization-test-worker";

/// The retention class that every enqueue stamps unless the caller says
/// otherwise.
const DEFAULT_RETENTION_CLASS_KEY: &str = "standard_30d";

/// Every baseline column is named. A sent task carries a command
/// fingerprint; a workflow task carries none. A CLAIMED row carries a lease.
/// A row with a worker carries the worker identity that the start write
/// records. Parameters: `$1` id, `$2` retention class, `$3` workflow task,
/// `$4` status, `$5` claimed_at, `$6` worker id.
const SEED_SQL: &str = "
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution,
        retention_class_key, sent_at, created_at,
        command_fingerprint_version, command_fingerprint,
        idempotency_key_digest, status, enqueued_at, claimed_at,
        claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
        finalizing_by_worker_id, worker_pid, worker_hostname,
        worker_process_name, retry_count, max_retries, next_retry_at,
        good_until, is_workflow_task, updated_at, retain_rerun_input,
        rerun_of_task_id, rerun_root_task_id
    )
    VALUES (
        CAST($1 AS uuid), 'terminalization.test', 'default', 100, '{}', NULL,
        'ISOLATED',
        $2, NOW(), NOW(),
        CASE WHEN $3 THEN NULL ELSE 1 END,
        CASE
            WHEN $3 THEN NULL
            ELSE sha256(convert_to(CAST(CAST($1 AS uuid) AS text), 'UTF8'))
        END,
        NULL, CAST($4 AS text), NOW(), $5,
        CAST($6 AS text),
        CASE
            WHEN CAST($4 AS text) = 'CLAIMED'
            THEN NOW() + INTERVAL '5 minutes'
        END,
        NOW(), NULL,
        NULL,
        CASE WHEN CAST($6 AS text) IS NOT NULL THEN 4242 END,
        CASE
            WHEN CAST($6 AS text) IS NOT NULL
            THEN 'terminalization-host'
        END,
        CASE
            WHEN CAST($6 AS text) IS NOT NULL
            THEN 'terminalization-proc'
        END,
        0, 0, NULL,
        NULL, $3, NOW(), FALSE,
        NULL, NULL
    )
";

/// Sets the row's good_until `$2` seconds before now. Parameter `$1` is the
/// task id.
const DEADLINE_SQL: &str = "
    UPDATE horsies_tasks
    SET good_until = CASE
            WHEN CAST($2 AS INTEGER) IS NULL THEN NULL
            ELSE NOW() - make_interval(secs => $2)
        END
    WHERE id = $1
";

/// The status of a task on whichever side holds it: the live row or its
/// history row.
const STATUS_SQL: &str = "
    SELECT status FROM horsies_tasks WHERE id = $1
    UNION ALL
    SELECT status FROM horsies_task_history WHERE task_id = $1
";

/// The batch sizes that the batch functions refuse: no bound, zero, and a
/// negative bound.
const INVALID_BATCH_SIZES: [&str; 3] = ["NULL", "0", "-1"];

/// The claim generation of the seeded rows.
fn generation() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 4, 9, 0, 0)
        .single()
        .expect("a fixed UTC time")
}

/// One task row to seed.
struct Seed {
    status: &'static str,
    worker_id: Option<&'static str>,
    claimed_at: Option<DateTime<Utc>>,
    is_workflow_task: bool,
}

impl Seed {
    /// A row of `status` that [`WORKER`] claimed at [`generation`].
    fn claimed_by_this_worker(status: &'static str, is_workflow_task: bool) -> Seed {
        Seed {
            status,
            worker_id: Some(WORKER),
            claimed_at: Some(generation()),
            is_workflow_task,
        }
    }
}

/// A copy of the installed schema and an autocommit session on it.
async fn installed() -> (TestDatabase, Session) {
    let database = TestDatabase::create().await;
    let session = Session::open(database.url()).await;
    (database, session)
}

/// Inserts the task row of `task` and returns its id.
async fn seed(session: &Session, task: &Seed) -> Uuid {
    let task_id = Uuid::new_v4();
    let _rows = session
        .client()
        .execute(
            SEED_SQL,
            &[
                &task_id,
                &DEFAULT_RETENTION_CLASS_KEY,
                &task.is_workflow_task,
                &task.status,
                &task.claimed_at,
                &task.worker_id,
            ],
        )
        .await
        .unwrap_or_else(|error| panic!("seed a task: {}", describe(&error)));
    task_id
}

/// The status of `task_id`; it is on exactly one side.
async fn status_of(session: &Session, task_id: Uuid) -> String {
    let rows = session
        .client()
        .query(STATUS_SQL, &[&task_id])
        .await
        .unwrap_or_else(|error| panic!("read the status: {}", describe(&error)));
    assert_eq!(rows.len(), 1, "the task is on exactly one side");
    rows[0].get(0)
}

/// Asserts that `outcome` is a server error whose message holds `fragment`.
fn assert_refused<T: std::fmt::Debug>(
    outcome: Result<T, tokio_postgres::Error>,
    fragment: &str,
    case: &str,
) {
    let error = outcome.expect_err(case);
    let database_error = error
        .as_db_error()
        .unwrap_or_else(|| panic!("{case}: not a server error: {error}"));
    assert!(
        database_error.message().contains(fragment),
        "{case}: {}",
        describe(&error)
    );
}

mod live_status_invariant {
    use super::*;

    /// A terminal task leaves the live table; the live row cannot hold a
    /// terminal status (CHECK horsies_tasks_status_live).
    #[tokio::test]
    async fn the_live_table_refuses_a_terminal_status() {
        let (_database, session) = installed().await;
        let task_id = seed(
            &session,
            &Seed {
                status: "PENDING",
                worker_id: None,
                claimed_at: None,
                is_workflow_task: false,
            },
        )
        .await;

        let updated = session
            .client()
            .execute(
                "UPDATE horsies_tasks SET status = 'CANCELLED' WHERE id = $1",
                &[&task_id],
            )
            .await;
        let error = updated.expect_err("the live table refuses CANCELLED");
        let database_error = error.as_db_error().expect("a server error");
        assert_eq!(
            database_error.code(),
            &SqlState::CHECK_VIOLATION,
            "{}",
            describe(&error)
        );
        assert_eq!(
            database_error.constraint(),
            Some("horsies_tasks_status_live")
        );
        assert!(database_error
            .message()
            .contains("horsies_tasks_status_live"));

        let status: String = session
            .client()
            .query_one(
                "SELECT status FROM horsies_tasks WHERE id = $1",
                &[&task_id],
            )
            .await
            .unwrap_or_else(|error| panic!("read the status: {}", describe(&error)))
            .get(0);
        assert_eq!(status, "PENDING");
    }
}

mod expire_pending_tasks {
    use super::*;

    /// LIMIT NULL means no limit at all, so NULL must be an error.
    ///
    /// The bound exists to keep one pass from committing an unbounded
    /// notification burst; a call that disables it is refused before any
    /// row is touched. One case per refused batch size, each on a database
    /// of its own.
    #[tokio::test]
    async fn an_unbounded_or_absurd_batch_mutates_nothing() {
        for batch_size in INVALID_BATCH_SIZES {
            let (_database, session) = installed().await;
            let task_id = seed(&session, &Seed::claimed_by_this_worker("PENDING", false)).await;
            let _rows = session
                .client()
                .execute(DEADLINE_SQL, &[&task_id, &60_i32])
                .await
                .unwrap_or_else(|error| panic!("set the deadline: {}", describe(&error)));

            let expired = session
                .client()
                .batch_execute(&format!(
                    "SELECT * FROM horsies_expire_pending_tasks(
                        CAST({batch_size} AS INTEGER), '{{}}', 'TASK_EXPIRED'
                    )"
                ))
                .await;
            assert_refused(expired, "positive integer", batch_size);

            assert_eq!(
                status_of(&session, task_id).await,
                "PENDING",
                "{batch_size}"
            );
        }
    }
}

mod cancel_orphaned_tasks {
    use super::*;

    /// One case per refused batch size, each on a database of its own.
    #[tokio::test]
    async fn an_invalid_bound_raises_before_mutation() {
        for batch_size in INVALID_BATCH_SIZES {
            let (_database, session) = installed().await;
            let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", true)).await;

            let cancelled = session
                .client()
                .batch_execute(&format!(
                    "SELECT * FROM horsies_cancel_orphaned_tasks(
                        CAST({batch_size} AS INTEGER)
                    )"
                ))
                .await;
            assert_refused(cancelled, "positive integer", batch_size);

            assert_eq!(
                status_of(&session, task_id).await,
                "CLAIMED",
                "{batch_size}"
            );
        }
    }
}

mod id_keyed_workflow_batches {
    use super::*;

    /// The id and generation arrays of each refused call, with the fragment
    /// of its error message. `one` stands for the id of the seeded task.
    const INVALID_ARRAYS: [(&str, &str, &str); 5] = [
        ("NULL", "ARRAY['2026-08-04 09:00:00+00']", "non-NULL"),
        ("ARRAY['one']", "NULL", "non-NULL"),
        (
            "ARRAY['one', '00000000-0000-4000-8000-000000000002']",
            "ARRAY['2026-08-04 09:00:00+00']",
            "lengths differ",
        ),
        (
            "ARRAY[NULL]::uuid[]",
            "ARRAY['2026-08-04 09:00:00+00']",
            "non-NULL",
        ),
        (
            "ARRAY['one', 'one']",
            "ARRAY['2026-08-04 09:00:00+00', '2026-08-04 09:00:01+00']",
            "distinct",
        ),
    ];

    /// Each function with each refused pair of arrays, each on a database of
    /// its own.
    #[tokio::test]
    async fn invalid_arrays_raise_before_mutation() {
        for function_name in ["horsies_abandon_owned_nodes", "horsies_cancel_owned_nodes"] {
            for (ids_sql, generations_sql, message) in INVALID_ARRAYS {
                let case = format!("{function_name} {ids_sql} {generations_sql}");
                let (_database, session) = installed().await;
                let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", true)).await;
                let ids = ids_sql.replace("one", &task_id.to_string());

                let refused = session
                    .client()
                    .query(
                        &format!(
                            "SELECT * FROM {function_name}(
                                CAST({ids} AS uuid[]),
                                CAST({generations_sql} AS TIMESTAMPTZ[]),
                                $1
                            )"
                        ),
                        &[&WORKER],
                    )
                    .await;
                assert_refused(refused, message, &case);

                assert_eq!(status_of(&session, task_id).await, "CLAIMED", "{case}");
            }
        }
    }
}
