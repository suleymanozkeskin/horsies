//! The lifecycle record of each attempt on a live worker: the attempt row
//! that the worker writes carries the phase record of migration 1001, with
//! the placement of the attempt on a child. The worker follows the placement
//! whether its telemetry is off (an app with no event handler) or on (an app
//! with event handlers, which start a handler child).
//!
//! The move deletes the attempt rows, so each test reads the record from
//! the attempt archive of the history row (version 2, `[attempts, open]`;
//! each attempt is an array of 21 values, times in epoch microseconds).
//!
//! ```text
//! HORSIES_TEST_DATABASE_URL=postgresql://... cargo test -p horsies-test-worker --test lifecycle_record
//! ```

use std::time::Duration;

use horsies_test_worker::harness::{succeeded, Suite};
use horsies_test_worker::instance::{Instance, Profile};
use horsies_test_worker::tasks::basic;
use serial_test::serial;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for the outcome of a task that a running worker runs.
const RESULT_WAIT: Duration = Duration::from_secs(15);

/// The tasks of each test.
const TASKS: i64 = 3;

/// The phase record of the one attempt that the history row of a task
/// archived, read from the archive with the JSON operators of the database.
#[derive(Debug)]
struct ArchivedPhases {
    archive_version: i16,
    attempts: i32,
    open_is_null: bool,
    phase_record_version: Option<String>,
    recorded_by: Option<String>,
    enqueued_at_us: Option<i64>,
    claimed_at_us: Option<i64>,
    attempt_claims: Option<i32>,
    first_claimed_at_us: Option<i64>,
    placed_at_us: Option<i64>,
    last_seen_at_us: Option<i64>,
}

/// The phase record of the first attempt of `task_id` in its history row.
async fn archived_phases(suite: &Suite, task_id: Uuid) -> ArchivedPhases {
    let client = suite.client().await;
    let row = client
        .query_one(
            "SELECT h.attempt_archive_version,
                    jsonb_array_length(j.archive -> 0),
                    jsonb_typeof(j.archive -> 1) = 'null',
                    j.archive -> 0 -> 0 ->> 12,
                    j.archive -> 0 -> 0 ->> 19,
                    (j.archive -> 0 -> 0 ->> 13)::bigint,
                    (j.archive -> 0 -> 0 ->> 14)::bigint,
                    (j.archive -> 0 -> 0 ->> 15)::integer,
                    (j.archive -> 0 -> 0 ->> 16)::bigint,
                    (j.archive -> 0 -> 0 ->> 17)::bigint,
                    (j.archive -> 0 -> 0 ->> 20)::bigint
             FROM horsies_task_history AS h
             CROSS JOIN LATERAL (
                 SELECT convert_from(h.attempt_snapshot, 'UTF8')::jsonb AS archive
             ) AS j
             WHERE h.task_id = $1",
            &[&task_id],
        )
        .await
        .unwrap_or_else(|error| panic!("read the archive of task {task_id}: {error}"));
    ArchivedPhases {
        archive_version: row.get(0),
        attempts: row.get(1),
        open_is_null: row.get(2),
        phase_record_version: row.get(3),
        recorded_by: row.get(4),
        enqueued_at_us: row.get(5),
        claimed_at_us: row.get(6),
        attempt_claims: row.get(7),
        first_claimed_at_us: row.get(8),
        placed_at_us: row.get(9),
        last_seen_at_us: row.get(10),
    }
}

/// Send [`TASKS`] tasks to a worker of `instance`, wait for each result, and
/// check the phase record of each attempt: version 1, written by the
/// worker, one counted claim whose first claim is the claim that ran it, a
/// due time at or before the claim, and a placement at or after the claim.
async fn each_attempt_records_its_placement(instance: Instance) {
    let suite = Suite::start(BIN, instance).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let simple = suite.function(basic::simple::function);
    let mut task_ids = Vec::new();
    for x in 0..TASKS {
        let handle = simple
            .send(basic::simple::Input { x })
            .await
            .expect("send e2e_simple");
        assert_eq!(succeeded(&handle, RESULT_WAIT).await, x * 2);
        task_ids.push(handle.task_id());
    }

    for task_id in task_ids {
        let phases = archived_phases(&suite, task_id).await;
        assert_eq!(phases.archive_version, 2, "{phases:?}");
        assert_eq!(phases.attempts, 1, "{phases:?}");
        assert!(
            phases.open_is_null,
            "the attempt ended with its row: {phases:?}"
        );
        assert_eq!(phases.phase_record_version.as_deref(), Some("1"));
        assert_eq!(phases.recorded_by.as_deref(), Some("WORKER"));
        assert_eq!(phases.attempt_claims, Some(1), "{phases:?}");
        assert_eq!(phases.first_claimed_at_us, phases.claimed_at_us);
        assert_eq!(phases.last_seen_at_us, None);
        match (
            phases.enqueued_at_us,
            phases.claimed_at_us,
            phases.placed_at_us,
        ) {
            (Some(enqueued_at), Some(claimed_at), Some(placed_at)) => {
                assert!(enqueued_at <= claimed_at, "{phases:?}");
                assert!(placed_at >= claimed_at, "{phases:?}");
            }
            _ => panic!("a due time, a claim and a placement: {phases:?}"),
        }
    }
}

/// With telemetry off, every attempt row of the worker keeps its placement.
#[tokio::test]
#[serial]
async fn test_each_attempt_records_its_placement_with_telemetry_off() {
    each_attempt_records_its_placement(Instance::Default).await;
}

/// With telemetry on, every attempt row of the worker keeps its placement,
/// as with telemetry off.
#[tokio::test]
#[serial]
async fn test_each_attempt_records_its_placement_with_telemetry_on() {
    each_attempt_records_its_placement(Instance::Handlers).await;
}
