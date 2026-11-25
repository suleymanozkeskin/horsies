//! The workflow events of migration 1001
//! (`core/migrations/1001_lifecycle_record.sql`): the table
//! `horsies_workflow_events` and the statements that write it.
//!
//! - `cases/workflow_events_cases.sql` holds the one-session cases: the
//!   CHECK and the foreign keys of the table, the event of each writer (the
//!   two moves and the workflow steps), the statements that write no event,
//!   a pause that rolls back, and the foreign-key cascades. It runs with the
//!   helpers of `cases/baseline_workflow_fixtures.sql`, in one transaction
//!   that is rolled back.
//! - The two-session test holds two workflow rows FOR UPDATE, as a workflow
//!   cancel does before it locks the task rows of its workflow, and moves a
//!   task of each workflow in a second session. A node event refers to its
//!   node only, so the moves and their events take no lock on a workflow
//!   row and do not wait.

use super::harness::{assert_passed_cases, describe, BaselineDatabase, PASSED_PREFIX};

const FIXTURES_SQL: &str = include_str!("cases/baseline_workflow_fixtures.sql");
const CASES_SQL: &str = include_str!("cases/workflow_events_cases.sql");

/// The checks of `workflow_events_cases.sql`: 56 statements. A check added
/// to the file changes it.
const PASSED_CASES: usize = 56;

/// Two workflows of one task node each. The task of the second is past its
/// deadline, so the pending expiry (the batch move) takes it.
const START_TWO_WORKFLOWS_SQL: &str = r#"
    SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000fa01', 'fail', '[{"idx":0}]');
    SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000fa02', 'fail', '[{"idx":0}]');
    UPDATE horsies_tasks SET good_until = now() - interval '1 second'
    WHERE id = wft_task_of('00000000-0000-7000-8000-00000000fa02', 0);
"#;

/// The lock of a workflow cancel before it locks the task rows: the two
/// workflow rows, FOR UPDATE, in an open transaction.
const HOLD_WORKFLOW_ROWS_SQL: &str = "
    BEGIN;
    SELECT 1 FROM horsies_workflows
    WHERE id IN ('00000000-0000-7000-8000-00000000fa01', '00000000-0000-7000-8000-00000000fa02')
    ORDER BY id
    FOR UPDATE;
";

/// A move that waited for a workflow row would fail with 55P03 after this
/// time, and fail the test with the server error.
const MOVE_LOCK_TIMEOUT_SQL: &str = "SET lock_timeout = '2s'";

/// The single move (phase 1 of the task of the first workflow) and the batch
/// move (the pending expiry of the task of the second), each in autocommit.
const MOVE_BOTH_TASKS_SQL: &str = "
    SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000fa01', 0), wft_ok('1'));
    SELECT count(*) FROM horsies_expire_pending_tasks(10, wft_err('TASK_EXPIRED'), 'TASK_EXPIRED');
";

const MOVED_EVENTS_SQL: &str = "
    SELECT count(*) FROM horsies_workflow_events
    WHERE workflow_id IN ('00000000-0000-7000-8000-00000000fa01', '00000000-0000-7000-8000-00000000fa02')
      AND event_kind = 'TASK_MOVED'
";

/// A failed case raises `case failed: <name>`, which fails this test with the
/// case name in the error.
#[tokio::test]
async fn workflow_event_cases_pass() {
    let database = BaselineDatabase::create(&[FIXTURES_SQL]).await;
    let session = database.session().await;
    session
        .run_script_rolled_back("workflow_events_cases.sql", CASES_SQL)
        .await;
    assert_passed_cases(&session.notices_after(PASSED_PREFIX), PASSED_CASES);
}

/// The single move and the batch move of a task of a node write their
/// TASK_MOVED events while another session holds the workflow rows FOR
/// UPDATE: neither waits for the workflow row. With a foreign key from every
/// event to its workflow row, each move would wait for the holder, and a
/// workflow cancel that holds the row and waits for the task row would close
/// a lock cycle.
#[tokio::test]
async fn a_move_and_its_event_take_no_lock_on_the_workflow_row() {
    let database = BaselineDatabase::create(&[FIXTURES_SQL]).await;
    let (holder, mover) = (database.session().await, database.session().await);
    mover
        .run_script("start two workflows", START_TWO_WORKFLOWS_SQL)
        .await;
    holder
        .run_script("hold the workflow rows", HOLD_WORKFLOW_ROWS_SQL)
        .await;

    mover
        .run_script("set the lock timeout", MOVE_LOCK_TIMEOUT_SQL)
        .await;
    mover
        .run_script("move a task of each held workflow", MOVE_BOTH_TASKS_SQL)
        .await;
    let moved: i64 = mover
        .client()
        .query_one(MOVED_EVENTS_SQL, &[])
        .await
        .unwrap_or_else(|error| panic!("read the moved events: {}", describe(&error)))
        .get(0);
    assert_eq!(moved, 2, "one TASK_MOVED event for each move");

    holder.run_script("COMMIT", "COMMIT").await;
}
