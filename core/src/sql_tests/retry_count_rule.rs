//! The retry count rule of the baseline writers, held by a test-only trigger.
//!
//! The rule:
//!
//! - A RUNNING row of `horsies_tasks` goes back to a claimable status
//!   (`PENDING` or `CLAIMED`) only with a higher `retry_count`.
//! - No update lowers `retry_count`.
//!
//! The live statuses are `PENDING`, `CLAIMED` and `RUNNING` only
//! (`horsies_tasks_status_live`), so a RUNNING row leaves RUNNING by an
//! update to a claimable status or by the move to history, which deletes
//! the row.
//!
//! The task code reads the attempt number (`retry_count + 1`) as its fencing
//! token. The token rises for each body that runs only while this rule
//! holds: a writer that puts a RUNNING row back in the queue at the same
//! count lets two bodies run with one token.
//!
//! [`RETRY_COUNT_RULE_SQL`] is a `BEFORE UPDATE` trigger that raises
//! [`RULE_SQLSTATE`] on an update that breaks the rule. The case files of the
//! lifecycle functions and of the workflow functions run with it installed,
//! so a baseline writer that breaks the rule fails its case with the task,
//! the two statuses and the two counts in the error. The trigger is not part
//! of the baseline: it costs nothing at run time.

use tokio_postgres::types::ToSql;
use uuid::Uuid;

use super::baseline_lifecycle_functions::{CASES_SQL as LIFECYCLE_CASES_SQL, INSERT_TASK_SQL};
use super::baseline_workflow_functions::{
    CASES_SQL as WORKFLOW_CASES_SQL, FIXTURES_SQL as WORKFLOW_FIXTURES_SQL,
};
use super::harness::{describe, BaselineDatabase, Session};

/// The SQLSTATE of a refusal of the rule. Its class is not a class of the
/// server, and no baseline function raises it, so no case handler that
/// expects a baseline error takes it.
const RULE_SQLSTATE: &str = "HT001";

/// The setup text: the trigger function and the trigger. Each refusal
/// names the task, the old and the new status, and the old and the new
/// count, as `task <id>: status <old> -> <new>, retry_count <old> -> <new>`.
const RETRY_COUNT_RULE_SQL: &str = r#"
CREATE FUNCTION test_retry_count_rule() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.retry_count < OLD.retry_count THEN
        RAISE EXCEPTION 'retry count rule: task %: status % -> %, retry_count % -> %: an update lowers retry_count',
            OLD.id, OLD.status, NEW.status, OLD.retry_count, NEW.retry_count
            USING ERRCODE = 'HT001';
    END IF;
    IF OLD.status = 'RUNNING' AND NEW.status IN ('PENDING', 'CLAIMED')
       AND NEW.retry_count <= OLD.retry_count THEN
        RAISE EXCEPTION 'retry count rule: task %: status % -> %, retry_count % -> %: a RUNNING row goes back to the queue only with a higher retry_count',
            OLD.id, OLD.status, NEW.status, OLD.retry_count, NEW.retry_count
            USING ERRCODE = 'HT001';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER test_retry_count_rule BEFORE UPDATE ON horsies_tasks
    FOR EACH ROW EXECUTE FUNCTION test_retry_count_rule();
"#;

/// The state of the trigger in the catalog: `O` when it fires in the
/// normal session role.
const RULE_TRIGGER_STATE_SQL: &str = "SELECT tgenabled::text FROM pg_trigger
    WHERE tgrelid = 'horsies_tasks'::regclass AND tgname = 'test_retry_count_rule'";

/// The queue of the tasks of the rule tests.
const RULE_QUEUE: &str = "q_retry_rule";

/// A RUNNING row back to PENDING, at the same count. The columns of the run
/// and of the claim are cleared as a retry writer clears them.
const BACK_TO_PENDING_SQL: &str = "UPDATE horsies_tasks
    SET status = 'PENDING', claimed_at = NULL, claimed_by_worker_id = NULL, started_at = NULL,
        worker_pid = NULL, worker_hostname = NULL, worker_process_name = NULL
    WHERE id = $1::uuid";

/// A RUNNING row back to CLAIMED, at the same count.
const BACK_TO_CLAIMED_SQL: &str = "UPDATE horsies_tasks
    SET status = 'CLAIMED', claim_expires_at = now() + interval '1 minute', started_at = NULL,
        worker_pid = NULL, worker_hostname = NULL, worker_process_name = NULL
    WHERE id = $1::uuid";

/// A RUNNING row back to PENDING with `retry_count + 1`, as a retry writer.
const RETRY_TO_PENDING_SQL: &str = "UPDATE horsies_tasks
    SET status = 'PENDING', retry_count = retry_count + 1, claimed_at = NULL,
        claimed_by_worker_id = NULL, started_at = NULL, worker_pid = NULL, worker_hostname = NULL,
        worker_process_name = NULL
    WHERE id = $1::uuid";

/// Sets the count of task `$1` to `$2`.
const SET_RETRY_COUNT_SQL: &str = "UPDATE horsies_tasks SET retry_count = $2 WHERE id = $1::uuid";

/// The status and the count of task `$1`.
const STATUS_AND_COUNT_SQL: &str =
    "SELECT status::text, retry_count FROM horsies_tasks WHERE id = $1::uuid";

/// The SQLSTATE and the message of a server error.
#[derive(Debug)]
struct Refusal {
    sqlstate: String,
    message: String,
}

/// A database with the baseline, each text of `setup`, and the trigger of
/// the rule, committed.
async fn database_with_the_rule(setup: &[&str]) -> BaselineDatabase {
    let texts: Vec<&str> = setup
        .iter()
        .copied()
        .chain(std::iter::once(RETRY_COUNT_RULE_SQL))
        .collect();
    BaselineDatabase::create(&texts).await
}

/// Asserts that the trigger of the rule fires on `session`'s database, so
/// a case run checks the rule and does not pass with no check.
async fn assert_rule_fires(session: &Session) {
    let state: String = session
        .client()
        .query_one(RULE_TRIGGER_STATE_SQL, &[])
        .await
        .unwrap_or_else(|error| panic!("read the trigger of the rule: {}", describe(&error)))
        .get(0);
    assert_eq!(state, "O", "the trigger of the rule does not fire");
}

/// Inserts one committed task of worker w1 in `status`, at count 0, and
/// returns its id.
async fn insert_task(session: &Session, status: &str) -> Uuid {
    let task_id = Uuid::new_v4();
    let _inserted = session
        .client()
        .execute(INSERT_TASK_SQL, &[&task_id, &RULE_QUEUE, &status])
        .await
        .unwrap_or_else(|error| panic!("insert a {status} task: {}", describe(&error)));
    task_id
}

/// Runs `statement`, which must change one row.
///
/// # Panics
///
/// When the statement fails or changes another number of rows.
async fn update_one(session: &Session, statement: &str, parameters: &[&(dyn ToSql + Sync)]) {
    let changed = session
        .client()
        .execute(statement, parameters)
        .await
        .unwrap_or_else(|error| panic!("{statement}: {}", describe(&error)));
    assert_eq!(changed, 1, "{statement}");
}

/// The server error of `statement`.
///
/// # Panics
///
/// When the statement succeeds, or fails with no server error.
async fn refusal(
    session: &Session,
    statement: &str,
    parameters: &[&(dyn ToSql + Sync)],
) -> Refusal {
    let error = session
        .client()
        .execute(statement, parameters)
        .await
        .err()
        .unwrap_or_else(|| panic!("{statement}: the rule let the update pass"));
    let server_error = error
        .as_db_error()
        .unwrap_or_else(|| panic!("{statement}: no server error: {error}"));
    Refusal {
        sqlstate: server_error.code().code().to_owned(),
        message: server_error.message().to_owned(),
    }
}

/// The status and the count of task `task_id`.
async fn status_and_count(session: &Session, task_id: Uuid) -> (String, i32) {
    let row = session
        .client()
        .query_one(STATUS_AND_COUNT_SQL, &[&task_id])
        .await
        .unwrap_or_else(|error| panic!("read task {task_id}: {}", describe(&error)));
    (row.get(0), row.get(1))
}

/// Every single-session case of the lifecycle functions keeps the rule: the
/// retry writers add 1, and the release writers act on CLAIMED rows only.
#[tokio::test]
async fn lifecycle_cases_keep_the_retry_count_rule() {
    let database = database_with_the_rule(&[]).await;
    let session = database.session().await;
    assert_rule_fires(&session).await;
    session
        .run_script_rolled_back("baseline_lifecycle_functions.sql", LIFECYCLE_CASES_SQL)
        .await;
}

/// Every single-session case of the workflow functions keeps the rule: a
/// pause, a cancel and a resume never put a RUNNING row back in the queue.
#[tokio::test]
async fn workflow_cases_keep_the_retry_count_rule() {
    let database = database_with_the_rule(&[WORKFLOW_FIXTURES_SQL]).await;
    let session = database.session().await;
    assert_rule_fires(&session).await;
    session
        .run_script_rolled_back("baseline_workflow_functions.sql", WORKFLOW_CASES_SQL)
        .await;
}

/// A RUNNING row that goes back to PENDING or to CLAIMED at the same count
/// is refused with the task, the statuses and the counts; the row does not
/// change. A writer that drops its `+ 1` fails the case tests above with
/// this refusal.
#[tokio::test]
async fn a_running_row_that_goes_back_at_the_same_count_is_refused() {
    let database = database_with_the_rule(&[]).await;
    let session = database.session().await;
    for (statement, new_status) in [
        (BACK_TO_PENDING_SQL, "PENDING"),
        (BACK_TO_CLAIMED_SQL, "CLAIMED"),
    ] {
        let task_id = insert_task(&session, "RUNNING").await;
        let refused = refusal(&session, statement, &[&task_id]).await;
        assert_eq!(refused.sqlstate, RULE_SQLSTATE, "{refused:?}");
        let subject = format!("task {task_id}: status RUNNING -> {new_status}, retry_count 0 -> 0");
        assert!(refused.message.contains(&subject), "{refused:?}");
        assert_eq!(
            status_and_count(&session, task_id).await,
            ("RUNNING".to_owned(), 0)
        );
    }
}

/// A RUNNING row that goes back to PENDING with `retry_count + 1` passes:
/// the rule refuses only the writers that break it.
#[tokio::test]
async fn a_running_row_that_goes_back_with_a_higher_count_passes() {
    let database = database_with_the_rule(&[]).await;
    let session = database.session().await;
    let task_id = insert_task(&session, "RUNNING").await;
    update_one(&session, RETRY_TO_PENDING_SQL, &[&task_id]).await;
    assert_eq!(
        status_and_count(&session, task_id).await,
        ("PENDING".to_owned(), 1)
    );
}

/// An update that lowers the count is refused in any status; the row does
/// not change.
#[tokio::test]
async fn an_update_that_lowers_the_count_is_refused() {
    let database = database_with_the_rule(&[]).await;
    let session = database.session().await;
    let task_id = insert_task(&session, "PENDING").await;
    let raised: i32 = 2;
    let lowered: i32 = 1;
    update_one(&session, SET_RETRY_COUNT_SQL, &[&task_id, &raised]).await;
    let refused = refusal(&session, SET_RETRY_COUNT_SQL, &[&task_id, &lowered]).await;
    assert_eq!(refused.sqlstate, RULE_SQLSTATE, "{refused:?}");
    let subject = format!("task {task_id}: status PENDING -> PENDING, retry_count 2 -> 1");
    assert!(refused.message.contains(&subject), "{refused:?}");
    assert_eq!(
        status_and_count(&session, task_id).await,
        ("PENDING".to_owned(), 2)
    );
}
