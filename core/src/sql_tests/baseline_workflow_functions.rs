//! The workflow engine functions of the baseline
//! (`0005_workflow_functions.sql`).
//!
//! The single-session cases are the case file
//! `cases/baseline_workflow_functions.sql`, with the helpers of
//! `cases/baseline_workflow_fixtures.sql`; they run in one transaction that
//! is rolled back. The two-session cases check the lock order: one session
//! holds a step open, a second session runs a conflicting step and waits on
//! a lock, the first commits, and the second finishes with the expected
//! outcome and no deadlock error. Each case has a database of its own with
//! the baseline and the fixture helpers committed.

use std::time::Duration;

use tokio_postgres::Row;

use super::harness::{describe, BaselineDatabase, Session};

pub(super) const FIXTURES_SQL: &str = include_str!("cases/baseline_workflow_fixtures.sql");
pub(super) const CASES_SQL: &str = include_str!("cases/baseline_workflow_functions.sql");

/// Polls of the waiting session's wait event before the case fails.
const LOCK_WAIT_POLLS: u32 = 200;

const LOCK_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Bound on the second session after the first committed.
const CONTENDER_TIMEOUT: Duration = Duration::from_secs(10);

const WAIT_EVENT_SQL: &str = "SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1";

/// One value of a result row, by the kind of its column.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Cell {
    Text(String),
    Integer(i64),
    Null,
}

impl Cell {
    fn text(value: &str) -> Cell {
        Cell::Text(value.to_owned())
    }
}

/// The values of `row`.
///
/// # Panics
///
/// On a column type that the cases do not compare.
fn cells(row: &Row) -> Vec<Cell> {
    (0..row.len())
        .map(|index| {
            let type_name = row.columns()[index].type_().name();
            match type_name {
                "text" | "varchar" => row
                    .get::<_, Option<String>>(index)
                    .map_or(Cell::Null, Cell::Text),
                "int4" => row
                    .get::<_, Option<i32>>(index)
                    .map_or(Cell::Null, |value| Cell::Integer(i64::from(value))),
                "int8" => row
                    .get::<_, Option<i64>>(index)
                    .map_or(Cell::Null, Cell::Integer),
                other => panic!("column {index} has type {other}, which the cases do not compare"),
            }
        })
        .collect()
}

/// One two-session case. `hold` runs in an open transaction of the first
/// session; `contend` runs in the second session while the first holds its
/// locks; `check`, when there is one, runs after both, with its expected
/// row. Each of the three is one statement.
struct LockOrderCase {
    name: &'static str,
    setup: &'static str,
    hold: &'static str,
    held: Vec<Cell>,
    contend: &'static str,
    contended: Vec<Cell>,
    check: Option<(&'static str, Vec<Cell>)>,
}

fn lock_order_cases() -> Vec<LockOrderCase> {
    vec![
        LockOrderCase {
            name: r#"a second start with the same id waits for the first, then sees EXISTS"#,
            setup: r#"SELECT 1"#,
            hold: r#"SELECT disposition FROM wft_start1('00000000-0000-7000-8000-00000000a001', 'fail', '[{"idx":0}]') WHERE row_kind = 'OUTCOME'"#,
            held: vec![Cell::text("STARTED")],
            contend: r#"SELECT disposition, detail FROM wft_start1('00000000-0000-7000-8000-00000000a001', 'fail', '[{"idx":0}]') WHERE row_kind = 'OUTCOME'"#,
            contended: vec![Cell::text("EXISTS"), Cell::text("wf")],
            check: None,
        },
        LockOrderCase {
            name: r#"a second start with the same id and another start command waits for the first, then sees OTHER_COMMAND and writes nothing"#,
            setup: r#"SELECT 1"#,
            hold: r#"SELECT disposition FROM wft_start1('00000000-0000-7000-8000-00000000a0b1', 'fail', '[{"idx":0}]') WHERE row_kind = 'OUTCOME'"#,
            held: vec![Cell::text("STARTED")],
            contend: r#"SELECT disposition, detail FROM wft_start1('00000000-0000-7000-8000-00000000a0b1', 'fail', '[{"idx":0},{"idx":1}]') WHERE row_kind = 'OUTCOME'"#,
            contended: vec![Cell::text("OTHER_COMMAND"), Cell::text("wf")],
            check: Some((
                r#"SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = '00000000-0000-7000-8000-00000000a0b1'"#,
                vec![Cell::Integer(1)],
            )),
        },
        LockOrderCase {
            name: r#"a start waits for an open delete of the row with its id, then starts a new workflow"#,
            setup: r#"SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000a0c1', 'fail', '[{"idx":0}]')"#,
            hold: r#"DELETE FROM horsies_workflows WHERE id = '00000000-0000-7000-8000-00000000a0c1' RETURNING status"#,
            held: vec![Cell::text("RUNNING")],
            contend: r#"SELECT disposition FROM wft_start1('00000000-0000-7000-8000-00000000a0c1', 'fail', '[{"idx":0},{"idx":1}]') WHERE row_kind = 'OUTCOME'"#,
            contended: vec![Cell::text("STARTED")],
            check: Some((
                r#"SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = '00000000-0000-7000-8000-00000000a0c1'"#,
                vec![Cell::Integer(2)],
            )),
        },
        LockOrderCase {
            name: r#"a cancel waits for a phase 2 that holds the workflow row, then cancels the tree"#,
            setup: r#"SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000a011', 'fail', '[{"idx":0},{"idx":1},{"idx":2,"deps":[0]}]'); SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a011', 0), wft_ok('1'))"#,
            hold: r#"SELECT disposition FROM horsies_workflow_node_finish(wft_task_of('00000000-0000-7000-8000-00000000a011', 0), TRUE, 'READ') WHERE row_kind = 'OUTCOME'"#,
            held: vec![Cell::text("APPLIED")],
            contend: r#"SELECT outcome, cardinality(cancelled_task_ids) FROM horsies_workflow_cancel('00000000-0000-7000-8000-00000000a011')"#,
            contended: vec![Cell::text("CANCELLED"), Cell::Integer(2)],
            check: Some((
                r#"SELECT string_agg(task_index || '=' || status, ',' ORDER BY task_index) FROM horsies_workflow_tasks WHERE workflow_id = '00000000-0000-7000-8000-00000000a011'"#,
                vec![Cell::text("0=COMPLETED,1=SKIPPED,2=SKIPPED")],
            )),
        },
        LockOrderCase {
            name: r#"a root pause waits for a child phase 2 that pauses the child and its child, in (depth, id) order"#,
            setup: r#"SELECT count(*) FROM wft_start('[{"id":"00000000-0000-7000-8000-00000000a021","on_error":"fail"},{"id":"00000000-0000-7000-8000-00000000a022","on_error":"pause","parent":"00000000-0000-7000-8000-00000000a021","parent_index":0,"depth":1},{"id":"00000000-0000-7000-8000-00000000a023","on_error":"fail","parent":"00000000-0000-7000-8000-00000000a022","parent_index":1,"depth":2}]', '[{"idx":0,"sub":true},{"wf":"00000000-0000-7000-8000-00000000a022","idx":0},{"wf":"00000000-0000-7000-8000-00000000a022","idx":1,"sub":true},{"wf":"00000000-0000-7000-8000-00000000a023","idx":0}]'); SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a022', 0), wft_err('USER'))"#,
            hold: r#"SELECT disposition FROM horsies_workflow_node_finish(wft_task_of('00000000-0000-7000-8000-00000000a022', 0), TRUE, 'READ') WHERE row_kind = 'OUTCOME'"#,
            held: vec![Cell::text("APPLIED")],
            contend: r#"SELECT outcome, cardinality(paused_workflow_ids) FROM horsies_workflow_pause('00000000-0000-7000-8000-00000000a021')"#,
            contended: vec![Cell::text("PAUSED"), Cell::Integer(1)],
            check: Some((
                r#"SELECT string_agg(depth || '=' || status, ',' ORDER BY depth) FROM horsies_workflows WHERE root_workflow_id = '00000000-0000-7000-8000-00000000a021'"#,
                vec![Cell::text("0=PAUSED,1=PAUSED,2=PAUSED")],
            )),
        },
        LockOrderCase {
            name: r#"a phase 2 waits for the raise record on the workflow row, then applies the record"#,
            setup: r#"SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000a031', 'fail', '[{"idx":0}]'); SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a031', 0), wft_ok('1'))"#,
            hold: r#"SELECT outcome FROM horsies_workflow_phase2_record_raise(wft_task_of('00000000-0000-7000-8000-00000000a031', 0), 25, '{}', '{}')"#,
            held: vec![Cell::text("COUNTED")],
            contend: r#"SELECT disposition FROM horsies_workflow_node_finish(wft_task_of('00000000-0000-7000-8000-00000000a031', 0), TRUE, 'READ') WHERE row_kind = 'OUTCOME'"#,
            contended: vec![Cell::text("APPLIED")],
            check: None,
        },
        LockOrderCase {
            name: r#"a child phase 2 waits for a pause that holds the child row, then promotes nothing"#,
            setup: r#"SELECT count(*) FROM wft_start('[{"id":"00000000-0000-7000-8000-00000000a041","on_error":"fail"},{"id":"00000000-0000-7000-8000-00000000a042","on_error":"fail","parent":"00000000-0000-7000-8000-00000000a041","parent_index":0,"depth":1}]', '[{"idx":0,"sub":true},{"wf":"00000000-0000-7000-8000-00000000a042","idx":0},{"wf":"00000000-0000-7000-8000-00000000a042","idx":1,"deps":[0]}]'); SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a042', 0), wft_ok('1'))"#,
            hold: r#"SELECT outcome FROM horsies_workflow_pause('00000000-0000-7000-8000-00000000a041')"#,
            held: vec![Cell::text("PAUSED")],
            contend: r#"SELECT disposition FROM horsies_workflow_node_finish(wft_task_of('00000000-0000-7000-8000-00000000a042', 0), TRUE, 'READ') WHERE row_kind = 'OUTCOME'"#,
            contended: vec![Cell::text("APPLIED")],
            check: Some((
                r#"SELECT string_agg(task_index || '=' || status, ',' ORDER BY task_index) FROM horsies_workflow_tasks WHERE workflow_id = '00000000-0000-7000-8000-00000000a042'"#,
                vec![Cell::text("0=COMPLETED,1=PENDING")],
            )),
        },
        LockOrderCase {
            name: r#"a claim-time settle waits for a resume that holds the workflow row, then dispatches the task"#,
            setup: r#"SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000a051', 'fail', '[{"idx":0}]'); SELECT horsies_workflow_pause('00000000-0000-7000-8000-00000000a051'); SELECT wft_claim(wft_task_of('00000000-0000-7000-8000-00000000a051', 0), '2030-01-01 00:00:00+00')"#,
            hold: r#"SELECT disposition FROM horsies_workflow_resume('00000000-0000-7000-8000-00000000a051') WHERE row_kind = 'OUTCOME'"#,
            held: vec![Cell::text("RESUMED")],
            contend: r#"SELECT disposition FROM horsies_workflow_claimed_tasks_settle(ARRAY[wft_task_of('00000000-0000-7000-8000-00000000a051', 0)], ARRAY['2030-01-01 00:00:00+00'::timestamptz], 'worker-1')"#,
            contended: vec![Cell::text("DISPATCH")],
            check: Some((
                r#"SELECT t.status || ',' || n.status FROM horsies_workflow_tasks n JOIN horsies_tasks t ON t.id = n.task_id WHERE n.workflow_id = '00000000-0000-7000-8000-00000000a051'"#,
                vec![Cell::text("CLAIMED,ENQUEUED")],
            )),
        },
        LockOrderCase {
            name: r#"a stop before start waits for a resume that holds the workflow row, then reports RESUMED"#,
            setup: r#"SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000a061', 'fail', '[{"idx":0}]'); SELECT horsies_workflow_pause('00000000-0000-7000-8000-00000000a061'); SELECT wft_claim(wft_task_of('00000000-0000-7000-8000-00000000a061', 0), '2030-01-01 00:00:00+00')"#,
            hold: r#"SELECT disposition FROM horsies_workflow_resume('00000000-0000-7000-8000-00000000a061') WHERE row_kind = 'OUTCOME'"#,
            held: vec![Cell::text("RESUMED")],
            contend: r#"SELECT outcome FROM horsies_workflow_task_stop_before_start(wft_task_of('00000000-0000-7000-8000-00000000a061', 0), 'worker-1', '2030-01-01 00:00:00+00', '{}', TRUE)"#,
            contended: vec![Cell::text("RESUMED")],
            check: Some((
                r#"SELECT t.status || ',' || n.status FROM horsies_workflow_tasks n JOIN horsies_tasks t ON t.id = n.task_id WHERE n.workflow_id = '00000000-0000-7000-8000-00000000a061'"#,
                vec![Cell::text("CLAIMED,ENQUEUED")],
            )),
        },
        LockOrderCase {
            name: r#"a paused expiry waits for a resume that holds the workflow row, then expires nothing"#,
            setup: r#"SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000a071', 'fail', '[{"idx":0}]'); SELECT horsies_workflow_pause('00000000-0000-7000-8000-00000000a071'); UPDATE horsies_workflows SET updated_at = now() - interval '2 hours' WHERE id = '00000000-0000-7000-8000-00000000a071'"#,
            hold: r#"SELECT disposition FROM horsies_workflow_resume('00000000-0000-7000-8000-00000000a071') WHERE row_kind = 'OUTCOME'"#,
            held: vec![Cell::text("RESUMED")],
            contend: r#"SELECT outcome, observed_status FROM horsies_workflow_expire_paused('00000000-0000-7000-8000-00000000a071', 3600000000, '{}')"#,
            contended: vec![Cell::text("NOT_EXPIRED"), Cell::text("RUNNING")],
            check: Some((
                r#"SELECT t.status || ',' || n.status FROM horsies_workflow_tasks n JOIN horsies_tasks t ON t.id = n.task_id WHERE n.workflow_id = '00000000-0000-7000-8000-00000000a071'"#,
                vec![Cell::text("PENDING,ENQUEUED")],
            )),
        },
        LockOrderCase {
            name: r#"a pause waits for a phase 2 and its child start in one transaction, then pauses the new child"#,
            setup: r#"SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000a081', 'fail', '[{"idx":0},{"idx":1,"deps":[0],"sub":true}]'); SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a081', 0), wft_ok('1'))"#,
            hold: r#"SELECT wft_finish_with_child('00000000-0000-7000-8000-00000000a081', 0, '00000000-0000-7000-8000-00000000a082')"#,
            held: vec![Cell::text("STARTED")],
            contend: r#"SELECT outcome, cardinality(paused_workflow_ids) FROM horsies_workflow_pause('00000000-0000-7000-8000-00000000a081')"#,
            contended: vec![Cell::text("PAUSED"), Cell::Integer(2)],
            check: Some((
                r#"SELECT string_agg(depth || '=' || status, ',' ORDER BY depth) FROM horsies_workflows WHERE root_workflow_id = '00000000-0000-7000-8000-00000000a081'"#,
                vec![Cell::text("0=PAUSED,1=PAUSED")],
            )),
        },
        LockOrderCase {
            name: r#"a recovery completion waits for a phase 2 that completes the workflow, then changes nothing"#,
            setup: r#"SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000a091', 'fail', '[{"idx":0}]'); SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a091', 0), wft_ok('1')); SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000a092', 'fail', '[{"idx":0}]'); UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = wft_ok('2'), completed_at = now() WHERE workflow_id = '00000000-0000-7000-8000-00000000a092'; SELECT outcome FROM horsies_workflow_recovery_audit_page(1000, '00000000-0000-7000-8000-00000000a0ee', 300000)"#,
            hold: r#"SELECT disposition FROM horsies_workflow_node_finish(wft_task_of('00000000-0000-7000-8000-00000000a091', 0), TRUE, 'NONE') WHERE row_kind = 'OUTCOME'"#,
            held: vec![Cell::text("APPLIED")],
            contend: r#"SELECT outcome, workflow_status FROM horsies_workflow_recover_completion('00000000-0000-7000-8000-00000000a091', '00000000-0000-7000-8000-00000000a0ee', 300000)"#,
            contended: vec![Cell::text("UNCHANGED"), Cell::text("COMPLETED")],
            check: None,
        },
        LockOrderCase {
            name: r#"a recovery raise record waits for a phase 2 on the workflow row, then counts"#,
            setup: r#"SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000a0a1', 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]'); SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a0a1', 0), wft_ok('1'))"#,
            hold: r#"SELECT disposition FROM horsies_workflow_node_finish(wft_task_of('00000000-0000-7000-8000-00000000a0a1', 0), TRUE, 'READ') WHERE row_kind = 'OUTCOME'"#,
            held: vec![Cell::text("APPLIED")],
            contend: r#"SELECT outcome, failed_passes FROM horsies_workflow_recovery_record_raise('00000000-0000-7000-8000-00000000a0a1', '00000000-0000-7000-8000-00000000a0f1', 'pending_terminal_deps', 1, 'KeyError', 'boom', 25)"#,
            contended: vec![Cell::text("COUNTED"), Cell::Integer(1)],
            check: None,
        },
    ]
}

/// A database with the baseline and the fixture helpers committed.
async fn workflow_database() -> BaselineDatabase {
    BaselineDatabase::create(&[FIXTURES_SQL]).await
}

#[tokio::test]
async fn workflow_function_cases_pass() {
    let database = workflow_database().await;
    let session = database.session().await;
    session
        .run_script_rolled_back("baseline_workflow_functions.sql", CASES_SQL)
        .await;
}

/// The same cases on a database whose default collation ignores `_`, as
/// glibc's `en_US.UTF-8` does: the envelope checks sort the keys of a result
/// by code point, so `__h_task_result__` stays first.
#[tokio::test]
async fn workflow_function_cases_pass_under_a_collation_that_ignores_punctuation() {
    let database = BaselineDatabase::create_ignoring_punctuation(&[FIXTURES_SQL]).await;
    let session = database.session().await;
    session
        .run_script_rolled_back("baseline_workflow_functions.sql", CASES_SQL)
        .await;
}

/// The values of the one row of `statement`.
///
/// # Panics
///
/// When the statement fails or does not return exactly one row.
async fn one_row(session: &Session, statement: &str) -> Vec<Cell> {
    let rows = session
        .client()
        .query(statement, &[])
        .await
        .unwrap_or_else(|error| panic!("{statement}: {}", describe(&error)));
    assert_eq!(rows.len(), 1, "{statement:?} returned {} rows", rows.len());
    cells(&rows[0])
}

/// Returns once session `pid` waits on a lock.
///
/// # Panics
///
/// When the session does not wait within [`LOCK_WAIT_POLLS`] reads.
async fn wait_until_blocked(observer: &Session, pid: i32) {
    for _ in 0..LOCK_WAIT_POLLS {
        let row = observer
            .client()
            .query_opt(WAIT_EVENT_SQL, &[&pid])
            .await
            .unwrap_or_else(|error| panic!("read the wait event: {}", describe(&error)));
        let waiting = row.is_some_and(|row| row.get::<_, Option<&str>>(0) == Some("Lock"));
        if waiting {
            return;
        }
        tokio::time::sleep(LOCK_WAIT_POLL_INTERVAL).await;
    }
    panic!("session {pid} never waited on a lock");
}

/// Every case of [`lock_order_cases`], each on a database of its own.
#[tokio::test]
async fn lock_order() {
    for case in lock_order_cases() {
        run_lock_order_case(&case).await;
    }
}

async fn run_lock_order_case(case: &LockOrderCase) {
    let database = workflow_database().await;
    let (setup, holder) = (database.session().await, database.session().await);
    let (contender, observer) = (database.session().await, database.session().await);
    setup.run_script(case.name, case.setup).await;
    holder.run_script("BEGIN", "BEGIN").await;
    assert_eq!(
        one_row(&holder, case.hold).await,
        case.held,
        "{}",
        case.name
    );
    let contended = one_row(&contender, case.contend);
    tokio::pin!(contended);
    tokio::select! {
        biased;
        row = &mut contended => panic!(
            "{}: the second session finished before the first committed: {row:?}",
            case.name,
        ),
        () = wait_until_blocked(&observer, contender.backend_pid()) => {}
    }
    holder.run_script("COMMIT", "COMMIT").await;
    let row = tokio::time::timeout(CONTENDER_TIMEOUT, &mut contended)
        .await
        .unwrap_or_else(|_elapsed| {
            panic!(
                "{}: the second session did not finish after the commit",
                case.name
            )
        });
    assert_eq!(row, case.contended, "{}", case.name);
    match &case.check {
        None => {}
        Some((check, checked)) => {
            assert_eq!(one_row(&observer, check).await, *checked, "{}", case.name);
        }
    }
}
