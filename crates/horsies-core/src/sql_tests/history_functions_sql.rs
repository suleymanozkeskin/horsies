//! The history and retention functions of the baseline
//! (`0006_history_functions.sql`), through their case files.
//!
//! `cases/history_functions_cases.sql` holds the one-session cases and runs
//! in one transaction that is rolled back.
//! `cases/history_functions_concurrency.sql` holds the two-session cases
//! (dblink peers) and commits its fixtures. Each file runs as one message on
//! a database of its own. Both files carry stand-ins for the published
//! history readers.

use super::harness::{
    assert_passed_cases, BaselineDatabase, Session, CREATE_DBLINK_SQL, PASSED_PREFIX,
};

const CASES_SQL: &str = include_str!("cases/history_functions_cases.sql");
const CONCURRENCY_SQL: &str = include_str!("cases/history_functions_concurrency.sql");

/// The checks of `history_functions_cases.sql`. A check added to the file
/// changes it.
const PASSED_CASES: usize = 99;

/// The checks of `history_functions_concurrency.sql`. A check added to the
/// file changes it.
const PASSED_CONCURRENCY_CASES: usize = 44;

/// A database with the baseline and dblink, and a session on it in
/// autocommit with the peer connection set.
async fn installed() -> (BaselineDatabase, Session) {
    let database = BaselineDatabase::create(&[CREATE_DBLINK_SQL]).await;
    let session = database.peer_session().await;
    (database, session)
}

/// A failed case raises `case failed: <name>`, which fails this test with the
/// case name in the error.
#[tokio::test]
async fn history_function_cases_pass() {
    let (_database, session) = installed().await;
    session
        .run_script_rolled_back("history_functions_cases.sql", CASES_SQL)
        .await;
    assert_passed_cases(&session.notices_after(PASSED_PREFIX), PASSED_CASES);
}

#[tokio::test]
async fn history_function_two_session_cases_pass() {
    let (_database, session) = installed().await;
    session
        .run_script("history_functions_concurrency.sql", CONCURRENCY_SQL)
        .await;
    assert_passed_cases(
        &session.notices_after(PASSED_PREFIX),
        PASSED_CONCURRENCY_CASES,
    );
}
