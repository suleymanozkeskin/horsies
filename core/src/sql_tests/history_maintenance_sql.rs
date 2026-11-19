//! The history maintenance functions of the baseline
//! (`0008_history_maintenance_functions.sql`), through their case files.
//!
//! `cases/history_maintenance_cases.sql` holds the one-session cases and
//! runs in one transaction that is rolled back.
//! `cases/history_maintenance_concurrency.sql` holds the two-session cases
//! (dblink peers) and commits its fixtures. Each file runs as one message on
//! a database of its own. Neither file uses stand-in readers: both publish
//! the readers with `horsies_publish_history_readers` and read through them.

use super::harness::{
    assert_passed_cases, BaselineDatabase, Session, CREATE_DBLINK_SQL, PASSED_PREFIX,
};

const CASES_SQL: &str = include_str!("cases/history_maintenance_cases.sql");
const CONCURRENCY_SQL: &str = include_str!("cases/history_maintenance_concurrency.sql");

/// The checks of `history_maintenance_cases.sql`. A check added to the file
/// changes it.
const PASSED_CASES: usize = 119;

/// The checks of `history_maintenance_concurrency.sql`. A check added to the
/// file changes it.
const PASSED_CONCURRENCY_CASES: usize = 40;

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
async fn history_maintenance_cases_pass() {
    let (_database, session) = installed().await;
    session
        .run_script_rolled_back("history_maintenance_cases.sql", CASES_SQL)
        .await;
    assert_passed_cases(&session.notices_after(PASSED_PREFIX), PASSED_CASES);
}

#[tokio::test]
async fn history_maintenance_two_session_cases_pass() {
    let (_database, session) = installed().await;
    session
        .run_script("history_maintenance_concurrency.sql", CONCURRENCY_SQL)
        .await;
    assert_passed_cases(
        &session.notices_after(PASSED_PREFIX),
        PASSED_CONCURRENCY_CASES,
    );
}
