//! The lifecycle record of migration 1001
//! (`core/migrations/1001_lifecycle_record.sql`), through its case file.
//!
//! `cases/lifecycle_record_cases.sql` holds the one-session cases: the
//! CHECKs of the new columns, the argument lists of the changed functions,
//! the claim count of the current attempt, the phase record of each attempt
//! writer, the refused placement pairs, the stale RUNNING recovery, the
//! version-2 attempt archive of each move and the monitoring detail. It runs
//! in one transaction that is rolled back, on a database that holds the
//! migration chain.

use super::harness::{assert_passed_cases, BaselineDatabase, PASSED_PREFIX};

const CASES_SQL: &str = include_str!("cases/lifecycle_record_cases.sql");

/// The checks of `lifecycle_record_cases.sql`: 75 statements, and 20 refused
/// placement pairs (5 pairs for each of the 4 writers) in one loop. A check
/// added to the file changes it.
const PASSED_CASES: usize = 95;

/// A failed case raises `case failed: <name>`, which fails this test with the
/// case name in the error.
#[tokio::test]
async fn lifecycle_record_cases_pass() {
    let database = BaselineDatabase::create(&[]).await;
    let session = database.session().await;
    session
        .run_script_rolled_back("lifecycle_record_cases.sql", CASES_SQL)
        .await;
    assert_passed_cases(&session.notices_after(PASSED_PREFIX), PASSED_CASES);
}
