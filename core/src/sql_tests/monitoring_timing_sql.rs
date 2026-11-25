//! The timing rows of the monitoring run read of migration 1001
//! (`horsies_monitoring_workflow_run`, `core/migrations/1001_lifecycle_record.sql`).
//!
//! `cases/monitoring_timing_cases.sql` holds the one-session cases: the
//! fixed number of statements of the read, the `RUN`, `NODE`, `EVENT` and
//! `TASK` rows of a run with a moved task and a live task, a pruned history
//! row, run events, a child run, a run that began before the capture start,
//! a run past the event bound, and the capture start that the bookkeeping
//! row of migration 1001 gives. It runs with the helpers of
//! `cases/baseline_workflow_fixtures.sql`, in one transaction that is rolled
//! back.

use super::harness::{assert_passed_cases, BaselineDatabase, PASSED_PREFIX};

const FIXTURES_SQL: &str = include_str!("cases/baseline_workflow_fixtures.sql");
const CASES_SQL: &str = include_str!("cases/monitoring_timing_cases.sql");

/// The checks of `monitoring_timing_cases.sql` that pass: 17. A check added
/// to the file changes it.
const PASSED_CASES: usize = 17;

/// A failed case raises `case failed: <name>`, which fails this test with the
/// case name in the error.
#[tokio::test]
async fn monitoring_timing_cases_pass() {
    let database = BaselineDatabase::create(&[FIXTURES_SQL]).await;
    let session = database.session().await;
    session
        .run_script_rolled_back("monitoring_timing_cases.sql", CASES_SQL)
        .await;
    assert_passed_cases(&session.notices_after(PASSED_PREFIX), PASSED_CASES);
}
