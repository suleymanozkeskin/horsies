//! The scheduler and monitoring functions of the baseline
//! (`0007_scheduler_monitoring_functions.sql`), through their case files.
//!
//! Two files hold the cases: `cases/baseline_scheduler_monitoring_cases.sql`
//! (the scheduler, the stale and expired lists, the task detail, list and
//! facets) and `cases/baseline_monitoring_reads_cases.sql` (the status
//! counts, the breakdown, the workflow and schedule reads). Each file runs
//! as one message on a database of its own that holds what the file
//! expects: the baseline, the published history readers of the forever
//! leaf, and dblink for the two-session cases.

use super::harness::{assert_passed_cases, describe, BaselineDatabase, CREATE_DBLINK_SQL};

/// Each case file with its number of cases, in file name order. A case added
/// to a file changes it.
const CASE_FILES: [(&str, &str, i64); 2] = [
    (
        "baseline_monitoring_reads_cases.sql",
        include_str!("cases/baseline_monitoring_reads_cases.sql"),
        83,
    ),
    (
        "baseline_scheduler_monitoring_cases.sql",
        include_str!("cases/baseline_scheduler_monitoring_cases.sql"),
        80,
    ),
];

/// The setup text that publishes the history readers.
const PUBLISH_READERS_SQL: &str = "SELECT outcome FROM horsies_publish_history_readers()";

/// The prefix of the notice of a passed case in these files.
const PASSED_PREFIX: &str = "ok ";

/// A failed case raises `CASE FAILED <name>: ...`, which fails this test with
/// the case name in the error.
#[tokio::test]
async fn scheduler_and_monitoring_sql_cases_pass() {
    for (case_file, cases_sql, case_count) in CASE_FILES {
        let database = BaselineDatabase::create(&[PUBLISH_READERS_SQL, CREATE_DBLINK_SQL]).await;
        let session = database.peer_session().await;
        session.run_script(case_file, cases_sql).await;
        let passed = session.notices_after(PASSED_PREFIX);
        let last_value: i64 = session
            .client()
            .query_one("SELECT last_value FROM test_passed_cases", &[])
            .await
            .unwrap_or_else(|error| panic!("{case_file}: {}", describe(&error)))
            .get(0);
        assert_eq!(last_value, case_count, "{case_file}: {passed:?}");
        let expected = usize::try_from(case_count).expect("a case count is not negative");
        assert_passed_cases(&passed, expected);
    }
}
