//! `<binary> check --live` as a process (Python
//! `clients/python/tests/e2e/test_check_live_command.py`).
//!
//! The test worker binary builds the app of [`Instance::Default`] from the
//! database URL of its environment and hands over to `horsies::cli::run`,
//! as the binary of an app does. Each test gives the process a URL of its
//! own and checks the exit status and the output.

use std::path::Path;
use std::time::Duration;

use horsies_test_support::process::TestProcess;
use horsies_test_worker::harness::Suite;
use horsies_test_worker::instance::{Instance, DATABASE_URL_ENV};
use serial_test::serial;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest run of one `check --live` process: the build of the app,
/// one connect and the reads of the live phases. One connect attempt waits
/// at most 10 s.
const CHECK_LIMIT: Duration = Duration::from_secs(60);

/// A URL whose port has no server (port 1). It holds a password, so the
/// connect reads no password source of the environment.
const UNREACHABLE_DATABASE_URL: &str = "postgresql://postgres:unused@127.0.0.1:1/horsies";

/// The end of one `check --live` process.
struct CheckEnd {
    /// The exit code; `None` when a signal ended the process.
    code: Option<i32>,
    /// Every line of stdout and stderr.
    output: String,
}

/// Run `check --live` with the environment `env` and wait for its end.
/// Panics with the output when the process does not end within
/// [`CHECK_LIMIT`]; dropping the process then kills it.
async fn run_check_live(env: &[(&'static str, String)]) -> CheckEnd {
    let mut process = TestProcess::spawn("check --live", Path::new(BIN), ["check", "--live"], env);
    match process.wait_exit_with_output(CHECK_LIMIT).await {
        Some(status) => CheckEnd {
            code: status.code(),
            output: process.logs().join("\n"),
        },
        None => panic!(
            "check --live did not end within {CHECK_LIMIT:?}\n{}",
            process.report()
        ),
    }
}

/// The environment of the processes of `suite`, with `database_url` as the
/// database URL of the app.
fn env_with_database_url(suite: &Suite, database_url: &str) -> Vec<(&'static str, String)> {
    suite
        .process_env()
        .into_iter()
        .map(|(name, value)| match name {
            DATABASE_URL_ENV => (name, database_url.to_owned()),
            _ => (name, value),
        })
        .collect()
}

/// `url` with the scheme `postgresql+psycopg`: a URL that names a driver.
fn with_driver_scheme(url: &str) -> String {
    let (_scheme, rest) = url
        .split_once("://")
        .expect("the database URL of the suite has a scheme");
    format!("postgresql+psycopg://{rest}")
}

/// `url` with the query parameter `parameter` (`name=value`) added.
fn with_query_parameter(url: &str, parameter: &str) -> String {
    match url.contains('?') {
        true => format!("{url}&{parameter}"),
        false => format!("{url}?{parameter}"),
    }
}

/// Python: `test_check_live_command.py::test_check_live_on_the_test_database_passes`.
/// The live phases connect, read the connection limit and compare each
/// worker profile of the instance with it.
#[tokio::test]
#[serial]
async fn test_check_live_on_the_test_database_passes() {
    let suite = Suite::start(BIN, Instance::Default).await;

    let end = run_check_live(&suite.process_env()).await;

    assert_eq!(end.code, Some(0), "{}", end.output);
    assert!(
        end.output.contains("ok: all validations passed"),
        "{}",
        end.output
    );
    assert!(!end.output.contains("error["), "{}", end.output);
}

/// Python: `test_check_live_command.py::test_check_live_refuses_a_url_that_names_a_driver_before_any_connect`.
/// The binary refuses the URL when it builds the app (`Horsies::new`), so
/// the check does not start: the binary prints the config refusal and
/// exits 1.
#[tokio::test]
#[serial]
async fn test_check_live_refuses_a_url_that_names_a_driver_before_any_connect() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let driver_named_url = with_driver_scheme(&suite.config().broker.database_url);

    let end = run_check_live(&env_with_database_url(&suite, &driver_named_url)).await;

    assert_eq!(end.code, Some(1), "{}", end.output);
    assert!(
        end.output
            .contains("error[HRS-203]: invalid database_url scheme \"postgresql+psycopg\""),
        "{}",
        end.output
    );
    assert!(
        !end.output.contains("broker connectivity check failed"),
        "{}",
        end.output
    );
    assert!(
        !end.output.contains("ok: all validations passed"),
        "{}",
        end.output
    );
}

/// Python: `test_check_live_command.py::test_check_live_reports_a_refused_url_setting_as_a_connectivity_failure`.
/// The config admits `keepalives` in the URL; the core refuses it when it
/// opens its first pool, in the live phase. The note names the parameter.
#[tokio::test]
#[serial]
async fn test_check_live_reports_a_refused_url_setting_as_a_connectivity_failure() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let refused_setting_url =
        with_query_parameter(&suite.config().broker.database_url, "keepalives=1");

    let end = run_check_live(&env_with_database_url(&suite, &refused_setting_url)).await;

    assert_eq!(end.code, Some(1), "{}", end.output);
    assert!(
        end.output
            .contains("error[HRS-203]: broker connectivity check failed"),
        "{}",
        end.output
    );
    assert!(
        end.output.contains("the parameter keepalives is refused"),
        "{}",
        end.output
    );
    assert!(
        !end.output.contains("ok: all validations passed"),
        "{}",
        end.output
    );
}

/// Python: `test_check_live_command.py::test_check_live_reports_a_port_with_no_server_as_a_connectivity_failure`.
/// The static check passes; the connect of the live phase is refused.
#[tokio::test]
#[serial]
async fn test_check_live_reports_a_port_with_no_server_as_a_connectivity_failure() {
    let suite = Suite::start(BIN, Instance::Default).await;

    let end = run_check_live(&env_with_database_url(&suite, UNREACHABLE_DATABASE_URL)).await;

    assert_eq!(end.code, Some(1), "{}", end.output);
    assert!(
        end.output
            .contains("error[HRS-203]: broker connectivity check failed"),
        "{}",
        end.output
    );
    assert!(
        !end.output.contains("ok: all validations passed"),
        "{}",
        end.output
    );
}
