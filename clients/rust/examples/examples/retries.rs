//! Automatic retries: `retry_policy` with `auto_retry_for`. A retry starts
//! when the task ends with one of the listed codes: a code of the task's own
//! enum, or a built-in code such as `TASK_TIMEOUT`.
//!
//! ```text
//! cargo run -p horsies-examples --example retries -- worker --profile default
//! cargo run -p horsies-examples --example retries -- demo
//! ```

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::time::Duration;

use horsies::{
    task, AppConfig, Horsies, HorsiesError, RetryPolicy, TaskError, TaskInfoParts, TaskOutcome,
    UserErrorCode, WaitLimit,
};
use horsies_examples::{
    app_refused, database_url, isolated_profile, main_of, DemoResult, DEFAULT_PROFILE,
};

/// The codes of `call_partner`'s own errors.
#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum PartnerCode {
    /// Retried: the partner asks to try later.
    RateLimited,
    /// Not retried: the request is wrong.
    Rejected,
}

/// Fails with `RATE_LIMITED` every time, so the demo sees each retry: one
/// run and two retries, 1 s and 2 s apart, then the final failure.
#[task(
    "call_partner",
    retry_policy = RetryPolicy::fixed(vec![1, 2], false).expect("a valid policy"),
    auto_retry_for = ["RATE_LIMITED", "TASK_TIMEOUT"],
    timeout_ms = 5_000
)]
async fn call_partner(request: String) -> Result<String, TaskError<PartnerCode>> {
    match request.is_empty() {
        true => Err(TaskError::user(PartnerCode::Rejected, "an empty request")),
        false => Err(TaskError::user(
            PartnerCode::RateLimited,
            format!("the partner is busy: {request}"),
        )),
    }
}

fn build_app() -> Result<Horsies, HorsiesError> {
    let mut config = AppConfig::from_database_url(database_url());
    config.worker_profiles = BTreeMap::from([(DEFAULT_PROFILE.to_owned(), isolated_profile())]);
    let mut app = Horsies::new(config)?;
    let _call_partner = call_partner::register(&mut app)?;
    Ok(app)
}

async fn demo(app: Horsies) -> DemoResult {
    let limit = WaitLimit::Within(Duration::from_secs(60));
    for request in ["sync-orders", ""] {
        let handle = call_partner::send(&app, request.to_owned()).await?;
        match handle.get(limit).await? {
            TaskOutcome::Succeeded(answer) => println!("{request:?}: {answer}"),
            TaskOutcome::Failed(error) => println!("{request:?}: failed: {error}"),
        }
        let info = handle.info(TaskInfoParts::default()).await?;
        println!(
            "{request:?}: {} retries of {} allowed",
            info.retry_count, info.max_retries
        );
    }
    Ok(())
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => main_of(app, demo),
        Err(error) => app_refused(&error),
    }
}
