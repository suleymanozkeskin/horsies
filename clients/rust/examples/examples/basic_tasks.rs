//! Async and blocking tasks, a task with codes of its own, and the reads of
//! their outcomes.
//!
//! ```text
//! cargo run -p horsies-examples --example basic_tasks -- worker --profile default
//! cargo run -p horsies-examples --example basic_tasks -- demo
//! ```

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Debug;
use std::process::ExitCode;
use std::time::Duration;

use horsies::{
    blocking_task, task, AppConfig, Horsies, HorsiesError, ReadError, TaskError, TaskOutcome,
    UserErrorCode, WaitLimit,
};
use horsies_examples::{
    app_refused, database_url, isolated_profile, main_of, DemoResult, DEFAULT_PROFILE,
};

/// The codes of `charge`'s own errors: `OVER_LIMIT`.
#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum ChargeCode {
    OverLimit,
}

/// The largest charge that `charge` accepts.
const CHARGE_LIMIT_CENTS: u64 = 100_000;

/// An async task. Stored input: `{"a": 2, "b": 3}`.
#[task("add")]
async fn add(a: i64, b: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(a + b)
}

/// A blocking task: a `fn` on a thread of its child.
#[blocking_task("word_count")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
fn word_count(text: String) -> Result<usize, TaskError<Infallible>> {
    Ok(text.split_whitespace().count())
}

/// A task that fails with a code of its own and data for the reader.
#[task("charge")]
async fn charge(amount_cents: u64) -> Result<String, TaskError<ChargeCode>> {
    match amount_cents > CHARGE_LIMIT_CENTS {
        true => Err(
            TaskError::user(ChargeCode::OverLimit, "the amount is over the limit")
                .with_data(serde_json::json!({ "limit_cents": CHARGE_LIMIT_CENTS })),
        ),
        false => Ok(format!("charged {amount_cents} cents")),
    }
}

fn build_app() -> Result<Horsies, HorsiesError> {
    let mut config = AppConfig::from_database_url(database_url());
    config.worker_profiles = BTreeMap::from([(DEFAULT_PROFILE.to_owned(), isolated_profile())]);
    let mut app = Horsies::new(config)?;
    let _add = add::register(&mut app)?;
    let _word_count = word_count::register(&mut app)?;
    let _charge = charge::register(&mut app)?;
    Ok(app)
}

/// Print the outcome of a read: the value, the task's error, or why the
/// read gave no outcome.
fn report<T: Debug, E: UserErrorCode>(label: &str, read: Result<TaskOutcome<T, E>, ReadError>) {
    match read {
        Ok(TaskOutcome::Succeeded(value)) => println!("{label}: {value:?}"),
        Ok(TaskOutcome::Failed(error)) => println!("{label}: failed: {error}"),
        Err(ReadError::WaitTimedOut { task_id, .. }) => {
            println!("{label}: task {task_id} has not finished; is a worker running?");
        }
        Err(error) => println!("{label}: the read failed ({:?}): {error}", error.code()),
    }
}

async fn demo(app: Horsies) -> DemoResult {
    let limit = WaitLimit::Within(Duration::from_secs(30));
    let sum = add::send(&app, 2, 3).await?;
    let words = word_count::send(&app, "the quick brown fox".to_owned()).await?;
    let small = charge::send(&app, 500).await?;
    let large = charge::send(&app, 500_000).await?;

    report("add", sum.get(limit).await);
    report("word_count", words.get(limit).await);
    report("charge 500", small.get(limit).await);
    let refused = large.get(limit).await;
    if let Ok(TaskOutcome::Failed(error)) = &refused {
        println!("charge 500000: data {:?}", error.data());
    }
    report("charge 500000", refused);
    Ok(())
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => main_of(app, demo),
        Err(error) => app_refused(&error),
    }
}
