//! App state and the task runtime: `provide` gives a value to task bodies,
//! which read it with `TaskRuntime::state`; `on_child_start` runs once in
//! each worker child before its first task; a task body sends another task
//! through the typed function that its runtime gives (`audit::handle`).
//!
//! Every worker child runs `main` again, so each child builds its own state.
//!
//! ```text
//! cargo run -p horsies-examples --example runtime_state -- worker --profile default
//! cargo run -p horsies-examples --example runtime_state -- demo
//! ```

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::process::ExitCode;
use std::time::Duration;

use horsies::{
    task, AppConfig, Horsies, HorsiesError, TaskError, TaskOutcome, TaskRuntime, UserErrorCode,
    WaitLimit,
};
use horsies_examples::{
    app_refused, database_url, isolated_profile, main_of, DemoResult, DEFAULT_PROFILE,
};

/// The state that task bodies read.
struct Greeter {
    greeting: String,
}

/// The codes of `welcome`'s own errors.
#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum WelcomeCode {
    NotConfigured,
    AuditNotSent,
}

#[task("welcome")]
async fn welcome(rt: TaskRuntime, user: String) -> Result<String, TaskError<WelcomeCode>> {
    let greeter = rt
        .state::<Greeter>()
        .map_err(|missing| TaskError::user(WelcomeCode::NotConfigured, missing.to_string()))?;
    let message = format!("{}, {user}", greeter.greeting);
    // A send from a task body goes through the app of its runtime and the
    // producer pool of the child.
    let audit_function = audit::handle(&rt)
        .map_err(|missing| TaskError::user(WelcomeCode::AuditNotSent, missing.to_string()))?;
    let _audit = audit_function
        .send(message.clone())
        .await
        .map_err(|error| TaskError::user(WelcomeCode::AuditNotSent, error.to_string()))?;
    Ok(message)
}

#[task("audit")]
async fn audit(entry: String) -> Result<usize, TaskError<Infallible>> {
    Ok(entry.len())
}

fn build_app() -> Result<Horsies, HorsiesError> {
    let mut config = AppConfig::for_database_url(database_url());
    config.worker_profiles = BTreeMap::from([(DEFAULT_PROFILE.to_owned(), isolated_profile())]);
    let mut app = Horsies::new(config)?;
    app.provide(Greeter {
        greeting: "welcome".to_owned(),
    })?;
    app.on_child_start(|| async {
        eprintln!("worker child {} is ready", std::process::id());
        Ok::<(), Infallible>(())
    });
    let _welcome = welcome::register(&mut app)?;
    let _audit = audit::register(&mut app)?;
    Ok(app)
}

async fn demo(app: Horsies) -> DemoResult {
    let handle = welcome::send(&app, "ada".to_owned()).await?;
    let limit = WaitLimit::parse(Some(Duration::from_secs(30)))?;
    match handle.get(limit).await? {
        TaskOutcome::Succeeded(message) => println!("{message}"),
        TaskOutcome::Failed(error) => println!("welcome failed: {error}"),
    }
    Ok(())
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => main_of(app, demo),
        Err(error) => app_refused(&error),
    }
}
