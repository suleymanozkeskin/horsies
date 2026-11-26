//! The smallest horsies app: one task, one worker profile, and a demo that
//! sends the task and reads its result.
//!
//! ```text
//! cargo run -p horsies-examples --example quick_start -- worker --profile default
//! cargo run -p horsies-examples --example quick_start -- demo
//! ```

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::process::ExitCode;
use std::time::Duration;

use horsies::{task, AppConfig, Horsies, HorsiesError, TaskError, TaskOutcome, WaitLimit};
use horsies_examples::{
    app_refused, database_url, isolated_profile, main_of, DemoResult, DEFAULT_PROFILE,
};

/// Stored input: `{"name": "world"}`.
#[task("greet")]
async fn greet(name: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("hello, {name}"))
}

/// Every process builds the same app: the demo, the worker and each child.
fn build_app() -> Result<Horsies, HorsiesError> {
    let mut config = AppConfig::for_database_url(database_url());
    config.worker_profiles = BTreeMap::from([(DEFAULT_PROFILE.to_owned(), isolated_profile())]);
    let mut app = Horsies::new(config)?;
    let _greet = greet::register(&mut app)?;
    Ok(app)
}

async fn demo(app: Horsies) -> DemoResult {
    let handle = greet::send(&app, "world".to_owned()).await?;
    let limit = WaitLimit::parse(Some(Duration::from_secs(30)))?;
    match handle.get(limit).await? {
        TaskOutcome::Succeeded(greeting) => println!("{greeting}"),
        TaskOutcome::Failed(error) => println!("greet failed: {error}"),
    }
    Ok(())
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => main_of(app, demo),
        Err(error) => app_refused(&error),
    }
}
