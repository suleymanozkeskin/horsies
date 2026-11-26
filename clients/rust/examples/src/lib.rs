//! What every example binary shares: the database URL, a worker profile,
//! and the `main` that runs the example's demo or hands over to horsies.
//!
//! Each example is one binary. The same binary is the worker, its children
//! and the producer:
//!
//! ```text
//! cargo run -p horsies-examples --example basic_tasks -- worker --profile default
//! cargo run -p horsies-examples --example basic_tasks -- demo
//! ```
//!
//! `DATABASE_URL` names the database; each example installs the schema at
//! its first operation.

use std::error::Error;
use std::future::Future;
use std::process::ExitCode;

use horsies::{ChildPreload, Horsies, IsolatedChildren, WorkerProfile};

/// The database when `DATABASE_URL` is not set.
pub const LOCAL_DATABASE_URL: &str =
    "postgresql://postgres:postgres@localhost:5432/horsies_examples";

/// The name of the worker profile that every example defines.
pub const DEFAULT_PROFILE: &str = "default";

/// The isolated children of [`isolated_profile`].
const ISOLATED_CHILDREN: u32 = 2;

/// The result of an example's demo.
pub type DemoResult = Result<(), Box<dyn Error>>;

/// `DATABASE_URL`, or [`LOCAL_DATABASE_URL`] when it is not set.
pub fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_unset| LOCAL_DATABASE_URL.to_owned())
}

/// A worker profile of two isolated children: each runs one task at a time
/// in its own process. A Rust worker starts its children with `exec` of its
/// own binary, so the profile has no preload process.
pub fn isolated_profile() -> WorkerProfile {
    WorkerProfile {
        isolated: IsolatedChildren::new(ISOLATED_CHILDREN).into(),
        preload: ChildPreload::NoPreload,
        ..WorkerProfile::default()
    }
}

/// The `main` of an example: `<example> demo` runs `demo` on a tokio
/// runtime of its own; any other command goes to `horsies::cli::run(app)`,
/// which serves the worker's children first, then `worker`, `scheduler` and
/// `check`.
pub fn main_of<F, Fut>(app: Horsies, demo: F) -> ExitCode
where
    F: FnOnce(Horsies) -> Fut,
    Fut: Future<Output = DemoResult>,
{
    match std::env::args_os().nth(1) {
        Some(command) if command == "demo" => run_demo(app, demo),
        Some(_) | None => horsies::cli::run(app),
    }
}

/// Run `demo` to its end on a new runtime; print its failure.
fn run_demo<F, Fut>(app: Horsies, demo: F) -> ExitCode
where
    F: FnOnce(Horsies) -> Fut,
    Fut: Future<Output = DemoResult>,
{
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("the demo runtime did not start: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(demo(app)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("the demo failed: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Print the build failure of an app and return the exit status 1.
pub fn app_refused(error: &horsies::HorsiesError) -> ExitCode {
    eprintln!("{error}");
    ExitCode::FAILURE
}
