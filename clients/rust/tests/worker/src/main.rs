//! The test worker binary: the worker, its children and the scheduler of
//! the end-to-end tests (Python `horsies worker tests.e2e.tasks.instance:app`).
//!
//! ```text
//! HORSIES_RUST_E2E_INSTANCE=default HORSIES_RUST_E2E_DATABASE_URL=postgresql://... \
//!     horsies-test-worker worker --profile isolated-1
//! ```
//!
//! Every line before `horsies::cli::run` runs in each child too: a child
//! builds the same app from the same environment.

use std::process::ExitCode;

use horsies_test_worker::build_app;
use horsies_test_worker::instance::{Connection, Instance};

fn main() -> ExitCode {
    let built = Instance::from_env().and_then(|instance| {
        let connection = Connection::from_env()?;
        build_app(instance, &connection).map_err(|error| error.to_string())
    });
    match built {
        Ok(app) => horsies::cli::run(app),
        Err(cause) => {
            eprintln!("horsies-test-worker: {cause}");
            ExitCode::FAILURE
        }
    }
}
