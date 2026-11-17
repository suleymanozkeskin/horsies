//! The shipped `horsies` binary: `web`, `get-docs` and `skills install`.
//! The worker, the scheduler and the check of an app run from the app's own
//! binary through `horsies::cli::run(app)`.

use std::process::ExitCode;

fn main() -> ExitCode {
    horsies::tools::run()
}
