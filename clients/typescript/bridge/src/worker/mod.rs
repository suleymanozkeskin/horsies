//! The worker parent: the worker manifest of an app, the preparation of a
//! worker from it, the run until a stop request, and the live check.
//!
//! - `manifest`: the sections of a worker manifest that the app config
//!   gives, with the bound of each manifest field. Pure.
//! - `start`: the members of a worker manifest that the start gives (the
//!   profile, the tasks, the handlers, the spans switch, the metrics
//!   endpoint, the child launch), and the check of a `--metrics-listen`
//!   address. Pure.
//! - `run`: `prepareWorker` (pure: the manifest, the profile choice) and the
//!   run of a prepared worker on the core's process runtime, with its stop
//!   request.
//! - `manifest_probe`: the test export that parses a whole manifest text
//!   (only with the cargo feature `test-probes`).
//! - `check`: the live check of `check --live`: the connectivity of the
//!   database and the connection fit of each worker profile.

pub mod check;
pub mod manifest;
#[cfg(feature = "test-probes")]
pub mod manifest_probe;
pub mod run;
pub mod start;
