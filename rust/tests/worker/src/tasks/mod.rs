//! E2E test task registry.
//!
//! Dispatches `register(app)` to a mode-specific module based on the app's
//! queue configuration. Mirrors Python's multi-instance pattern
//! (`instance.py`, `instance_custom.py`, `instance_recovery.py`).

use std::collections::HashSet;

use horsies::{Horsies, QueueMode};

pub mod custom_default_recovery;
pub mod custom_hnl;
pub mod default_mode;
pub mod defs;

// Re-export every task fn, input type, and `#[task]`-macro module so external
// tests can keep using `horsies_test_worker::tasks::{wf_step, ChildLabelInput, …}`.
pub use defs::*;

pub fn register(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
    match app.config().queue_mode {
        QueueMode::Default => default_mode::register(app),
        QueueMode::Custom => {
            let names: HashSet<&str> = app
                .config()
                .custom_queues
                .as_ref()
                .map(|qs| qs.iter().map(|q| q.name.as_str()).collect())
                .unwrap_or_default();

            if ["high", "normal", "low"].iter().all(|q| names.contains(q)) {
                custom_hnl::register(app)
            } else if names.contains("recovery") {
                custom_default_recovery::register(app)
            } else {
                Err(format!("no task-registration scheme for custom_queues: {:?}", names,).into())
            }
        }
    }
}
