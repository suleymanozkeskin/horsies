//! Log targets of core records that the Python client logs on a logger other
//! than the one of the record's Rust module.
//!
//! A core event's target is its Rust module path unless the event names one.
//! The Python bridge sends a record whose target is the name of a `horsies`
//! logger to that logger (`crates/horsies-python/src/logging/names.rs`), so
//! each record below reaches the logger that the Python client used for the
//! same write. A Rust subscriber filters these records by these names, not
//! by their module path.

/// The stale RUNNING recovery pass: a candidate whose crash result does not
/// serialize, a candidate whose statement fails, each outcome, a failed
/// retry wake, and a full scan batch. Python logs them on `horsies.broker`
/// (`PostgresBroker.mark_stale_tasks_as_failed`).
pub const STALE_RUNNING_PASS: &str = "horsies.broker";

/// The failed attempt writes under the claimed-row rules `REFUSE` and
/// `RELEASE`. Python logs them on `horsies.lifecycle`
/// (`horsies.core.worker.failed_attempt`).
pub const FAILED_ATTEMPT: &str = "horsies.lifecycle";

/// Phase 1 of a workflow task's success. Python logs it on
/// `horsies.lifecycle` (`horsies.core.worker.workflow_success`).
pub const WORKFLOW_SUCCESS: &str = "horsies.lifecycle";

/// The outcome of each terminal write of a terminalization command, the
/// success batch included. Python logs every one on `horsies.lifecycle`
/// (`horsies.core.lifecycle.persistence`).
pub const TERMINALIZATION_OUTCOME: &str = "horsies.lifecycle";
