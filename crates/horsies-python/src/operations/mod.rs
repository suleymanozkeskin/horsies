//! The core operations exposed to Python, one module per area. Each module
//! holds the operation future, its argument and record classes, and the
//! conversion of its outcome; `handle.rs` binds them to `CoreHandle`.

pub mod arguments;
pub mod attempt_records;
pub mod cancel;
pub mod claim;
pub mod coverage;
pub mod enqueue;
pub mod expiry_and_orphans;
pub mod failed_attempt;
pub mod finalizing;
pub mod monitoring_reads;
pub mod monitoring_window;
pub mod paused_expiry;
pub mod pruning;
pub mod queue_starts;
pub mod release;
pub mod result;
pub mod schema;
pub mod stale_claims;
pub mod stale_running;
pub mod start;
pub mod success;
pub mod task_info;
pub mod task_lists;
pub mod worker_ping;
pub mod worker_reads;
pub mod workflow_lifecycle;
pub mod workflow_reads;
pub mod workflow_retention;
