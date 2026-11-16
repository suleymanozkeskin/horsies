//! Rows that the broker statements read.
//!
//! No type here reads `horsies_worker_states`. The worker state reads of
//! `PostgresBroker` answer `WorkerStateSnapshot`, and the crate exports no row
//! type of that table. Each path below fails to compile:
//!
//! ```compile_fail
//! use horsies_core::WorkerStateRow;
//! ```
//!
//! ```compile_fail
//! use horsies_core::broker::WorkerStateRow;
//! ```
//!
//! ```compile_fail
//! use horsies_core::broker::row::worker_state::WorkerStateRow;
//! ```

pub mod heartbeat;
pub mod task;
pub mod workflow;
