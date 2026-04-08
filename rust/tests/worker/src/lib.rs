#![allow(clippy::unwrap_used)]

//! Shared task definitions for e2e tests.
//!
//! Exposes generated task modules so integration tests can use typed
//! `node()` / `node_with()` helpers instead of raw `TaskNode::new(...)`.

pub mod tasks;
