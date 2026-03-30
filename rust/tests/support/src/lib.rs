#![allow(clippy::unwrap_used)]

//! Shared test infrastructure for horsies integration and e2e tests.
//!
//! Mirrors Python's `tests/conftest.py` and `tests/integration/conftest.py`.
//!
//! # Usage
//!
//! Add to `[dev-dependencies]`:
//! ```toml
//! horsies-test-support = { path = "../horsies-test-support" }
//! ```
//!
//! Requires `DATABASE_URL` env var or defaults to local test DB.

pub mod db;
pub mod e2e;
pub mod fixtures;
pub mod tasks;
pub mod workflow_helpers;
