//! Database tests of the baseline SQL (`crates/horsies-core/baseline/`).
//!
//! - The case files are in `cases/`. The tests compile them in with
//!   `include_str!`, so the files ship with the crate.
//! - Each test creates the databases it uses ([`harness::BaselineDatabase`]):
//!   the baseline migration text applied to an empty database in one
//!   transaction, with no reader publication and no client family row unless
//!   the test applies them.
//! - A case file runs as one simple-query message. A failed case raises, and
//!   the server error fails the test with the case name in it.

mod baseline_lifecycle_functions;
mod baseline_scheduler_monitoring_sql;
mod baseline_sql;
mod baseline_workflow_functions;
mod harness;
mod history_functions_sql;
mod history_maintenance_sql;
