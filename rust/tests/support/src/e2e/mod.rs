/// E2E test infrastructure for horsies-rust.
///
/// Mirrors Python's `tests/e2e/helpers/` — provides worker process management,
/// DB polling helpers, and assertion utilities.
///
/// Key difference from Python: Rust tasks are compiled into a test worker binary
/// (`horsies-test-worker`), not discovered at runtime via module import.
pub mod assertions;
pub mod db_poll;
pub mod worker;
pub mod workflow;
