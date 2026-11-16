//! Ports of the Python lifecycle and terminalization tests.
//!
//! Python is the specification of the core. Each file here ports one Python
//! test file of `tests/integration/`; each inner module ports one Python test
//! class, and each test keeps the Python test's name. A test whose behaviour
//! the core does not have yet is `#[ignore]` and names its issue.

mod autocommit_outcomes;
mod finalize_status_guard;
mod support;
mod terminal_writer_characterization;
mod terminalization_claim_fence;
mod terminalization_functions;
