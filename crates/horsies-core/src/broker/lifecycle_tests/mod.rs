//! Ports of the Python lifecycle and terminalization tests.
//!
//! Python is the specification of the core. Each file here ports one Python
//! test file of `tests/integration/`; each inner module ports one Python test
//! class, and each test keeps the Python test's name. A test whose behaviour
//! the core does not have yet is `#[ignore]` and names its issue.

mod autocommit_outcomes;
mod claim_per_class;
mod claim_start_release;
mod failed_attempt_finish;
mod failed_attempt_round_trips;
mod finalize_batch_rows;
mod finalize_status_guard;
mod fresh_schema_rules;
mod lifecycle_regressions;
mod soft_cap_start_check;
mod stale_recovery_functions;
mod status_events;
mod success_batch_write;
pub(crate) mod support;
mod task_attempt_history;
mod task_history_terminalization_cancellation;
mod task_history_terminalization_expiry;
mod task_history_terminalization_failure;
mod task_history_terminalization_move;
mod task_history_terminalization_prototypes;
mod task_history_terminalization_workflow_nodes;
mod task_lock_order;
mod task_row_plans;
mod terminal_writer_characterization;
mod terminalization_claim_fence;
mod terminalization_functions;
mod workflow_success_finish;
