//! The log and the counter of the reaper steps that fail.
//!
//! A reaper step never stops on a live worker: a step that fails runs again
//! later. The log of one step is:
//! - the first failure after a run without failure: one line with the cause;
//! - then one line per [`RECOVERY_FAILURE_LOG_EVERY`] failures in a row, with
//!   the count and the last cause;
//! - the first run without failure after failures: one line with the count.
//!
//! Each failure adds 1 to `horsies.recovery.pass.failures{step}` when the
//! worker records metrics. A run that did not happen (the orphan audit lock
//! was busy) changes nothing.

use std::fmt::Display;

use crate::telemetry::labels::RecoveryStep;
use crate::telemetry::registry::OptionalCounter;

/// The failures in a row of one step from one of its log lines to the next.
/// With the default check interval of 30 s, a step that fails at every pass
/// logs one line per 5 minutes.
pub const RECOVERY_FAILURE_LOG_EVERY: u64 = 10;

/// The log line of one result of a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureLogLine {
    /// The first failure after a run without failure: a line with the cause.
    FirstFailure,
    /// A failure whose count in a row is a multiple of
    /// [`RECOVERY_FAILURE_LOG_EVERY`]: a line with the count and the cause.
    StillFailing { failures: u64 },
    /// A failure between two lines: no line.
    Quiet,
    /// The first run without failure after `failures` failures in a row: a
    /// line with the count.
    Recovered { failures: u64 },
    /// A run without failure after a run without failure: no line.
    NoFailure,
}

/// The line of a failure that is the `failures`-th in a row (1 or more).
pub fn failure_line(failures: u64) -> FailureLogLine {
    match (failures, failures % RECOVERY_FAILURE_LOG_EVERY) {
        (1, _) => FailureLogLine::FirstFailure,
        (_, 0) => FailureLogLine::StillFailing { failures },
        (_, _) => FailureLogLine::Quiet,
    }
}

/// The line of a run without failure after `failures` failures in a row.
pub fn success_line(failures: u64) -> FailureLogLine {
    match failures {
        0 => FailureLogLine::NoFailure,
        failures => FailureLogLine::Recovered { failures },
    }
}

/// The level of the failure lines of a step. The pending expiry keeps the
/// warning level of its earlier line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureLevel {
    Error,
    Warning,
}

fn failure_level(step: RecoveryStep) -> FailureLevel {
    match step {
        RecoveryStep::PendingExpiry => FailureLevel::Warning,
        RecoveryStep::OwnerGone
        | RecoveryStep::StaleClaimedRequeue
        | RecoveryStep::OrphanTaskAudit
        | RecoveryStep::StaleRunningRecovery
        | RecoveryStep::WorkflowRecovery
        | RecoveryStep::Phase2Recovery
        | RecoveryStep::RetentionCleanup
        | RecoveryStep::EndedWorkerRows
        | RecoveryStep::PausedWorkflowExpiry
        | RecoveryStep::PartitionCoverage
        | RecoveryStep::KeyReservationCleanup
        | RecoveryStep::PartitionPruning => FailureLevel::Error,
    }
}

/// The failures in a row of each reaper step, and the failure counter. It
/// lives as long as the reaper loop.
pub struct RecoveryFailures {
    in_a_row: [u64; RecoveryStep::ALL.len()],
    counter: OptionalCounter<(RecoveryStep,)>,
}

impl RecoveryFailures {
    /// No failure yet; each failure adds to `counter`.
    pub fn new(counter: OptionalCounter<(RecoveryStep,)>) -> Self {
        Self {
            in_a_row: [0; RecoveryStep::ALL.len()],
            counter,
        }
    }

    /// A failure of `step` with its `cause`: count it and log it by the rule
    /// of this module.
    pub fn failed(&mut self, step: RecoveryStep, cause: &dyn Display) {
        let failures = self.in_a_row[step as usize].saturating_add(1);
        self.in_a_row[step as usize] = failures;
        match &self.counter {
            OptionalCounter::NotRecorded => {}
            OptionalCounter::Recording(counter) => {
                let _recorded = counter.add((step,), 1);
            }
        }
        log_failure(step, failure_line(failures), cause);
    }

    /// A run of `step` without failure: log the recovery after failures.
    pub fn succeeded(&mut self, step: RecoveryStep) {
        let failures = std::mem::replace(&mut self.in_a_row[step as usize], 0);
        match success_line(failures) {
            FailureLogLine::Recovered { failures } => {
                let step_name = step.as_str();
                tracing::info!(
                    step = step_name,
                    failures,
                    "Reaper step {step_name} ran without a failure after {failures} failures \
                     in a row",
                );
            }
            FailureLogLine::NoFailure
            | FailureLogLine::FirstFailure
            | FailureLogLine::StillFailing { .. }
            | FailureLogLine::Quiet => {}
        }
    }

    /// The failures in a row of `step`.
    #[cfg(test)]
    pub fn in_a_row(&self, step: RecoveryStep) -> u64 {
        self.in_a_row[step as usize]
    }
}

/// Write the line of a failure, if it has one.
fn log_failure(step: RecoveryStep, line: FailureLogLine, cause: &dyn Display) {
    let step_name = step.as_str();
    match (line, failure_level(step)) {
        (FailureLogLine::FirstFailure, FailureLevel::Error) => {
            tracing::error!(
                step = step_name,
                error = %cause,
                "Reaper step {step_name} failed and runs again later: {cause}",
            );
        }
        (FailureLogLine::FirstFailure, FailureLevel::Warning) => {
            tracing::warn!(
                step = step_name,
                error = %cause,
                "Reaper step {step_name} failed and runs again later: {cause}",
            );
        }
        (FailureLogLine::StillFailing { failures }, FailureLevel::Error) => {
            tracing::error!(
                step = step_name,
                failures,
                error = %cause,
                "Reaper step {step_name} failed {failures} times in a row and runs again \
                 later: {cause}",
            );
        }
        (FailureLogLine::StillFailing { failures }, FailureLevel::Warning) => {
            tracing::warn!(
                step = step_name,
                failures,
                error = %cause,
                "Reaper step {step_name} failed {failures} times in a row and runs again \
                 later: {cause}",
            );
        }
        (
            FailureLogLine::Quiet | FailureLogLine::Recovered { .. } | FailureLogLine::NoFailure,
            _,
        ) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::catalog::MetricId;
    use crate::telemetry::declared::{DeclaredNames, ErrorCodeValues};
    use crate::telemetry::registry::{
        ProcessFacts, Registry, RegistryLabels, SampleValue, WorkerFacts,
    };
    use crate::test_support::LogCapture;

    #[test]
    fn the_first_failure_and_each_multiple_of_the_period_have_a_line() {
        assert_eq!(failure_line(1), FailureLogLine::FirstFailure);
        for failures in 2..RECOVERY_FAILURE_LOG_EVERY {
            assert_eq!(failure_line(failures), FailureLogLine::Quiet, "{failures}");
        }
        assert_eq!(
            failure_line(RECOVERY_FAILURE_LOG_EVERY),
            FailureLogLine::StillFailing {
                failures: RECOVERY_FAILURE_LOG_EVERY
            }
        );
        assert_eq!(
            failure_line(RECOVERY_FAILURE_LOG_EVERY + 1),
            FailureLogLine::Quiet
        );
        assert_eq!(
            failure_line(2 * RECOVERY_FAILURE_LOG_EVERY),
            FailureLogLine::StillFailing {
                failures: 2 * RECOVERY_FAILURE_LOG_EVERY
            }
        );
    }

    #[test]
    fn a_run_without_failure_has_a_line_only_after_failures() {
        assert_eq!(success_line(0), FailureLogLine::NoFailure);
        assert_eq!(success_line(3), FailureLogLine::Recovered { failures: 3 });
    }

    /// The step error lines of `capture`: (level, message).
    fn lines(capture: &LogCapture) -> Vec<(tracing::Level, String)> {
        capture
            .events()
            .into_iter()
            .filter(|event| event.message.starts_with("Reaper step "))
            .map(|event| (event.level, event.message))
            .collect()
    }

    #[test]
    fn the_first_failure_logs_its_cause() {
        let capture = LogCapture::new();
        let _default = capture.set_default();
        let mut failures = RecoveryFailures::new(OptionalCounter::NotRecorded);

        failures.failed(RecoveryStep::Phase2Recovery, &"connection lost");

        assert_eq!(
            lines(&capture),
            vec![(
                tracing::Level::ERROR,
                "Reaper step phase2_recovery failed and runs again later: connection lost"
                    .to_owned()
            )]
        );
        let event = &capture.events()[0];
        assert_eq!(event.field("step"), Some("phase2_recovery"));
        assert_eq!(event.field("error"), Some("connection lost"));
        assert_eq!(failures.in_a_row(RecoveryStep::Phase2Recovery), 1);
    }

    #[test]
    fn failures_between_two_lines_log_nothing_and_each_period_logs_one_line() {
        let capture = LogCapture::new();
        let _default = capture.set_default();
        let mut failures = RecoveryFailures::new(OptionalCounter::NotRecorded);

        for _ in 0..(2 * RECOVERY_FAILURE_LOG_EVERY + 1) {
            failures.failed(RecoveryStep::PartitionPruning, &"leaf locked");
        }

        let still = |count: u64| {
            (
                tracing::Level::ERROR,
                format!(
                    "Reaper step partition_pruning failed {count} times in a row and runs \
                     again later: leaf locked"
                ),
            )
        };
        assert_eq!(
            lines(&capture),
            vec![
                (
                    tracing::Level::ERROR,
                    "Reaper step partition_pruning failed and runs again later: leaf locked"
                        .to_owned()
                ),
                still(RECOVERY_FAILURE_LOG_EVERY),
                still(2 * RECOVERY_FAILURE_LOG_EVERY),
            ]
        );
    }

    #[test]
    fn the_first_run_without_failure_after_failures_logs_one_line() {
        let capture = LogCapture::new();
        let _default = capture.set_default();
        let mut failures = RecoveryFailures::new(OptionalCounter::NotRecorded);
        for _ in 0..3 {
            failures.failed(RecoveryStep::OwnerGone, &"pool closed");
        }

        failures.succeeded(RecoveryStep::OwnerGone);
        failures.succeeded(RecoveryStep::OwnerGone);

        let recovered: Vec<(tracing::Level, String)> = lines(&capture)
            .into_iter()
            .filter(|(level, _)| *level == tracing::Level::INFO)
            .collect();
        assert_eq!(
            recovered,
            vec![(
                tracing::Level::INFO,
                "Reaper step owner_gone ran without a failure after 3 failures in a row".to_owned()
            )]
        );
        assert_eq!(failures.in_a_row(RecoveryStep::OwnerGone), 0);
        // After the recovery, the next failure is a first failure again.
        failures.failed(RecoveryStep::OwnerGone, &"pool closed");
        assert_eq!(
            lines(&capture).last(),
            Some(&(
                tracing::Level::ERROR,
                "Reaper step owner_gone failed and runs again later: pool closed".to_owned()
            ))
        );
    }

    #[test]
    fn a_run_without_failure_after_no_failure_logs_nothing() {
        let capture = LogCapture::new();
        let _default = capture.set_default();
        let mut failures = RecoveryFailures::new(OptionalCounter::NotRecorded);

        failures.succeeded(RecoveryStep::RetentionCleanup);

        assert_eq!(lines(&capture), Vec::new());
    }

    #[test]
    fn the_steps_count_their_failures_apart_and_pending_expiry_logs_a_warning() {
        let capture = LogCapture::new();
        let _default = capture.set_default();
        let mut failures = RecoveryFailures::new(OptionalCounter::NotRecorded);

        failures.failed(RecoveryStep::PendingExpiry, &"statement timeout");
        failures.failed(RecoveryStep::PendingExpiry, &"statement timeout");
        failures.failed(RecoveryStep::WorkflowRecovery, &"statement timeout");

        assert_eq!(failures.in_a_row(RecoveryStep::PendingExpiry), 2);
        assert_eq!(failures.in_a_row(RecoveryStep::WorkflowRecovery), 1);
        let levels: Vec<tracing::Level> = lines(&capture)
            .into_iter()
            .map(|(level, _)| level)
            .collect();
        assert_eq!(levels, vec![tracing::Level::WARN, tracing::Level::ERROR]);
    }

    #[test]
    fn each_failure_adds_one_to_the_counter_of_its_step() {
        let registry = Registry::new(
            &[MetricId::RecoveryPassFailures],
            RegistryLabels {
                tasks: DeclaredNames::none(),
                queues: DeclaredNames::none(),
                handlers: DeclaredNames::none(),
                schedules: DeclaredNames::none(),
                error_codes: ErrorCodeValues::none(),
                process: ProcessFacts::Worker(WorkerFacts {
                    version: "0.8.0".to_owned(),
                    profile: "default".to_owned(),
                    worker_id: "worker-1".to_owned(),
                }),
            },
        )
        .expect("the registry builds");
        let counter = registry
            .counter::<(RecoveryStep,)>(MetricId::RecoveryPassFailures)
            .expect("the handle of the metric");
        let mut failures = RecoveryFailures::new(OptionalCounter::Recording(counter));

        for _ in 0..(RECOVERY_FAILURE_LOG_EVERY + 2) {
            failures.failed(RecoveryStep::PartitionCoverage, &"stopped");
        }
        failures.succeeded(RecoveryStep::PartitionCoverage);
        failures.failed(RecoveryStep::PartitionCoverage, &"stopped");
        failures.failed(RecoveryStep::KeyReservationCleanup, &"stopped");

        let mut counts: Vec<(Vec<String>, SampleValue)> = registry
            .snapshot()
            .into_iter()
            .find(|family| family.metric == MetricId::RecoveryPassFailures)
            .expect("the registry holds the metric")
            .series
            .into_iter()
            .map(|series| (series.label_texts, series.value))
            .collect();
        counts.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            counts,
            vec![
                (
                    vec!["key_reservation_cleanup".to_owned()],
                    SampleValue::Counter(1)
                ),
                (
                    vec!["partition_coverage".to_owned()],
                    SampleValue::Counter(RECOVERY_FAILURE_LOG_EVERY + 3)
                ),
            ]
        );
    }
}
