//! The points of the end of an attempt: the end reached the worker, and its
//! write returned.
//!
//! | Point | Event | Metrics |
//! |---|---|---|
//! | [`WorkerTelemetry::attempt_ended`], a task that reached no child | `ClaimReleased` | `horsies.task.claims.released` |
//! | [`WorkerTelemetry::attempt_ended`], a stop before RUNNING | `AttemptNotStarted` | `horsies.task.starts.refused` |
//! | [`WorkerTelemetry::attempt_ended`], a run | `AttemptEnded` | `horsies.task.attempts`, `horsies.task.attempts.end_not_known`, `horsies.task.run.duration` |
//! | [`WorkerTelemetry::attempt_recorded`] | `AttemptRecorded` | `horsies.task.record.duration`, `horsies.task.retries` |
//! | [`WorkerTelemetry::finalize_retried`] | none | `horsies.finalize.retries` |

use chrono::Utc;
use tokio::time::Instant;

use super::{TelemetryOn, WorkerTelemetry};
use crate::core::task::error::{error_code_label, OperationalErrorCode, TaskErrorCode};
use crate::events::{
    self, AttemptEnded, AttemptErrorCode, AttemptNotStarted, AttemptRecord, AttemptRecorded,
    AttemptStart, ChildResult, Event, RetryCount, RunTrace, WorkerDecision,
};
use crate::telemetry::declared::ErrorCodeValue;
use crate::telemetry::labels::ReleaseReason;
use crate::worker::executor::protocol::{ProcessId, StopReason};
use crate::worker::executor::slots::outcome::{EndView, ResultRead, WriteStage};
use crate::worker::executor::slots::{
    ChildPlacement, ClaimedTask, LoopKillCode, Placed, Settlement, TerminalState, WrittenEnd,
};
use crate::TaskAttemptOutcome;

/// What the end point noted for the record point of the same attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndNoted {
    /// The record point records nothing: telemetry is off, or the task
    /// reached no child.
    NothingToRecord,
    /// The end reached the worker at `seen`. A retry that the write of the
    /// end schedules counts under `error_code`, the label of the code that
    /// ended the attempt.
    Seen {
        seen: Instant,
        error_code: ErrorCodeValue,
    },
}

/// The start of a run that its child reported. The result frames of
/// protocol version 2 carry no start time, so every reported run reads as
/// `NotReported`. When the result frames carry `started_at`, this is the one
/// place that takes it.
fn reported_start() -> AttemptStart {
    AttemptStart::NotReported
}

/// How a run that reached a child ended.
#[derive(Debug, Clone, Copy)]
enum RanEnd<'a> {
    /// The child reported a result envelope.
    Result(&'a ResultRead),
    /// The worker code of the child failed.
    WorkerFailure,
    /// The worker ended the attempt.
    Decided(WorkerDecision),
    /// The child was killed and its exit was not seen.
    NotKnown(ProcessId),
}

/// What an end is, for its point.
enum EndKind<'a> {
    /// The task reached no child.
    NotDispatched(crate::worker::executor::slots::NotRunCause),
    /// The child stopped before it set the row RUNNING.
    NotStarted(StopReason),
    /// The task ran on a child.
    Ran(RanEnd<'a>),
}

/// The decision of the worker that a blocked loop made.
fn loop_decision(code: LoopKillCode) -> WorkerDecision {
    match code {
        LoopKillCode::TaskBlockedLoop => WorkerDecision::TaskBlockedLoop,
        LoopKillCode::NeighbourBlockedLoop => WorkerDecision::NeighbourBlockedLoop,
    }
}

fn kind_of(end: EndView<'_>) -> EndKind<'_> {
    match end {
        EndView::NotDispatched(cause) => EndKind::NotDispatched(cause),
        EndView::StoppedBeforeRunning(reason) => EndKind::NotStarted(reason),
        EndView::Result(read) => EndKind::Ran(RanEnd::Result(read)),
        EndView::WorkerFailure => EndKind::Ran(RanEnd::WorkerFailure),
        EndView::TimedOut => EndKind::Ran(RanEnd::Decided(WorkerDecision::TimedOut)),
        EndView::KilledForLoop(code) => EndKind::Ran(RanEnd::Decided(loop_decision(code))),
        EndView::Crashed => EndKind::Ran(RanEnd::Decided(WorkerDecision::ChildLost)),
        EndView::EndUnknown { pid } => EndKind::Ran(RanEnd::NotKnown(pid)),
    }
}

/// The code of a built-in failure.
fn built_in(code: OperationalErrorCode) -> TaskErrorCode {
    TaskErrorCode::BuiltIn(code.into())
}

/// How a run ended, as the event holds it.
fn event_end(ran: RanEnd<'_>, trace: &RunTrace, placed: &Placed) -> events::AttemptEnd {
    match ran {
        RanEnd::Result(read) => events::AttemptEnd::ReportedByChild {
            result: child_result(read),
            start: reported_start(),
        },
        RanEnd::WorkerFailure => events::AttemptEnd::ReportedByChild {
            result: ChildResult::WorkerFailure,
            start: reported_start(),
        },
        RanEnd::Decided(reason) => events::AttemptEnd::DecidedByWorker {
            reason,
            trace: trace.clone(),
            placed_at: placed.wall,
        },
        RanEnd::NotKnown(child_pid) => events::AttemptEnd::NotKnown { child_pid },
    }
}

fn child_result(read: &ResultRead) -> ChildResult {
    match read {
        ResultRead::Value => ChildResult::Completed,
        ResultRead::TaskError(code) => ChildResult::Failed {
            error_code: AttemptErrorCode::from(code),
        },
        ResultRead::Unreadable => ChildResult::Failed {
            error_code: AttemptErrorCode::BuiltIn {
                code: OperationalErrorCode::WorkerSerializationError.into(),
            },
        },
    }
}

/// The outcome of a run and the `error_code` label of its code.
enum RanOutcome {
    Known(TaskAttemptOutcome, ErrorCodeValue),
    /// The exit of the child was not seen: recovery decides the row.
    NotKnown,
}

/// What a settlement recorded, as the event holds it.
///
/// Errors: the retry count of a scheduled retry when it is not 1 or more:
/// the write of a retry adds 1 to a count of 0 or more.
fn record_of(settled: Settlement) -> Result<AttemptRecord, i32> {
    Ok(match settled {
        Settlement::Written(WrittenEnd::Terminal(TerminalState::Completed)) => {
            AttemptRecord::Completed
        }
        Settlement::Written(WrittenEnd::Terminal(TerminalState::Failed)) => AttemptRecord::Failed,
        Settlement::Written(WrittenEnd::RetryScheduled {
            retry_count,
            next_retry_at,
        }) => AttemptRecord::RetryScheduled {
            retry_count: u32::try_from(retry_count)
                .ok()
                .and_then(|count| RetryCount::try_from(count).ok())
                .ok_or(retry_count)?,
            next_retry_at,
        },
        Settlement::Written(WrittenEnd::ClaimReleased) => AttemptRecord::ClaimReleased,
        Settlement::NothingOwed => AttemptRecord::NothingOwed,
        Settlement::NotHeld => AttemptRecord::NotHeld,
        Settlement::LeftToRecovery => AttemptRecord::LeftToRecovery,
        Settlement::RetriesExhausted => AttemptRecord::RetriesExhausted,
        Settlement::WriteFailed => AttemptRecord::WriteFailed,
    })
}

impl WorkerTelemetry {
    /// The end of `task` reached the worker. A task that reached no child
    /// gives `ClaimReleased`; a stop before RUNNING gives
    /// `AttemptNotStarted`; a run gives `AttemptEnded` and its metrics, with
    /// the run time from `placement`. Returns what the record point of the
    /// same attempt needs.
    pub(crate) fn attempt_ended(
        &self,
        task: &ClaimedTask,
        placement: &ChildPlacement,
        end: EndView<'_>,
    ) -> EndNoted {
        match self {
            Self::Off => EndNoted::NothingToRecord,
            Self::On(on) => on.ended(task, placement, end),
        }
    }

    /// The write of the end of `task` returned `settled`: one
    /// `AttemptRecorded` and its metrics, after an end that `noted` saw.
    pub(crate) fn attempt_recorded(
        &self,
        task: &ClaimedTask,
        noted: EndNoted,
        settled: Settlement,
    ) {
        match (self, noted) {
            (Self::Off, _) | (_, EndNoted::NothingToRecord) => {}
            (Self::On(on), EndNoted::Seen { seen, error_code }) => {
                on.recorded(task, seen, error_code, settled);
            }
        }
    }

    /// A finalize write of `stage` failed with an error that may clear, and
    /// is sent again.
    pub(crate) fn finalize_retried(&self, stage: WriteStage) {
        match self {
            Self::Off => {}
            Self::On(on) => {
                let _recorded = on.metrics.finalize_retries.add((stage,), 1);
            }
        }
    }
}

impl TelemetryOn {
    /// The `error_code` label of `code` of task `task_name`.
    fn code_label(&self, task_name: &str, code: &TaskErrorCode) -> ErrorCodeValue {
        self.registry
            .labels()
            .error_codes
            .value_of(error_code_label(self.codes.of(task_name), code))
    }

    fn ended(&self, task: &ClaimedTask, placement: &ChildPlacement, end: EndView<'_>) -> EndNoted {
        let seen = Instant::now();
        match kind_of(end) {
            EndKind::NotDispatched(cause) => {
                self.released(
                    task.task_id,
                    &task.task_name,
                    &task.queue_name,
                    ReleaseReason::from(cause),
                );
                EndNoted::NothingToRecord
            }
            EndKind::NotStarted(reason) => {
                self.not_started(task, reason);
                EndNoted::Seen {
                    seen,
                    error_code: ErrorCodeValue::NO_ERROR_CODE,
                }
            }
            EndKind::Ran(ran) => match placement {
                ChildPlacement::Placed(placed) => self.ran(task, placed, ran, seen),
                ChildPlacement::NotWatched | ChildPlacement::NotPlaced => {
                    // A child sent this end, so it took the task: the
                    // placement of a watched task is told before its end.
                    tracing::error!(
                        task_id = %task.task_id,
                        attempt = task.retry.attempt().get(),
                        "the end of task {} came from a child with no placement; its end is \
                         not recorded",
                        task.task_id,
                    );
                    EndNoted::NothingToRecord
                }
            },
        }
    }

    fn not_started(&self, task: &ClaimedTask, reason: StopReason) {
        let _recorded = self.metrics.starts_refused.add((reason,), 1);
        let _emitted = self.emit(|| {
            Event::AttemptNotStarted(AttemptNotStarted {
                at: Utc::now(),
                task_id: task.task_id,
                task_name: events::TaskName::cut(&task.task_name),
                queue: events::QueueName::cut(&task.queue_name),
                attempt: task.retry.attempt(),
                reason,
            })
        });
    }

    /// The outcome of `ran` and the label of its code.
    fn outcome_of(&self, task_name: &str, ran: RanEnd<'_>) -> RanOutcome {
        let failed = |code: &TaskErrorCode| {
            RanOutcome::Known(TaskAttemptOutcome::Failed, self.code_label(task_name, code))
        };
        match ran {
            RanEnd::Result(ResultRead::Value) => {
                RanOutcome::Known(TaskAttemptOutcome::Completed, ErrorCodeValue::NO_ERROR_CODE)
            }
            RanEnd::Result(ResultRead::TaskError(code)) => failed(code),
            RanEnd::Result(ResultRead::Unreadable) => {
                failed(&built_in(OperationalErrorCode::WorkerSerializationError))
            }
            RanEnd::WorkerFailure => RanOutcome::Known(
                TaskAttemptOutcome::WorkerFailure,
                self.code_label(task_name, &built_in(OperationalErrorCode::BrokerError)),
            ),
            RanEnd::Decided(reason) => failed(&TaskErrorCode::BuiltIn(reason.error_code())),
            RanEnd::NotKnown(_) => RanOutcome::NotKnown,
        }
    }

    fn ran(&self, task: &ClaimedTask, placed: &Placed, ran: RanEnd<'_>, seen: Instant) -> EndNoted {
        let run_duration = seen.saturating_duration_since(placed.at);
        let task_label = self.task_label(&task.task_name);
        let queue_label = self.queue_label(&task.queue_name);
        let error_code = match self.outcome_of(&task.task_name, ran) {
            RanOutcome::Known(outcome, error_code) => {
                let _counted = self.metrics.attempts.add(
                    (task_label, queue_label, task.execution, outcome, error_code),
                    1,
                );
                let _observed = self.metrics.run_duration.observe(
                    (task_label, queue_label, task.execution, outcome),
                    run_duration,
                );
                error_code
            }
            RanOutcome::NotKnown => {
                let _counted = self
                    .metrics
                    .end_not_known
                    .add((task_label, queue_label, task.execution), 1);
                ErrorCodeValue::NO_ERROR_CODE
            }
        };
        let _emitted = self.emit(|| {
            Event::AttemptEnded(AttemptEnded {
                at: Utc::now(),
                task_id: task.task_id,
                task_name: events::TaskName::cut(&task.task_name),
                queue: events::QueueName::cut(&task.queue_name),
                execution: task.execution,
                attempt: task.retry.attempt(),
                end: event_end(ran, &task.trace, placed),
                run_duration,
            })
        });
        EndNoted::Seen { seen, error_code }
    }

    fn recorded(
        &self,
        task: &ClaimedTask,
        seen: Instant,
        error_code: ErrorCodeValue,
        settled: Settlement,
    ) {
        let record_duration = Instant::now().saturating_duration_since(seen);
        let _observed = self
            .metrics
            .record_duration
            .observe((task.execution,), record_duration);
        match settled {
            Settlement::Written(WrittenEnd::RetryScheduled { .. }) => {
                let _counted = self.metrics.retries.add(
                    (
                        self.task_label(&task.task_name),
                        self.queue_label(&task.queue_name),
                        error_code,
                    ),
                    1,
                );
            }
            Settlement::Written(WrittenEnd::Terminal(_) | WrittenEnd::ClaimReleased)
            | Settlement::NothingOwed
            | Settlement::NotHeld
            | Settlement::LeftToRecovery
            | Settlement::RetriesExhausted
            | Settlement::WriteFailed => {}
        }
        let record = match record_of(settled) {
            Ok(record) => record,
            Err(retry_count) => {
                tracing::error!(
                    task_id = %task.task_id,
                    attempt = task.retry.attempt().get(),
                    retry_count,
                    "the retry of task {} has retry count {retry_count}, not 1 or more; its \
                     AttemptRecorded is not emitted",
                    task.task_id,
                );
                return;
            }
        };
        let _emitted = self.emit(|| {
            Event::AttemptRecorded(AttemptRecorded {
                at: Utc::now(),
                task_id: task.task_id,
                task_name: events::TaskName::cut(&task.task_name),
                queue: events::QueueName::cut(&task.queue_name),
                attempt: task.retry.attempt(),
                record,
                record_duration,
            })
        });
    }
}
