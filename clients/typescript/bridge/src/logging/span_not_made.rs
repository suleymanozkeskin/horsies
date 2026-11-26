//! The record of a span that the package did not make, or did not
//! complete: the user's OpenTelemetry SDK threw at the span of a send, a
//! workflow start, an attempt in a Node child or in a seat isolate, or an
//! attempt that the worker ended; or the handler child could not hand a
//! worker-ended span to JavaScript.
//!
//! - One form for every kind: one record at WARNING on `horsies.telemetry`
//!   (the logger and the level of the Python client), through the core
//!   subscriber of the process, so the thresholds, the text bound and the
//!   sink of the core log apply. A field names the task or the workflow.
//! - Once per process and kind: the first span of each kind that is not made
//!   is logged; later ones are not. The flags are statics of this addon, one
//!   set per process. A seat isolate loads no addon: it sends its report to
//!   the main thread of its child, which calls [`log_span_not_made`].

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::logging::bridge::{started_log, LogBridge};
use crate::process_core::runtime::{CallRefusal, StartFailure};

/// A span that the package did not make or did not complete.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotMadeSpan {
    /// The span `send <task_name>` of a send of task `task_id`.
    Send { task_id: String },
    /// The span `start <workflow_name>` of the start of `workflow_id`.
    WorkflowStart { workflow_id: String },
    /// The attempt span of a body of task `task_id` in a Node child.
    Attempt { task_id: String },
    /// The attempt span of a body of task `task_id` in a seat isolate.
    SeatAttempt { task_id: String },
    /// The span of an attempt of task `task_id` that the worker ended, which
    /// the handler child makes.
    WorkerEnded { task_id: String },
}

/// The kinds of [`NotMadeSpan`].
pub const NOT_MADE_KINDS: usize = 5;

/// One flag per kind: whether a span of that kind was logged.
pub type NotMadeFlags = [AtomicBool; NOT_MADE_KINDS];

/// The flags of this process.
static PROCESS_FLAGS: NotMadeFlags = [const { AtomicBool::new(false) }; NOT_MADE_KINDS];

impl NotMadeSpan {
    /// The position of the kind of this span in a [`NotMadeFlags`].
    fn kind_index(&self) -> usize {
        match self {
            Self::Send { .. } => 0,
            Self::WorkflowStart { .. } => 1,
            Self::Attempt { .. } => 2,
            Self::SeatAttempt { .. } => 3,
            Self::WorkerEnded { .. } => 4,
        }
    }
}

/// Write the WARNING record of `span`, not made for `cause` (the type and
/// the message of what the SDK threw), once per process and kind. Starts no
/// core; starts the log bridge of the process, and a forwarder thread that
/// cannot start throws `CORE_START_FAILED`.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
pub fn log_span_not_made(span: NotMadeSpan, cause: String) -> Envelope<()> {
    sync_call("log_span_not_made", || match started_log() {
        Err(failure) => Envelope::Thrown(
            CallRefusal::StartFailed(StartFailure::LogForwarderStartFailed {
                cause: failure.cause,
            })
            .thrown(),
        ),
        Ok(log) => {
            report_not_made(log, &span, &cause, &PROCESS_FLAGS);
            Envelope::Value(())
        }
    })
}

/// The report of a span that this addon did not make (a worker-ended span
/// that the handler child could not hand to JavaScript), with the flags of
/// the process. A log bridge that cannot start gives one stderr line.
pub fn report_span_not_made(span: &NotMadeSpan, cause: &str) {
    match started_log() {
        Ok(log) => report_not_made(log, span, cause, &PROCESS_FLAGS),
        Err(failure) => {
            let _written = writeln!(
                std::io::stderr(),
                "{} The core log did not start: {}.",
                not_made_text(span, cause),
                failure.cause
            );
        }
    }
}

/// Write the record of `span` on `log` when `flags` hold no span of its kind,
/// and mark the kind.
fn report_not_made(log: &LogBridge, span: &NotMadeSpan, cause: &str, flags: &NotMadeFlags) {
    match flags[span.kind_index()].swap(true, Ordering::Relaxed) {
        true => {}
        false => write_not_made(log, span, cause),
    }
}

/// The WARNING record of `span`, not made for `cause`, on `log`.
fn write_not_made(log: &LogBridge, span: &NotMadeSpan, cause: &str) {
    let text = not_made_text(span, cause);
    tracing::dispatcher::with_default(log.dispatch(), || match span {
        NotMadeSpan::WorkflowStart { workflow_id } => tracing::warn!(
            target: "horsies.telemetry",
            workflow_id = %workflow_id,
            "{text}"
        ),
        NotMadeSpan::Send { task_id }
        | NotMadeSpan::Attempt { task_id }
        | NotMadeSpan::SeatAttempt { task_id }
        | NotMadeSpan::WorkerEnded { task_id } => tracing::warn!(
            target: "horsies.telemetry",
            task_id = %task_id,
            "{text}"
        ),
    });
}

/// The text of the record of `span`, not made for `cause`. Pure.
fn not_made_text(span: &NotMadeSpan, cause: &str) -> String {
    let (subject, goes_on) = match span {
        NotMadeSpan::Send { task_id } => {
            (format!("the send of task {task_id}"), "The send goes on")
        }
        NotMadeSpan::WorkflowStart { workflow_id } => (
            format!("the start of workflow {workflow_id}"),
            "The start goes on",
        ),
        NotMadeSpan::Attempt { task_id } => {
            (format!("the attempt of task {task_id}"), "The run goes on")
        }
        NotMadeSpan::SeatAttempt { task_id } => (
            format!("the attempt of task {task_id} in a seat isolate"),
            "The run goes on",
        ),
        NotMadeSpan::WorkerEnded { task_id } => (
            format!("the attempt of task {task_id} that the worker ended"),
            "The event still goes to the handlers",
        ),
    };
    format!(
        "the span of {subject} was not made or is not complete: {cause}. {goes_on}. Later spans \
         of this kind that are not made are not logged."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::bridge::{CoreLogRecord, FieldValue, RecordField};
    use crate::logging::levels::RecordLevel;
    use crate::logging::names::LoggerName;

    const WORKFLOW_ID: &str = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b";
    const TASK_ID: &str = "0192f6a1-7b3c-7d4e-8f00-0123456789ab";

    fn new_flags() -> NotMadeFlags {
        [const { AtomicBool::new(false) }; NOT_MADE_KINDS]
    }

    /// The one record that a report of `span` writes on a new bridge with new
    /// flags.
    fn record_of(span: &NotMadeSpan) -> CoreLogRecord {
        let (log, records) = LogBridge::new();
        report_not_made(&log, span, "Error: the SDK is broken", &new_flags());
        let record = records.try_recv().expect("one record");
        assert!(records.try_recv().is_err(), "one record only");
        record
    }

    fn task_field() -> Vec<RecordField> {
        vec![RecordField {
            name: "task_id",
            value: FieldValue::Text(TASK_ID.to_owned()),
        }]
    }

    fn assert_form(record: &CoreLogRecord, subject: &str, goes_on: &str) {
        assert_eq!(record.logger, LoggerName::Telemetry);
        assert_eq!(record.level, RecordLevel::Warning);
        assert_eq!(
            record.message,
            format!(
                "the span of {subject} was not made or is not complete: Error: the SDK is \
                 broken. {goes_on}. Later spans of this kind that are not made are not logged."
            )
        );
    }

    #[test]
    fn a_send_span_is_one_warning_on_the_telemetry_logger() {
        let record = record_of(&NotMadeSpan::Send {
            task_id: TASK_ID.to_owned(),
        });
        assert_form(
            &record,
            &format!("the send of task {TASK_ID}"),
            "The send goes on",
        );
        assert_eq!(record.fields, task_field());
    }

    #[test]
    fn a_start_span_is_one_warning_on_the_telemetry_logger() {
        let record = record_of(&NotMadeSpan::WorkflowStart {
            workflow_id: WORKFLOW_ID.to_owned(),
        });
        assert_form(
            &record,
            &format!("the start of workflow {WORKFLOW_ID}"),
            "The start goes on",
        );
        assert_eq!(
            record.fields,
            vec![RecordField {
                name: "workflow_id",
                value: FieldValue::Text(WORKFLOW_ID.to_owned()),
            }]
        );
    }

    #[test]
    fn an_attempt_span_of_a_node_child_is_one_warning_on_the_telemetry_logger() {
        let record = record_of(&NotMadeSpan::Attempt {
            task_id: TASK_ID.to_owned(),
        });
        assert_form(
            &record,
            &format!("the attempt of task {TASK_ID}"),
            "The run goes on",
        );
        assert_eq!(record.fields, task_field());
    }

    #[test]
    fn an_attempt_span_of_a_seat_isolate_is_one_warning_on_the_telemetry_logger() {
        let record = record_of(&NotMadeSpan::SeatAttempt {
            task_id: TASK_ID.to_owned(),
        });
        assert_form(
            &record,
            &format!("the attempt of task {TASK_ID} in a seat isolate"),
            "The run goes on",
        );
        assert_eq!(record.fields, task_field());
    }

    #[test]
    fn a_worker_ended_span_is_one_warning_on_the_telemetry_logger() {
        let record = record_of(&NotMadeSpan::WorkerEnded {
            task_id: TASK_ID.to_owned(),
        });
        assert_form(
            &record,
            &format!("the attempt of task {TASK_ID} that the worker ended"),
            "The event still goes to the handlers",
        );
        assert_eq!(record.fields, task_field());
    }

    #[test]
    fn the_first_span_of_each_kind_is_logged_and_later_ones_are_not() {
        let (log, records) = LogBridge::new();
        let flags = new_flags();
        let send = NotMadeSpan::Send {
            task_id: TASK_ID.to_owned(),
        };
        let attempt = NotMadeSpan::Attempt {
            task_id: TASK_ID.to_owned(),
        };
        report_not_made(&log, &send, "Error: first", &flags);
        report_not_made(&log, &send, "Error: second", &flags);
        report_not_made(&log, &attempt, "Error: first", &flags);
        let messages: Vec<String> = records.try_iter().map(|record| record.message).collect();
        assert_eq!(messages.len(), 2, "{messages:?}");
        assert!(
            messages[0].starts_with("the span of the send of task"),
            "{messages:?}"
        );
        assert!(
            messages[1].starts_with("the span of the attempt of task"),
            "{messages:?}"
        );
    }
}
