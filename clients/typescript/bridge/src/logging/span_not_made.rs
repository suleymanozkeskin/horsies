//! The record of a span that the package did not make: the user's
//! OpenTelemetry SDK threw at the open or the end of the span of a workflow
//! start, or of the attempt span of a body in a seat isolate.
//!
//! - The form is the one of a worker-ended span that is not made
//!   (`handler_child/ended_spans.rs`): one record at ERROR with the id of
//!   the start or the task as a field, through the core subscriber of the
//!   process, so the thresholds, the text bound and the sink of the core log
//!   apply.
//! - The span of a start goes to `horsies.app`: a start runs in the app, not
//!   in a worker. The span of a seat attempt goes to `horsies.worker`.
//! - The package calls this once per process and kind of span; a seat
//!   isolate, which loads no addon, sends its report to the main thread of
//!   its child, which calls it.

use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::logging::bridge::{started_log, LogBridge};
use crate::process_core::runtime::{CallRefusal, StartFailure};

/// A span that the package did not make.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotMadeSpan {
    /// The span `start <workflow_name>` of the start of `workflow_id`.
    WorkflowStart { workflow_id: String },
    /// The attempt span of a body of task `task_id` in a seat isolate.
    SeatAttempt { task_id: String },
}

/// Write the ERROR record of `span`, not made for `cause` (the type and the
/// message of what the SDK threw). Starts no core; starts the log bridge of
/// the process, and a forwarder thread that cannot start throws
/// `CORE_START_FAILED`.
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
            write_not_made(log, &span, &cause);
            Envelope::Value(())
        }
    })
}

/// The ERROR record of `span`, not made for `cause`, on `log`.
fn write_not_made(log: &LogBridge, span: &NotMadeSpan, cause: &str) {
    tracing::dispatcher::with_default(log.dispatch(), || match span {
        NotMadeSpan::WorkflowStart { workflow_id } => tracing::error!(
            target: "horsies.app",
            workflow_id = %workflow_id,
            "the span of the start of workflow {workflow_id} was not made: {cause}. The start \
             goes on with no carrier; this span is not complete. Later spans of a start that \
             are not made are not logged."
        ),
        NotMadeSpan::SeatAttempt { task_id } => tracing::error!(
            target: "horsies.worker",
            task_id = %task_id,
            "the span of the attempt of task {task_id} in a seat isolate was not made: \
             {cause}. The run goes on; this span is not complete. Later attempt spans of a \
             seat isolate that are not made are not logged."
        ),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::bridge::{FieldValue, RecordField};
    use crate::logging::levels::RecordLevel;
    use crate::logging::names::LoggerName;

    const WORKFLOW_ID: &str = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b";
    const TASK_ID: &str = "0192f6a1-7b3c-7d4e-8f00-0123456789ab";

    #[test]
    fn the_span_of_a_start_is_one_error_record_on_the_app_logger() {
        let (log, records) = LogBridge::new();
        let span = NotMadeSpan::WorkflowStart {
            workflow_id: WORKFLOW_ID.to_owned(),
        };
        write_not_made(&log, &span, "Error: the SDK is broken");
        let record = records.try_recv().expect("one record");
        assert_eq!(record.logger, LoggerName::App);
        assert_eq!(record.level, RecordLevel::Error);
        assert_eq!(
            record.message,
            format!(
                "the span of the start of workflow {WORKFLOW_ID} was not made: Error: the SDK \
                 is broken. The start goes on with no carrier; this span is not complete. Later \
                 spans of a start that are not made are not logged."
            )
        );
        assert_eq!(
            record.fields,
            vec![RecordField {
                name: "workflow_id",
                value: FieldValue::Text(WORKFLOW_ID.to_owned()),
            }]
        );
        assert!(records.try_recv().is_err(), "one record only");
    }

    #[test]
    fn the_span_of_a_seat_attempt_is_one_error_record_on_the_worker_logger() {
        let (log, records) = LogBridge::new();
        let span = NotMadeSpan::SeatAttempt {
            task_id: TASK_ID.to_owned(),
        };
        write_not_made(&log, &span, "TypeError: no exporter");
        let record = records.try_recv().expect("one record");
        assert_eq!(record.logger, LoggerName::Worker);
        assert_eq!(record.level, RecordLevel::Error);
        assert!(
            record.message.starts_with(&format!(
                "the span of the attempt of task {TASK_ID} in a seat isolate was not made: \
                 TypeError: no exporter."
            )),
            "{}",
            record.message
        );
        assert_eq!(
            record.fields,
            vec![RecordField {
                name: "task_id",
                value: FieldValue::Text(TASK_ID.to_owned()),
            }]
        );
    }
}
