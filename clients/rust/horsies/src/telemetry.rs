//! The telemetry of the client's task sends, workflow starts and task
//! bodies.
//!
//! - With the feature `otel`, the spans and the producer metrics go to the
//!   user's global OpenTelemetry providers (`horsies::otel`).
//! - Without it, the client makes no span and records no metric: a send and
//!   a start carry no trace carrier, a worker child runs its task bodies
//!   with no span (`BodyScope::NoScope`), and no
//!   OpenTelemetry crate is compiled.
//!
//! The send paths call the same three items in both builds: `TaskSendSpan`,
//! `WorkflowStartSpan` and `body_scope`.

use uuid::Uuid;

#[cfg(feature = "otel")]
pub(crate) use crate::otel::{body_scope, TaskSendSpan, WorkflowStartSpan};

#[cfg(not(feature = "otel"))]
pub(crate) use untraced::{body_scope, TaskSendSpan, WorkflowStartSpan};

/// The facts of one task send that its span carries.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TaskSendSubject<'a> {
    pub(crate) task_id: Uuid,
    pub(crate) task_name: &'a str,
    pub(crate) queue_name: &'a str,
}

/// The facts of one workflow start that its span carries.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WorkflowStartSubject<'a> {
    pub(crate) workflow_id: Uuid,
    pub(crate) workflow_name: &'a str,
}

/// How the call of a span ended: with success, or with the code `C` of its
/// error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallEnd<C> {
    Succeeded,
    Failed(C),
}

/// The telemetry of a client built without the feature `otel`: nothing.
#[cfg(not(feature = "otel"))]
mod untraced {
    use horsies_core::telemetry::trace_carrier::SendTrace;
    use horsies_core::{BodyScope, TaskSendErrorCode, WorkflowStartErrorCode};

    use super::{CallEnd, TaskSendSubject, WorkflowStartSubject};

    /// A task send with no span.
    #[derive(Debug)]
    pub(crate) struct TaskSendSpan;

    impl TaskSendSpan {
        /// No span is opened. Pure.
        pub(crate) fn open(_subject: &TaskSendSubject<'_>) -> TaskSendSpan {
            TaskSendSpan
        }

        /// The send carries no trace carrier.
        pub(crate) fn trace(&self) -> SendTrace {
            SendTrace::NoCarrier
        }

        /// Nothing is recorded.
        pub(crate) fn end(self, _end: CallEnd<TaskSendErrorCode>) {}
    }

    /// A workflow start with no span.
    #[derive(Debug)]
    pub(crate) struct WorkflowStartSpan;

    impl WorkflowStartSpan {
        /// No span is opened. Pure.
        pub(crate) fn open(_subject: &WorkflowStartSubject<'_>) -> WorkflowStartSpan {
            WorkflowStartSpan
        }

        /// The start carries no trace carrier.
        pub(crate) fn trace(&self) -> SendTrace {
            SendTrace::NoCarrier
        }

        /// Nothing is recorded.
        pub(crate) fn end(self, _end: CallEnd<WorkflowStartErrorCode>) {}
    }

    /// A worker child runs its task bodies with no span.
    pub(crate) fn body_scope() -> BodyScope {
        BodyScope::NoScope
    }
}

/// Without the feature `otel`: no span, no carrier, no body scope.
#[cfg(all(test, not(feature = "otel")))]
mod tests {
    use horsies_core::telemetry::trace_carrier::SendTrace;
    use horsies_core::{BodyScope, TaskSendErrorCode, WorkflowStartErrorCode};

    use super::*;

    #[test]
    fn without_the_otel_feature_a_send_and_a_start_carry_no_trace() {
        let send = TaskSendSpan::open(&TaskSendSubject {
            task_id: Uuid::new_v4(),
            task_name: "send_invoice",
            queue_name: "default",
        });
        assert_eq!(send.trace(), SendTrace::NoCarrier);
        send.end(CallEnd::Failed(TaskSendErrorCode::EnqueueFailed));
        let start = WorkflowStartSpan::open(&WorkflowStartSubject {
            workflow_id: Uuid::new_v4(),
            workflow_name: "nightly",
        });
        assert_eq!(start.trace(), SendTrace::NoCarrier);
        start.end(CallEnd::Failed(WorkflowStartErrorCode::EnqueueFailed));
    }

    #[test]
    fn without_the_otel_feature_a_child_runs_its_bodies_with_no_span() {
        assert!(matches!(body_scope(), BodyScope::NoScope));
    }
}
