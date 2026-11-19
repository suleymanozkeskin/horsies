//! Where the core gets the spec of a sub-workflow child.
//!
//! A workflow start pre-builds the child of each sub-workflow root
//! (`start_statement.rs`), and a step that promotes a sub-workflow node owes
//! its child start (`child_start.rs`). Both ask the client's
//! [`ChildSpecSource`] for the child's spec: the client builds the child in
//! its own language, such as the `build_with` of the Python client, which
//! the bridge runs on its callback thread. A worker, a handle or a reaper
//! holds the source as `Arc<dyn ChildSpecSource>` and lends it to each step
//! as `&dyn ChildSpecSource`. The answer is a [`ClientSpec`], the spec that
//! the client sent as data, with the retention class of each task queue that
//! the client resolved.
//!
//! The core checks the depth limit before it asks: a child past
//! [`crate::workflow_engine::start::MAX_SUBWORKFLOW_DEPTH`] fails its node,
//! and no source runs.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use uuid::Uuid;

use crate::core::workflow::node::SubWorkflowParams;
use crate::core::{TaskError, WorkflowSpec};
use crate::workflow_engine::error::WorkflowError;
pub use crate::workflow_engine::steps::ChildStartSource;

/// The future of one answer of a [`ChildSpecSource`].
pub type ChildSpecFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ClientChildSpec, ChildSpecUnavailable>> + Send + 'a>>;

/// A client that builds the child spec of a sub-workflow node.
///
/// The core calls it inside the step that needs the child, with the step's
/// transaction open: the answer must not write to the database. A source is
/// shared by every step of a process, so it is `Send` and `Sync`.
pub trait ChildSpecSource: Send + Sync {
    /// The child spec of the node of `request`, or the error that the node
    /// stores when the child cannot start.
    ///
    /// Failure exits: [`ChildSpecUnavailable`]. The step that asked rolls
    /// back; nothing of it commits.
    fn child_spec(&self, request: ChildSpecRequest) -> ChildSpecFuture<'_>;
}

/// The sub-workflow node whose child a source builds, with the facts that
/// the build reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildSpecRequest {
    /// The workflow of the node.
    pub parent_workflow_id: Uuid,
    /// The name of that workflow: the node errors name the workflow by it.
    pub parent_workflow_name: String,
    /// The index of the sub-workflow node.
    pub task_index: i32,
    /// The depth of the child, within the depth limit.
    pub child_depth: i32,
    /// The definition key that resolves the child; `None` when the node
    /// stores none (the load then fails the node).
    pub definition_key: Option<String>,
    /// Whether the node gives the child params.
    pub params: SubWorkflowParams,
    /// The node's stored input, as the client encoded it.
    pub node_input: String,
    /// The node's `args_from` as JSON text (member name to dependency
    /// index); `None` when it has none. A root has none.
    pub args_from: Option<String>,
    /// The terminal dependencies of the node, whose results the `args_from`
    /// members take. A root has none.
    pub sources: Vec<ChildStartSource>,
}

/// A spec that a client sent as data, read by the core: the spec, and the
/// stored retention class key of each queue of its task nodes, as the
/// client's queue mapping resolves it. Only the parse of the spec data
/// builds it, and the parse checks that every task queue has a class.
#[derive(Debug, Clone)]
pub struct ClientSpec {
    pub(crate) spec: WorkflowSpec,
    pub(crate) queue_classes: BTreeMap<String, String>,
}

impl ClientSpec {
    /// The spec and the classes. Only the parse of the spec data calls it.
    pub(crate) fn new(spec: WorkflowSpec, queue_classes: BTreeMap<String, String>) -> Self {
        Self {
            spec,
            queue_classes,
        }
    }

    /// The workflow name of the spec.
    pub fn name(&self) -> &str {
        &self.spec.name
    }

    /// The spec.
    pub fn spec(&self) -> &WorkflowSpec {
        &self.spec
    }
}

/// The answer of a [`ChildSpecSource`].
#[derive(Debug, Clone)]
pub enum ClientChildSpec {
    /// The child built: its spec.
    Built(ClientSpec),
    /// The child cannot start, and its node fails. `message` is the error
    /// text for the log; `failure_result` is the result envelope that the
    /// node stores, as the client encoded it.
    NodeFails {
        message: String,
        failure_result: String,
    },
}

/// Why a [`ChildSpecSource`] gave no answer. The step that asked rolls back.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChildSpecUnavailable {
    /// The build did not run: the client could not take it now (its callback
    /// queue was full, its callback thread did not start, its interpreter is
    /// shutting down). Transient: the step runs again through its retry or
    /// through workflow recovery.
    #[error("{subject} did not run: {cause}")]
    NotRun { subject: String, cause: String },
    /// The build was interrupted (the Python client's `KeyboardInterrupt` or
    /// `SystemExit`). Not retryable: the caller raises the interrupt again.
    #[error("{subject} was interrupted: {cause}")]
    Interrupted { subject: String, cause: String },
    /// The build raised an exception of the client that is not an interrupt
    /// (the Python client's `Exception`, such as `SerializationError` for task
    /// options of a child root that do not parse). It is not a defect of the
    /// core. Not retryable: the same build raises again. `cause` is the type
    /// and the text of the exception; a client that keeps the exception
    /// raises it again to the caller of the operation.
    #[error("{subject} raised {cause}")]
    Raised { subject: String, cause: String },
}

impl ChildSpecUnavailable {
    /// Whether the same step can succeed later. Pure.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::NotRun { .. } => true,
            Self::Interrupted { .. } | Self::Raised { .. } => false,
        }
    }
}

/// The retention class key that a task node on `queue` stores: the key that
/// the client resolved for the queue (`classes`, by queue name, from
/// [`ClientSpec`]), as the client wrote it. Pure.
///
/// Errors: [`WorkflowError::Validation`] for a queue with no class. The
/// parse of the spec data refuses such a spec, so this names a spec that did
/// not come from that parse.
pub(crate) fn queue_class_key<'c>(
    classes: &'c BTreeMap<String, String>,
    queue: &str,
) -> Result<&'c str, WorkflowError> {
    match classes.get(queue) {
        Some(key) => Ok(key.as_str()),
        None => Err(WorkflowError::Validation(format!(
            "the client resolved no retention class for queue {queue:?} of the spec"
        ))),
    }
}

/// Why the child of a node cannot start: the error that the node stores.
#[derive(Debug, Clone)]
pub(crate) enum NodeFailure {
    /// An error that the core made.
    Core(TaskError),
    /// An error that the client made: its text for the log, and the result
    /// envelope that the node stores, as the client encoded it.
    Client {
        message: String,
        failure_result: String,
    },
}

impl NodeFailure {
    /// The result envelope that the node stores.
    ///
    /// Errors: [`WorkflowError::Serialization`] when a core error does not
    /// encode.
    pub(crate) fn into_stored_result(self) -> Result<String, WorkflowError> {
        match self {
            Self::Core(error) => {
                crate::workflow_engine::start_statement::node_failure_result(error)
            }
            Self::Client { failure_result, .. } => Ok(failure_result),
        }
    }
}

/// The error text of a node failure, for the log: the core's error with its
/// code (`[CODE] message`), or the client's text.
impl std::fmt::Display for NodeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Core(error) => write!(f, "{error}"),
            Self::Client { message, .. } => f.write_str(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_build_that_did_not_run_is_retryable_and_an_interrupt_is_not() {
        let not_run = ChildSpecUnavailable::NotRun {
            subject: "build_with of definition 'report.v1'".to_owned(),
            cause: "the callback queue holds 1024 callbacks".to_owned(),
        };
        let interrupted = ChildSpecUnavailable::Interrupted {
            subject: "build_with of definition 'report.v1'".to_owned(),
            cause: "KeyboardInterrupt: ".to_owned(),
        };
        assert!(not_run.is_retryable());
        assert!(!interrupted.is_retryable());
        assert_eq!(
            not_run.to_string(),
            "build_with of definition 'report.v1' did not run: the callback queue holds 1024 \
             callbacks"
        );
    }

    #[test]
    fn a_build_that_raised_is_not_retryable_and_names_the_exception() {
        let raised = ChildSpecUnavailable::Raised {
            subject: "build of the child of nightly:0 (definition report.v1)".to_owned(),
            cause: "SerializationError: Corrupt task_options".to_owned(),
        };
        assert!(!raised.is_retryable());
        assert_eq!(
            raised.to_string(),
            "build of the child of nightly:0 (definition report.v1) raised SerializationError: \
             Corrupt task_options"
        );
    }

    #[test]
    fn a_client_queue_without_a_class_is_refused() {
        let classes = BTreeMap::from([("default".to_owned(), "standard_30d".to_owned())]);
        assert_eq!(
            queue_class_key(&classes, "default").expect("mapped"),
            "standard_30d"
        );
        assert!(matches!(
            queue_class_key(&classes, "bulk"),
            Err(WorkflowError::Validation(message)) if message.contains("\"bulk\"")
        ));
    }

    #[test]
    fn a_client_failure_stores_the_client_envelope_as_written() {
        let failure = NodeFailure::Client {
            message: "Failed to load subworkflow definition for parent:1".to_owned(),
            failure_result: r#"{"err":{"error_code":"SUBWORKFLOW_LOAD_FAILED"}}"#.to_owned(),
        };
        assert_eq!(
            failure.to_string(),
            "Failed to load subworkflow definition for parent:1"
        );
        assert_eq!(
            failure.into_stored_result().expect("stored"),
            r#"{"err":{"error_code":"SUBWORKFLOW_LOAD_FAILED"}}"#
        );
    }
}
