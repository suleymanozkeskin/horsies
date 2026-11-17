//! Where the core gets the spec of a sub-workflow child.
//!
//! A workflow start pre-builds the child of each sub-workflow root
//! (`start_statement.rs`), and a step that promotes a sub-workflow node owes
//! its child start (`child_start.rs`). Both ask for the child's spec in the
//! same way ([`crate::workflow_engine::steps::ChildBuild`]):
//!
//! - `Registry`: the registered definitions of the Rust client
//!   ([`crate::core::registry::workflow::WorkflowSpecRegistry`]); the
//!   retention config of the process gives the class of each task queue.
//! - `Client`: a [`ChildSpecSource`] of a client that builds the child in its
//!   own language, such as the `build_with` of the Python client, which the
//!   bridge runs on its callback thread. The answer is a [`ClientSpec`], the
//!   spec that the client sent as data, with the retention class of each
//!   task queue that the client resolved.
//!
//! The core checks the depth limit before it asks: a child past
//! [`crate::workflow_engine::start::MAX_SUBWORKFLOW_DEPTH`] fails its node,
//! and no source runs.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use uuid::Uuid;

use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::workflow::node::SubWorkflowParams;
use crate::core::{TaskError, WorkflowSpec};
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::steps::ChildBuild;
pub use crate::workflow_engine::steps::ChildStartSource;

/// Where the child builds of one process get their specs: the registered
/// definitions of the Rust client, or the client's [`ChildSpecSource`]. A
/// worker, a handle or a reaper holds one and lends it to each step.
#[derive(Clone)]
pub enum ChildSpecs {
    /// The registered definitions of the Rust client.
    Registry(Arc<WorkflowSpecRegistry>),
    /// A client that builds each child in its own language.
    Client(Arc<dyn ChildSpecSource>),
}

impl From<Arc<WorkflowSpecRegistry>> for ChildSpecs {
    /// The registered definitions of the Rust client.
    fn from(registry: Arc<WorkflowSpecRegistry>) -> Self {
        Self::Registry(registry)
    }
}

impl ChildSpecs {
    /// The build that a step takes: the task queues of a registry child take
    /// their class from `retention`; a client child carries its own classes.
    pub(crate) fn build<'a>(&'a self, retention: &'a RetentionConfig) -> ChildBuild<'a> {
        match self {
            Self::Registry(registry) => ChildBuild::Registry {
                registry: registry.as_ref(),
                retention,
            },
            Self::Client(source) => ChildBuild::Client(source.as_ref()),
        }
    }
}

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
}

impl ChildSpecUnavailable {
    /// Whether the same step can succeed later. Pure.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::NotRun { .. } => true,
            Self::Interrupted { .. } => false,
        }
    }
}

/// Where the retention class of a task queue of one spec comes from.
#[derive(Debug, Clone)]
pub(crate) enum QueueClasses<'a> {
    /// The retention config of the process (a spec of the Rust client).
    Config(&'a RetentionConfig),
    /// The classes that the client resolved for the spec, by queue name
    /// ([`ClientSpec`]).
    Client(Cow<'a, BTreeMap<String, String>>),
}

impl QueueClasses<'_> {
    /// The retention class key that a task node on `queue` stores: `None`
    /// for forever from the config; a client's key as the client wrote it.
    ///
    /// Errors: [`WorkflowError::Validation`] for a client queue with no
    /// class. The parse of the spec data refuses such a spec, so this names
    /// a spec that did not come from that parse.
    pub(crate) fn class_key(&self, queue: &str) -> Result<Option<String>, WorkflowError> {
        match self {
            Self::Config(retention) => Ok(retention.resolve_queue_class(queue)),
            Self::Client(classes) => match classes.get(queue) {
                Some(key) => Ok(Some(key.clone())),
                None => Err(WorkflowError::Validation(format!(
                    "the client resolved no retention class for queue {queue:?} of the spec"
                ))),
            },
        }
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
    /// The error text, for the log.
    pub(crate) fn message(&self) -> Option<&str> {
        match self {
            Self::Core(error) => error.message.as_deref(),
            Self::Client { message, .. } => Some(message),
        }
    }

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
    fn a_client_queue_without_a_class_is_refused() {
        let classes = BTreeMap::from([("default".to_owned(), "standard_30d".to_owned())]);
        let client = QueueClasses::Client(Cow::Owned(classes));
        assert_eq!(
            client.class_key("default").expect("mapped"),
            Some("standard_30d".to_owned())
        );
        assert!(matches!(
            client.class_key("bulk"),
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
            failure.message(),
            Some("Failed to load subworkflow definition for parent:1")
        );
        assert_eq!(
            failure.into_stored_result().expect("stored"),
            r#"{"err":{"error_code":"SUBWORKFLOW_LOAD_FAILED"}}"#
        );
    }
}
