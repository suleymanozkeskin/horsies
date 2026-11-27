//! The child spec source of the core's tests: the children of sub-workflow
//! nodes from a map of definition key to a spec, or to a build over the
//! builder input text. It answers through the public steps that a client's
//! source runs around its own builder ([`merged_child_input`],
//! [`subworkflow_load_failed_error`], [`params_refused_error`],
//! [`build_failed_error`], [`node_failure_result`]), so a test drives the
//! core's child builds as a client does.
//!
//! The task queues of a spec take the retention class that the test's
//! [`RetentionConfig`] resolves for them, with the forever class for a queue
//! mapped to forever: the class keys that the node rows store.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::core::config::retention::RetentionConfig;
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::{HorsiesError, TaskError, WorkflowSpec};
use crate::workflow_engine::child_spec::{
    ChildSpecFuture, ChildSpecRequest, ChildSpecSource, ChildSpecUnavailable, ClientChildSpec,
    ClientSpec,
};
use crate::workflow_engine::start::ChildSpecError;
use crate::workflow_engine::start_statement::{
    build_failed_error, merged_child_input, node_failure_result, params_refused_error,
    subworkflow_load_failed_error, NodeInput, RefusedMember,
};

/// The name of the builder of a test child that the node errors of a member
/// refusal give, unless a test names another ([`TestChildSpecs::with_builder_hook`]).
pub(crate) const TEST_BUILDER_HOOK: &str = "spec_builder";

/// The build of a test child with params: its spec from the builder input
/// text ([`crate::workflow_engine::args_merge::ChildBuildInput::text`]).
pub(crate) type TestChildBuild =
    Arc<dyn Fn(&str) -> Result<WorkflowSpec, TestBuildRefused> + Send + Sync + 'static>;

/// Why the build of a test child gave no spec.
#[derive(Debug, Clone)]
pub(crate) enum TestBuildRefused {
    /// The input member `key` does not decode; the node stores
    /// `WORKER_SERIALIZATION_ERROR` ([`params_refused_error`]).
    Member { key: String, cause: String },
    /// The build failed; the node stores `WORKFLOW_ENQUEUE_FAILED`
    /// ([`build_failed_error`]).
    Failed(HorsiesError),
}

/// What a definition key gives.
#[derive(Clone)]
enum TestChild {
    /// One spec, whatever the node input.
    Spec(WorkflowSpec),
    /// A build over the builder input.
    Build(TestChildBuild),
}

/// The children of a test, by definition key, the retention config that
/// gives the class of each task queue, and the name of the builder that the
/// node errors of a member refusal give.
#[derive(Clone)]
pub(crate) struct TestChildSpecs {
    children: BTreeMap<String, TestChild>,
    retention: RetentionConfig,
    builder_hook: &'static str,
}

impl TestChildSpecs {
    /// No child: every child build fails its node with
    /// `SUBWORKFLOW_LOAD_FAILED`. The default retention config.
    pub(crate) fn empty() -> Self {
        Self {
            children: BTreeMap::new(),
            retention: RetentionConfig::default(),
            builder_hook: TEST_BUILDER_HOOK,
        }
    }

    /// Each of `specs` under its definition key.
    ///
    /// Panics: a spec without a definition key, or two specs with one key
    /// (a defect of the test).
    pub(crate) fn of(specs: &[WorkflowSpec]) -> Self {
        specs.iter().fold(Self::empty(), |children, spec| {
            children.with_spec(spec.clone())
        })
    }

    /// These children with `spec` under its definition key.
    ///
    /// Panics: as [`Self::of`].
    pub(crate) fn with_spec(self, spec: WorkflowSpec) -> Self {
        let key = spec
            .definition_key
            .clone()
            .unwrap_or_else(|| panic!("test child '{}' has no definition key", spec.name));
        self.with_child(&key, TestChild::Spec(spec))
    }

    /// These children with `build` under `definition_key`.
    ///
    /// Panics: a key that another child has.
    pub(crate) fn with_build(self, definition_key: &str, build: TestChildBuild) -> Self {
        self.with_child(definition_key, TestChild::Build(build))
    }

    /// These children with the task queues classed by `retention`.
    pub(crate) fn with_retention(self, retention: RetentionConfig) -> Self {
        Self { retention, ..self }
    }

    /// These children, whose member refusals name the builder `hook`.
    pub(crate) fn with_builder_hook(self, builder_hook: &'static str) -> Self {
        Self {
            builder_hook,
            ..self
        }
    }

    fn with_child(mut self, definition_key: &str, child: TestChild) -> Self {
        match self.children.insert(definition_key.to_owned(), child) {
            None => self,
            Some(_) => panic!("two test children under definition key '{definition_key}'"),
        }
    }

    /// `spec` as a client sends it: with the class of each of its task
    /// queues. Pure.
    pub(crate) fn client_spec(&self, spec: &WorkflowSpec) -> ClientSpec {
        client_spec_of(spec, &self.retention)
    }

    /// The answer for the sub-workflow node of `request`, in the order of a
    /// client's source:
    ///
    /// 1. no child under the node's definition key: the node fails with
    ///    `SUBWORKFLOW_LOAD_FAILED`;
    /// 2. the builder input ([`merged_child_input`]): its node failures fail
    ///    the node; a stored input that the merge refuses is
    ///    [`ChildSpecUnavailable::Raised`];
    /// 3. the spec, or the build over the input text: a member refusal fails
    ///    the node with `WORKER_SERIALIZATION_ERROR`, a failed build with
    ///    `WORKFLOW_ENQUEUE_FAILED`;
    /// 4. the spec, with its task queues classed.
    ///
    /// Errors: [`ChildSpecUnavailable::Raised`] for a refused stored input
    /// or a node error that does not encode. Pure; runs the test's build.
    pub(crate) fn answer(
        &self,
        request: &ChildSpecRequest,
    ) -> Result<ClientChildSpec, ChildSpecUnavailable> {
        let Some(child) = request
            .definition_key
            .as_deref()
            .and_then(|key| self.children.get(key))
        else {
            return node_fails(
                request,
                subworkflow_load_failed_error(
                    &request.parent_workflow_name,
                    request.task_index,
                    request.definition_key.as_deref(),
                ),
            );
        };
        let input = match merged_child_input(request) {
            Ok(NodeInput::Input(input)) => input,
            Ok(NodeInput::NodeFails(error)) => return node_fails(request, error),
            Err(refused) => return Err(build_raised(request, refused.to_string())),
        };
        let built = match child {
            TestChild::Spec(spec) => Ok(spec.clone()),
            TestChild::Build(build) => build(input.text()),
        };
        let failure = match built {
            Ok(spec) => return Ok(ClientChildSpec::Built(self.client_spec(&spec))),
            Err(TestBuildRefused::Member { key, cause }) => params_refused_error(
                request.parent_workflow_id,
                request.task_index,
                RefusedMember {
                    key: &key,
                    cause: &cause,
                },
                &input,
                self.builder_hook,
            ),
            Err(TestBuildRefused::Failed(error)) => build_failed_error(
                request.parent_workflow_id,
                &request.parent_workflow_name,
                request.task_index,
                &ChildSpecError::of_builder(&error),
            ),
        };
        node_fails(request, failure)
    }
}

/// The child specs of a process with no child: every child build fails its
/// node with `SUBWORKFLOW_LOAD_FAILED`.
pub(crate) fn no_child_specs() -> Arc<dyn ChildSpecSource> {
    Arc::new(TestChildSpecs::empty())
}

/// The source answers when the future is made: the build runs on the
/// step's task.
impl ChildSpecSource for TestChildSpecs {
    fn child_spec(&self, request: ChildSpecRequest) -> ChildSpecFuture<'_> {
        let answer = self.answer(&request);
        Box::pin(async move { answer })
    }
}

/// `spec` with the class that `retention` resolves for each of its task
/// queues, the forever class for a queue mapped to forever. A task node
/// without a queue adds no class: the start refuses it as a node without a
/// resolved queue. Pure.
pub(crate) fn client_spec_of(spec: &WorkflowSpec, retention: &RetentionConfig) -> ClientSpec {
    let queue_classes = spec
        .tasks
        .iter()
        .filter(|node| !node.is_subworkflow)
        .filter_map(|node| node.queue.as_deref())
        .map(|queue| {
            let class = retention
                .resolve_queue_retention_class(queue)
                .unwrap_or_else(|| FOREVER_CLASS_KEY.to_owned());
            (queue.to_owned(), class)
        })
        .collect::<BTreeMap<String, String>>();
    ClientSpec::new(spec.clone(), queue_classes)
}

/// The answer that fails the node of `request` with `error`, in its result
/// envelope.
///
/// Errors: [`ChildSpecUnavailable::Raised`] when the envelope does not
/// encode. Pure.
fn node_fails(
    request: &ChildSpecRequest,
    error: TaskError,
) -> Result<ClientChildSpec, ChildSpecUnavailable> {
    let message = error.to_string();
    match node_failure_result(error) {
        Ok(failure_result) => Ok(ClientChildSpec::NodeFails {
            message,
            failure_result,
        }),
        Err(refused) => Err(build_raised(request, refused.to_string())),
    }
}

/// The refusal of the build of the child of `request` for `cause`. Pure.
fn build_raised(request: &ChildSpecRequest, cause: String) -> ChildSpecUnavailable {
    ChildSpecUnavailable::Raised {
        subject: format!(
            "build of the child of {}:{}",
            request.parent_workflow_name, request.task_index
        ),
        cause,
    }
}
