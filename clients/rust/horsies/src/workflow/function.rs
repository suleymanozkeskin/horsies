//! The registered forms of a workflow, and the app's registration methods.
//!
//! - [`WorkflowFunction<T>`]: a definition without params, registered with
//!   `Horsies::register_workflow_definition`; it starts its one spec.
//! - [`WorkflowTemplate<P, T>`]: a definition with params, registered with
//!   `Horsies::workflow_template`; it builds a spec from params and starts
//!   it, and makes the sub-workflow nodes of the definition.
//!
//! Each registration also serves the process-wide starts
//! [`start_workflow`] and [`start_workflow_with`], as the first
//! `register` of a task serves its generated `send`.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, OnceLock};

use horsies_core::{StartRefusalCause, WorkflowStartError};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::definition::WorkflowDefinition;
use super::handle::WorkflowHandle;
use super::registry::{CaseBuild, CheckCase, Definition};
use super::spec::WorkflowSpec;
use super::start::{new_workflow_id, WorkflowStarter};
use super::sub_workflow::{SubWorkflowNode, SubWorkflowParams};
use crate::{ErrorCode, Horsies, HorsiesError};

/// A registered workflow definition without params, with the result type
/// `T`. Clones share one app.
pub struct WorkflowFunction<T> {
    spec: Arc<WorkflowSpec>,
    starter: WorkflowStarter,
    types: PhantomData<fn() -> T>,
}

impl<T> Clone for WorkflowFunction<T> {
    fn clone(&self) -> Self {
        Self {
            spec: Arc::clone(&self.spec),
            starter: self.starter.clone(),
            types: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for WorkflowFunction<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowFunction")
            .field("name", &self.spec.name())
            .field("definition_key", &self.spec.definition_key())
            .finish_non_exhaustive()
    }
}

impl<T> WorkflowFunction<T>
where
    T: DeserializeOwned + 'static,
{
    /// The workflow name.
    pub fn name(&self) -> &str {
        self.spec.name()
    }

    /// The definition key.
    pub fn definition_key(&self) -> Option<&str> {
        self.spec.definition_key()
    }

    /// The spec.
    pub fn spec(&self) -> &WorkflowSpec {
        &self.spec
    }

    /// Start the workflow with a new id.
    ///
    /// Errors: the start errors of `WorkflowStartErrorCode`.
    pub async fn start(&self) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        self.starter.start(&self.spec, new_workflow_id()).await
    }

    /// Start the workflow with the id `workflow_id`. A workflow that already
    /// has this id and this name is not started again: its handle is
    /// returned.
    ///
    /// Errors: the start errors; `WORKFLOW_ID_CONFLICT` for an id of another
    /// workflow name.
    pub async fn start_with_id(
        &self,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        self.starter.start(&self.spec, workflow_id).await
    }

    /// Start the workflow again after a failed start, with its id.
    ///
    /// Errors: `VALIDATION_FAILED` for an error that is not
    /// `ENQUEUE_FAILED` or `ASYNC_CONTEXT`, or of another workflow; else the
    /// start errors.
    pub async fn retry_start(
        &self,
        error: &WorkflowStartError,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        self.starter.retry_start(&self.spec, error).await
    }

    /// The handle of the workflow `workflow_id`, started earlier. No I/O.
    pub fn handle(&self, workflow_id: Uuid) -> WorkflowHandle<T> {
        self.starter.handle_of(workflow_id)
    }
}

/// The build of a template: a spec from params.
type TemplateBuild<P> = Arc<dyn Fn(P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync>;

/// A registered workflow definition with params `P`, with the result type
/// `T`. Clones share one app.
pub struct WorkflowTemplate<P, T> {
    name: &'static str,
    definition_key: &'static str,
    build: TemplateBuild<P>,
    starter: WorkflowStarter,
    types: PhantomData<fn() -> T>,
}

impl<P, T> Clone for WorkflowTemplate<P, T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            definition_key: self.definition_key,
            build: Arc::clone(&self.build),
            starter: self.starter.clone(),
            types: PhantomData,
        }
    }
}

impl<P, T> std::fmt::Debug for WorkflowTemplate<P, T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowTemplate")
            .field("name", &self.name)
            .field("definition_key", &self.definition_key)
            .finish_non_exhaustive()
    }
}

impl<P, T> WorkflowTemplate<P, T>
where
    P: serde::Serialize + DeserializeOwned + 'static,
    T: DeserializeOwned + 'static,
{
    /// The workflow name.
    pub fn name(&self) -> &str {
        self.name
    }

    /// The definition key.
    pub fn definition_key(&self) -> &str {
        self.definition_key
    }

    /// A sub-workflow node of this definition.
    ///
    /// Errors: for `Given(params)`, the refusals of
    /// `SubWorkflowNode::set_params`.
    pub fn node(
        &self,
        params: SubWorkflowParams<P>,
    ) -> Result<SubWorkflowNode<P, T>, HorsiesError> {
        SubWorkflowNode::named(self.name, self.definition_key).set_params(params)
    }

    /// The spec of the definition with `params`.
    ///
    /// Errors: the refusals of `build_with` and of the spec build.
    pub fn build(&self, params: P) -> Result<WorkflowSpec, HorsiesError> {
        (self.build)(params)
    }

    /// Build the spec with `params` and start it with a new id.
    ///
    /// Errors: `VALIDATION_FAILED` for a build that refused; else the start
    /// errors.
    pub async fn start(&self, params: P) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        self.start_with_id(params, new_workflow_id()).await
    }

    /// Build the spec with `params` and start it with the id `workflow_id`.
    ///
    /// Errors: as [`Self::start`].
    pub async fn start_with_id(
        &self,
        params: P,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        let spec = self
            .build(params)
            .map_err(|error| WorkflowStartError::ValidationFailed {
                message: error.to_string(),
                workflow_name: self.name.to_owned(),
                workflow_id,
                cause: StartRefusalCause::Check,
            })?;
        self.starter.start(&spec, workflow_id).await
    }

    /// The handle of the workflow `workflow_id`, started earlier. No I/O.
    pub fn handle(&self, workflow_id: Uuid) -> WorkflowHandle<T> {
        self.starter.handle_of(workflow_id)
    }
}

/// A workflow builder that the check runs on each case: it builds the spec
/// of one case.
type CaseSpecBuilder<P> = Arc<dyn Fn(&P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync>;

/// The check cases of one workflow builder, registered with
/// [`Self::register`] (Python `@app.workflow_builder(cases=...)`).
pub struct WorkflowCheckBuilderRegistration<'a, P> {
    app: &'a Horsies,
    name: String,
    builder: CaseSpecBuilder<P>,
    cases: Vec<P>,
}

impl<P> WorkflowCheckBuilderRegistration<'_, P>
where
    P: Send + Sync + 'static,
{
    /// Add one case.
    pub fn case(mut self, case: P) -> Self {
        self.cases.push(case);
        self
    }

    /// Add each of `cases`.
    pub fn cases<I: IntoIterator<Item = P>>(mut self, cases: I) -> Self {
        self.cases.extend(cases);
        self
    }

    /// Register the cases; the app's check builds and checks each.
    ///
    /// Errors: `WORKFLOW_CHECK_CASES_REQUIRED` (HRS-027) for a builder with
    /// no case.
    pub fn register(self) -> Result<(), HorsiesError> {
        if self.cases.is_empty() {
            return Err(HorsiesError::new(format!(
                "workflow builder '{}' has no check case",
                self.name
            ))
            .with_code(ErrorCode::WorkflowCheckCasesRequired)
            .with_help("add at least one case with .case(...) or .cases(...)"));
        }
        let cases = self
            .cases
            .into_iter()
            .map(|case| {
                let builder = Arc::clone(&self.builder);
                let build: CaseBuild = Arc::new(move || builder(&case));
                CheckCase {
                    builder_name: self.name.clone(),
                    build,
                }
            })
            .collect();
        self.app.workflows().add_check_cases(cases);
        Ok(())
    }
}

impl Horsies {
    /// Register the workflow definition `D`, which takes no params, and
    /// return its function (Python `WorkflowDefinition` registered for
    /// `start`). The first registration of `D` in a process also serves
    /// [`start_workflow::<D>()`](start_workflow).
    ///
    /// Errors: the refusals of `D::build`, and of the registry (a name or a
    /// definition key that another definition has, a task queue that the
    /// app does not admit).
    pub fn register_workflow_definition<D>(
        &mut self,
    ) -> Result<WorkflowFunction<D::Output>, HorsiesError>
    where
        D: WorkflowDefinition<Params = ()>,
        D::Output: DeserializeOwned + Send + Sync,
    {
        let definition = Definition::without_params::<D>()?;
        let spec = match &definition.kind {
            super::registry::DefinitionKind::WithoutParams(spec) => Arc::clone(spec),
            super::registry::DefinitionKind::WithParams(_) => {
                unreachable!("Definition::without_params makes a definition without params")
            }
        };
        self.workflows().register(definition)?;
        let function = WorkflowFunction {
            spec,
            starter: WorkflowStarter::of(self),
            types: PhantomData,
        };
        store_global::<D, WorkflowFunction<D::Output>>(function.clone());
        Ok(function)
    }

    /// Register the workflow definition `D`, which takes params, and return
    /// its template (Python `WorkflowDefinition` with `build_with`). The
    /// first registration of `D` in a process also serves
    /// [`start_workflow_with::<D>(params)`](start_workflow_with).
    ///
    /// Errors: the refusals of the registry.
    pub fn workflow_template<D>(
        &mut self,
    ) -> Result<WorkflowTemplate<D::Params, D::Output>, HorsiesError>
    where
        D: WorkflowDefinition,
        D::Params: Send + Sync,
        D::Output: DeserializeOwned + Send + Sync,
    {
        self.workflows().register(Definition::with_params::<D>())?;
        let template = WorkflowTemplate {
            name: D::name(),
            definition_key: D::definition_key(),
            build: Arc::new(|params: D::Params| -> Result<WorkflowSpec, HorsiesError> {
                let spec = D::build_with(params)?;
                spec.check_output_type::<D::Output>()?;
                Ok(spec)
            }),
            starter: WorkflowStarter::of(self),
            types: PhantomData,
        };
        store_global::<D, WorkflowTemplate<D::Params, D::Output>>(template.clone());
        Ok(template)
    }

    /// Register check cases of a workflow builder: the check builds a spec
    /// from each case with `builder` and checks it (Python
    /// `@app.workflow_builder(cases=...)`).
    pub fn check_workflow_builder<P, F>(
        &self,
        name: &str,
        builder: F,
    ) -> WorkflowCheckBuilderRegistration<'_, P>
    where
        P: Send + Sync + 'static,
        F: Fn(&P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static,
    {
        WorkflowCheckBuilderRegistration {
            app: self,
            name: name.to_owned(),
            builder: Arc::new(builder),
            cases: Vec::new(),
        }
    }

    /// Start `spec` with a new id, with the result type `T` (Python
    /// `WorkflowSpec.start`). The spec needs no registration; its
    /// sub-workflow children must be registered.
    ///
    /// Errors: the start errors of `WorkflowStartErrorCode`.
    pub async fn start<T>(
        &self,
        spec: &WorkflowSpec,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError>
    where
        T: DeserializeOwned + 'static,
    {
        WorkflowStarter::of(self)
            .start(spec, new_workflow_id())
            .await
    }

    /// Start `spec` with the id `workflow_id`, as [`Self::start`].
    pub async fn start_with_id<T>(
        &self,
        spec: &WorkflowSpec,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError>
    where
        T: DeserializeOwned + 'static,
    {
        WorkflowStarter::of(self).start(spec, workflow_id).await
    }

    /// The handle of the workflow `workflow_id`, with the result type `T`.
    /// No I/O.
    pub fn workflow_handle<T>(&self, workflow_id: Uuid) -> WorkflowHandle<T> {
        WorkflowStarter::of(self).handle_of(workflow_id)
    }
}

// ---------------------------------------------------------------------------
// Process-wide starts
// ---------------------------------------------------------------------------

type GlobalWorkflows = Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>;

/// The registered function or template of each definition type, the first
/// registration of each in this process.
fn global_workflows() -> &'static GlobalWorkflows {
    static WORKFLOWS: OnceLock<GlobalWorkflows> = OnceLock::new();
    WORKFLOWS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn store_global<D: 'static, V: Any + Send + Sync>(value: V) {
    let mut workflows = global_workflows()
        .lock()
        .expect("the process-wide workflow map is never held across a panic");
    let _first = workflows
        .entry(TypeId::of::<D>())
        .or_insert_with(|| Box::new(value));
}

fn global<D: 'static, V: Any + Clone>() -> Option<V> {
    let workflows = global_workflows()
        .lock()
        .expect("the process-wide workflow map is never held across a panic");
    workflows
        .get(&TypeId::of::<D>())
        .and_then(|value| value.downcast_ref::<V>())
        .cloned()
}

/// The refusal of a process-wide start of `name` before its registration,
/// with the id of that start (Python: the id is minted before the checks).
fn not_registered(name: &str, method: &str, workflow_id: Uuid) -> WorkflowStartError {
    WorkflowStartError::ValidationFailed {
        message: format!(
            "workflow '{name}' is not registered in this process; call app.{method}::<...>() when \
             main builds the app"
        ),
        workflow_name: name.to_owned(),
        workflow_id,
        cause: StartRefusalCause::Check,
    }
}

/// Start the workflow definition `D`, which takes no params, from any call
/// site, through its first registration in this process.
///
/// Errors: `VALIDATION_FAILED` before `register_workflow_definition::<D>()`;
/// else the start errors.
pub async fn start_workflow<D>() -> Result<WorkflowHandle<D::Output>, WorkflowStartError>
where
    D: WorkflowDefinition<Params = ()>,
    D::Output: DeserializeOwned + Send + Sync,
{
    start_workflow_as::<D>(new_workflow_id()).await
}

/// [`start_workflow`] with the id `workflow_id`, which the refusal before
/// the registration carries too.
pub(crate) async fn start_workflow_as<D>(
    workflow_id: Uuid,
) -> Result<WorkflowHandle<D::Output>, WorkflowStartError>
where
    D: WorkflowDefinition<Params = ()>,
    D::Output: DeserializeOwned + Send + Sync,
{
    let function = global::<D, WorkflowFunction<D::Output>>()
        .ok_or_else(|| not_registered(D::name(), "register_workflow_definition", workflow_id))?;
    function.start_with_id(workflow_id).await
}

/// Start the workflow definition `D` with `params` from any call site,
/// through its first registration in this process.
///
/// Errors: `VALIDATION_FAILED` before `workflow_template::<D>()`; else the
/// errors of `WorkflowTemplate::start`.
pub async fn start_workflow_with<D>(
    params: D::Params,
) -> Result<WorkflowHandle<D::Output>, WorkflowStartError>
where
    D: WorkflowDefinition,
    D::Params: Send + Sync,
    D::Output: DeserializeOwned + Send + Sync,
{
    start_workflow_with_as::<D>(params, new_workflow_id()).await
}

/// [`start_workflow_with`] with the id `workflow_id`, which the refusal
/// before the registration carries too.
pub(crate) async fn start_workflow_with_as<D>(
    params: D::Params,
    workflow_id: Uuid,
) -> Result<WorkflowHandle<D::Output>, WorkflowStartError>
where
    D: WorkflowDefinition,
    D::Params: Send + Sync,
    D::Output: DeserializeOwned + Send + Sync,
{
    let template = global::<D, WorkflowTemplate<D::Params, D::Output>>()
        .ok_or_else(|| not_registered(D::name(), "workflow_template", workflow_id))?;
    template.start_with_id(params, workflow_id).await
}

#[cfg(test)]
mod tests;
