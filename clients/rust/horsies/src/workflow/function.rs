//! The registered forms of a workflow, and the app's registration methods.
//!
//! - [`WorkflowFunction<T>`]: a definition without parameters, registered with
//!   `Horsies::register_workflow_definition`; it starts its one spec.
//! - [`WorkflowTemplate<P, T>`]: a definition with parameters, registered with
//!   `Horsies::register_workflow_definition_with`; it builds a spec from parameters and starts
//!   it, and makes the sub-workflow nodes of the definition.
//!
//! A start of a registered definition by its type names the app:
//! `app.start_workflow::<D>()` and `app.start_workflow_with::<D>(parameters)`.
//! The app finds `D` in its own workflow registry. No start reads a
//! process-wide registration, so two apps of one process start apart.

use std::marker::PhantomData;
use std::sync::Arc;

use horsies_core::{StartRefusalCause, WorkflowStartError};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::definition::WorkflowDefinition;
use super::handle::WorkflowHandle;
use super::registry::{CaseBuild, CheckCase, Definition, DefinitionKind};
use super::spec::{WorkflowSpec, WorkflowSpecBuilder};
use super::start::{new_workflow_id, WorkflowStarter};
use super::sub_workflow::{SubWorkflowNode, SubWorkflowParameters};
use crate::{Horsies, HorsiesError, HorsiesErrorCode};

/// A registered workflow definition without parameters, with the result type
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
    /// has this id and this start command is not started again: its handle
    /// is returned.
    ///
    /// Errors: the start errors; `WORKFLOW_ID_CONFLICT` for an id of another
    /// workflow name; `WORKFLOW_COMMAND_CONFLICT` for an id of a run of this
    /// name with another start command, or of a sub-workflow.
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
    pub async fn resend_start(
        &self,
        error: &WorkflowStartError,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        self.starter.resend_start(&self.spec, error).await
    }

    /// The handle of the workflow `workflow_id`, started earlier. No I/O.
    pub fn handle(&self, workflow_id: Uuid) -> WorkflowHandle<T> {
        self.starter.handle_of(workflow_id)
    }
}

/// The build of a template: a spec from parameters.
type TemplateBuild<P> = Arc<dyn Fn(P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync>;

/// A registered workflow definition with parameters `P`, with the result type
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
    /// Errors: for `Given(parameters)`, the refusals of
    /// `SubWorkflowNode::set_parameters`.
    pub fn node(
        &self,
        parameters: SubWorkflowParameters<P>,
    ) -> Result<SubWorkflowNode<P, T>, HorsiesError> {
        SubWorkflowNode::named(self.name, self.definition_key).set_parameters(parameters)
    }

    /// The spec of the definition with `parameters`.
    ///
    /// Errors: the refusals of `build_with` and of the spec build.
    pub fn build(&self, parameters: P) -> Result<WorkflowSpec, HorsiesError> {
        (self.build)(parameters)
    }

    /// Build the spec with `parameters` and start it with a new id.
    ///
    /// Errors: `VALIDATION_FAILED` for a build that refused; else the start
    /// errors.
    pub async fn start(&self, parameters: P) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        self.start_with_id(parameters, new_workflow_id()).await
    }

    /// Build the spec with `parameters` and start it with the id `workflow_id`.
    ///
    /// Errors: as [`Self::start`].
    pub async fn start_with_id(
        &self,
        parameters: P,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        let spec =
            self.build(parameters)
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
/// of one case on the workflow builder of the app that it gets.
type CaseSpecBuilder<P> =
    Arc<dyn Fn(WorkflowSpecBuilder, &P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync>;

/// The check cases of one workflow builder, registered with
/// [`Self::register`] (Python `@app.check_workflow_builder(cases=...)`).
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

    /// Register the cases; the app's check builds and checks each, with a
    /// new workflow builder of the app named after this check builder.
    ///
    /// Errors: `WORKFLOW_CHECK_CASES_REQUIRED` (HRS-027) for a builder with
    /// no case.
    pub fn register(self) -> Result<(), HorsiesError> {
        if self.cases.is_empty() {
            return Err(HorsiesError::new(format!(
                "workflow builder '{}' has no check case",
                self.name
            ))
            .with_code(HorsiesErrorCode::WorkflowCheckCasesRequired)
            .with_help("add at least one case with .case(...) or .cases(...)"));
        }
        let cases = self
            .cases
            .into_iter()
            .map(|case| {
                let builder = Arc::clone(&self.builder);
                let tasks = Arc::clone(self.app.functions());
                let name = self.name.clone();
                let build: CaseBuild = Arc::new(move || {
                    builder(
                        WorkflowSpecBuilder::new(name.clone(), Arc::clone(&tasks)),
                        &case,
                    )
                });
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
    /// A workflow builder of the workflow `name` on this app: each task node
    /// resolves against the tasks of this app when it is added
    /// (`WorkflowSpecBuilder::task`).
    pub fn workflow_builder(&self, name: impl Into<String>) -> WorkflowSpecBuilder {
        WorkflowSpecBuilder::new(name, Arc::clone(self.functions()))
    }

    /// Register the workflow definition `D`, which takes no parameters, and
    /// return its function (Python `WorkflowDefinition` registered for
    /// `start`). `D::build` runs now on a builder of this app, so the tasks
    /// of its nodes register first. [`Self::start_workflow`] starts `D`
    /// through this registration.
    ///
    /// Errors: the refusals of `D::build` (`HRS-302` for a node of a task
    /// that this app does not register), and of the registry (a name or a
    /// definition key that another definition has, a task queue that the
    /// app does not admit).
    pub fn register_workflow_definition<D>(
        &mut self,
    ) -> Result<WorkflowFunction<D::Output>, HorsiesError>
    where
        D: WorkflowDefinition<Parameters = ()>,
        D::Output: DeserializeOwned + Send + Sync,
    {
        let definition = Definition::without_params::<D>(self.functions())?;
        let spec = match &definition.kind {
            DefinitionKind::WithoutParams(spec) => Arc::clone(spec),
            DefinitionKind::WithParams(_) => {
                unreachable!("Definition::without_params makes a definition without parameters")
            }
        };
        self.workflows().register(definition)?;
        Ok(self.function_of::<D::Output>(spec))
    }

    /// Register the workflow definition `D`, which takes parameters, and return
    /// its template (Python `WorkflowDefinition` with `build_with`). Each
    /// build runs `D::build_with` on a new builder of this app.
    /// [`Self::start_workflow_with`] starts `D` through this registration.
    ///
    /// Errors: the refusals of the registry.
    pub fn register_workflow_definition_with<D>(
        &mut self,
    ) -> Result<WorkflowTemplate<D::Parameters, D::Output>, HorsiesError>
    where
        D: WorkflowDefinition,
        D::Parameters: Send + Sync,
        D::Output: DeserializeOwned + Send + Sync,
    {
        self.workflows()
            .register(Definition::with_params::<D>(self.functions()))?;
        Ok(self.template_of::<D>())
    }

    /// The function of the spec `spec` of a definition without parameters on
    /// this app. No I/O.
    fn function_of<T>(&self, spec: Arc<WorkflowSpec>) -> WorkflowFunction<T> {
        WorkflowFunction {
            spec,
            starter: WorkflowStarter::of(self),
            types: PhantomData,
        }
    }

    /// The template of the definition `D` with parameters on this app: each
    /// build runs `D::build_with` on a new builder of this app, then checks
    /// the output type. No I/O; it does not register `D`.
    fn template_of<D>(&self) -> WorkflowTemplate<D::Parameters, D::Output>
    where
        D: WorkflowDefinition,
        D::Parameters: Send + Sync,
        D::Output: DeserializeOwned + Send + Sync,
    {
        let tasks = Arc::clone(self.functions());
        WorkflowTemplate {
            name: D::name(),
            definition_key: D::definition_key(),
            build: Arc::new(
                move |parameters: D::Parameters| -> Result<WorkflowSpec, HorsiesError> {
                    let builder = WorkflowSpecBuilder::new(D::name(), Arc::clone(&tasks));
                    let spec = D::build_with(builder, parameters)?;
                    spec.check_output_type::<D::Output>()?;
                    Ok(spec)
                },
            ),
            starter: WorkflowStarter::of(self),
            types: PhantomData,
        }
    }

    /// Start the workflow definition `D`, which takes no parameters, with a new
    /// id, through its registration on this app
    /// ([`Self::register_workflow_definition`]).
    ///
    /// Errors: `VALIDATION_FAILED` with the cause `PreconditionFailed`
    /// (`StartRefusalCause::Check`) when this app does not register `D`
    /// without parameters; nothing is written. Else the start errors.
    pub async fn start_workflow<D>(&self) -> Result<WorkflowHandle<D::Output>, WorkflowStartError>
    where
        D: WorkflowDefinition<Parameters = ()>,
        D::Output: DeserializeOwned + Send + Sync,
    {
        self.start_workflow_as::<D>(new_workflow_id()).await
    }

    /// [`Self::start_workflow`] with the id `workflow_id`, which the refusal
    /// carries too.
    pub(crate) async fn start_workflow_as<D>(
        &self,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<D::Output>, WorkflowStartError>
    where
        D: WorkflowDefinition<Parameters = ()>,
        D::Output: DeserializeOwned + Send + Sync,
    {
        let spec = match self
            .workflows()
            .definition_of::<D>()
            .map(|found| found.kind)
        {
            Some(DefinitionKind::WithoutParams(spec)) => spec,
            Some(DefinitionKind::WithParams(_)) | None => {
                return Err(not_registered(
                    D::name(),
                    "register_workflow_definition",
                    workflow_id,
                ))
            }
        };
        self.function_of::<D::Output>(spec)
            .start_with_id(workflow_id)
            .await
    }

    /// Start the workflow definition `D` with `parameters` and a new id, through
    /// its registration on this app ([`Self::register_workflow_definition_with`]): the
    /// build of the template, then its start.
    ///
    /// Errors: `VALIDATION_FAILED` with the cause `PreconditionFailed`
    /// (`StartRefusalCause::Check`) when this app does not register `D`
    /// with parameters; the build does not run and nothing is written. Else the
    /// errors of `WorkflowTemplate::start`.
    pub async fn start_workflow_with<D>(
        &self,
        parameters: D::Parameters,
    ) -> Result<WorkflowHandle<D::Output>, WorkflowStartError>
    where
        D: WorkflowDefinition,
        D::Parameters: Send + Sync,
        D::Output: DeserializeOwned + Send + Sync,
    {
        self.start_workflow_with_as::<D>(parameters, new_workflow_id())
            .await
    }

    /// [`Self::start_workflow_with`] with the id `workflow_id`, which the
    /// refusal carries too.
    pub(crate) async fn start_workflow_with_as<D>(
        &self,
        parameters: D::Parameters,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<D::Output>, WorkflowStartError>
    where
        D: WorkflowDefinition,
        D::Parameters: Send + Sync,
        D::Output: DeserializeOwned + Send + Sync,
    {
        match self
            .workflows()
            .definition_of::<D>()
            .map(|found| found.kind)
        {
            Some(DefinitionKind::WithParams(_)) => {}
            Some(DefinitionKind::WithoutParams(_)) | None => {
                return Err(not_registered(
                    D::name(),
                    "register_workflow_definition_with",
                    workflow_id,
                ))
            }
        }
        self.template_of::<D>()
            .start_with_id(parameters, workflow_id)
            .await
    }

    /// Register check cases of a workflow builder: the check builds a spec
    /// from each case with `builder` and checks it (Python
    /// `@app.check_workflow_builder(cases=...)`). `builder` gets a new workflow
    /// builder of this app named `name`, and the case.
    pub fn check_workflow_builder<P, F>(
        &self,
        name: &str,
        builder: F,
    ) -> WorkflowCheckBuilderRegistration<'_, P>
    where
        P: Send + Sync + 'static,
        F: Fn(WorkflowSpecBuilder, &P) -> Result<WorkflowSpec, HorsiesError>
            + Send
            + Sync
            + 'static,
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

/// The refusal of a start of the definition `name` that the app does not
/// register in the form of `method`, with the id of that start (Python: the
/// id is minted before the checks). Pure.
fn not_registered(name: &str, method: &str, workflow_id: Uuid) -> WorkflowStartError {
    WorkflowStartError::ValidationFailed {
        message: format!(
            "workflow '{name}' is not registered on this app; call app.{method}::<...>() on the \
             app that starts it"
        ),
        workflow_name: name.to_owned(),
        workflow_id,
        cause: StartRefusalCause::Check,
    }
}

#[cfg(test)]
mod tests;
