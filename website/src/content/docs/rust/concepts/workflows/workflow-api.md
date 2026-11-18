---
title: Workflow API
summary: Define a workflow with WorkflowDefinition, register it, start it, and read and control it through WorkflowHandle.
related: [typed-node-builder, subworkflows, ../../tasks/retrieving-results, ../../../concepts/workflows/workflow-semantics]
tags: [rust, workflow, WorkflowDefinition, WorkflowSpecBuilder, WorkflowHandle, WorkflowStartError]
---

# Workflow API

A workflow is a DAG of task nodes and sub-workflow nodes. The client builds the spec as data: `WorkflowSpecBuilder::build` checks it with the rules and the `HRS-xxx` codes of the Python client before anything is written. The statuses, the joins, the failure propagation, pause and resume are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/).

## Define and Start a Workflow

```rust
use std::convert::Infallible;
use std::time::Duration;

use horsies::{
    task, Horsies, HorsiesError, TaskError, TaskOutcome, WaitLimit, WorkflowDefConfig,
    WorkflowDefinition, WorkflowFunction, WorkflowSpecBuilder,
};

#[task("fetch_report")]
async fn fetch_report(region: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("report of {region}"))
}

#[task("publish_report")]
async fn publish_report(
    report: TaskOutcome<String, Infallible>,
) -> Result<usize, TaskError<Infallible>> {
    match report {
        TaskOutcome::Succeeded(text) => Ok(text.len()),
        TaskOutcome::Failed(_error) => Ok(0),
    }
}

struct DailyReport;

impl WorkflowDefinition for DailyReport {
    type Output = usize;
    type Params = ();

    fn name() -> &'static str {
        "daily_report"
    }

    fn definition_key() -> &'static str {
        "reports.daily_report.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetched = builder.task(
            fetch_report::node()?.set(fetch_report::params::region(), "eu".to_owned())?,
        );
        let published = builder.task(
            publish_report::node()?.arg_from(publish_report::params::report(), fetched),
        );
        Ok(WorkflowDefConfig::new().output(published))
    }
}

fn register_daily_report(app: &mut Horsies) -> Result<WorkflowFunction<usize>, HorsiesError> {
    let _fetch = fetch_report::register(app)?;
    let _publish = publish_report::register(app)?;
    app.register_workflow_definition::<DailyReport>()
}

async fn run_daily_report(
    daily_report: &WorkflowFunction<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let handle = daily_report.start().await?;
    let limit = WaitLimit::parse(Some(Duration::from_secs(60)))?;
    match handle.get(limit).await? {
        TaskOutcome::Succeeded(length) => println!("published {length} bytes"),
        TaskOutcome::Failed(error) => println!("the workflow ended with {}", error.code().text()),
    }
    Ok(())
}
```

- Register the tasks first, then the definitions. `register_workflow_definition` builds the spec at once, and `task_name::node()` refuses before `task_name::register`.
- Register every definition when `main` builds the app, so that each process, the worker children included, has it.

The nodes, the field tokens and the data flow are on [Typed Node Builder](/horsies/rust/concepts/workflows/typed-node-builder/). A node whose run is a child workflow is on [Subworkflows](/horsies/rust/concepts/workflows/subworkflows/).

## WorkflowDefinition

| Item | Rule |
|------|------|
| `type Output` | The type of the workflow's result: the output node's type, or `OutputlessResults` for a workflow with no output node |
| `type Params` | The params of `build_with` (`Serialize + DeserializeOwned`); `()` for a definition without params |
| `fn name()` | The workflow name, unique in the app |
| `fn definition_key()` | The stable identity, unique in the app. A start and the engine find the workflow by it |
| `fn on_error()` | Default `OnError::Fail`. `OnError::Pause` pauses the workflow at a failed node |
| `fn define(builder)` | A definition without params adds its nodes to `builder` and returns a `WorkflowDefConfig`. The builder has the name, the definition key and `on_error` |
| `fn build_with(params)` | A definition with params builds its spec from the params. See [Subworkflows](/horsies/rust/concepts/workflows/subworkflows/#a-definition-with-params) |
| `fn build()` | Provided: `define` on a new builder, then `WorkflowSpecBuilder::build`. Refuses an `Output` that is not the output node's type (`HRS-025`) |
| `fn node(params)` | Provided: a sub-workflow node of this definition |

A definition implements `define` or `build_with`. A definition with neither is refused when it is built.

`WorkflowDefConfig::new()` has no output node and no success policy. `.output(node)` sets the node whose result is the workflow's result; `.success_policy(policy)` sets when the workflow succeeds.

## WorkflowSpecBuilder

| Method | Does |
|--------|------|
| `WorkflowSpecBuilder::new(name)` | A builder with `on_error` FAIL and no node |
| `.task(node)` | Adds a task node. Returns `TypedNodeRef<T>`, `T` the task's output type |
| `.sub_workflow(node)` | Adds a sub-workflow node. Returns `TypedNodeRef<T>`, `T` the child's output type |
| `.definition_key(key)` | Sets the definition key. A start refuses a spec without one |
| `.on_error(OnError)` | Sets what a failed node does to the workflow |
| `.output(node)` | Sets the node whose result is the workflow's result |
| `.success_policy(policy)` | Sets when the workflow succeeds |
| `.build()` | `Result<WorkflowSpec, HorsiesError>` |

- A node is added after the nodes it references, so a spec has no cycle.
- A reference that another builder returned is refused: `HRS-006` in a node, `HRS-011` as the output, `HRS-012` in the success policy.
- A node with no given node id gets `<workflow name slug>:<index>`.
- `build` checks in two gates. The first gate checks the name, the node count (at most 10,000), the node ids and the references, and stops the build at a refusal. The second gate reports its refusals together: the context sources, the joins, the priorities, the success policy, and the input of each node read with its task's input type. One refusal keeps its code; several are one `HorsiesError` with a note each. The codes are on the [Errors Reference](/horsies/tasks/errors/#workflow-validation-hrs-001-hrs-099).

`WorkflowSpec` is immutable:

| Method | Returns |
|--------|---------|
| `name()` | The workflow name |
| `definition_key()` | `Option<&str>` |
| `on_error()` | `OnError` |
| `node_count()` | The number of nodes |
| `node_ids()` | The node ids in node order |
| `key(node_ref)` | `Option<NodeKey<T>>`: the key of the node's result, with its node id as built. `None` for a reference of another builder |

## Register and Start

| Method | Returns |
|--------|---------|
| `app.register_workflow_definition::<D>()` | `Result<WorkflowFunction<D::Output>, HorsiesError>`, for a definition without params. Builds the spec now |
| `app.workflow_template::<D>()` | `Result<WorkflowTemplate<D::Params, D::Output>, HorsiesError>`, for a definition with params |
| `horsies::start_workflow::<D>()` | Starts `D` through its first registration in the process |
| `horsies::start_workflow_with::<D>(params)` | Starts `D` with `params` through its first registration in the process |
| `app.start::<T>(&spec)`, `app.start_with_id::<T>(&spec, workflow_id)` | Starts a spec that needs no registration. Its sub-workflow children must be registered |
| `rt.start::<T>(&spec)`, `rt.start_with_id::<T>(&spec, workflow_id)` | The same, from a task body through its `TaskRuntime` |
| `app.workflow_handle::<T>(workflow_id)` | The `WorkflowHandle<T>` of a started workflow. No I/O |

The registration refuses an empty name (`HRS-001`), an empty definition key (`HRS-016`), a name or a definition key that another registered definition has (`HRS-017`), and, for a definition without params, a task node on a queue that the app does not admit (`HRS-014`). After a refusal nothing is registered.

| `WorkflowFunction<T>` | `WorkflowTemplate<P, T>` |
|-----------------------|--------------------------|
| `start()` | `start(params)` |
| `start_with_id(workflow_id)` | `start_with_id(params, workflow_id)` |
| `retry_start(&error)` | – |
| `handle(workflow_id)` | `handle(workflow_id)` |
| `spec()`, `name()`, `definition_key()` (`Option<&str>`) | `build(params)`, `node(params)`, `name()`, `definition_key()` (`&str`) |

Clones of a function or a template share one app.

A start runs these steps in order:

1. The definition key of the spec: a spec without one is `VALIDATION_FAILED`.
2. The output type of the spec against the handle's type `T` (`HRS-025`).
3. The check of the spec against the app's registered definitions, and its conversion to the core's spec data.
4. The connect of the handle and the schema install.
5. The start of the workflow through the core.

Steps 1 to 3 refuse with `ValidationFailed` before any I/O.

- A start with an id that a workflow of the same name already has starts nothing and returns the handle of that workflow. An id of another workflow name is `WORKFLOW_ID_CONFLICT`.
- `start_workflow` and `start_workflow_with` before the registration in the process are `VALIDATION_FAILED`.
- With `AppConfig.resend_on_transient_err`, the core runs a retryable start failure again with the same workflow id and start time, up to 3 times.
- In a worker child a start goes through the child's producer handle, as a send does.

### Start from a Task

A task can build a spec at run time and start it through its `TaskRuntime`. The task does not wait for the workflow: a wait in a worker child is refused with `WAIT_INSIDE_TASK`.

```rust
use std::convert::Infallible;

use horsies::{
    task, HorsiesError, TaskError, TaskOutcome, TaskRuntime, UserErrorCode, WorkflowSpec,
    WorkflowSpecBuilder,
};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum ReportCode {
    SpecRefused,
    StartFailed,
}

#[task("fetch_sales")]
async fn fetch_sales(region: String) -> Result<u64, TaskError<Infallible>> {
    Ok(region.len() as u64 * 1_000)
}

#[task("format_report")]
async fn format_report(
    sales: TaskOutcome<u64, Infallible>,
) -> Result<String, TaskError<Infallible>> {
    match sales {
        TaskOutcome::Succeeded(cents) => Ok(format!("sales: {cents} cents")),
        TaskOutcome::Failed(error) => Ok(format!("no sales figure: {error}")),
    }
}

fn report_spec(region: String) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("regional_report");
    builder.definition_key("reports.regional_report.v1");
    let sales = builder.task(fetch_sales::node()?.set(fetch_sales::params::region(), region)?);
    let report =
        builder.task(format_report::node()?.arg_from(format_report::params::sales(), sales));
    builder.output(report);
    builder.build()
}

#[task("start_report")]
async fn start_report(rt: TaskRuntime, region: String) -> Result<Uuid, TaskError<ReportCode>> {
    let spec = report_spec(region)
        .map_err(|refused| TaskError::user(ReportCode::SpecRefused, refused.to_string()))?;
    let handle = rt
        .start::<String>(&spec)
        .await
        .map_err(|error| TaskError::user(ReportCode::StartFailed, error.to_string()))?;
    Ok(handle.workflow_id())
}
```

`rt.start_with_id` with an id that the task derives from its input starts the workflow once: after a retry of the task, the start finds the workflow and starts nothing.

### WorkflowStartError

`WorkflowStartError` has `code`, `message`, `retryable`, `workflow_name` and `workflow_id` (`Option<Uuid>`).

| `WorkflowStartErrorCode` | When |
|--------------------------|------|
| `ValidationFailed` | Before any I/O: no definition key, an output type that is not the handle's (`HRS-025`), a sub-workflow node of a definition that is not registered (`HRS-020`), params given to a definition without params (`HRS-022`), a template build that refused, a start before the registration in the process. At the start: a spec or a sub-workflow child spec that the core refuses |
| `WorkflowIdConflict` | The id belongs to another workflow name. Nothing was written |
| `EnqueueFailed` | The connect, the schema install, the client family check, the database, or the build of a sub-workflow child failed. `retryable` is the classification of the cause. When the commit reply was lost, a start again with the same id finds the workflow |
| `AsyncContext` | A blocking start of `horsies::blocking` on a thread that runs a tokio runtime. Nothing was written; `workflow_id` is the id of the start. Not retryable. See [Blocking API](/horsies/rust/tasks/blocking-api/) |

`WorkflowFunction::retry_start(&error)` starts again with the error's `workflow_id`. It refuses with `ValidationFailed` an error that is not `EnqueueFailed`, an error of another workflow name, and an error with no workflow id. For a template, call `start_with_id(params, workflow_id)` with the error's id. The meaning of each code is on the [Errors Reference](/horsies/tasks/errors/#workflow-start-error-codes).

## WorkflowHandle

| Method | Returns |
|--------|---------|
| `workflow_id()` | `Uuid` |
| `get(limit)` | `Result<TaskOutcome<T, UserCode>, WorkflowReadError>`: waits while the workflow runs, up to `limit: WaitLimit` |
| `result_for::<V, E>(&key)` | `Result<TaskOutcome<V, E>, WorkflowReadError>`: the outcome of one node, read with its types. Not a wait |
| `status()` | `HandleResult<WorkflowStatus>` |
| `results()` | `HandleResult<BTreeMap<String, NodeResult>>`: the stored result of each node that has one, by node id. A node stored without a node id is left out |
| `tasks()` | `HandleResult<Vec<WorkflowNodeInfo>>`: every node in index order |
| `cancel()` | `HandleResult<()>`: cancels the workflow and its descendants. A workflow that ended is left as it is |
| `pause()` | `HandleResult<bool>`: `true` when a RUNNING workflow paused, `false` when it is not RUNNING |
| `resume()` | `HandleResult<bool>`: `true` when a PAUSED workflow resumed, `false` when it is not PAUSED. The children that the resume owes build from the app's registered definitions |

`HandleResult<T>` is `Result<T, HandleOperationError>`. Clones of a handle share one workflow.

`get`:

- reads user codes as checked text (`UserCode`), because a workflow holds the codes of many tasks;
- returns `Failed` with the stored error of a FAILED, CANCELLED or EXPIRED workflow, or with the code of its status when it stored none;
- returns `Failed` with `WORKFLOW_PAUSED` for a PAUSED workflow, which a resume can continue;
- in a worker child, is refused with `WaitInsideTask` before any statement.

A workflow with no output node is read as `OutputlessResults`: the outcome of each node by node id, as `TaskOutcome<JsonText, UserCode>`. The value is its stored text: `get()` gives the text, `decode::<T>()` reads it with the node's type, an integer of any width with all its digits. It has `get(node_id)`, `iter()`, `len()` and `is_empty()`.

`NodeResult` is `Outcome(TaskOutcome<JsonText, UserCode>)` or `Unreadable { cause }`.

`WorkflowNodeInfo`:

| Field | Type |
|-------|------|
| `node_id` | `Option<String>` |
| `index` | `i32` |
| `name` | `String`: the task name, or the child's workflow name |
| `status` | `WorkflowTaskStatus` |
| `result` | `Option<NodeResult>` |
| `started_at`, `completed_at` | `Option<DateTime<Utc>>` |
| `sub_workflow_id` | `Option<Uuid>`: the child of a sub-workflow node |
| `sub_workflow_summary` | `Option<SubWorkflowSummary>`: the summary of the ended child |

### WorkflowReadError

| Variant | Code | When |
|---------|------|------|
| `WaitTimedOut { workflow_id }` | `WAIT_TIMEOUT` | The workflow had not ended when the limit passed. A later read may find it ended |
| `WorkflowNotFound { workflow_id }` | `WORKFLOW_NOT_FOUND` | No workflow has the id |
| `ResultNotReady { workflow_id, node_id }` | `RESULT_NOT_READY` | `result_for`: the node stores no result yet, or no node has the id |
| `StoredResultUnreadable { workflow_id, cause }` | `RESULT_DESERIALIZATION_ERROR` | The stored value does not read with the handle's types; `cause` names no value |
| `UnknownUserCode { workflow_id, cause }` | `RESULT_DESERIALIZATION_ERROR` | The stored user code is not a code of `E` |
| `WaitInsideTask { message }` | `WAIT_INSIDE_TASK` | A wait in a worker child. Nothing was read |
| `AsyncContext { message }` | `ASYNC_CONTEXT` | A blocking `get` or `result_for` (`handle.blocking()`) on a thread that runs a tokio runtime. Nothing was read |
| `Database { code, message, retryable }` | `BROKER_ERROR` | The database or the connection failed; `code` is the `HandleErrorCode` |

`WorkflowReadError::code()` returns the library code of the failure.

### HandleOperationError

`status`, `results`, `tasks`, `cancel`, `pause` and `resume` return `HandleOperationError` with `code`, `message`, `retryable` and `workflow_id`:

| `HandleErrorCode` | Returned by |
|-------------------|-------------|
| `WorkflowNotFound` | Each operation |
| `DbOperationFailed` | Each operation: the connect, a statement or the commit failed |
| `StoredDataInvalid` | `status`, `tasks`, `resume`: a stored value does not parse. Nothing changed. Not retryable |
| `ResumeRecoveryFailed` | `resume`: the resume committed and the recovery pass after it failed. The workflow is RUNNING. Not retryable |
| `AsyncContext` | Each blocking form (`handle.blocking()`) on a thread that runs a tokio runtime. Nothing ran. Not retryable |

The meaning of each code is on the [Errors Reference](/horsies/tasks/errors/#workflow-handle-error-codes).

## Success Policy and on_error

```rust
use horsies::{SuccessCase, SuccessPolicy, TypedNodeRef, WorkflowDefConfig};

fn checkout_config(
    charged: TypedNodeRef<u64>,
    receipt: TypedNodeRef<String>,
    audit: TypedNodeRef<bool>,
) -> WorkflowDefConfig {
    let policy = SuccessPolicy::new(vec![SuccessCase::new([charged.untyped(), receipt.untyped()])])
        .optional([audit]);
    WorkflowDefConfig::new().output(receipt).success_policy(policy)
}
```

| Item | Does |
|------|------|
| `SuccessCase::new(required)` | One way to succeed: every node of `required` COMPLETED |
| `SuccessPolicy::new(cases)` | The workflow succeeds when one case has every required node COMPLETED |
| `.optional(nodes)` | Records the nodes that may fail. Declarative: it has no runtime effect |

- The build refuses a policy with no case and a case with no required node (`HRS-012`).
- `OnError::Fail` (the default): a failed node fails the workflow after the DAG resolves. `OnError::Pause`: a failed node pauses the workflow; `handle.resume()` continues it.

The rules are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/#success-cases-partial-success-policy).

## Statuses

| `WorkflowStatus` | `WorkflowTaskStatus` |
|------------------|----------------------|
| `Pending`, `Running`, `Completed`, `Failed`, `Paused`, `Cancelled`, `Expired` | `Pending`, `Ready`, `Enqueued`, `Running`, `Completed`, `Failed`, `Skipped` |

Both have `is_terminal()`. The transitions are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/#workflow-status-lifecycle).

## Check Cases of a Dynamic Builder

The check builds each definition without params. A function that builds a spec at run time has no registration that the check can build: register check cases for it.

<!-- rust-check: alone -->
```rust
use std::convert::Infallible;

use horsies::{task, Horsies, HorsiesError, TaskError, WorkflowSpec, WorkflowSpecBuilder};

#[task("fetch_report")]
async fn fetch_report(region: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("report of {region}"))
}

fn region_spec(region: &String) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("region_fetch");
    builder.definition_key("reports.region_fetch.v1");
    let fetched = builder.task(
        fetch_report::node()?.set(fetch_report::params::region(), region.clone())?,
    );
    builder.output(fetched);
    builder.build()
}

fn register_region_checks(app: &Horsies) -> Result<(), HorsiesError> {
    app.check_workflow_builder("region_spec", region_spec)
        .cases(["eu".to_owned(), "us".to_owned()])
        .register()
}
```

`check_workflow_builder(name, builder)` returns a `WorkflowCheckBuilderRegistration` with `case(case)`, `cases(cases)` and `register()`. `register` refuses a builder with no case (`HRS-027`). `app.check()` builds and checks a spec from each case. See [CLI Reference](/horsies/rust/cli/).

## Things to Avoid

**Do not wait for a workflow in a task body.** `handle.get(..)` in a worker child is `WaitInsideTask`. Start the workflow and return its id, or make the waiting work a node of the workflow.

**Do not register a definition before its tasks.** A definition without params builds at registration, and `task_name::node()` refuses before `task_name::register`.

**Do not start a spec without a definition key.** The builder of a spec built by hand has none: call `builder.definition_key(..)`.
