# horsies (Rust) — Workflows

Reference for building, starting and reading workflow DAGs with the
`horsies` crate: workflow definitions, the spec builder, task nodes, data
flow, the workflow context, sub-workflows, the starts and the workflow
handle. The behaviour (statuses, joins, failure propagation, pause and
resume) is on the shared docs page `concepts/workflows/workflow-semantics`.

The DSL builds a spec as data in the client. `WorkflowSpecBuilder::build`
checks the spec with Python's rules and `HRS-xxx` codes before anything is
written.

## A Workflow Definition

```rust
use std::convert::Infallible;

use horsies::{
    task, HorsiesError, NodeKey, TaskError, TaskOutcome, WorkflowContext, WorkflowDefinition,
    WorkflowSpec, WorkflowSpecBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    total_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckoutParams {
    order: Order,
}

#[task("charge_order")]
async fn charge_order(order: Order) -> Result<u64, TaskError<Infallible>> {
    Ok(order.total_cents)
}

#[task("send_receipt")]
async fn send_receipt(
    order_id: String,
    charged: TaskOutcome<u64, Infallible>,
) -> Result<String, TaskError<Infallible>> {
    match charged {
        TaskOutcome::Succeeded(cents) => Ok(format!("{order_id}: charged {cents}")),
        TaskOutcome::Failed(error) => Ok(format!("{order_id}: {}", error.code().text())),
    }
}

#[task("audit_checkout", workflow_ctx)]
async fn audit_checkout(workflow_ctx: WorkflowContext) -> Result<bool, TaskError<Infallible>> {
    let charged = workflow_ctx.result_for::<u64, Infallible>(&NodeKey::new("charge"));
    Ok(matches!(charged, Ok(TaskOutcome::Succeeded(_))))
}

struct Checkout;

impl WorkflowDefinition for Checkout {
    type Output = String;
    type Params = CheckoutParams;

    fn name() -> &'static str {
        "checkout"
    }

    fn definition_key() -> &'static str {
        "shop.checkout.v1"
    }

    fn build_with(params: CheckoutParams) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(Self::name());
        builder.definition_key(Self::definition_key());
        let order_id = params.order.order_id.clone();
        let charged = builder.task(
            charge_order::node()?
                .node_id("charge")
                .set(charge_order::params::order(), params.order)?,
        );
        let receipt = builder.task(
            send_receipt::node()?
                .node_id("receipt")
                .set(send_receipt::params::order_id(), order_id)?
                .arg_from(send_receipt::params::charged(), charged),
        );
        let _audit = builder.task(
            audit_checkout::node()?
                .waits_for(charged)
                .workflow_ctx_from([charged]),
        );
        builder.output(receipt);
        builder.build()
    }
}
```

`WorkflowDefinition`:

| Item | Rule |
|---|---|
| `type Output` | The type of the workflow's result: the output node's type, or `OutputlessResults` for a workflow with no output node |
| `type Params` | The params of `build_with` (`Serialize + DeserializeOwned`); `()` for a definition without params |
| `fn name()` | The workflow name, unique in the app |
| `fn definition_key()` | The stable identity, unique in the app. A start and the engine find the workflow by it |
| `fn on_error()` | Default `OnError::Fail`; `OnError::Pause` pauses the workflow at a failure |
| `fn define(builder)` | A definition without params adds its nodes and returns `WorkflowDefConfig::new().output(node).success_policy(policy)`. The builder has the name, the key and `on_error` |
| `fn build_with(params)` | A definition with params builds its spec. The engine calls it for each sub-workflow node that gives params, and may call it more than once for one node: give it no side effects |
| `fn build()` | Default: `define` on a new builder, then `build`. Refuses an `Output` that is not the output node's type (`HRS-025`) |
| `fn node(params)` | A sub-workflow node of this definition (see Sub-Workflows) |

A definition implements `define` or `build_with`. One with neither is
refused at build. In `build_with`, set the definition key and the
`on_error` on the builder yourself.

## `WorkflowSpecBuilder` and `WorkflowSpec`

| Method | Does |
|---|---|
| `WorkflowSpecBuilder::new(name)` | A builder with `on_error` FAIL and no node |
| `.task(node)` | Adds a task node. Returns `TypedNodeRef<T>`, `T` the task's output type |
| `.sub_workflow(node)` | Adds a sub-workflow node. Returns `TypedNodeRef<T>`, `T` the child's output type |
| `.definition_key(key)` | Sets the definition key. A start refuses a spec without one |
| `.on_error(OnError)` | Sets what a failed node does to the workflow |
| `.output(node)` | Sets the node whose result is the workflow's result |
| `.success_policy(policy)` | Sets when the workflow succeeds |
| `.build()` | `Result<WorkflowSpec, HorsiesError>` |

- A node is added after the nodes it references, so a spec has no cycle.
  A reference of another builder is refused (`HRS-006`, `HRS-011`,
  `HRS-012`).
- `WorkflowSpec` is immutable: `name()`, `definition_key()`, `on_error()`,
  `node_count()`, `node_ids()`, and `key(node_ref)` (the `NodeKey<T>` of a
  node, with its node id as built).
- A node with no `node_id` gets `<workflow name slug>:<index>`.
- `NodeRef` is a reference with no output type. `TypedNodeRef<T>` has
  `untyped()` and converts with `Into<NodeRef>`.

## Task Nodes

`task_name::node()` returns `Result<TaskNode<T, task_name::Input>,
HorsiesError>` with the task's queue and options and the input `{}`.
`TaskFunction::node()` returns the node directly.

| Method | Does |
|---|---|
| `.set(field, value)` | Sets one parameter. Encoded and read back as a send does. `Result<Self, HorsiesError>` |
| `.set_input(input)` | Sets the whole input from a value of `task_name::Input`. `Result<Self, HorsiesError>` |
| `.input_json(text)` | Sets the whole input from one JSON object in the stored form. `Result<Self, HorsiesError>` |
| `.arg_from(field, source)` | Gives the outcome of `source` to a parameter of type `TaskOutcome<S, E>`. The node waits for `source` |
| `.waits_for(node)`, `.waits_for_all(nodes)` | Waits for nodes, with no data |
| `.workflow_ctx_from(nodes)` | Gives the results of these dependencies in the workflow context |
| `.node_id(id)` | Unique in the spec, `[A-Za-z0-9_\-:.]`, at most 128 characters |
| `.queue(name)` | Runs on this queue in place of the task's queue |
| `.priority(p)` | 1 (first) to 100 (last), in place of the queue's priority |
| `.allow_failed_deps(true)` | Runs even when a dependency failed |
| `.join_all()` (default), `.join_any()`, `.join_quorum(n)` | When the node becomes ready |
| `.good_until(deadline)` | The task expires when it has not started by then |
| `.key()` | `Option<NodeKey<T>>`: the key when the node has a given node id |

- `set` and `arg_from` take the field tokens of the task's own module:
  `task_name::params::<name>()`. A token of another task, or a source of
  another output type, is a compile error.
- `E` in `TaskOutcome<S, E>` reads the source's user codes. `UserCode`
  reads any user code as checked text.
- The field `S` of `arg_from` must be the source's output type.

Data flow rules:

| Dependency | Default | `allow_failed_deps(true)` |
|---|---|---|
| COMPLETED | Runs with `TaskOutcome::Succeeded(value)` | Same |
| FAILED | The node is SKIPPED | Runs with `TaskOutcome::Failed(error)` |
| SKIPPED | The node is SKIPPED | Runs with `Failed` and the code `UPSTREAM_SKIPPED` |

`arg_from` takes a field of type `TaskOutcome<S, E>`, which has no default.
A node with `join_any()` or `join_quorum(n)` that has an `arg_from` is
refused with `HRS-013`, because such a node can start before a source
ended. Give that node its dependency results through the workflow context.

## The Workflow Context

A task that takes the context has the attribute `workflow_ctx` and a
parameter named `workflow_ctx` of type `WorkflowContext`. The child of the
worker writes the context into that parameter.

| Method | Returns |
|---|---|
| `workflow_id()` | The workflow's `Uuid` |
| `task_index()` | The index of the task's node |
| `task_name()` | The task name of the node |
| `result_for::<T, E>(&key)` | `Result<TaskOutcome<T, E>, WorkflowContextError>` |
| `has_result(node_id)` | Whether the context holds the node's result |
| `summary_for(&key)` | `Result<&SubWorkflowSummary, WorkflowContextError>` of a sub-workflow node |
| `output_for::<T>(&key)` | `Result<Option<T>, WorkflowContextError>`: the child's output |

- `WorkflowContextError`: `NotInContext { node_id }` (the node is not in
  `workflow_ctx_from`, or it did not end), `Unreadable { node_id, cause }`,
  `UnknownUserCode { node_id, cause }`.
- Each node of `workflow_ctx_from` must be a dependency of the node.
- `workflow_ctx_from` on a task without the `workflow_ctx` attribute is
  refused (`HRS-010`).
- Use a given `node_id` for each context source, and read it with
  `NodeKey::new(id)`.

## Sub-Workflows

```rust
use horsies::{
    HorsiesError, OutputlessResults, SubWorkflowParams, WorkflowDefConfig, WorkflowDefinition,
    WorkflowInput, WorkflowSpec, WorkflowSpecBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, WorkflowInput)]
struct RegionParams {
    region: String,
}

struct RegionReport;

impl WorkflowDefinition for RegionReport {
    type Output = OutputlessResults;
    type Params = RegionParams;

    fn name() -> &'static str {
        "region_report"
    }

    fn definition_key() -> &'static str {
        "reports.region_report.v1"
    }

    fn build_with(params: RegionParams) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(Self::name());
        builder.definition_key(Self::definition_key());
        let _fetched = builder.task(
            fetch_report::node()?.set(fetch_report::params::region(), params.region)?,
        );
        builder.build()
    }
}

struct AllRegions;

impl WorkflowDefinition for AllRegions {
    type Output = OutputlessResults;
    type Params = ();

    fn name() -> &'static str {
        "all_regions"
    }

    fn definition_key() -> &'static str {
        "reports.all_regions.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let _eu = builder.sub_workflow(
            RegionReport::node(SubWorkflowParams::Given(RegionParams { region: "eu".to_owned() }))?
                .node_id("eu"),
        );
        let _us = builder.sub_workflow(
            RegionReport::node(SubWorkflowParams::NotGiven)?
                .set(RegionParams::field_region(), "us".to_owned())?
                .node_id("us"),
        );
        Ok(WorkflowDefConfig::new())
    }
}
```

`fetch_report` is a task with one parameter `region: String`.

- `#[derive(WorkflowInput)]` on a named-field struct generates one token
  per field that serde reads: `RegionParams::field_region()`. It applies
  `rename` and `rename_all`, and makes no token for a `skip` or
  `skip_deserializing` field. A `flatten` field is a compile error.
- `SubWorkflowParams::NotGiven`: the child builds as defined and stores the
  input `{}`. `Given(params)`: the engine calls the child's `build_with`.
  Params given to a definition without params are refused (`HRS-022`).
- `SubWorkflowNode` has `set`, `set_params`, `arg_from`, `waits_for`,
  `waits_for_all`, `node_id`, `allow_failed_deps`, `join_all`,
  `join_any`, `join_quorum` and `key`.
- `SubWorkflowNode::of::<D>(params)` and a registered template's
  `template.node(params)` also make one.
- The depth limit of nested children is 32. `check()` reports a nesting
  that reaches it for certain (`HRS-036`) or repeats a definition with the
  same params at once (`HRS-037`).
- A failed child gives its node a `TaskError` whose `sub_workflow()` is the
  `SubWorkflowFailure`.

## Register and Start

Register the tasks first, then the workflow definitions.
`register_workflow_definition` builds the spec at once, and
`task_name::node()` refuses before `task_name::register`.

| Method | Returns |
|---|---|
| `app.register_workflow_definition::<D>()` | `Result<WorkflowFunction<D::Output>, HorsiesError>`, for a definition without params |
| `app.workflow_template::<D>()` | `Result<WorkflowTemplate<D::Params, D::Output>, HorsiesError>`, for a definition with params |
| `horsies::start_workflow::<D>()` | Starts `D` through its first registration in the process |
| `horsies::start_workflow_with::<D>(params)` | Starts `D` with params through its first registration |
| `app.start::<T>(&spec)`, `app.start_with_id::<T>(&spec, id)` | Starts a spec that needs no registration. Its sub-workflow children must be registered |
| `rt.start::<T>(&spec)`, `rt.start_with_id::<T>(&spec, id)` | The same, from a task body through `TaskRuntime` |
| `app.workflow_handle::<T>(workflow_id)` | The handle of a started workflow. No I/O |

| `WorkflowFunction<T>` | `WorkflowTemplate<P, T>` |
|---|---|
| `start()` | `start(params)` |
| `start_with_id(workflow_id)` | `start_with_id(params, workflow_id)` |
| `retry_start(&error)` | — |
| `handle(workflow_id)` | `handle(workflow_id)` |
| `spec()`, `name()`, `definition_key()` | `build(params)`, `node(params)`, `name()`, `definition_key()` |

```rust
use std::time::Duration;

use horsies::{Horsies, HorsiesError, TaskOutcome, WaitLimit, WorkflowTemplate};

fn register_checkout(
    app: &mut Horsies,
) -> Result<WorkflowTemplate<CheckoutParams, String>, HorsiesError> {
    let _charge = charge_order::register(app)?;
    let _receipt = send_receipt::register(app)?;
    let _audit = audit_checkout::register(app)?;
    app.workflow_template::<Checkout>()
}

async fn run_checkout(
    checkout: &WorkflowTemplate<CheckoutParams, String>,
    order: Order,
) -> Result<(), Box<dyn std::error::Error>> {
    let handle = checkout.start(CheckoutParams { order }).await?;
    let limit = WaitLimit::parse(Some(Duration::from_secs(60)))?;
    match handle.get(limit).await? {
        TaskOutcome::Succeeded(receipt) => println!("{receipt}"),
        TaskOutcome::Failed(error) => println!("checkout ended with {}", error.code().text()),
    }
    Ok(())
}
```

- A start with an id that a workflow of the same name already has starts
  nothing and returns its handle. An id of another workflow name is
  `WORKFLOW_ID_CONFLICT`.
- `start_workflow` and `start_workflow_with` before the registration in the
  process return `VALIDATION_FAILED`.
- With `AppConfig.resend_on_transient_err`, the core runs a retryable start
  failure again with the same workflow id, up to 3 times.
- In a worker child a start goes through the child's producer handle. A
  task that starts a workflow does not wait for it: a wait in a worker
  child is `WaitInsideTask`.
- Register every definition in `main`, so that each process (the worker
  children included) has it.

### `WorkflowStartError`

Fields: `code`, `message`, `retryable`, `workflow_name`, `workflow_id`
(`Option<Uuid>`).

| `WorkflowStartErrorCode` | When |
|---|---|
| `ValidationFailed` | Before any I/O: no definition key, an output type that is not the handle's (`HRS-025`), a sub-workflow node of a definition that is not registered (`HRS-020`), params given to a definition without params (`HRS-022`), a template build that refused, or a start before the registration in the process |
| `WorkflowIdConflict` | The id belongs to another workflow name. Nothing was written |
| `EnqueueFailed` | The connect, the schema install or the start failed. `retryable` is the classification of the cause. A start again with the same id finds the workflow when the commit reply was lost |

`WorkflowFunction::retry_start(&error)` starts again with the error's
`workflow_id`. It refuses with `ValidationFailed` an error that is not
`EnqueueFailed`, an error of another workflow name, and an error with no
workflow id. For a template, call `start_with_id(params, workflow_id)`
with the error's id.

## `WorkflowHandle<T>`

| Method | Returns |
|---|---|
| `workflow_id()` | `Uuid` |
| `get(limit)` | `Result<TaskOutcome<T, UserCode>, WorkflowReadError>`: waits while the workflow runs, up to `limit` |
| `result_for::<V, E>(&key)` | `Result<TaskOutcome<V, E>, WorkflowReadError>`: one node, read with its types. Not a wait |
| `status()` | `HandleResult<WorkflowStatus>` |
| `results()` | `HandleResult<BTreeMap<String, NodeResult>>`: the stored result of each node with a node id |
| `tasks()` | `HandleResult<Vec<WorkflowNodeInfo>>`: every node in index order |
| `cancel()` | `HandleResult<()>`: cancels the workflow and its descendants. An ended workflow stays as it is |
| `pause()` | `HandleResult<bool>`: `true` when a RUNNING workflow paused |
| `resume()` | `HandleResult<bool>`: `true` when a PAUSED workflow resumed. The owed children build from the app's registered definitions |

- `get` reads user codes as checked text (`UserCode`), because a workflow
  holds the codes of many tasks. A FAILED, CANCELLED or EXPIRED workflow
  is `Failed` with its stored error, or the code of its status. A PAUSED
  workflow is `Failed` with `WORKFLOW_PAUSED`; a resume can continue it.
- A workflow with no output node is read as `OutputlessResults`: `get(id)`,
  `iter()`, `len()`, `is_empty()`, each outcome as
  `TaskOutcome<serde_json::Value, UserCode>`.
- `NodeResult` is `Outcome(TaskOutcome<serde_json::Value, UserCode>)` or
  `Unreadable { cause }`.
- `WorkflowNodeInfo` has `node_id`, `index`, `name`, `status`
  (`WorkflowTaskStatus`), `result`, `started_at`, `completed_at`,
  `sub_workflow_id`, `sub_workflow_summary`.

| `WorkflowReadError` | Code | When |
|---|---|---|
| `WaitTimedOut { workflow_id }` | `WAIT_TIMEOUT` | The workflow had not ended when the limit passed |
| `WorkflowNotFound { workflow_id }` | `WORKFLOW_NOT_FOUND` | No workflow has the id |
| `ResultNotReady { workflow_id, node_id }` | `RESULT_NOT_READY` | `result_for`: the node stores no result yet, or no node has the id |
| `StoredResultUnreadable { workflow_id, cause }` | `RESULT_DESERIALIZATION_ERROR` | The stored value does not read with the handle's types |
| `UnknownUserCode { workflow_id, cause }` | `RESULT_DESERIALIZATION_ERROR` | The stored user code is not a code of `E` |
| `WaitInsideTask { message }` | `WAIT_INSIDE_TASK` | A wait in a worker child. Nothing was read |
| `Database { code, message, retryable }` | `BROKER_ERROR` | The database or the connection failed |

`HandleOperationError` (from `status`, `results`, `tasks`, `cancel`,
`pause`, `resume`) has `code`, `message`, `retryable`, `workflow_id`. The
operations document these codes: `WorkflowNotFound`, `DbOperationFailed`,
`StoredDataInvalid` (`status`, `tasks`, `resume`), and
`ResumeRecoveryFailed` (`resume` committed and the work after it failed:
the workflow is RUNNING).

## Success Policy and `on_error`

```rust
use horsies::{SuccessCase, SuccessPolicy, TypedNodeRef, WorkflowDefConfig};

fn checkout_policy(
    charged: TypedNodeRef<u64>,
    receipt: TypedNodeRef<String>,
    audit: TypedNodeRef<bool>,
) -> WorkflowDefConfig {
    let policy = SuccessPolicy::new(vec![SuccessCase::new([charged.untyped(), receipt.untyped()])])
        .optional([audit]);
    WorkflowDefConfig::new().output(receipt).success_policy(policy)
}
```

- The workflow succeeds when one case has every required node COMPLETED.
  The optional nodes may fail.
- `OnError::Fail` (default): a failed node fails the workflow after the DAG
  resolves. `OnError::Pause`: a failed node pauses the workflow;
  `handle.resume()` continues it.

## Statuses

| `WorkflowStatus` | `WorkflowTaskStatus` |
|---|---|
| `Pending`, `Running`, `Completed`, `Failed`, `Paused`, `Cancelled`, `Expired` | `Pending`, `Ready`, `Enqueued`, `Running`, `Completed`, `Failed`, `Skipped` |

`JoinType`: `All`, `Any`, `Quorum { min_success }` (1 to the node's
dependencies).

## Check Cases of a Dynamic Builder

A function that builds a spec at run time has no registration that
`check()` can build. Register check cases for it:

```rust
use horsies::{Horsies, HorsiesError, WorkflowSpec, WorkflowSpecBuilder};

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

`check()` builds and checks a spec from each case. A builder with no case
is refused (`HRS-027`).

## Validation Codes

| Code | Refusal |
|---|---|
| `HRS-004` | Two nodes with one node id |
| `HRS-006`, `HRS-011`, `HRS-012` | A reference of another builder (dependency, output, success policy) |
| `HRS-010` | `workflow_ctx_from` on a task without `workflow_ctx` |
| `HRS-013` | A quorum outside 1 to the node's dependencies, or `arg_from` on an `any` or quorum join |
| `HRS-016` | A definition with an empty definition key |
| `HRS-017` | A definition key or a workflow name that another registered definition has |
| `HRS-019` | An input member that binds to no field, or a member name of the engine (`__h_...`, `__horsies_...`) |
| `HRS-020` | A missing parameter; at start, a sub-workflow node of an unregistered definition |
| `HRS-021` | A parameter that both `set` and `arg_from` give |
| `HRS-022` | Params given to a definition without params |
| `HRS-025` | An output type that is not the declared or the handle's type |
| `HRS-026` | An input that is not a JSON object |
| `HRS-031` | A value that does not encode, does not read back, or has another type |
| `HRS-034` | A priority outside 1 to 100 |
| `HRS-036`, `HRS-037` | A sub-workflow nesting that reaches the depth limit, or repeats at once |

## Common Pitfalls

**Do not wait for a workflow in a task body.** `handle.get(..)` in a worker
child is `WaitInsideTask`. Start the workflow and return, or make the
waiting work a node of the workflow.

**Give `build_with` no side effects.** The engine can call it more than
once for one node.

**Register the tasks before the definitions.** A definition without params
builds at registration, and `task_name::node()` refuses before
`task_name::register`.

**Set the definition key in `build_with`.** The builder of `build_with` is
new: set `definition_key` (and `on_error`) on it. A start refuses a spec
without a key.

**Use the context for an `any` or quorum join.** `arg_from` on such a node
is `HRS-013`.

## All Key Imports

```rust
use horsies::{
    // Definitions and specs
    WorkflowDefinition, WorkflowDefConfig, WorkflowSpec, WorkflowSpecBuilder, OnError,
    SuccessPolicy, SuccessCase, OutputlessResults,
    // Nodes
    TaskNode, SubWorkflowNode, SubWorkflowParams, NodeRef, TypedNodeRef, NodeKey,
    InputField, JoinType, WorkflowInput,
    // Context
    WorkflowContext, WorkflowContextError, SubWorkflowSummary, WorkflowMeta,
    // Starts
    WorkflowFunction, WorkflowTemplate, WorkflowCheckBuilderRegistration,
    start_workflow, start_workflow_with, WorkflowStartError, WorkflowStartErrorCode,
    // Handle
    WorkflowHandle, WorkflowReadError, NodeResult, WorkflowNodeInfo, WorkflowStatus,
    WorkflowTaskStatus, HandleOperationError, HandleErrorCode, HandleResult,
    TaskOutcome, UserCode, WaitLimit,
};
```
