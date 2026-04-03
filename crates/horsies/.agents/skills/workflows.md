---
name: horsies-rust-workflows
description: Workflow DAG guidance for horsies-rust, including unified `horsies::Horsies`, `WorkflowFunction`, `WorkflowSpec`, `TaskNode`, `SubWorkflowNode`, `WorkflowHandle`, failure semantics, and validation. Use when building, starting, or troubleshooting workflows.
---

# horsies-rust — Workflows

Detailed reference for building, starting, and managing workflow DAGs.

## WorkflowSpec Construction

### `WorkflowDefinition` (primary reusable path)

```rust
use horsies::{
    HorsiesError, TaskNode, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder,
};

struct Pipeline;

impl WorkflowDefinition for Pipeline {
    type Output = SaveResult;
    type Params = ();

    fn name() -> &'static str { "etl_pipeline" }
    fn definition_key() -> &'static str { "myapp.etl_pipeline.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetch_ref = builder.task(TaskNode::<RawData>::new("fetch_data").node_id("fetch"));
        let process_ref = builder.task(
            TaskNode::<Processed>::new("process_data")
                .waits_for(fetch_ref)
                .args_from("data", fetch_ref)
                .node_id("process"),
        );
        let save_ref = builder.task(
            TaskNode::<SaveResult>::new("save_result")
                .waits_for(process_ref)
                .args_from("result", process_ref)
                .node_id("save"),
        );
        Ok(WorkflowDefConfig::new().output(save_ref))
    }
}
```

Register and start it with:

```rust
let workflow = app.register_workflow_definition::<Pipeline>()?;
let handle = workflow.start().await?;
```

### `WorkflowRegistrationBuilder` (secondary / local path)

Good for local one-off reusable workflows that do not merit a named
definition type.

```rust
let mut app = horsies::Horsies::new(config)?;
let mut wb = app.workflow::<()>("my_pipeline");
wb.definition_key("myapp.pipeline.v1");
wb.task(TaskNode::<()>::new("step_a"));
let step_b = wb.task(TaskNode::<()>::new("step_b"));
wb.output(step_b);

let workflow = wb.build()?;
let handle = workflow.start().await?;
```

### Builder API (advanced / ad hoc spec construction)

```rust
use horsies::{WorkflowSpecBuilder, TaskNode, OnError};

let mut builder = WorkflowSpecBuilder::new("etl_pipeline");
builder.definition_key("myapp.etl_pipeline.v1");

let fetch_ref = builder.task(TaskNode::<RawData>::new("fetch_data"));
let process_ref = builder.task(
    TaskNode::<Processed>::new("process_data")
        .waits_for(fetch_ref)
        .args_from("data", fetch_ref),
);
let save_ref = builder.task(
    TaskNode::<SaveResult>::new("save_result")
        .waits_for(process_ref)
        .args_from("result", process_ref),
);

builder.on_error(OnError::Fail);
builder.output(save_ref);
let spec = builder.build()?;
```

### Parameterized definitions via `build_with()`

For reusable parameterized workflow-definition types, override
`WorkflowDefinition::build_with(...)` and start them through
`app.workflow_template::<...>()`:

```rust
use horsies::{HorsiesError, TaskNode, WorkflowDefConfig, WorkflowDefinition, WorkflowSpec, WorkflowSpecBuilder};

struct RegionalPipeline;

impl WorkflowDefinition for RegionalPipeline {
    type Output = ();
    type Params = String;

    fn name() -> &'static str { "regional_pipeline" }
    fn definition_key() -> &'static str { "myapp.regional_pipeline.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let step = builder.task(TaskNode::<()>::new("placeholder"));
        Ok(WorkflowDefConfig::new().output(step))
    }

    fn build_with(region: Self::Params) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(format!("regional_{region}"));
        builder.definition_key(format!("myapp.regional_pipeline.{region}.v1"));
        let step = builder.task(
            TaskNode::<()>::new("run_region")
                .args_json(serde_json::to_string(&region).unwrap()),
        );
        builder.output(step);
        builder.build()
    }
}

let regional = app.workflow_template::<RegionalPipeline>();
let handle = regional.start("eu-west".to_owned()).await?;
```

### Explicit workflow builders for `check()`

Rust does not use Python-style decorators/import scanning. Instead, register
builders explicitly on the app:

```rust
let mut registration = app.workflow_builder("regional_builder", |_app, region: &String| {
    let mut builder = WorkflowSpecBuilder::new(format!("regional_{region}"));
    builder.definition_key(format!("myapp.regional.{region}.v1"));
    let step = builder.task(TaskNode::<()>::new("run_region"));
    builder.output(step);
    builder.build()
})?;

registration.cases(["us-east".to_owned(), "eu-west".to_owned()]);
registration.register()?;

app.check()?;
```

Behavior:
- `workflow_builder0(...)` auto-invokes zero-arg builders once during `check()`
- `workflow_builder(...)` requires typed `.case(...)` / `.cases(...)` for parameterized builders
- builders execute under internal send suppression during `check()`
- missing cases -> `HRS-027`
- missing `definition_key` on the produced spec -> `HRS-016`
- builder panics / untyped failures -> `HRS-029`

### Using `TaskFunction::node()`

`TaskFunction::node()` returns a `TaskNode<T>` pre-configured with the task's name, queue, priority, good_until, and task_options:

```rust
let fetch = fetch_data::register(&mut app)?;
let process = process_data::register(&mut app)?;

let mut builder = WorkflowSpecBuilder::new("pipeline");
let fetch_ref = builder.task(fetch.node());
let proc_ref = builder.task(process.node().waits_for(fetch_ref).args_from("data", fetch_ref));
builder.output(proc_ref);
let spec = builder.build()?;
```

## `WorkflowSpec`

Immutable, validated workflow definition.

```rust
pub struct WorkflowSpec {
    pub name: String,
    pub definition_key: Option<String>,
    pub tasks: Vec<AnyNode>,
    pub on_error: OnError,
    pub output_index: Option<usize>,
    pub success_policy: Option<SuccessPolicy>,
}
```

`WorkflowSpec` is the definition-only workflow type. It is IO-free and implemented in the internal `horsies::core` module. The primary executable workflow object is `WorkflowFunction<T>`; the lower-level executable form is `BoundWorkflowSpec<T>`.

## `TaskNode<T>`

Typed task node in a workflow DAG.

```rust
TaskNode::<MyOutput>::new("task_name")
    .kwargs(json_string)          // serialized keyword arguments
    .waits_for(dep_ref)           // add dependency
    .waits_for_all(&[ref_a, ref_b])
    .args_from("key", dep_ref)    // inject upstream result as kwarg
    .queue("critical")            // override queue
    .priority(1)                  // override priority
    .good_until(deadline)         // task expiry
    .task_options(json)           // serialized TaskOptions
    .allow_failed_deps(true)      // run even if deps failed
    .join_any()                   // run when ANY dep completes
    .join_quorum(2)               // run when N deps complete
    .node_id("my_node")          // explicit node ID
```

### Auto-assigned `node_id`

When not set explicitly, `WorkflowSpecBuilder::build()` assigns `"{slugified_workflow_name}:{index}"`.

### `NodeRef`

Returned by `builder.task(node)`. Used for wiring dependencies and output selection.

## `SubWorkflowNode<T>`

Child workflow node. Resolved at execution time via `WorkflowDefinition` or registry lookup.

```rust
SubWorkflowNode::<ChildOutput>::new("child_workflow_name")
    .waits_for(dep_ref)
    .args_from("input", dep_ref)
```

## Starting a Workflow

Three paths depending on context. All return `WorkflowStartResult<WorkflowHandle<T>>`.

### 1. Reusable workflow definition via `register_workflow_definition()` (primary)

Best for fixed DAGs known at setup time. Returns a `WorkflowFunction<T>` that
can be started multiple times.

```rust
let workflow = app.register_workflow_definition::<Pipeline>()?;
let handle = workflow.start().await?;
let result = handle.get(Some(Duration::from_secs(60))).await?;
```

`WorkflowFunction<T>` is the workflow-side equivalent of task `TaskFunction<A, T>`:
- `start()`
- `start_with_id(id)`
- `retry_start(&err)`
- `handle(id).await`
- `spec()`

### 2. Parameterized reusable workflow via `workflow_template()` (primary for params)

Best for reusable workflows whose DAG shape or fixed node inputs depend on
typed runtime params.

```rust
let regional = app.workflow_template::<RegionalPipeline>();
let handle = regional.start("eu-west".to_owned()).await?;
let result = handle.get(Some(Duration::from_secs(60))).await?;
```

`WorkflowTemplate<P, T>` exposes:
- `build(params)`
- `start(params)`
- `start_with_id(params, id)`

### 3. Dynamic workflow via `app.start()` (ad hoc, runtime-built)

Best for dynamic/parameterized workflows where the DAG is built at runtime
from parameters, user input, or external state.

```rust
let spec = WorkflowSpecBuilder::new("enrichment")
    .task(node_a)
    .task(node_b)
    .definition_key("myapp.enrichment.v1")
    .build()?;

let handle = app.start::<Output>(spec).await?;
let result = handle.get(Some(Duration::from_secs(60))).await?;
```

Internally registers the spec and starts it in one step. No broker, registry,
or binding code required.

### 4. Inside a worker via `TaskRuntime` (primary for dynamic in-task starts)

Best for tasks that need to build a dynamic `WorkflowSpec` and start it from
inside a running task, after `app` has been consumed by `run_worker_with()`.

```rust
use horsies::{task, TaskError, TaskRuntime};

#[task("scrape_detail")]
async fn scrape_detail(rt: TaskRuntime, input: ScrapeInput) -> Result<(), TaskError> {
    if let Some(spec) = build_enrichment_spec(&input)? {
        let handle = rt.start::<Output>(spec).await?;
        tracing::info!(workflow_id = %handle.workflow_id(), "started enrichment workflow");
    }
    Ok(())
}
```

`TaskRuntime` is captured automatically by `#[task]` / `#[blocking_task]` when
it appears as the first parameter in the task signature.

Sub-workflows referenced by a dynamically-built spec must be registered
before the worker starts (i.e. before the app is consumed).

### 5. `WorkflowStarter` (advanced / lower-level)

`WorkflowStarter` remains the lower-level workflow launcher that powers
`TaskRuntime`. Use it when you explicitly want a cloneable launcher object
outside the task-macro injection path.

```rust
let starter = app.workflow_starter();
let handle = starter.start::<Output>(spec).await?;
```

### Auto-retry on start

When `resend_on_transient_err` is true, `WorkflowFunction::start()` and `BoundWorkflowSpec::start()` retry up to 3 times (4 total attempts) with exponential backoff (200ms, 400ms, 800ms, cap 2000ms) on transient DB errors. The workflow_id is generated once and reused across retries for idempotency.

### Retry a failed start

```rust
match workflow.start().await {
    Ok(handle) => { /* ... */ }
    Err(err) if err.retryable => {
        let handle = workflow.retry_start(&err).await?;
    }
    Err(err) => { /* permanent failure */ }
}
```

`retry_start` validates:
- Only `ENQUEUE_FAILED` errors are eligible
- Cross-workflow retry is rejected (error workflow_name must match spec)

Uses the internal `validate_start_retry()` helper in `horsies::core::workflow::start_types`.

### Reconnect to an existing workflow

```rust
let handle = workflow.handle("known-workflow-uuid").await?;
let status = handle.status().await?;
```

## `BoundWorkflowSpec<T>` (advanced plumbing)

Low-level executable wrapper around a `WorkflowSpec`, bound to a `PgPool`,
`WorkflowSpecRegistry`, and retry config. Most users should use
`app.start()`, `app.workflow().build()`, or `TaskRuntime` instead.

This exists for cases where you already hold a pool and registry directly
(e.g. custom orchestration outside the `Horsies` app).

```rust
use horsies::{BoundWorkflowSpec, WorkflowSpecExt};

let bound: BoundWorkflowSpec<T> = spec.bind_with_broker(&broker, registry, resend);
let handle = bound.start().await?;
```

## `WorkflowHandle<T>`

Workflow handle bound to a pool and registry. All operations are direct method calls.

### Methods

```rust
// Query
async fn status(&self) -> HandleResult<WorkflowStatus>
async fn get(&self, timeout: Option<Duration>) -> HandleResult<TaskResult<T>>
async fn results(&self) -> HandleResult<HashMap<String, TaskResult<serde_json::Value>>>
async fn result_for<V: DeserializeOwned>(&self, node_id: &str) -> HandleResult<TaskResult<V>>
async fn tasks(&self) -> HandleResult<Vec<WorkflowTaskInfo>>

// Lifecycle
async fn cancel(&self) -> HandleResult<()>
async fn pause(&self) -> HandleResult<bool>   // Ok(true)=paused
async fn resume(&self) -> HandleResult<bool>  // Ok(true)=resumed

// Identity
fn workflow_id(&self) -> &str
```

### `get()` semantics

- Subscribes to `workflow_done` PG NOTIFY channel via shared listener.
- `COMPLETED` → returns output task's `TaskResult`.
- `FAILED` / `CANCELLED` → returns `TaskResult::Err(TaskError(...))`.
- `PAUSED` → returns immediately with `TaskError(WorkflowPaused)`.
- Timeout → returns `TaskError(WaitTimeout)`.

## `WorkflowStartError` / `WorkflowStartErrorCode`

```rust
pub struct WorkflowStartError {
    pub code: WorkflowStartErrorCode,
    pub message: String,
    pub retryable: bool,
    pub workflow_name: String,
    pub workflow_id: String,
}
```

| Code | Retryable | When |
|---|---|---|
| `ValidationFailed` | No | DAG validation, serialization, or retry validation failure |
| `EnqueueFailed` | Maybe | Schema init or DB transaction failed |
| `InternalFailed` | No | Unexpected error |

Note: Rust has no `BrokerNotConfigured` — the bound approach ensures broker is always present.

## `HandleOperationError` / `HandleErrorCode`

```rust
pub struct HandleOperationError {
    pub code: HandleErrorCode,
    pub message: String,
    pub retryable: bool,
    pub workflow_id: String,
}
```

| Code | Retryable | When |
|---|---|---|
| `WorkflowNotFound` | No | Workflow ID doesn't exist |
| `DbOperationFailed` | Maybe | DB query/commit failure |
| `LoopRunnerFailed` | No | Sync bridge failure |
| `InternalFailed` | No | Unexpected exception |

## Enums

### `WorkflowStatus`

```
PENDING → RUNNING → COMPLETED | FAILED | PAUSED | CANCELLED
```

Terminal: `COMPLETED`, `FAILED`, `CANCELLED`. **`PAUSED` is NOT terminal.**

### `WorkflowTaskStatus`

```
PENDING → READY → ENQUEUED → RUNNING → COMPLETED | FAILED | SKIPPED
```

Terminal: `COMPLETED`, `FAILED`, `SKIPPED`.

### `OnError`

| Variant | Behavior |
|---|---|
| `Fail` (default) | DAG continues; failed dependents are SKIPPED; workflow becomes FAILED when all tasks terminal |
| `Pause` | Workflow immediately becomes PAUSED on first task failure |

### `JoinType`

| Variant | Triggers when |
|---|---|
| `All` (default) | ALL deps terminal |
| `Any` | ANY dep COMPLETED |
| `Quorum` | `min_success` deps COMPLETED |

## `WorkflowTaskInfo`

```rust
pub struct WorkflowTaskInfo {
    pub node_id: Option<String>,
    pub index: i32,
    pub name: String,
    pub status: WorkflowTaskStatus,
    pub result: Option<TaskResult<serde_json::Value>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub sub_workflow_id: Option<String>,
    pub sub_workflow_summary: Option<String>,
}
```

## `SuccessPolicy` / `SuccessCase`

```rust
let policy = SuccessPolicy {
    cases: vec![
        SuccessCase { required: vec![door_ref] },
        SuccessCase { required: vec![neighbor_ref] },
    ],
    optional: vec![notification_ref],
};
```

Workflow `COMPLETED` if any case has all required tasks `COMPLETED`. Without policy: any failure → `FAILED`.

## Failure Semantics

### SKIPPED cascade

Failed task → dependents SKIPPED (unless `allow_failed_deps`). Cascades transitively.

### `allow_failed_deps`

| Upstream | `false` (default) | `true` |
|---|---|---|
| COMPLETED | Runs | Runs |
| FAILED | SKIPPED | Runs (receives `TaskResult::Err`) |
| SKIPPED | SKIPPED | Runs (receives `TaskResult::Err(UpstreamSkipped)`) |

### `args_from` data flow

Injects the full `TaskResult` (not raw value) as a kwarg. Receiving function parameter must accept serialized `TaskResult`.

## Validation Errors

| Code | Name | When |
|---|---|---|
| HRS-001 | `WorkflowNoName` | Missing name |
| HRS-002 | `WorkflowNoNodes` | No tasks |
| HRS-003 | `WorkflowInvalidNodeId` | Bad node_id format |
| HRS-004 | `WorkflowDuplicateNodeId` | Duplicate node_id |
| HRS-005 | `WorkflowNoRootTasks` | All tasks have deps |
| HRS-006 | `WorkflowInvalidDependency` | waits_for references unknown node |
| HRS-007 | `WorkflowCycleDetected` | Cycle in DAG |
| HRS-008 | `WorkflowInvalidArgsFrom` | args_from node not in waits_for |
| HRS-009 | `WorkflowInvalidCtxFrom` | workflow_ctx_from node not in waits_for |
| HRS-011 | `WorkflowInvalidOutput` | output node not in task list |
| HRS-012 | `WorkflowInvalidSuccessPolicy` | Policy references unknown nodes |
| HRS-013 | `WorkflowInvalidJoin` | quorum without valid min_success |
| HRS-016 | `WorkflowNoDefinitionKey` | Missing definition_key |
| HRS-017 | `WorkflowDuplicateDefinitionKey` | Two specs share same key |

## All Key Imports

```rust
// All from the unified crate
use horsies::{
    // App + spec construction
    Horsies, WorkflowFunction, WorkflowStarter, WorkflowSpec, WorkflowSpecBuilder,
    WorkflowDefinition,
    // Nodes
    TaskNode, SubWorkflowNode, NodeRef, NodeKey, AnyNode,
    // Policies
    OnError, SuccessPolicy, SuccessCase, JoinType,
    // Context
    WorkflowContext, WorkflowMeta, SubWorkflowSummary,
    // Handles
    WorkflowHandle, WorkflowTaskInfo,
    // Enums
    WorkflowStatus, WorkflowTaskStatus,
    // Result types
    HandleResult, HandleOperationError, HandleErrorCode,
    WorkflowStartResult, WorkflowStartError, WorkflowStartErrorCode,
};
```
