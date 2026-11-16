---
name: horsies-rust-workflows
description: Workflow DAG guidance for horsies-rust, including builders, UUID handles, node timing, pause relocation, paused expiry, failure semantics, and validation. Use when building, starting, or troubleshooting workflows.
---

# horsies-rust — Workflows

Detailed reference for building, starting, and managing workflow DAGs.

## WorkflowSpec Construction

### `WorkflowDefinition` (primary reusable path)

```rust
use horsies::{
    HorsiesError, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder,
};

struct ETLPipeline;

impl WorkflowDefinition for ETLPipeline {
    type Output = SaveResult;
    type Params = ();

    fn name() -> &'static str { "etl_pipeline" }
    fn definition_key() -> &'static str { "myapp.etl_pipeline.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetch_ref = builder.task(fetch_data::node()?.node_id("fetch"));
        let process_ref = builder.task(
            process_data::node()?
                .waits_for(fetch_ref)
                .arg_from(ProcessDataInput::field_data(), fetch_ref)
                .node_id("process"),
        );
        let save_ref = builder.task(
            save_result::node()?
                .waits_for(process_ref)
                .arg_from(SaveResultInput::field_result(), process_ref)
                .node_id("save"),
        );
        Ok(WorkflowDefConfig::new().output(save_ref))
    }
}
```

Register and start it with:

```rust
let workflow = app.register_workflow_definition::<ETLPipeline>()?;
match workflow.start().await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

### `WorkflowRegistrationBuilder` (secondary / local path)

Good for local one-off reusable workflows that do not merit a named
definition type.

```rust
let mut app = horsies::Horsies::new(config)?;
let mut wb = app.workflow::<SaveResult>("etl_pipeline");
wb.definition_key("myapp.etl_pipeline.v1");
let fetch = wb.task(fetch_data::node()?);
let process = wb.task(
    process_data::node()?
        .waits_for(fetch)
        .arg_from(ProcessDataInput::field_data(), fetch),
);
let save = wb.task(
    save_result::node()?
        .waits_for(process)
        .arg_from(SaveResultInput::field_result(), process),
);
wb.output(save);

let workflow = wb.build()?;
match workflow.start().await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

### Builder API (advanced / ad hoc spec construction)

```rust
use horsies::{WorkflowSpecBuilder, OnError};

let mut builder = WorkflowSpecBuilder::new("etl_pipeline");
builder.definition_key("myapp.etl_pipeline.v1");

let fetch_ref = builder.task(fetch_data::node()?);
let process_ref = builder.task(
    process_data::node()?
        .waits_for(fetch_ref)
        .arg_from(ProcessDataInput::field_data(), fetch_ref),
);
let save_ref = builder.task(
    save_result::node()?
        .waits_for(process_ref)
        .arg_from(SaveResultInput::field_result(), process_ref),
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
use horsies::{HorsiesError, WorkflowDefinition, WorkflowSpec, WorkflowSpecBuilder};

struct ChildPipeline;

impl WorkflowDefinition for ChildPipeline {
    type Output = Processed;
    type Params = String;

    fn name() -> &'static str { "child_pipeline" }
    fn definition_key() -> &'static str { "myapp.child_pipeline.v1" }

    fn build_with(source_url: Self::Params) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new("child_pipeline");
        builder.definition_key("myapp.child_pipeline.v1");
        let fetch = builder.task(
            fetch_data::node()?
                .set_input(FetchDataInput { source_url })?
                .node_id("fetch"),
        );
        let process = builder.task(
            process_data::node()?
                .waits_for(fetch)
                .arg_from(ProcessDataInput::field_data(), fetch)
                .node_id("process"),
        );
        builder.output(process);
        builder.build()
    }
}

let child = app.workflow_template::<ChildPipeline>();
match child.start("https://example.com/data.json".to_owned()).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

Parameterized workflows do not need a placeholder `define()` implementation.
Override `build_with(params)` directly and use typed `node()` helpers with
`.set_input(...)`, `.set(...)`, and `.arg_from(...)`.

For child workflows built from runtime params, prefer
`app.register_parameterized_workflow(...)` over manually constructing a
placeholder `RegisteredWorkflowSpec`. It returns a `WorkflowTemplate<P, T>`
that can both `start(params)` and create a child node with `template.node()`:

```rust
let child = app.register_parameterized_workflow::<ChildParams, ChildOut, _>(
    "child_pipeline",
    "myapp.child_pipeline.v1",
    move |params| build_child_pipeline(params),
)?;

let child_ref = builder.sub_workflow(
    child
        .node()
        .queue("default") // required: sub-workflow nodes are not queue-resolved
        .set(ChildParams::field_limit(), 25)?
        .arg_from(ChildParams::field_input_result(), upstream),
);
```

> **Sub-workflow nodes require an explicit `.queue(...)`.** Unlike task nodes,
> a `SubWorkflowNode` is skipped by queue/priority resolution
> (`resolve_node_queue_priority`) and by the node queue validation in the spec
> resolver, but the start path still needs a resolved queue for its bookkeeping
> row — omitting `.queue(...)` fails at workflow start with "no resolved queue".
> Priority defaults to `100` if unset (these rows are never claimed as tasks).

Use `register_parameterized_workflow(...)` for leaf child workflows. If the
builder may itself emit nested child workflows, use
`register_parameterized_workflow_with_children(...)` and declare the child
workflow definition keys so registration-time cycle detection and `app.check()`
can validate those edges before runtime.

Use the binding style that matches the source of the value:

- `.set_input(value)?` for the node's whole explicit input
- `.set(task_name::params::field(), value)?` for one explicit parameter
- `.arg_from(task_name::params::field(), dep)` for upstream `TaskResult<_>` injection

Mixed-source nodes look like this:

```rust
#[horsies::task("notify_user")]
async fn notify_user(
    data: horsies::TaskResult<String>,
    urgent: bool,
) -> Result<(), horsies::TaskError> {
    let _ = data;
    let _ = urgent;
    Ok(())
}

let notify = builder.task(
    notify_user::node()?
        .waits_for(process)
        .arg_from(notify_user::params::data(), process)
        .set(notify_user::params::urgent(), true)?
        .node_id("notify"),
);
```

### Checked dynamic builders via `check_workflow_builder()`

Use this when the workflow shape depends on typed params and you want
`app.check()` to validate representative cases:

```rust
let fetch = fetch_data::register(&mut app)?;
let process = process_data::register(&mut app)?;

let mut registration = app.check_workflow_builder(
    "build_child_workflow",
    move |source_url: &String| {
        let mut builder = WorkflowSpecBuilder::new("child_pipeline");
        builder.definition_key("myapp.child_pipeline.v1");
        let fetch_ref = builder.task(fetch.node().set_input(FetchDataInput {
            source: source_url.clone(),
        })?);
        let process_ref = builder.task(
            process
                .node()
                .waits_for(fetch_ref)
                .arg_from(ProcessInput::field_data(), fetch_ref),
        );
        builder.output(process_ref);
        builder.build()
    },
)?;

registration.cases([
    "https://example.com/source-a.json".to_owned(),
    "https://example.com/source-b.json".to_owned(),
]);
registration.register()?;
app.check()?;
```

Use `app.check_workflow_builder0(...)` for zero-arg builders.

### Using `TaskFunction::node()`

`TaskFunction::node()` returns a `TaskNode<T, A>` pre-configured with the task's name, queue, priority, and retry task_options:

```rust
let fetch = fetch_data::register(&mut app)?;
let process = process_data::register(&mut app)?;
let save = save_result::register(&mut app)?;

let mut builder = WorkflowSpecBuilder::new("etl_pipeline");
let fetch_ref = builder.task(fetch.node());
let proc_ref = builder.task(
    process
        .node()
        .waits_for(fetch_ref)
        .arg_from(ProcessDataInput::field_data(), fetch_ref),
);
let save_ref = builder.task(
    save
        .node()
        .waits_for(proc_ref)
        .arg_from(SaveResultInput::field_result(), proc_ref),
);
builder.output(save_ref);
let spec = builder.build()?;
```

### Queue and priority resolution

`resolve_node_queue_priority` resolves each non-subworkflow node's queue and
priority — at workflow registration, at child-spec materialization, and during
`check()`:

- **Queue:** node override (`.queue(...)`) > the registered task's default queue.
- **Priority:** node override (`.priority(...)`) if set; otherwise the
  **resolved queue's** configured priority (`effective_priority`).

Consequence: overriding a node's `queue` while leaving `priority` unset adopts
the **new** queue's configured priority, not the task's original one. A real task
node that somehow reaches start with an unresolved priority fails closed
(`WorkflowError::Validation`, "no resolved priority") rather than silently
defaulting. (Sub-workflow nodes are exempt from this resolution — see
`SubWorkflowNode` below — and keep the `100` bookkeeping default.)

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

`WorkflowSpec` is the definition-only workflow type. It is IO-free and implemented in the internal `horsies::core` module. The primary executable workflow objects are `WorkflowFunction<T>` and `WorkflowTemplate<P, T>`.

## `TaskNode<T, A>`

Typed task node in a workflow DAG. Create nodes via the generated `#[task]` helpers:

```rust
// From a registered #[task] module:
my_task::node()?                  // TaskNode<T, A> with registered queue/priority/options
my_task::node()?.set_input(typed_args)? // same, with typed input pre-serialized
my_task::node()?.set(my_task::params::flag(), true)? // bind one explicit parameter

// Chaining methods:
my_task::node()?
    .waits_for(dep_ref)           // add dependency
    .waits_for_all(&[ref_a, ref_b])
    .arg_from(my_task::params::data(), dep_ref) // inject upstream TaskResult into a typed parameter
    .workflow_ctx_from([dep_ref]) // include deps in WorkflowContext
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

### `TypedNodeRef<T>` and `NodeRef`

`builder.task(node)` returns `TypedNodeRef<T>`, preserving the upstream
output type for methods like `arg_from(...)`.

Most workflow APIs accept `Into<NodeRef>`, so typed refs usually work without
manual conversion:

```rust
let fetch = builder.task(fetch_data::node()?);
let process = builder.task(
    process_data::node()?
        .waits_for(fetch)
        .arg_from(process_data::params::input_result(), fetch),
);
builder.output(process);
```

Use `NodeRef` only when you need a heterogeneous collection of refs with
different output types:

```rust
let mut deps: Vec<NodeRef> = Vec::new();
deps.push(fetch.into());
deps.push(process.into());
```

That erased form is for mixed collections and low-level wiring only. Keep refs
typed when possible.

## `SubWorkflowNode<T>`

Child workflow node. Resolved at execution time via `WorkflowDefinition` or registry lookup.

```rust
SubWorkflowNode::<(), ChildOutput>::typed("child_workflow_name")
    .queue("default") // required: sub-workflow nodes are not queue-resolved
    .set_input(child_params)?
    // or mix static + injected params:
    .set(ChildParams::field_limit(), 100)?
    .waits_for(dep_ref)
    .arg_from(ChildParams::field_input(), dep_ref)
```

A `SubWorkflowNode` must set `.queue(...)` explicitly (and may set `.priority(...)`);
see the note under the parameterized sub-workflow example above.

## Starting a Workflow

The blessed public start paths are:

- `horsies::start_workflow::<D>()` for global dispatch of zero-param workflows
- `horsies::start_workflow_with::<D>(params)` for global dispatch of parameterized workflows
- `WorkflowFunction<T>::start()` for reusable fixed workflows (setup/HTTP context)
- `WorkflowTemplate<P, T>::start(params)` for reusable parameterized workflows (setup/HTTP context)
- `app.start::<T>(spec)` for ad hoc external dynamic specs
- `TaskRuntime::start::<T>(spec)` for dynamic starts inside running tasks

All return `WorkflowStartResult<WorkflowHandle<T>>`.

### 1. Reusable workflow definition via `register_workflow_definition()` (primary)

Best for fixed DAGs known at setup time. Returns a `WorkflowFunction<T>` that
can be started multiple times.

```rust
let workflow = app.register_workflow_definition::<ETLPipeline>()?;
match workflow.start().await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
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
let child = app.workflow_template::<ChildPipeline>();
match child.start("https://example.com/data.json".to_owned()).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

`WorkflowTemplate<P, T>` exposes:
- `build(params)`
- `start(params)`
- `start_with_id(params, id)`

### 3. Dynamic workflow via `app.start()` (ad hoc, runtime-built)

Best for dynamic/parameterized workflows where the DAG is built at runtime
from parameters, user input, or external state.

```rust
let spec = WorkflowSpecBuilder::new("child_pipeline")
    .task(node_a)
    .task(node_b)
    .definition_key("myapp.child_pipeline.dynamic.v1")
    .build()?;

match app.start::<Output>(spec).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

Internally registers the spec and starts it in one step. No broker, registry,
or binding code required.

### 4. Inside a worker via `TaskRuntime` (primary for dynamic in-task starts)

Best for tasks that need to build a dynamic `WorkflowSpec` and start it from
inside a running task, after `app` has been consumed by `run_worker_with()`.

```rust
use horsies::{task, TaskError, TaskRuntime};

#[task("build_child_workflow")]
async fn build_child_workflow(
    rt: TaskRuntime,
    input: ChildInput,
) -> Result<(), TaskError> {
    if let Some(spec) = build_child_spec(&input)? {
        match rt.start::<Output>(spec).await {
            Ok(handle) => {
                tracing::info!(workflow_id = %handle.workflow_id(), "started child workflow");
            }
            Err(err) => {
                tracing::warn!(error = %err.message, "failed to start child workflow");
            }
        }
    }
    Ok(())
}
```

`TaskRuntime` is captured automatically by `#[task]` / `#[blocking_task]` when
it appears as the first parameter in the task signature.

Sub-workflows referenced by a dynamically-built spec must be registered
before the worker starts (i.e. before the app is consumed).

### 5. Global task dispatch from anywhere

Register once at startup, then call `task_name::send(args)` or
`task_name::schedule(delay, args)` from anywhere:

```rust
use horsies::{task, TaskError, TaskRuntime};

#[task("enqueue_add_numbers")]
async fn enqueue_add_numbers(rt: TaskRuntime) -> Result<(), TaskError> {
    match add_numbers::send(AddNumbersInput { a: 2, b: 3 }).await {
        Ok(handle) => {
            tracing::info!(task_id = %handle.task_id(), "sent add_numbers");
        }
        Err(err) => {
            tracing::warn!(error = %err.message, "failed to send add_numbers");
        }
    }

    match add_numbers::schedule(
        std::time::Duration::from_secs(30),
        AddNumbersInput { a: 5, b: 8 },
    )
    .await
    {
        Ok(handle) => {
            tracing::info!(task_id = %handle.task_id(), "scheduled add_numbers");
        }
        Err(err) => {
            tracing::warn!(error = %err.message, "failed to schedule add_numbers");
        }
    }

    // Explicit handle-based path (testing / advanced):
    let add_numbers_task = add_numbers::handle(&rt)?;
    match add_numbers_task.send(AddNumbersInput { a: 13, b: 21 }).await {
        Ok(handle) => {
            tracing::info!(task_id = %handle.task_id(), "sent add_numbers via handle");
        }
        Err(err) => {
            tracing::warn!(error = %err.message, "failed to send add_numbers via handle");
        }
    }
    Ok(())
}

add_numbers::register(&mut app)?;
enqueue_add_numbers::register(&mut app)?;
```

### 6. Global workflow dispatch from anywhere

Register once at startup, then start from anywhere:

```rust
// Zero-param workflow (after app.register_workflow_definition::<ETLPipeline>()):
match horsies::start_workflow::<ETLPipeline>().await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}

// Parameterized workflow (after app.workflow_template::<ChildPipeline>()):
match horsies::start_workflow_with::<ChildPipeline>("https://example.com/data.json".to_owned()).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

### 7. Inside a worker via `TaskRuntime::state()` (typed runtime state)

Use `app.provide(...)` for arbitrary app-owned runtime state such as config,
clients, or domain services:

```rust
struct AppSettings {
    bundesland: String,
}

app.provide(AppSettings {
    bundesland: "berlin".to_owned(),
})?;

#[task("use_settings")]
async fn use_settings(rt: TaskRuntime) -> Result<(), TaskError> {
    let settings = rt.state::<AppSettings>()?;
    tracing::info!(bundesland = %settings.bundesland);
    Ok(())
}
```

`rt.state::<T>()` returns `Result<Arc<T>, TaskError>`. Missing state is a task
error, not a panic.

### Auto-retry on start

When `resend_on_transient_err` is true, workflow starts retry up to 3 times (4 total attempts) with exponential backoff (200ms, 400ms, 800ms, cap 2000ms) on transient DB errors. The workflow_id is generated once and reused across retries for idempotency.

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
let handle = workflow.handle(known_workflow_uuid).await?;
let status = handle.status().await?;
```

## `WorkflowHandle<T>`

Workflow handle bound to a pool and registry. All operations are direct method calls.

### Methods

```rust
// Query
async fn status(&self) -> HandleResult<WorkflowStatus>
async fn get(&self, timeout: Option<Duration>) -> TaskResult<T>
async fn results(&self) -> HandleResult<HashMap<String, TaskResult<serde_json::Value>>>
async fn result_for<V: DeserializeOwned>(&self, node_id: &str) -> TaskResult<V>
async fn result_for_key<V: DeserializeOwned>(&self, key: &NodeKey<V>) -> TaskResult<V>  // typed-key variant
async fn tasks(&self) -> HandleResult<Vec<WorkflowTaskInfo>>

// Lifecycle
async fn cancel(&self) -> HandleResult<()>
async fn pause(&self) -> HandleResult<bool>   // Ok(true)=paused
async fn resume(&self) -> HandleResult<bool>  // Ok(true)=resumed

// Identity
fn workflow_id(&self) -> uuid::Uuid
```

### `get()` semantics

- Subscribes to `workflow_done` PG NOTIFY channel via shared listener.
- `COMPLETED` with an explicit `builder.output(...)` → returns that output task's `TaskResult`.
- `COMPLETED` without an explicit output → returns `TaskResult::Ok` containing a JSON object of terminal output task results keyed by `node_id`.
- `FAILED` / `CANCELLED` → returns `TaskResult::Err(TaskError(...))`.
- `EXPIRED` → returns the stored structured `WORKFLOW_EXPIRED` error.
- `PAUSED` → returns immediately with `TaskError(WorkflowPaused)`.
- Timeout → returns `TaskError(WaitTimeout)`.
- Infrastructure/query failures are folded into `TaskResult::Err(TaskError(BROKER_ERROR | WORKFLOW_NOT_FOUND, ...))`.

## `WorkflowStartError` / `WorkflowStartErrorCode`

```rust
pub struct WorkflowStartError {
    pub code: WorkflowStartErrorCode,
    pub message: String,
    pub retryable: bool,
    pub workflow_name: String,
    pub workflow_id: Option<uuid::Uuid>,
}
```

| Code | Retryable | When |
|---|---|---|
| `BrokerNotConfigured` | No | No broker attached to the app/runtime starting the workflow |
| `ValidationFailed` | No | DAG validation, serialization, or retry validation failure |
| `EnqueueFailed` | Maybe | Schema init or DB transaction failed |
| `InternalFailed` | No | Unexpected error |

## `HandleOperationError` / `HandleErrorCode`

```rust
pub struct HandleOperationError {
    pub code: HandleErrorCode,
    pub message: String,
    pub retryable: bool,
    pub workflow_id: uuid::Uuid,
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

```text
PENDING → RUNNING → COMPLETED | FAILED | CANCELLED
                  → PAUSED → EXPIRED
```

Terminal: `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`.
`PAUSED` is not terminal.

Set `AppConfig.retention.paused_workflow_auto_cancel_after` to expire old
paused workflows. `None` disables the policy. The stored error names the policy
and configured age. An expired child propagates to its parent like a cancelled
child.

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

A claimed backing task is abandoned as `CANCELLED` during pause. Its terminal
record moves to task history. The node returns to `READY` and clears its task
ID and `started_at`. Resume creates a fresh backing task.

A `PENDING` backing task stays live. A task that is already executing is not
interrupted.

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
    pub sub_workflow_id: Option<uuid::Uuid>,
    pub sub_workflow_summary: Option<String>,
}
```

For a regular task node, `started_at` stays `None` while the node is
`ENQUEUED`. The first ownership handoff to `RUNNING` stamps it. A replay against
an already-running node preserves the value. A requeue or pause reset clears
it before the node returns to `READY`.

A sub-workflow node stamps `started_at` when child launch begins.

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

### `arg_from` data flow

Injects the full `TaskResult` (not raw value) into a typed parameter token.
Prefer the task-module-generated `task_name::params::*` tokens on
multi-parameter tasks:

```rust
#[horsies::task("process_data")]
async fn process_data(
    input_result: horsies::TaskResult<FetchResult>,
) -> Result<TransformResult, horsies::TaskError> {
    let input = input_result?;
    Ok(TransformResult {
        processed_count: input.items.len(),
        data: input.items,
    })
}
```

`#[derive(WorkflowInput)]` on a receiving struct is still supported when you
want a named input type, but it is now the fallback path rather than the
default.

## Validation Errors

`app.check()` validates registered specs and checked-builder (`run_case`) specs.
Beyond structural DAG checks, Phase 2.11 **dry-runs each node's fully-static
kwargs against the referenced task's declared input type** (the same typed
deserialize the worker uses), reporting a mismatch as `HRS-019` at check-time
instead of at execution. Nodes with `args_from` or a node-level
`workflow_ctx_from` are skipped, since their static payload is intentionally
partial (the missing fields are injected at runtime).

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
| HRS-010 | `WorkflowCtxParamMissing` | ctx-capable task node missing required ctx param |
| HRS-011 | `WorkflowInvalidOutput` | output node not in task list |
| HRS-012 | `WorkflowInvalidSuccessPolicy` | Policy references unknown nodes |
| HRS-013 | `WorkflowInvalidJoin` | quorum without valid min_success |
| HRS-014 | `WorkflowUnresolvedQueue` | Node queue unresolved or invalid for the queue mode |
| HRS-015 | `WorkflowUnresolvedPriority` | Node priority unresolved at start |
| HRS-016 | `WorkflowNoDefinitionKey` | Missing definition_key |
| HRS-017 | `WorkflowDuplicateDefinitionKey` | Two specs share same key |
| HRS-018 | `WorkflowSubworkflowAppMissing` | Sub-workflow start without an app/runtime broker |
| HRS-019 | `WorkflowInvalidKwargKey` | Reserved/colliding kwarg key, or static node kwargs that don't match the task's declared input type (check Phase 2.11) |
| HRS-020 | `WorkflowMissingRequiredParams` | Node requires input but has none (args/kwargs/args_from) |
| HRS-025 | `WorkflowOutputTypeMismatch` | Declared output type mismatch |
| HRS-027 | `WorkflowCheckCasesRequired` | Parameterized builder checked without cases |
| HRS-028 | `WorkflowCheckCaseInvalid` | A checked builder case is invalid |
| HRS-029 | `WorkflowCheckBuilderException` | Builder panicked / returned wrong type during check |
| HRS-030 | `WorkflowCheckUndecoratedBuilder` | Checked builder not registered as a workflow builder |
| HRS-032 | `WorkflowArgsWithInjection` | Positional args combined with a runtime-injection source |

## All Key Imports

```rust
// All from the unified crate
use horsies::{
    // App + spec construction
    Horsies, WorkflowFunction, WorkflowSpec, WorkflowSpecBuilder, WorkflowDefinition,
    WorkflowTemplate,
    // Nodes
    TaskNode, SubWorkflowNode, NodeRef, AnyNode,
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
