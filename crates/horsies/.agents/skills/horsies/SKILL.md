---
name: horsies-rust-quick-reference
description: Quick orientation for the horsies Rust task queue and workflow engine. Use when users need a concise overview and routing to detailed guidance for tasks, workflows, and configuration.
---

# horsies-rust — Quick Reference

PostgreSQL-backed background task queue and workflow engine for Rust.
Port of the Python `horsies` library with the same mental model.

This is an **introductory quick reference** — it covers core concepts and
patterns at a glance. For production-level guidance, see the dedicated
skill files in this directory:

| File | When to open |
|---|---|
| `tasks.md` | `#[horsies::task]`, send and schedule options, UUID task IDs, idempotency, retention, history reads, and rerun |
| `workflows.md` | workflow builders, DAG behavior, `EXPIRED`, pause relocation, and node timing |
| `configs.md` | `AppConfig`, `RetentionConfig`, recovery, queues, scheduling, checks, and PgBouncer split URLs |

## Package architecture

| Package / module | Role |
|---|---|
| `horsies` | Unified public app facade. Also contains the internal `core`, `broker`, `workflow_engine`, and `worker` modules. |
| `horsies-macros` | `#[task]` / `#[blocking_task]` proc macros. |
| `horsies::core` | Internal, mostly IO-free types, config, registries, and validation. |
| `horsies::broker` | Internal broker implementation and row/handle types, re-exported via `horsies::`. |
| `horsies::workflow_engine` | Internal workflow runtime, binding, start/query/lifecycle, re-exported via `horsies::`. |
| `horsies::worker` | Internal worker runtime, recovery, scheduler service, re-exported via `horsies::`. |

## Alpha.26 storage model

`horsies_tasks` stores live work only. Its statuses are `PENDING`, `CLAIMED`,
and `RUNNING`.

A terminal task moves to partitioned `horsies_task_history`. The move and the
terminal outcome use one transaction. Result and task-info APIs read both
locations.

Task and workflow IDs are `uuid::Uuid`. New task IDs use UUIDv7.

Each enqueue fixes a retention class. The default is `standard_30d`. An
explicit forever choice prevents pruning. Idempotency keys deduplicate equal
requests and reject unequal requests under the same key.

Rerun creates a new task from retained terminal input. It records source and
root lineage. It never edits the terminal source.

Existing databases at migration 0032 need the offline cutover. Stop all
processes and take a named backup first. Follow the
[cutover runbook](https://suleymanozkeskin.github.io/horsies-rust/operations/cutover-runbook/).

## Define a Task

```rust
use horsies::{task, TaskError, TaskResult};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct AddNumbersInput { a: i32, b: i32 }

#[task("add_numbers")]
async fn add_numbers(args: AddNumbersInput) -> Result<i32, TaskError> {
    Ok(args.a + args.b)
}
```

Every task function returns `Result<T, TaskError>` where `T: Serialize + DeserializeOwned`.
The `#[task]` macro generates a `register()` function for one-line registration.
Tasks may take:
- one typed input value
- multiple typed parameters
- optional leading `TaskRuntime`

## Register and Send

```rust
use horsies::Horsies;

let mut app = Horsies::new(config)?;

let add_numbers_task = add_numbers::register(&mut app)?;

match add_numbers_task.send(AddNumbersInput { a: 5, b: 3 }).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(30))).await;
        match result {
            TaskResult::Ok(value) => println!("result: {value}"),
            TaskResult::Err(err) => eprintln!("task failed or timed out: {:?}", err),
        }
    }
    Err(err) => {
        eprintln!("send failed: {}", err.message);
    }
}
```

### Schedule with delay

```rust
match add_numbers_task.schedule(Duration::from_secs(60), args).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(90))).await;
        match result {
            TaskResult::Ok(value) => println!("result: {value}"),
            TaskResult::Err(err) => eprintln!("task failed or timed out: {:?}", err),
        }
    }
    Err(err) => {
        eprintln!("schedule failed: {}", err.message);
    }
}
```

### Retry a failed send

```rust
match add_numbers_task.send(args).await {
    Ok(handle) => { /* use handle */ }
    Err(err) if err.retryable => {
        let handle = add_numbers_task.retry_send(&err).await?;
    }
    Err(err) => { /* permanent failure */ }
}
```

### Send with identity and retention controls

```rust
use horsies::TaskSendOptions;

let handle = add_numbers_task
    .with_options(
        TaskSendOptions::new()
            .idempotency_key("request:42")
            .retention_class("audit_7d"),
    )
    .send(args)
    .await?;

let task_id: uuid::Uuid = handle.task_id();
```

Use `.retain_forever()` instead of `.retention_class(...)` when the terminal
record must not age out.

## Define and Start a Workflow

### Reusable definition (primary path)

```rust
use horsies::{
    Horsies, HorsiesError, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder,
};

let mut app = Horsies::new(config)?;

struct ETLPipeline;

impl WorkflowDefinition for ETLPipeline {
    type Output = Processed;
    type Params = ();

    fn name() -> &'static str { "etl_pipeline" }
    fn definition_key() -> &'static str { "myapp.etl_pipeline.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetch = builder.task(fetch_data::node()?.node_id("fetch"));
        let process = builder.task(
            process_data::node()?
                .waits_for(fetch)
                .arg_from(ProcessDataInput::field_data(), fetch)
                .node_id("process"),
        );
        let save = builder.task(
            save_result::node()?
                .waits_for(process)
                .arg_from(SaveResultInput::field_result(), process)
                .node_id("save"),
        );
        Ok(WorkflowDefConfig::new().output(save))
    }
}

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

### Parameterized reusable definition

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

For child workflows whose params come from a parent workflow, prefer
`app.register_parameterized_workflow(...)`. It returns a `WorkflowTemplate<P, T>`
that can both `start(params)` and create a child node via `template.node()`.
If the builder may emit nested child workflows, use
`register_parameterized_workflow_with_children(...)` and declare those child
definition keys so cycle checks can run before runtime.

### Dynamic (runtime-built spec)

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

### Inside a worker (dynamic workflow start)

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

`TaskRuntime` is the primary path for starting dynamic workflows from inside a
running task.

`builder.task(...)` returns `TypedNodeRef<T>`. Keep refs typed when possible;
use `.into()` only when you need a heterogeneous `Vec<NodeRef>` for mixed
output types.

### Checked dynamic workflow builders

Use `app.check_workflow_builder(...)` for representative-case validation of
runtime-built workflow specs during `app.check()`:

```rust
fetch_data::register(&mut app)?;
process_data::register(&mut app)?;

let mut registration = app.check_workflow_builder(
    "build_child_workflow",
    move |source_url: &String| {
        let mut builder = WorkflowSpecBuilder::new("child_pipeline");
        builder.definition_key("myapp.child_pipeline.v1");
        let fetch_ref = builder.task(fetch_data::node()?.set_input(FetchDataInput {
            source: source_url.clone(),
        })?);
        let process_ref = builder.task(
            process_data::node()?
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

`app.check()` also dry-runs schedule and workflow-node static input against each
task's declared input type, surfacing type mismatches at check-time rather than
at execution (details in `configs.md` and `workflows.md`).

For workflow data flow, prefer multi-parameter receiving tasks plus
`task_name::params::*` tokens. Use `#[derive(WorkflowInput)]` only when you
intentionally want a named receiving struct.

### Task dispatch from anywhere

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

### Workflow dispatch from anywhere

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

### Inside a worker (typed runtime state)

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

### Reconnect to an existing workflow

```rust
let handle = workflow.handle(known_workflow_uuid).await?;
let status = handle.status().await?;
```

## Blessed Start Patterns

- Global dispatch: `horsies::start_workflow::<D>()` and `horsies::start_workflow_with::<D>(params)` after startup registration
- Reusable setup/HTTP starts: `WorkflowFunction<T>` and `WorkflowTemplate<P, T>`
- Ad hoc external dynamic starts: `app.start::<T>(spec)`
- In-task dynamic starts: `TaskRuntime::start::<T>(spec)`

## Result Types

| Operation | Result type | Ok | Err |
|---|---|---|---|
| Task execution | `Result<T, TaskError>` | value `T` | `TaskError` |
| `task.send()` | `TaskSendResult<TaskHandle<T>>` | `TaskHandle` | `TaskSendError` |
| `TaskHandle::get()` | `TaskResult<T>` | `TaskResult::Ok(T)` | `TaskResult::Err(TaskError)` |
| `workflow.start()` | `WorkflowStartResult<WorkflowHandle<T>>` | `WorkflowHandle` | `WorkflowStartError` |
| `WorkflowHandle::get()` / `result_for()` | `TaskResult<T>` | `TaskResult::Ok(T)` | `TaskResult::Err(TaskError)` |
| `WorkflowHandle` query ops | `HandleResult<T>` | value `T` | `HandleOperationError` |
| Broker infra | `BrokerResult<T>` | value `T` | `BrokerOperationError` |

## Error Code Families

- `OperationalErrorCode` — infra failures (UnhandledError, BrokerError, WorkerCrashed, etc.)
- `ContractCode` — API contract violations (ReturnTypeMismatch, ArgumentTypeMismatch)
- `RetrievalCode` — result retrieval (WaitTimeout, TaskNotFound, ResultNotReady)
- `OutcomeCode` — terminal lifecycle outcomes (TaskCancelled, TaskExpired, WorkflowPaused)
