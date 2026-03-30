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
| `tasks.md` | `#[horsies::task]`, `TaskFunction`, `my_task::register()`, send/schedule/retry APIs, serialization |
| `workflows.md` | unified `horsies::Horsies`, `WorkflowFunction`, `TaskNode`, `WorkflowHandle`, DAG construction, failure semantics |
| `configs.md` | `AppConfig`, `PostgresConfig`, queues, recovery, scheduling, `Horsies::check()`, `check_live()`, workflow builders |

## Package architecture

| Package / module | Role |
|---|---|
| `horsies` | Unified public app facade. Also contains the internal `core`, `broker`, `workflow_engine`, and `worker` modules. |
| `horsies-macros` | `#[task]` / `#[blocking_task]` proc macros. |
| `horsies::core` | Internal, mostly IO-free types, config, registries, and validation. |
| `horsies::broker` | Internal broker implementation and row/handle types, re-exported via `horsies::`. |
| `horsies::workflow_engine` | Internal workflow runtime, binding, start/query/lifecycle, re-exported via `horsies::`. |
| `horsies::worker` | Internal worker runtime, recovery, scheduler service, re-exported via `horsies::`. |

## Define a Task

```rust
use horsies::{task, TaskError};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct AddArgs { a: i32, b: i32 }

#[task("add")]
async fn add(args: AddArgs) -> Result<i32, TaskError> {
    Ok(args.a + args.b)
}
```

Every task function returns `Result<T, TaskError>` where `T: Serialize + DeserializeOwned`.
The `#[task]` macro generates a `register()` function for one-line registration.

## Register and Send

```rust
use horsies::Horsies;

let mut app = Horsies::new(config)?;

let add_task = add::register(&mut app)?;

let handle = add_task.send(AddArgs { a: 5, b: 3 }).await?;
let result = handle.get(Some(Duration::from_secs(30))).await;
```

### Schedule with delay

```rust
let handle = add_task.schedule(Duration::from_secs(60), args).await?;
```

### Retry a failed send

```rust
match add_task.send(args).await {
    Ok(handle) => { /* use handle */ }
    Err(err) if err.retryable => {
        let handle = add_task.retry_send(&err).await?;
    }
    Err(err) => { /* permanent failure */ }
}
```

## Define and Start a Workflow

### Static (reusable, pre-registered)

```rust
use horsies::{Horsies, TaskNode};

let mut app = Horsies::new(config)?;
let mut wf = app.workflow::<Processed>("my_pipeline");
wf.definition_key("myapp.pipeline.v1");
let fetch = wf.task(TaskNode::<RawData>::new("fetch_data"));
let process = wf.task(
    TaskNode::<Processed>::new("process_data")
        .waits_for(fetch)
        .args_from("data", fetch),
);
wf.output(process);

let workflow = wf.build()?;
let handle = workflow.start().await?;
let result = handle.get(Some(Duration::from_secs(60))).await?;
```

### Dynamic (runtime-built spec)

```rust
let spec = WorkflowSpecBuilder::new("enrichment")
    .task(node_a)
    .task(node_b)
    .definition_key("myapp.enrichment.v1")
    .build()?;

let handle = app.start::<Output>(spec).await?;
```

### Inside a worker (after app is consumed)

```rust
// Before consuming:
let starter = app.workflow_starter();
app.run_worker_with(config).await?;

// Inside a task:
let handle = starter.start::<Output>(spec).await?;
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
let handle = workflow.handle("known-workflow-uuid").await?;
let status = handle.status().await?;
```

## Result Types

| Operation | Result type | Ok | Err |
|---|---|---|---|
| Task execution | `Result<T, TaskError>` | value `T` | `TaskError` |
| `task.send()` | `TaskSendResult<TaskHandle<T>>` | `TaskHandle` | `TaskSendError` |
| `workflow.start()` | `WorkflowStartResult<WorkflowHandle<T>>` | `WorkflowHandle` | `WorkflowStartError` |
| Broker infra | `BrokerResult<T>` | value `T` | `BrokerOperationError` |
| Handle ops | `HandleResult<T>` | value `T` | `HandleOperationError` |

## Error Code Families

- `OperationalErrorCode` — infra failures (UnhandledError, BrokerError, WorkerCrashed, etc.)
- `ContractCode` — API contract violations (ReturnTypeMismatch, ArgumentTypeMismatch)
- `RetrievalCode` — result retrieval (WaitTimeout, TaskNotFound, ResultNotReady)
- `OutcomeCode` — terminal lifecycle outcomes (TaskCancelled, TaskExpired, WorkflowPaused)
