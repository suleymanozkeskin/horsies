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
struct AddNumbersInput { a: i32, b: i32 }

#[task("add_numbers")]
async fn add_numbers(args: AddNumbersInput) -> Result<i32, TaskError> {
    Ok(args.a + args.b)
}
```

Every task function returns `Result<T, TaskError>` where `T: Serialize + DeserializeOwned`.
The `#[task]` macro generates a `register()` function for one-line registration.

## Register and Send

```rust
use horsies::Horsies;

let mut app = Horsies::new(config)?;

let add_numbers_task = add_numbers::register(&mut app)?;

match add_numbers_task.send(AddNumbersInput { a: 5, b: 3 }).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(30))).await;
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

## Define and Start a Workflow

### Reusable definition (primary path)

```rust
use horsies::{
    Horsies, HorsiesError, TaskNode, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder,
};

let mut app = Horsies::new(config)?;

struct ETLPipeline;

impl WorkflowDefinition for ETLPipeline {
    type Output = Processed;
    type Params = ();

    fn name() -> &'static str { "etl_pipeline" }
    fn definition_key() -> &'static str { "myapp.etl_pipeline.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetch = builder.task(TaskNode::<RawData>::new("fetch_data").node_id("fetch"));
        let process = builder.task(
            TaskNode::<Processed>::new("process_data")
                .waits_for(fetch)
                .args_from("data", fetch)
                .node_id("process"),
        );
        let save = builder.task(
            TaskNode::<SaveResult>::new("save_result")
                .waits_for(process)
                .args_from("result", process)
                .node_id("save"),
        );
        Ok(WorkflowDefConfig::new().output(save))
    }
}

let workflow = app.register_workflow_definition::<ETLPipeline>()?;
match workflow.start().await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await?;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

### Parameterized reusable definition

```rust
let regional = app.workflow_template::<RegionalPipeline>();
match regional.start("eu-west".to_owned()).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await?;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

### Dynamic (runtime-built spec)

```rust
let spec = WorkflowSpecBuilder::new("enrichment")
    .task(node_a)
    .task(node_b)
    .definition_key("myapp.enrichment.v1")
    .build()?;

match app.start::<Output>(spec).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await?;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

### Inside a worker (dynamic workflow start)

```rust
use horsies::{task, TaskError, TaskRuntime};

#[task("build_regional_workflow")]
async fn build_regional_workflow(
    rt: TaskRuntime,
    input: RegionalInput,
) -> Result<(), TaskError> {
    if let Some(spec) = build_regional_spec(&input)? {
        match rt.start::<Output>(spec).await {
            Ok(handle) => {
                tracing::info!(workflow_id = %handle.workflow_id(), "started regional workflow");
            }
            Err(err) => {
                tracing::warn!(error = %err.message, "failed to start regional workflow");
            }
        }
    }
    Ok(())
}
```

`TaskRuntime` is the primary path for starting dynamic workflows from inside a
running task.

### Inside a worker (task-to-task dispatch)

Registered tasks now generate runtime helpers automatically:

```rust
use horsies::{task, TaskError, TaskRuntime};

#[task("enqueue_add_numbers")]
async fn enqueue_add_numbers(rt: TaskRuntime) -> Result<(), TaskError> {
    match add_numbers::send(&rt, AddNumbersInput { a: 2, b: 3 }).await {
        Ok(handle) => {
            tracing::info!(task_id = %handle.task_id(), "sent add_numbers");
        }
        Err(err) => {
            tracing::warn!(error = %err.message, "failed to send add_numbers");
        }
    }

    match add_numbers::schedule(
        &rt,
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
