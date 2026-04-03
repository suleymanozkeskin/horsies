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

### Reusable definition (primary path)

```rust
use horsies::{
    Horsies, HorsiesError, TaskNode, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder,
};

let mut app = Horsies::new(config)?;

struct Pipeline;

impl WorkflowDefinition for Pipeline {
    type Output = Processed;
    type Params = ();

    fn name() -> &'static str { "my_pipeline" }
    fn definition_key() -> &'static str { "myapp.pipeline.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetch = builder.task(TaskNode::<RawData>::new("fetch_data").node_id("fetch"));
        let process = builder.task(
            TaskNode::<Processed>::new("process_data")
                .waits_for(fetch)
                .args_from("data", fetch)
                .node_id("process"),
        );
        Ok(WorkflowDefConfig::new().output(process))
    }
}

let workflow = app.register_workflow_definition::<Pipeline>()?;
let handle = workflow.start().await?;
let result = handle.get(Some(Duration::from_secs(60))).await?;
```

### Parameterized reusable definition

```rust
let regional = app.workflow_template::<RegionalPipeline>();
let handle = regional.start("eu-west".to_owned()).await?;
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

### Inside a worker (dynamic workflow start)

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

`TaskRuntime` is the primary path for starting dynamic workflows from inside a
running task. `WorkflowStarter` still exists as the lower-level launcher when
you need to work with runtime plumbing directly.

### Inside a worker (task-to-task dispatch)

Registered tasks now generate runtime helpers automatically:

```rust
use horsies::{task, TaskError, TaskRuntime};

#[task("enqueue_extract_jobs")]
async fn enqueue_extract_jobs(rt: TaskRuntime) -> Result<(), TaskError> {
    extract_attachment_text::send(
        &rt,
        ExtractTextInput {
            file_id: 42,
            bundesland: "berlin".to_owned(),
        },
    )
    .await
    .map_err(|err| TaskError::user("SEND_FAILED", err.message))?;

    extract_attachment_text::schedule(
        &rt,
        std::time::Duration::from_secs(30),
        ExtractTextInput {
            file_id: 43,
            bundesland: "hamburg".to_owned(),
        },
    )
    .await
    .map_err(|err| TaskError::user("SCHEDULE_FAILED", err.message))?;

    let extract = extract_attachment_text::handle(&rt)?;
    extract
        .send(ExtractTextInput {
            file_id: 44,
            bundesland: "berlin".to_owned(),
        })
        .await
        .map_err(|err| TaskError::user("SEND_FAILED", err.message))?;
    Ok(())
}

extract_attachment_text::register(&mut app)?;
enqueue_extract_jobs::register(&mut app)?;
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
