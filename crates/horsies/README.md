<p align="center">
  <img src="https://suleymanozkeskin.github.io/horsies/galloping-horsie.jpg" alt="Horsies Logo" width="200" style="border-radius: 20px" />
</p>

# Horsies Rust

**PostgreSQL-backed background task queue and workflow engine for Rust.**

Rust port of [horsies](https://github.com/suleymanozkeskin/horsies) (Python).

## Features

- **Tasks** with typed arguments, retry policies, exception mapping, and scheduling
- **Workflow DAGs** with dependencies, data flow between nodes, join modes, success policies
- **PostgreSQL broker** with LISTEN/NOTIFY, connection pooling, automatic schema migrations
- **Worker runtime** with heartbeats, stale task recovery, graceful shutdown
- **Scheduler** for recurring tasks (interval, hourly, daily, weekly, monthly)
- **Validation** via `app.check()` — catches DAG errors, config issues, and type mismatches before deploy

## Quick Start

### Define a task

```rust
use horsies::{task, TaskError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct AddNumbersInput {
    a: i32,
    b: i32,
}

#[task("add_numbers")]
async fn add_numbers(args: AddNumbersInput) -> Result<i32, TaskError> {
    Ok(args.a + args.b)
}
```

### Register and send

```rust
let mut app = Horsies::new(config)?;
let add_numbers_task = add_numbers::register(&mut app)?;

match add_numbers_task.send(AddNumbersInput { a: 2, b: 3 }).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(30))).await?;
    }
    Err(err) => {
        eprintln!("send failed: {}", err.message);
    }
}
```

### Build and start a workflow

Reusable workflows should be defined with `WorkflowDefinition` and registered once:

```rust
use horsies::{
    HorsiesError, TaskNode, WorkflowDefConfig, WorkflowDefinition, WorkflowFunction,
    WorkflowSpecBuilder,
};

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
            TaskNode::<Processed>::new("save_result")
                .waits_for(process)
                .args_from("result", process)
                .node_id("save"),
        );
        Ok(WorkflowDefConfig::new().output(save))
    }
}

let workflow: WorkflowFunction<Processed> =
    app.register_workflow_definition::<ETLPipeline>()?;

match workflow.start().await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await?;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

Parameterized reusable workflows should use `WorkflowDefinition::build_with(...)` plus `app.workflow_template::<...>()`:

```rust
struct RegionalPipeline;

impl WorkflowDefinition for RegionalPipeline {
    type Output = Processed;
    type Params = String;

    fn name() -> &'static str { "regional_pipeline" }
    fn definition_key() -> &'static str { "myapp.regional_pipeline.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetch = builder.task(TaskNode::<Processed>::new("fetch_data").node_id("fetch"));
        let process = builder.task(
            TaskNode::<Processed>::new("process_data")
                .waits_for(fetch)
                .args_from("data", fetch)
                .node_id("process"),
        );
        Ok(WorkflowDefConfig::new().output(process))
    }

    fn build_with(region: Self::Params) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(format!("regional_{region}"));
        builder.definition_key(format!("myapp.regional_pipeline.{region}.v1"));
        let fetch = builder.task(
            TaskNode::<Processed>::new("fetch_data")
                .node_id("fetch")
                .args_json(serde_json::to_string(&region).unwrap()),
        );
        let process = builder.task(
            TaskNode::<Processed>::new("process_data")
                .waits_for(fetch)
                .args_from("data", fetch)
                .node_id("process"),
        );
        builder.output(process);
        builder.build()
    }
}

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

Ad hoc dynamic workflows can still use `app.start()`:

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

Starting an ad hoc workflow from inside a running task should use `TaskRuntime`:

```rust
use horsies::{task, TaskError, TaskRuntime, WorkflowSpecBuilder};

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

`TaskRuntime` is captured automatically by `#[task]` / `#[blocking_task]` when
it appears as the first parameter.

### Send and schedule tasks from inside tasks

Registered tasks now expose generated runtime helpers, so task-to-task dispatch
does not require globals or manual handle wiring:

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

### Provide typed runtime state to tasks

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

### Validate and run

```rust
app.check()?;
app.check_live().await?;

app.run_worker().await?;
app.run_scheduler().await?;
```

## Crate Structure

| Crate | Role |
|---|---|
| [`horsies`](./horsies) | Public API facade |
| [`horsies-macros`](./macros) | `#[task]` / `#[blocking_task]` proc macros |

## Monitoring

Horsies includes **Syce**, a terminal-based UI for monitoring your cluster in real-time.

![Syce Dashboard](https://suleymanozkeskin.github.io/horsies/images/syce/dashboard.png)

[Syce Setup & Usage](https://suleymanozkeskin.github.io/horsies/monitoring/syce-overview/)

## License

MIT
