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
struct AddArgs {
    a: i32,
    b: i32,
}

#[task("add")]
async fn add(args: AddArgs) -> Result<i32, TaskError> {
    Ok(args.a + args.b)
}
```

### Register and send

```rust
let mut app = Horsies::new(config)?;
let add_task = add::register(&mut app)?;

let handle = add_task.send(AddArgs { a: 2, b: 3 }).await?;
let result = handle.get(Some(Duration::from_secs(30))).await?;
```

### Build and start a workflow

Reusable workflows should be defined with `WorkflowDefinition` and registered once:

```rust
use horsies::{
    HorsiesError, TaskNode, WorkflowDefConfig, WorkflowDefinition, WorkflowFunction,
    WorkflowSpecBuilder,
};

struct Pipeline;

impl WorkflowDefinition for Pipeline {
    type Output = Processed;
    type Params = ();

    fn name() -> &'static str { "pipeline" }
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

let workflow: WorkflowFunction<Processed> = app.register_workflow_definition::<Pipeline>()?;
let handle = workflow.start().await?;
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
        let step = builder.task(TaskNode::<Processed>::new("run_region").node_id("run_region"));
        Ok(WorkflowDefConfig::new().output(step))
    }

    fn build_with(region: Self::Params) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(format!("regional_{region}"));
        builder.definition_key(format!("myapp.regional_pipeline.{region}.v1"));
        let step = builder.task(
            TaskNode::<Processed>::new("run_region")
                .node_id("run_region")
                .args_json(serde_json::to_string(&region).unwrap()),
        );
        builder.output(step);
        builder.build()
    }
}

let regional = app.workflow_template::<RegionalPipeline>();
let handle = regional.start("eu-west".to_owned()).await?;
```

Ad hoc dynamic workflows can still use `app.start()`:

```rust
let spec = WorkflowSpecBuilder::new("enrichment")
    .task(node_a)
    .task(node_b)
    .definition_key("myapp.enrichment.v1")
    .build()?;

let handle = app.start::<Output>(spec).await?;
```

Starting an ad hoc workflow from inside a running task should use `TaskRuntime`:

```rust
use horsies::{task, TaskError, TaskRuntime, WorkflowSpecBuilder};

#[task("scrape_detail")]
async fn scrape_detail(
    rt: TaskRuntime,
    input: ScrapeInput,
) -> Result<(), TaskError> {
    if let Some(spec) = build_enrichment_spec(&input)? {
        let handle = rt.start::<Output>(spec).await?;
        tracing::info!(workflow_id = %handle.workflow_id(), "started enrichment workflow");
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
