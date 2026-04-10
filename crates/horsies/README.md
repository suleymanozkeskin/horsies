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

The examples in this section are mirrored by compile-checked examples in
[`examples/examples/readme_reference.rs`](./examples/examples/readme_reference.rs),
[`examples/examples/dynamic_runtime_start.rs`](./examples/examples/dynamic_runtime_start.rs),
[`examples/examples/runtime_state_dispatch.rs`](./examples/examples/runtime_state_dispatch.rs),
and [`examples/examples/checked_workflow_builder.rs`](./examples/examples/checked_workflow_builder.rs).

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
        let result = handle.get(Some(Duration::from_secs(30))).await;
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
    HorsiesError, WorkflowDefConfig, WorkflowDefinition, WorkflowFunction,
    WorkflowSpecBuilder,
};

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

let workflow: WorkflowFunction<Processed> =
    app.register_workflow_definition::<ETLPipeline>()?;

match workflow.start().await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

Parameterized reusable workflows should use `WorkflowDefinition::build_with(...)` plus `app.workflow_template::<...>()`:

```rust
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

For dependency injection, prefer multi-parameter receiving tasks plus the
generated `task_name::params::*` tokens. That keeps the function signature as
the contract and avoids authoring wrapper structs solely for workflow wiring.

Use the binding style that matches where the value comes from:

- `.set_input(value)?` when you already have the task's full input value
- `.set(task::params::x(), value)?` when you are filling one explicit parameter
- `.arg_from(task::params::y(), dep)` when the value should come from an upstream node

Mixed explicit and injected inputs look like this:

```rust
#[task("notify_user")]
async fn notify_user(
    data: TaskResult<String>,
    urgent: bool,
) -> Result<(), TaskError> {
    let _ = data;
    let _ = urgent;
    Ok(())
}

let process = builder.task(
    process_data::node()?
        .waits_for(fetch)
        .arg_from(ProcessDataInput::field_data(), fetch)
        .node_id("process"),
);

let notify = builder.task(
    notify_user::node()?
        .waits_for(process)
        .arg_from(notify_user::params::data(), process)
        .set(notify_user::params::urgent(), true)?
        .node_id("notify"),
);
```

`#[derive(WorkflowInput)]` is still available when you intentionally want a
named receiving input struct, but it is now the fallback path rather than the
default pattern for `arg_from(...)`.

For child workflows built from runtime params, prefer
`app.register_parameterized_workflow(...)` over hand-rolling a placeholder
registered spec:

```rust
let child = app.register_parameterized_workflow::<ChildParams, ChildOut, _>(
    "child_pipeline",
    "myapp.child_pipeline.v1",
    move |params| build_child_pipeline(params),
)?;

let child_ref = builder.sub_workflow(
    child
        .node()
        .set(ChildParams::field_limit(), 25)?
        .arg_from(ChildParams::field_input_result(), upstream),
);
```

### Start workflows from anywhere

Registered workflows also expose global dispatch. Register once at startup,
then start from anywhere:

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

Ad hoc dynamic workflows can still use `app.start()`:

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

Starting an ad hoc workflow from inside a running task should use `TaskRuntime`:

```rust
use horsies::{task, TaskError, TaskRuntime, WorkflowSpecBuilder};

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
it appears as the first parameter.

### Validate dynamic workflow builders at check time

Use `app.check_workflow_builder(...)` when a workflow spec is built from typed
runtime parameters and you want `app.check()` to validate representative cases:

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
                .arg_from(ProcessDataInput::field_data(), fetch_ref),
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

### Send and schedule tasks from anywhere

Registered tasks expose global helpers. Register once at startup, then call
`task_name::send(args)` or `task_name::schedule(delay, args)` from anywhere:

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

    // Explicit handle-based path (useful for testing or advanced scenarios):
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

### Coverage

The repo uses `cargo-llvm-cov` for coverage.

```bash
./scripts/coverage.sh summary
./scripts/coverage.sh html
./scripts/coverage.sh full
```

`summary` / `html` run the core suite and skip the worker e2e package so you
can get a stable local baseline quickly. `full` runs the whole workspace.
All modes exclude non-library workspace crates from the reported coverage so
the summary reflects the main published crates.

## Start Patterns

Use these start paths consistently:

- Global dispatch (primary):
  `horsies::start_workflow::<D>()` or `horsies::start_workflow_with::<D>(params)`
- Setup / HTTP / reusable start:
  `WorkflowFunction<T>` or `WorkflowTemplate<P, T>`
- Ad hoc external dynamic start:
  `app.start::<T>(spec)`
- Dynamic start from inside a running task:
  `rt.start::<T>(spec)`

## Migration Notes

From `alpha.5` to `alpha.6`:

- Task global helpers no longer take `&rt`:
  `task_name::send(args)`, `task_name::schedule(delay, args)`.
  The explicit path `task_name::handle(&rt)?.send(args)` still works for testing/advanced use.
- Workflow global dispatch added:
  `horsies::start_workflow::<D>()` for zero-param workflows (after `app.register_workflow_definition::<D>()`),
  `horsies::start_workflow_with::<D>(params)` for parameterized workflows (after `app.workflow_template::<D>()`).

From `alpha.3` / `alpha.4` to `alpha.5`:

- `WorkflowStarter` is not public.
  For external reusable starts, keep `WorkflowFunction<T>` or `WorkflowTemplate<P, T>` in app state.
  For dynamic starts inside tasks, use `TaskRuntime`.
- For check-time validation of parameterized or runtime-built workflow specs, use:
  `app.check_workflow_builder(...)` or `app.check_workflow_builder0(...)`.

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
