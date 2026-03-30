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

Static workflows (reusable, pre-registered):

```rust
let mut wf = app.workflow::<Processed>("pipeline");
wf.definition_key("myapp.pipeline.v1");
let fetch = wf.task(fetch_data.node());
let process = wf.task(process_data.node().waits_for(fetch).args_from("data", fetch));
wf.output(process);

let pipeline = wf.build()?;
let handle = pipeline.start().await?;
```

Dynamic workflows (runtime-built):

```rust
let spec = WorkflowSpecBuilder::new("enrichment")
    .task(node_a)
    .task(node_b)
    .definition_key("myapp.enrichment.v1")
    .build()?;

let handle = app.start::<Output>(spec).await?;
```

Starting workflows inside a worker (after app is consumed):

```rust
let starter = app.workflow_starter();
app.run_worker_with(config).await?;

// Inside a task:
let handle = starter.start::<Output>(spec).await?;
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
