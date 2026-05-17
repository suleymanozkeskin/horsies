<p align="center">
  <img src="https://suleymanozkeskin.github.io/horsies-rust/galloping-horsie.jpg" alt="Horsies Logo" width="200" style="border-radius: 20px" />
</p>

# Horsies Rust

**PostgreSQL-backed background task queue and workflow engine for Rust.**

Rust port of [horsies](https://github.com/suleymanozkeskin/horsies) (Python).

[**Full Documentation**](https://suleymanozkeskin.github.io/horsies-rust/) | [**crates.io**](https://crates.io/crates/horsies) | [**GitHub**](https://github.com/suleymanozkeskin/horsies-rust)

---

## Features

- Typed task inputs and outputs
- Structured `TaskError` values
- Workflow DAGs with typed node wiring
- PostgreSQL broker with LISTEN/NOTIFY
- PgBouncer transaction-pool support with a direct/session URL for LISTEN/NOTIFY
- Automatic schema initialization on normal startup paths
- Worker heartbeats and stale-task recovery
- Recurring scheduler for interval, hourly, daily, weekly, and monthly jobs
- `app.check()` / `app.check_live()` validation before runtime

## Quick Start

```rust
use std::time::Duration;

use horsies::{task, AppConfig, Horsies, TaskError, TaskResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AddNumbersInput {
    a: i32,
    b: i32,
}

#[task("add_numbers")]
async fn add_numbers(input: AddNumbersInput) -> Result<i32, TaskError> {
    Ok(input.a + input.b)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::for_database_url("postgresql://localhost/mydb");
    let mut app = Horsies::new(config)?;

    add_numbers::register(&mut app)?;
    app.check()?;

    let handle = add_numbers::send(AddNumbersInput { a: 2, b: 3 }).await?;

    match handle.get(Some(Duration::from_secs(30))).await {
        TaskResult::Ok(value) => println!("result = {}", value),
        TaskResult::Err(err) => eprintln!("task failed: {:?}", err.error_code),
    }

    Ok(())
}
```

## Workflow Example

```rust
use horsies::{
    task, HorsiesError, TaskError, TaskResult, WorkflowDefConfig, WorkflowDefinition,
    WorkflowSpecBuilder,
};

#[task("fetch_data")]
async fn fetch_data() -> Result<String, TaskError> {
    Ok("raw".to_owned())
}

#[task("process_data")]
async fn process_data(data: TaskResult<String>) -> Result<String, TaskError> {
    let data = match data {
        TaskResult::Ok(v) => v,
        TaskResult::Err(err) => {
            return Err(TaskError::new(
                "UPSTREAM_FAILED",
                format!("fetch failed: {:?}", err.error_code),
            ))
        }
    };

    Ok(format!("processed: {}", data))
}

struct ExampleWorkflow;

impl WorkflowDefinition for ExampleWorkflow {
    type Output = String;
    type Params = ();

    fn name() -> &'static str { "example_workflow" }
    fn definition_key() -> &'static str { "example.workflow.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetch = builder.task(fetch_data::node()?.node_id("fetch"));
        let process = builder.task(
            process_data::node()?
                .node_id("process")
                .waits_for(fetch)
                .arg_from(process_data::params::data(), fetch),
        );

        Ok(WorkflowDefConfig::new().output(process))
    }
}
```

## Documentation

- [Getting Started](https://suleymanozkeskin.github.io/horsies-rust/quick-start/getting-started/)
- [Defining Tasks](https://suleymanozkeskin.github.io/horsies-rust/tasks/defining-tasks/)
- [Sending Tasks](https://suleymanozkeskin.github.io/horsies-rust/tasks/sending-tasks/)
- [Defining Workflows](https://suleymanozkeskin.github.io/horsies-rust/quick-start/03-defining-workflows/)
- [Workflow API](https://suleymanozkeskin.github.io/horsies-rust/concepts/workflows/workflow-api/)
- [Scheduler Overview](https://suleymanozkeskin.github.io/horsies-rust/scheduling/scheduler-overview/)

## Monitoring

Horsies includes **Syce**, a terminal UI for monitoring your cluster in real time.

![Syce Dashboard](https://suleymanozkeskin.github.io/horsies-rust/images/syce/dashboard.png)

[**Syce Setup & Usage**](https://suleymanozkeskin.github.io/horsies-rust/monitoring/syce-overview/)

## License

MIT
