---
title: Getting Started
summary: Minimal working example with a task producer and worker.
related: [../../tasks/defining-tasks, ../../tasks/sending-tasks, ../../concepts/task-lifecycle]
tags: [quickstart, installation, tutorial]
---

## Prerequisites

- PostgreSQL 12+
- Stable Rust

## Installation

```sh
cargo add horsies
```

## 1. Create the App Instance

```rust
use horsies::{Horsies, AppConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::for_database_url(
        "postgresql://user:password@localhost:5432/mydb"
    );

    let mut app = Horsies::new(config)?;
    Ok(())
}
```

## 2. Define Tasks

Create task functions annotated with `#[task]`:

```rust
use horsies::{task, TaskError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct AddInput { a: i32, b: i32 }

#[task("add_numbers")]
async fn add_numbers(input: AddInput) -> Result<i32, TaskError> {
    Ok(input.a + input.b)
}

#[derive(Serialize, Deserialize)]
struct ProcessInput { data: Vec<String> }

#[task("process_data")]
async fn process_data(input: ProcessInput) -> Result<String, TaskError> {
    if input.data.is_empty() {
        return Err(TaskError::new(
            "EMPTY_DATA",
            "Data cannot be empty",
        ));
    }
    Ok(format!("Processed {} items", input.data.len()))
}
```

Register tasks on the app:

```rust
add_numbers::register(&mut app)?;
process_data::register(&mut app)?;
```

Requirements:

- Every task returns `Result<T, TaskError>`
- `Ok(value)` for success
- `Err(TaskError::new(...))` for domain errors
- Each task must be registered via `task_name::register(&mut app)`

## 3. Send Tasks

```rust
use std::time::Duration;
use horsies::TaskResult;

// Send and wait for result
match add_numbers::send(AddInput { a: 5, b: 3 }).await {
    Ok(handle) => {
        match handle.get(Some(Duration::from_secs(5))).await {
            TaskResult::Ok(value) => println!("Result: {}", value),
            TaskResult::Err(err) => {
                println!("Task failed: {:?} - {:?}", err.error_code, err.message);
            }
        }
    }
    Err(send_err) => {
        println!("Send failed: {} - {}", send_err.code, send_err.message);
    }
}
```

## 4. Run the Worker

```rust
// In your binary's main function:
app.run_worker().await?;
```

Or with custom configuration:

```rust
use horsies::WorkerConfig;

let worker_config = WorkerConfig {
    concurrency: 50,
    ..Default::default()
};

app.run_worker_with(worker_config).await?;
```

The worker connects to PostgreSQL, subscribes to LISTEN/NOTIFY, claims tasks, executes them, and stores results.

## 5. Delayed Execution

Schedule a task to run after a delay:

```rust
use std::time::Duration;

match add_numbers::schedule(
    Duration::from_secs(300),
    AddInput { a: 10, b: 20 },
).await {
    Ok(handle) => println!("Scheduled: {}", handle.task_id()),
    Err(err) => println!("Schedule failed: {}", err.code),
}
```

## Project Structure

Simplest start — an overview of components:

```text
myproject/
├── Cargo.toml
├── src/
│   ├── main.rs          # App setup, registration, entry point
│   ├── config.rs        # AppConfig construction
│   ├── tasks.rs         # Task definitions (#[task])
│   └── workflows.rs     # Workflow definitions (WorkflowDefinition)
```

Adjust to your project needs.
Isolate entry points, configs, and workflow definitions.

## Next Steps

- [Task Lifecycle](../../concepts/task-lifecycle) — task states and transitions
- [Queue Modes](../../concepts/queue-modes) — multiple queues
- [Scheduler Overview](../../scheduling/scheduler-overview) — scheduled tasks
- [Recovery Config](../../configuration/recovery-config) — crash recovery
