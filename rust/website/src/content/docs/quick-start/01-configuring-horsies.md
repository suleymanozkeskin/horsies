---
title: Configuring Horsies
summary: Set up the app instance and broker connection.
related: [02-producing-tasks]
tags: [quickstart, configuration]
---

## Prerequisites

- PostgreSQL 12+
- Stable Rust

## Installation

```sh
cargo add horsies 
```

## Basic Configuration

```rust
use horsies::{
    Horsies, AppConfig, QueueMode, CustomQueueConfig,
};

let config = AppConfig::for_database_url(
    "postgresql://user:password@localhost:5432/mydb"
);

let mut app = Horsies::new(config)?;
```

## Custom Queues with Priorities

Different operations have different urgency levels can be defined with priority values.
1-100 where 1 is priority numero uno.

```rust
use horsies::{Horsies, AppConfig, QueueMode, CustomQueueConfig};

let config = AppConfig {
    queue_mode: QueueMode::Custom,
    custom_queues: Some(vec![
        CustomQueueConfig { name: "urgent".into(),   priority: 1,   max_concurrency: 10 },
        CustomQueueConfig { name: "standard".into(), priority: 50,  max_concurrency: 20 },
        CustomQueueConfig { name: "low".into(),      priority: 100, max_concurrency: 5 },
    ]),
    ..AppConfig::for_database_url(
        "postgresql://user:password@localhost:5432/db_name"
    )
};

let mut app = Horsies::new(config)?;
```

| Queue | Priority | Use Case |
|-------|----------|----------|
| `urgent` | 1 | The most important queue |
| `standard` | 50 | Things in between |
| `low` | 100 | The least important, can wait |

## Task Registration

Tasks are registered explicitly at startup. Each `#[task]` macro generates a companion module with a `register` function:

```rust
// Register individual tasks
add_numbers::register(&mut app)?;
process_data::register(&mut app)?;
```

There is no module auto-discovery. Each task must be registered before the worker starts.

## Running the Worker

```rust
app.run_worker().await?;
```

Or with custom config:

```rust
use horsies::WorkerConfig;

app.run_worker_with(WorkerConfig {
    concurrency: 50,
    ..Default::default()
}).await?;
```
