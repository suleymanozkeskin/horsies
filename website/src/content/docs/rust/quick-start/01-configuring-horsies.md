---
title: Configuring Horsies
summary: Move the app into its own file, define the four queues of the Acme shop, register tasks with discover, and connect through PgBouncer.
related: [getting-started, 02-producing-tasks, ../configuration/app-config, ../workers/worker-setup]
tags: [rust, quickstart, configuration, queues, AppConfig]
---

# Configuring Horsies

This page continues the package of [Getting Started](/horsies/rust/quick-start/getting-started/). It moves the app into its own file and gives the app four queues. The next pages add the tasks of a clothing shop, Acme Clothing.

## Prerequisites

- The package of Getting Started.
- PostgreSQL 14+.
- Rust 1.91+.

## The File Layout

From this page on, the package has one file for each part of the app:

```text
src/
  lib.rs            the modules of the package
  app.rs            build_app: the config and the registrations
  domain.rs         the code enum and the result types (page 2)
  tasks/
    mod.rs
    notify.rs       send_welcome_email, then the notification tasks
    orders.rs       the order tasks (page 2)
  workflows/        one file for each workflow (page 3)
  main.rs           the worker
  bin/send.rs       a sender
```

The task `send_welcome_email` moves from `src/lib.rs` to `src/tasks/notify.rs`. Each later page adds files to this layout and renames none.

## Move the First Task

In `src/tasks/notify.rs`:

```rust
use horsies::{task, CustomErrorCode, Horsies, HorsiesError, TaskError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, CustomErrorCode)]
pub enum WelcomeCode {
    InvalidEmail, // INVALID_EMAIL
}

#[task("send_welcome_email", queue = "notifications")]
pub async fn send_welcome_email(email: String) -> Result<String, TaskError<WelcomeCode>> {
    if !email.contains('@') {
        return Err(TaskError::custom(WelcomeCode::InvalidEmail, "the address has no @"));
    }
    Ok(format!("sent to {email}"))
}

pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _send_welcome_email = send_welcome_email::register(app)?;
    Ok(())
}
```

- The task has a new attribute: `queue = "notifications"`. The next section explains why.
- `register` is the registrar of this module. It registers each task of the module on the app.

In `src/tasks/mod.rs`:

<!-- rust-check: skip partial -- the module files of the example package are not part of the check -->
```rust
pub mod notify;
```

In `src/lib.rs`, replace the code of Getting Started with:

<!-- rust-check: skip partial -- the module files of the example package are not part of the check -->
```rust
pub mod app;
pub mod tasks;
```

## The App with Custom Queues

In `src/app.rs`:

<!-- rust-check: skip partial -- the tasks module of the example package is not part of the check -->
```rust
use std::collections::BTreeMap;

use horsies::{
    AppConfig, CustomQueueConfig, Horsies, HorsiesError, IsolatedChildren, QueueMode,
    WorkerProfile,
};

use crate::tasks;

const DATABASE_URL: &str = "postgresql://user:password@localhost:5432/mydb";

fn queue(name: &str, priority: u32, max_concurrency: u32) -> CustomQueueConfig {
    CustomQueueConfig {
        name: name.to_owned(),
        priority,
        max_concurrency: Some(max_concurrency),
    }
}

pub fn build_app() -> Result<Horsies, HorsiesError> {
    let profile = WorkerProfile {
        isolated: IsolatedChildren::new(2).into(),
        ..WorkerProfile::default()
    };
    let config = AppConfig {
        queue_mode: QueueMode::Custom,
        custom_queues: Some(vec![
            queue("payments", 1, 4),
            queue("fulfillment", 10, 8),
            queue("notifications", 50, 6),
            queue("analytics", 90, 4),
        ]),
        worker_profiles: BTreeMap::from([("default".to_owned(), profile)]),
        ..AppConfig::from_database_url(DATABASE_URL)
    };
    let mut app = Horsies::new(config)?;
    app.discover([tasks::notify::register])?;
    Ok(app)
}
```

`priority` is a number from 1 to 100. The worker claims a task of priority 1 before a task of priority 100. `max_concurrency` is the most tasks of the queue that run at one time.

| Queue | Priority | Use |
|-------|----------|-----|
| `payments` | 1 | Authorize, capture and refund |
| `fulfillment` | 10 | Validate, reserve, pick and pack |
| `notifications` | 50 | Emails and text messages |
| `analytics` | 90 | Reports and audits, which can wait |

In `Custom` queue mode, every task names its queue. `register` refuses a task with no queue (`HRS-200`) and a task with an unknown queue (`HRS-103`). In `Default` queue mode, a task that names a queue is refused. See [App Config](/horsies/rust/configuration/app-config/#queues).

Update the imports of the two binaries:

- `src/main.rs`: `getting_started::app::build_app()` in place of `getting_started::build_app()`.
- `src/bin/send.rs`: `use getting_started::app::build_app;` and `use getting_started::tasks::notify::{send_welcome_email, WelcomeCode};`.

## Task Discovery

`app.discover(registrars)` calls each registrar function with the app, in order. It stops at the first refusal.

<!-- rust-check: skip partial -- a line of build_app above, with the module of the next page -->
```rust
app.discover([tasks::notify::register, tasks::orders::register])?;
```

The second line of the block is the app of the next page, which adds the module `orders`.

- A Rust app has no module paths to import. The tasks are compiled into the binary, so `discover` only calls the registrars that you list.
- A task that no registrar registers does not exist for the app. A send of it is refused with `ValidationFailed`, and a worker that claims it has no body to run.
- Each process builds the app with `build_app`: the producer, the worker, and each child of the worker.

## PgBouncer and Transaction Pooling

A transaction-pooled PgBouncer URL serves the ordinary statements. Schema setup and `LISTEN`/`NOTIFY` need a second URL, which is direct or session-pooled.

```rust
use horsies::{AppConfig, Horsies, HorsiesError, PostgresConfig};

pub fn build_pooled_app(pooled_url: &str, direct_url: &str) -> Result<Horsies, HorsiesError> {
    let config = AppConfig {
        database: PostgresConfig::from_pgbouncer_urls(pooled_url, direct_url),
        ..AppConfig::from_database_url(pooled_url)
    };
    Ok(Horsies::new(config)?)
}
```

- `from_pgbouncer_urls` sets `pgbouncer_transaction_mode` and the session URL.
- A worker does not support a PgBouncer-only URL, because `LISTEN` needs a persistent session.
- A managed provider can show a pooled and a direct connection string. Read the current URLs and ports in the dashboard of the provider.

See [App Config](/horsies/rust/configuration/app-config/) for `PostgresConfig` and the pool size.

## Run the Worker

```bash
cargo run --bin getting-started -- worker --profile default --log-level INFO
```

The profile `default` runs two isolated children. See [Worker Setup](/horsies/rust/workers/worker-setup/).
