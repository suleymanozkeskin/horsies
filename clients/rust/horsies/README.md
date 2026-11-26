<p align="center">
  <img src="https://suleymanozkeskin.github.io/horsies/galloping-horsie.svg" alt="Horsies Logo" width="200" style="border-radius: 20px" />
</p>

# horsies

**The Rust client of horsies, a PostgreSQL-backed task queue and workflow engine.**

[**Documentation**](https://suleymanozkeskin.github.io/horsies/rust/) | [**crates.io**](https://crates.io/crates/horsies) | [**GitHub**](https://github.com/suleymanozkeskin/horsies)

---

The crate runs on `horsies-core`, the engine that the Python package `horsies` also runs on: one schema, one set of SQL functions, one worker parent and one scheduler serve both clients. The crate holds the typed API: tasks, sends and result reads, workflows, schedules, worker profiles, monitoring reads and the CLI runner.

- Task bodies never run in the worker process. The worker starts the app's own binary again as a child process, for each of the three execution classes (`isolated`, `shared_async`, `shared_thread`).
- One binary is the producer, the worker, each child and the scheduler: `main` builds the app and hands the command line to `horsies::cli::run`.
- A database belongs to one client: the core refuses a database that the Python client installed, and a database of `0.1.0-alpha`.

From 0.8.0, `horsies`, `horsies-macros`, `horsies-core`, `horsies-core-derive` and the Python package carry one version number.

## Quick Start

```bash
cargo add horsies
cargo add tokio --features rt-multi-thread,macros
```

```rust
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::process::ExitCode;
use std::time::Duration;

use horsies::{
    task, AppConfig, Horsies, IsolatedChildren, TaskError, TaskOutcome, WaitLimit,
    WorkerProfile,
};

const DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/horsies";

#[task("greet")]
async fn greet(name: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("hello, {name}"))
}

/// Every process builds the same app: the producer, the worker and each child.
fn build_app() -> Result<Horsies, Box<dyn std::error::Error>> {
    let profile = WorkerProfile {
        isolated: IsolatedChildren::new(2).into(),
        ..WorkerProfile::default()
    };
    let config = AppConfig {
        worker_profiles: BTreeMap::from([("default".to_owned(), profile)]),
        ..AppConfig::from_database_url(DATABASE_URL)
    };
    let mut app = Horsies::new(config)?;
    let _greet = greet::register(&mut app)?;
    Ok(app)
}

async fn send_greeting(app: &Horsies) -> Result<(), Box<dyn std::error::Error>> {
    let handle = greet::send(app, "world".to_owned()).await?;
    match handle.get(WaitLimit::Within(Duration::from_secs(30))).await? {
        TaskOutcome::Succeeded(greeting) => println!("{greeting}"),
        TaskOutcome::Failed(error) => println!("greet failed: {}", error.code().text()),
    }
    Ok(())
}

fn run_send(app: Horsies) -> ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("the runtime did not start: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(send_greeting(&app)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    let app = match build_app() {
        Ok(app) => app,
        Err(refused) => {
            eprintln!("{refused}");
            return ExitCode::FAILURE;
        }
    };
    match std::env::args_os().nth(1) {
        Some(command) if command == "send" => run_send(app),
        Some(_) | None => horsies::cli::run(app),
    }
}
```

```bash
cargo run -- worker --profile default   # the worker
cargo run -- send                       # in a second terminal: prints "hello, world"
cargo run -- check                      # checks the app without a database
```

`Horsies::new` does no I/O: the app connects at its first operation and installs the schema there. The worker starts this binary again for each child, and `horsies::cli::run` serves the child; build the app there and start nothing else before it.

## Cargo Features

| Feature | Default | Content |
| --- | --- | --- |
| `cli` | on | `horsies::cli::run` (worker, scheduler and check from the app's binary) and the `horsies` binary (`web`, `get-docs`, `skills install`). |
| `web` | off | `horsies::web`: the axum mount of the core's monitoring handler and web UI. |
| `tokio-postgres` | off | Rerun of a task in the caller's `tokio_postgres::Transaction`. |
| `sqlx` | off | Rerun of a task in the caller's `sqlx::Transaction<'_, Postgres>`. |

Minimum Rust version: 1.91.

## Documentation

- [Getting Started](https://suleymanozkeskin.github.io/horsies/rust/quick-start/getting-started/)
- [Defining Tasks](https://suleymanozkeskin.github.io/horsies/rust/tasks/defining-tasks/)
- [Sending Tasks](https://suleymanozkeskin.github.io/horsies/rust/tasks/sending-tasks/)
- [Workflow API](https://suleymanozkeskin.github.io/horsies/rust/concepts/workflows/workflow-api/)
- [Worker Setup](https://suleymanozkeskin.github.io/horsies/rust/workers/worker-setup/)
- [Schedule Config](https://suleymanozkeskin.github.io/horsies/rust/scheduling/schedule-config/)
- [Web UI Mount](https://suleymanozkeskin.github.io/horsies/rust/monitoring/web-ui-mount/)
- [Switching to 0.8.0](https://suleymanozkeskin.github.io/horsies/rust/migrations/switching-to-0-8-0/) from `0.1.0-alpha`

The changes of each release are in the [changelog](https://github.com/suleymanozkeskin/horsies/blob/main/CHANGELOG.md).

## License

MIT
