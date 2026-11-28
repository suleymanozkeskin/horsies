---
title: Showcase Application
summary: A runnable Acme Clothing app in the repository that exercises the Rust client against a real database.
related: [getting-started, 05-workflow-patterns, ../monitoring/web-ui-mount, ../tasks/defining-tasks]
tags: [rust, showcase, example, demo, workflows, monitoring]
---

# Showcase Application

The repository holds a complete application under `clients/rust/examples/showcase/`: Acme Clothing, a fictional clothing retailer with orders, payments, stock, shipments and returns. The package is `acme-showcase`. It is not published.

The domain state is real. Products, orders and payments are rows in `acme_*` tables that the tasks read and write. Only the outside world is simulated: the payment provider, the courier, the label printer and the mail gateway.

The showcase registers 36 tasks, 12 workflows, 32 schedule entries (29 enabled) and 4 queues. Every task returns `TaskError<AcmeCode>`, and `AcmeCode` has 18 codes.

## Why and When

Read the quick-start pages to learn the API. Run the showcase to see what the API does:

- A retry adds attempts to the history of a task.
- A workflow pauses for a human decision.
- A timeout kills the child process of one task while other tasks continue.
- A panic in a task body arrives in the dashboard as data, not as a stack trace.

The showcase is also the reference for how the parts fit in one codebase: one binary that is the producer, the worker, each worker child, the scheduler and the web server; typed tasks with a code enum; and idempotent writes in tasks that can run again.

## How To

You need Rust 1.91, a PostgreSQL instance and Docker (for the compose file). Run every command from the root of the repository.

Start PostgreSQL. The compose service listens on port 5433:

```bash
docker compose -f clients/rust/examples/showcase/docker-compose.yml up -d
export ACME_DATABASE_URL=postgresql://postgres:postgres@localhost:5433/acme_demo
```

Create the database, the tables and the catalog:

```bash
cargo run -p acme-showcase --bin acme -- seed
```

Start the three processes, each in its own terminal:

```bash
cargo run -p acme-showcase --bin acme -- worker
cargo run -p acme-showcase --bin acme -- scheduler
cargo run -p acme-showcase --features web --bin acme -- web --authorization none
```

Place orders in a fourth terminal:

```bash
cargo run -p acme-showcase --bin acme -- steady --orders 10 --cover-errors
```

Open `http://127.0.0.1:8600`.

`worker` runs the profile `acme`: 6 isolated children for the tasks that use the Acme tables, and 1 shared async child with 32 seats for the other tasks. `--authorization none` is valid only on a loopback host.

### Validate Without a Database

`check` builds every registered workflow and validates every schedule. It needs no database and no worker:

```bash
cargo run -p acme-showcase --bin acme -- check
```

### Scenarios

Each scenario prints what it started.

| Scenario | What it shows |
|----------|---------------|
| `seed` | The tables and the catalog |
| `steady` | The order workflow, at a steady rate. Every sixth order starts a return, every twentieth a restock |
| `rush` | The queue limits under a burst of 50 orders |
| `problem-child` | Declined payments, short stock and returns |
| `bulk-import` | A fan-out of 40 import chunks |
| `flash-sale` | Two campaigns and 80 price updates that expire |
| `chaos` | Recovery after a worker crash |
| `maintenance` | The five back-office workflows |
| `simulate` | Samples of the deterministic simulation |

### Database Selection

The showcase uses its own database, `acme_demo`, so a demo never shares rows with a development database. The URL comes from the first of these sources that is set:

1. `ACME_DATABASE_URL` in the environment.
2. `ACME_DATABASE_URL` in the `.env` file of the repository.
3. `DATABASE_URL` in the environment, with the database name changed to `acme_demo`.
4. `DATABASE_URL` in `.env`, with the database name changed to `acme_demo`.
5. `postgresql://postgres:postgres@localhost:5432/acme_demo`.

The `seed` scenario prints the rule that it used.

## Things to Avoid

**Do not copy the recovery tuning of the showcase into a deployment.** The values are for a demo:

<!-- rust-check: skip elided -- three lines of the config of the showcase, shown for reference -->
```rust
config.recovery.worker_state_snapshot_interval_ms = 10_000; // lively charts, more writes
config.recovery.check_interval_ms = 10_000;                 // frequent reaper passes
config.retention.terminal_record_retention_hours = Some(24); // short retention
```

The defaults are the right start. See [Recovery Config](/horsies/configuration/recovery-config/).

**Do not model failure rates on the showcase.** One order in five meets a payment provider outage, and three in a hundred stall their invoice render. A demo that fails rarely shows nothing. A real system with these rates has a problem.

## Reference

| Path | Contents |
|------|----------|
| `clients/rust/examples/showcase/src/app.rs` | The queues, the worker profile, the recovery tuning, the 32 schedules |
| `clients/rust/examples/showcase/src/tuning.rs` | Every rate and duration of the showcase |
| `clients/rust/examples/showcase/src/tasks/` | The 36 tasks, in eight modules |
| `clients/rust/examples/showcase/src/workflows/` | The 12 workflow definitions |
| `clients/rust/examples/showcase/src/scenarios/` | The runnable scenarios |
| `clients/rust/examples/showcase/README.md` | The failure table and the deployment inputs |
| `clients/rust/examples/showcase/Procfile` | Worker, scheduler and web together |
| `clients/rust/examples/showcase/docker-compose.yml` | PostgreSQL 16 on port 5433 |
