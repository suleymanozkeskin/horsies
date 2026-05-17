# syce Structure

`syce` is the terminal monitoring UI for Horsies. It connects directly to the
same PostgreSQL database used by Horsies workers and renders workers, tasks,
workflows, filters, and maintenance actions.

## Project Layout

```text
syce/
├── src/
│   ├── main.rs              # Entry point
│   ├── cli.rs               # Database URLs, PgBouncer mode, tick/frame rates
│   ├── app.rs               # Main app state, data loading, routing, rendering
│   ├── action.rs            # UI actions, data update events, listener state
│   ├── listener.rs          # PostgreSQL LISTEN/NOTIFY bridge
│   ├── state.rs             # In-memory UI state and filters
│   ├── theme.rs             # Theme palette
│   ├── tui.rs               # Terminal setup and event loop
│   ├── errors.rs            # thiserror-based error types
│   ├── components.rs        # Component trait and component exports
│   ├── components/          # Dashboard, workers, tasks, workflows, detail panels
│   ├── db/
│   │   ├── mod.rs
│   │   ├── connection.rs    # Connection pooling is handled in app.rs
│   │   └── queries.rs       # Query wrappers and embedded SQL
│   └── models/
│       ├── metrics.rs       # Worker and task aggregate rows
│       ├── task.rs          # Task list/detail/attempt/filter rows
│       └── workflow.rs      # Workflow list/detail rows
├── sql/
│   ├── tasks/               # Task aggregate/list/detail/filter SQL
│   ├── worker/              # Worker health/capacity/queue SQL
│   └── workflows/           # Workflow list/detail/summary SQL
├── Cargo.toml
└── README.md
```

## Runtime Flow

1. `main.rs` parses `Cli`, initializes the terminal, and creates `App`.
2. `App` opens a `PgPool` when `DATABASE_URL` is provided.
3. In PgBouncer transaction mode, SQLx statement cache is disabled for the
   query pool.
4. `NotifyListenerHandle` opens its own session URL, subscribes to Horsies
   channels, and sends debounced
   refresh events into the TUI event loop.
5. `App` handles actions from keyboard input, mouse input, ticks, and NOTIFY
   batches.
6. Data loaders in `app.rs` call `db::queries`, update `state.rs`, and render
   the active tab through components.

## NOTIFY Channels

`listener.rs` listens on:

- `horsies_task_status`
- `horsies_workflow_status`
- `horsies_worker_state`
- `task_new`
- `task_done`
- `workflow_done`

The first three are the broad monitoring channels. The latter three preserve
compatibility with worker-oriented Horsies triggers.

## Database Tables

The UI reads the current Horsies schema:

- `horsies_worker_states`
- `horsies_tasks`
- `horsies_task_attempts`
- `horsies_workflows`
- `horsies_workflow_tasks`

SQL lives under `syce/sql/` and is embedded or mirrored by wrappers in
`src/db/queries.rs`.

## Main Views

- Dashboard: cluster overview and aggregate health.
- Workers: active/stale workers, capacity, queues, utilization, and history.
- Tasks: aggregate status, filtered task list, task detail, attempts, failures.
- Workflows: workflow list, task DAG/detail view, progress, terminal results.
- Maintenance: operational actions and cleanup-oriented views.
- Search/help/status bar/error modal: cross-cutting UI components.

## Dependencies

- `ratatui` and `crossterm` for terminal UI and input.
- `tokio` for async runtime and background tasks.
- `sqlx` for PostgreSQL queries.
- `serde` and `serde_json` for JSONB payloads.
- `chrono` for timestamps.
- `clap` and `dotenvy` for configuration.
- `thiserror` for errors.
- `strum`, `arboard`, and `catppuccin` for enum helpers, clipboard, and theme.

## Running

```bash
export DATABASE_URL="postgresql://user:pass@localhost/horsies"
cargo run
```

or:

```bash
cargo run -- --database-url "postgresql://user:pass@localhost/horsies"
```

For transaction-pooled PgBouncer:

```bash
cargo run -- \
  --database-url "$DATABASE_URL_POOLED" \
  --session-database-url "$DATABASE_URL_DIRECT" \
  --pgbouncer-transaction-mode
```

Without a database URL, syce can still run in demo/no-pool paths where supported
by the app state.
