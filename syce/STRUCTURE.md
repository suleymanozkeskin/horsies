# syce Structure

TUI monitoring tool for horsies task library.

## Project Structure

```
syce/
├── src/
│   ├── main.rs              # Entry point
│   ├── cli.rs               # CLI argument parsing
│   ├── errors.rs            # Error types (thiserror)
│   ├── tui.rs               # Terminal setup/teardown
│   ├── app.rs               # Main application state
│   ├── action.rs            # UI action events
│   │
│   ├── db/
│   │   ├── mod.rs           # Database module exports
│   │   ├── connection.rs    # SQLx connection pool
│   │   └── queries.rs       # Database queries
│   │
│   ├── models/
│   │   ├── mod.rs           # Model exports
│   │   ├── worker.rs        # WorkerState struct
│   │   └── task.rs          # TaskSummary struct
│   │
│   └── components/
│       ├── mod.rs           # Component exports
│       └── home.rs          # Home component (to be updated)
│
├── Cargo.toml
└── README.md
```

## Dependencies

### Core
- **ratatui** - TUI framework
- **crossterm** - Terminal handling
- **tokio** - Async runtime

### Database
- **sqlx** - PostgreSQL client with compile-time checked queries
  - Features: `runtime-tokio`, `postgres`, `chrono`, `json`

### Data
- **serde**, **serde_json** - Serialization for JSONB fields
- **chrono** - DateTime handling

### Utilities
- **clap** - CLI args (with env variable support)
- **dotenvy** - .env file loading
- **thiserror** - Error handling
- **strum** - Enum utilities

## Database Module

### `db::Database`
Connection pool manager:
```rust
use crate::db::Database;

let db = Database::new(&database_url).await?;
let pool = db.pool();
```

### `db::queries`
Pre-built queries for worker monitoring:

```rust
use crate::db::queries;

// Get active workers (seen in last 2 minutes)
let workers = queries::get_active_workers(pool).await?;

// Get all workers (including stale)
let all_workers = queries::get_all_workers(pool).await?;

// Get task summary
let summary = queries::get_task_summary(pool).await?;

// Get worker history for charts
let history = queries::get_worker_history(pool, worker_id, 60).await?;
```

## Models

### `WorkerState`
Represents a worker state snapshot from `worker_states` table:

```rust
pub struct WorkerState {
    pub worker_id: String,
    pub snapshot_at: DateTime<Utc>,
    pub hostname: String,
    pub pid: i32,

    // Configuration
    pub processes: i32,
    pub tasks_running: i32,
    pub tasks_claimed: i32,

    // System metrics
    pub memory_percent: Option<f64>,
    pub cpu_percent: Option<f64>,

    // ... other fields
}

// Helper methods
worker.utilization();      // -> f64 (percentage)
worker.is_alive();         // -> bool (seen in last 2 min)
worker.uptime_string();    // -> String ("2h 30m 15s")
```

### `TaskSummary`
Cluster-wide task statistics:

```rust
pub struct TaskSummary {
    pub pending: Option<i64>,
    pub claimed: Option<i64>,
    pub running: Option<i64>,
    pub completed: Option<i64>,
    pub failed: Option<i64>,
}

summary.total();   // Total tasks
summary.active();  // Pending + Claimed + Running
```

## Components

Components follow the ratatui component pattern with `Component` trait.

### To Create a New Component

1. Create `src/components/your_component.rs`
2. Implement the `Component` trait:
   ```rust
   use ratatui::prelude::*;
   use crate::action::Action;
   use crate::errors::Result;

   pub struct YourComponent {
       // component state
   }

   impl Component for YourComponent {
       fn register_action_handler(&mut self, tx: UnboundedSender<Action>) -> Result<()> {
           // Register action handlers
           Ok(())
       }

       fn update(&mut self, action: Action) -> Result<Option<Action>> {
           // Handle actions
           Ok(None)
       }

       fn draw(&mut self, f: &mut Frame, rect: Rect) -> Result<()> {
           // Draw UI
           Ok(())
       }
   }
   ```

3. Add to `src/components/mod.rs`:
   ```rust
   pub mod your_component;
   pub use your_component::YourComponent;
   ```

## Usage

### Run with DATABASE_URL env var
```bash
export DATABASE_URL="postgresql://user:pass@localhost/horsies"
cargo run
```

### Run with CLI arg
```bash
cargo run -- --database-url "postgresql://user:pass@localhost/horsies"
```

### Run with .env file
```bash
echo 'DATABASE_URL="postgresql://user:pass@localhost/horsies"' > .env
cargo run
```

## Next Steps

1. **Update `src/components/home.rs`** to display worker data:
   - Fetch workers using `db::queries::get_active_workers`
   - Display in table using `ratatui::widgets::Table`

2. **Create `src/components/workers.rs`**:
   - Worker list view
   - Worker detail view
   - Live metrics charts

3. **Create `src/components/tasks.rs`**:
   - Task summary overview
   - Task queue breakdown

4. **Add periodic refresh**:
   - Use `Action::Tick` to trigger DB refresh every 5 seconds
   - Update component state with fresh data

5. **Add tabs navigation**:
   - Workers tab
   - Tasks tab
   - Charts tab

## Database Schema Reference

See `../horsies/sql/observartion/worker-monitoring.sql` for all available queries.

Key tables:

- `worker_states` - Worker state snapshots (timeseries, every 5s)
- `tasks` - All tasks (PENDING, CLAIMED, RUNNING, COMPLETED, FAILED)
- `heartbeats` - Task heartbeats for liveness detection
