---
title: Syce Overview
summary: Terminal-based monitoring dashboard for Horsies clusters.
related: [../workers/worker-architecture, ../workers/heartbeats-recovery, ../internals/database-schema]
tags: [monitoring, syce, tui, dashboard]
---

Syce is a real-time terminal UI (TUI) for monitoring Horsies workers, tasks, and workflows. Built in Rust with [ratatui](https://ratatui.rs), it connects directly to the Horsies PostgreSQL database and renders live cluster state in the terminal.

## Installation

### From crates.io

```bash
cargo install syce
```

### From source

```bash
cd syce
cargo build --release
```

The binary is at `target/release/syce`.

## Configuration

Syce needs the same PostgreSQL database URL used by Horsies workers.

### Environment variable

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/mydb"
syce
```

### CLI argument

```bash
syce --database-url "postgresql://user:pass@localhost:5432/mydb"
```

### `.env` file

```bash
# .env
DATABASE_URL="postgresql://user:pass@localhost:5432/mydb"
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--database-url`, `-d` | `$DATABASE_URL` | PostgreSQL connection string |
| `--tick-rate`, `-t` | `4.0` | Data refresh rate (ticks per second) |
| `--frame-rate`, `-f` | `60.0` | UI render rate (frames per second) |

## Tabs

Syce organizes monitoring into five tabs, accessible via number keys `1`-`5`.

### 1 - Dashboard

Cluster-level overview:

- **Cluster Capacity** — Active workers, total capacity, utilization percentage, running tasks
- **Task Status Distribution** — Breakdown by status (Pending, Claimed, Running, Completed, Failed)
- **Workflow Summary** — Workflow counts by status
- **Utilization Trend** — Braille line chart of cluster utilization over the past hour
- **Active Alerts** — Overloaded workers and stale claim warnings

### 2 - Workers

Per-worker monitoring:

- Worker list with hostname, PID, process count, running/claimed tasks, CPU/memory usage, uptime
- Select a worker to see detailed metrics and load charts
- Adjustable time window for charts: 5m, 30m, 1h, 6h, 24h

### 3 - Tasks

Task distribution and inspection:

- Aggregated task breakdown by worker
- Status filters (Pending, Claimed, Running, Completed, Failed)
- Expand a worker row to see individual task IDs
- Task detail modal with full JSON payload
- Copy task data to clipboard

### 4 - Workflows

Workflow tracking:

- Workflow list with ID, name, status, task count, progress
- Status filters (Pending, Running, Completed, Failed, Paused, Cancelled)
- Workflow detail modal showing constituent tasks and their states
- Copy workflow data to clipboard

### 5 - Maintenance

Operational health:

- Snapshot age distribution (histogram of worker state freshness)
- Dead worker detection

## Keyboard Shortcuts

### Global

| Key | Action |
|-----|--------|
| `1`-`5` | Switch tab |
| `/` | Search (context-aware) |
| `?` | Toggle help overlay |
| `r` | Refresh current tab |
| `t` | Cycle theme |
| `q`, `Esc` | Quit |
| `Ctrl+z` | Suspend to background |

### Workers Tab

| Key | Action |
|-----|--------|
| `Up`/`Down` | Select worker |
| `[`, `]` | Cycle time window (5m / 30m / 1h / 6h / 24h) |

### Tasks Tab

| Key | Action |
|-----|--------|
| `Up`/`Down` | Navigate workers / task IDs |
| `Enter` | Expand row / open task detail |
| `Esc` | Collapse expanded row |
| `y` | Copy task JSON to clipboard (in detail view) |

#### Status Filters

| Key | Action |
|-----|--------|
| `p` | Toggle Pending |
| `c` | Toggle Claimed |
| `u` | Toggle Running |
| `o` | Toggle Completed |
| `f` | Toggle Failed |
| `a` | Select all statuses |
| `n` | Clear all statuses |

### Workflows Tab

| Key | Action |
|-----|--------|
| `Up`/`Down` | Navigate workflows |
| `Enter` | Open workflow detail |
| `Esc` | Close detail |
| `y` | Copy workflow JSON to clipboard |

## Theming

Syce ships with [Catppuccin](https://catppuccin.com) color themes. Press `t` to cycle through available flavors (Latte, Frappe, Macchiato, Mocha) with automatic light/dark detection.

## Data Sources

Syce reads from the same PostgreSQL tables Horsies workers write to:

| Table | Purpose |
|-------|---------|
| `horsies_worker_states` | Worker state snapshots (captured every 5s) |
| `horsies_tasks` | Task records with status, arguments, results |
| `horsies_heartbeats` | Task liveness heartbeats |
| `horsies_workflows` | Workflow definitions and status |
