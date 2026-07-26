---
title: Showcase Application
summary: A runnable demonstration app in the repository that exercises every horsies capability against a real database.
related: [quick-start/getting-started, quick-start/05-workflow-patterns, monitoring/web-ui-overview, tasks/retry-policy]
tags: [showcase, example, demo, workflows, monitoring]
---

The repository contains a complete application under `showcase/`: Acme Clothing,
a fictional clothing retailer with orders, payments, stock, shipments, and
returns. Its domain state is real — products, orders, and payments are rows in
`acme_*` tables that the tasks read and write. Only the outside world is
simulated: the payment provider, the courier API, the label printer, the mail
gateway.

It exists to be run and watched. Every capability documented elsewhere in these
pages appears in it at least once, under load, with the dashboard open.

## Why and When

Read the quick-start pages to learn the API. Run the showcase to see the
behaviour the API produces — retries accumulating attempt history, a workflow
pausing for a human decision, a timeout killing a child process and its siblings
recovering, a task crashing and arriving in the dashboard as a data structure
rather than a stack trace.

It is also the reference for how the patterns fit together in one codebase:
keyword-only task parameters, `TaskResult` on both branches, `build_with`
returning a fresh spec per call, and idempotent writes in tasks that can be
replayed.

## How To

Requires Python 3.13, a PostgreSQL instance, and `uv`. Run everything from the
repository root.

Create the database, tables, and catalog:

```bash
uv run python -m showcase.acme.scenarios seed
```

Start the three processes, each in its own terminal:

```bash
uv run horsies worker showcase.acme.app:app --processes 12
uv run horsies scheduler showcase.acme.app:app
uv run horsies web showcase.acme.app:app --enable-actions
```

Place orders continuously in a fourth:

```bash
uv run python -m showcase.acme.scenarios steady
```

Open `http://127.0.0.1:8600`.

Working from a clone, build the dashboard assets once before starting the web
process. A `horsies` installed from PyPI already ships them:

```bash
cd webui && bun install && bun run build
```

### Validate without a database

`horsies check` imports the app, builds every registered workflow, and validates
each DAG without a broker or a worker:

```bash
uv run horsies check showcase.acme.app:app
```

### Scenarios

Each scenario prints the dashboard links for what it started.

| Scenario | What it demonstrates |
|---|---|
| `seed` | Table creation and catalog load |
| `steady` | The flagship order workflow, continuously |
| `rush` | Per-queue concurrency caps under a burst |
| `problem-child` | Paused runs, declined payments, an out-of-stock order |
| `bulk-import` | Cancelling a long fan-out mid-run |
| `flash-sale` | `SuccessPolicy`, `join='any'`, and `good_until` expiry |
| `chaos` | Worker crash recovery |
| `maintenance` | The five back-office workflow definitions |

### Database selection

The showcase uses its own database, `acme_demo`, so a demo never shares rows
with a development database. `showcase/acme/settings.py` resolves the URL from
`ACME_DATABASE_URL`, then from `DATABASE_URL` with the database name replaced,
then from a built-in default. The `seed` scenario creates the database when it
does not exist and prints which rule it used.

## Things to Avoid

**Do not copy the showcase's recovery tuning into a deployment.** It is tuned
for a demo, not for production:

```python
# showcase/acme/app.py — demo values
RecoveryConfig(
    worker_state_snapshot_interval_ms=10_000,  # lively charts, more writes
    check_interval_ms=10_000,                  # frequent reaper passes
    terminal_record_retention_hours=24,        # short retention
)
```

The defaults exist because they are the right starting point. See
[Recovery Configuration](/configuration/recovery-config/).

**Do not model failure rates on the showcase's.** Roughly one order in five hits
a payment-provider outage and one in thirty stalls its invoice render, because a
demo that fails rarely shows nothing. Real systems that fail at those rates have
a problem.

## Reference

| Path | Contents |
|---|---|
| `showcase/acme/app.py` | Queue configuration, recovery tuning, 31 schedules, global exception mapper |
| `showcase/acme/tuning.py` | Every rate and duration the showcase uses |
| `showcase/acme/tasks/` | 35 tasks across eight modules |
| `showcase/acme/workflows/` | 12 workflow definitions |
| `showcase/acme/scenarios/` | The eight runnable scenarios |
| `showcase/README.md` | Capability-to-code table and the full walkthrough |
| `showcase/Procfile` | Worker, scheduler, and web together |
| `showcase/docker-compose.yml` | PostgreSQL 16 on port 5433 |
