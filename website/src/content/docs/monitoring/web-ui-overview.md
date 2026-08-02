---
title: Web UI Overview
summary: Browser dashboard for tasks, workflows, and workers, shipped in the horsies[web] extra.
related: [web-ui-deployment, action-semantics, syce-overview, worker-health]
tags: [monitoring, web, dashboard, ui, sse]
---

The web UI is a browser dashboard for a single horsies deployment. It ships as
`horsies.web`, a FastAPI sub-app behind the `[web]` extra, which serves both the
JSON API under `/api` and the single-page app that consumes it. The dashboard
reads through `horsies.monitoring`, a query package in core that needs no extra.

The UI is read-only unless a deployment enables actions. See
[Deployment & Authentication](web-ui-deployment) for how to turn them on and
[Action Semantics](action-semantics) for what each action does.

## Installation

```bash
pip install "horsies[web]"
```

```bash
uv add "horsies[web]"
```

The extra adds `fastapi` and `uvicorn`. `horsies.monitoring` imports neither and
is available without the extra.

## Surfaces

Three surfaces, one per route.

### Tasks

Server-paginated, server-sorted, server-filtered task list, plus:

- Status cards for the seven `TaskStatus` values, always rendered in fixed order
  and zero-filled. Clicking a card toggles that status in the filter.
- Scoped facet filters for worker, task name, queue, and error code. Values OR
  within a dimension and AND across dimensions.
- An error-code rollup by taxonomy family (`OPERATIONAL`, `CONTRACT`,
  `RETRIEVAL`, `OUTCOME`, `DOMAIN`). It counts tasks carrying an error code,
  which includes completed tasks that returned a domain error, so it is not a
  failure count.
- A grouping pivot by worker, task name, or queue, with a `TOTAL` rollup row.
- A per-task detail panel with timings, worker, and full attempt history.

Filters, sort, pagination, and the open task are held in the URL, so a filtered
view is shareable and survives a reload.

### Workflows

Run picker plus two views of the selected run:

- **Graph** — the run's DAG. Click a node for its detail, double-click a
  subworkflow node to open the child run inside the same panel. Failure
  navigation steps through failed nodes; node search jumps by name.
- **Timeline** — each node's execution span on a shared axis from the run's
  start, which shows serialization and stragglers the DAG cannot.

The selected run and open node are URL search parameters (`?run=&node=`).

### Workers

- Liveness banner: database reachability with round-trip latency, and the count
  of workers that answered the last ping. The ping is an active round trip over
  LISTEN/NOTIFY, so it runs on demand only — never on a timer — and the banner
  states the age of the last one.
- Per-worker cards with running/claimed counts, queues, CPU and memory, and
  uptime. A worker whose snapshot is older than 120 seconds is marked stale.
  Stale workers collapse into a separate group.
- Load and resource timeseries for the focused worker.
- The schedule table: last run, next run, and run count per registered schedule.

## Freshness

Freshness is events-first. The web app runs its own `PostgresListener` on the
monitoring NOTIFY channels (`horsies_task_status`, `horsies_workflow_status`,
`horsies_worker_state`) and fans coalesced invalidation events out over SSE at
`GET /api/events`. Payloads are ids, never data: an event says which surface
changed, and the browser refetches through the normal endpoints.

Events drive the row-level surfaces: the task list, task detail, and workflow
run/node views. The task aggregates (stats, facets, breakdown) are
timer-driven in both modes — they are whole-table aggregations whose value
does not change meaningfully per event, and under load the server emits a
coalesced task event up to 4×/s:

| Query | Cadence | Reason |
|---|---|---|
| Task stats | 10 s, always | Aggregate; event-decoupled |
| Facets | 30 s, always | Aggregate; event-decoupled |
| Breakdown | 12 s, while the grouping view is active | Aggregate; event-decoupled |
| Schedules | 15 s, always | `horsies_schedule_state` has no NOTIFY trigger |
| Liveness ping | Manual only | An active round trip, not a read |

When the stream drops, the client reconnects with exponential backoff from 1 s
to a 30 s ceiling. If it stays down longer than 5 seconds, interval polling
takes over for the event-driven queries at these cadences until it reconnects:

| Query | Interval | Condition |
|---|---|---|
| Task list | 8 s | only while a visible row is PENDING, CLAIMED, or RUNNING |
| Task detail | 4 s | only while the task is PENDING, CLAIMED, or RUNNING |
| Workers | 5 s | always |
| Worker history | 10 s | while a worker is focused |
| Workflow runs | 5 s | only while a listed run is PENDING or RUNNING |
| Workflow run detail | 4 s | only while the run is PENDING, RUNNING, READY, or ENQUEUED |
| Node detail | 4 s | only while the node is PENDING, RUNNING, READY, or ENQUEUED |

On reconnect the client invalidates every event-covered query once — and the
timer-driven aggregates once as well, because an arbitrary number of changes
were missed while it was down.

Stats, facets, and the grouping breakdown aggregate over `horsies_tasks`;
unfiltered, each facet dimension is an index-only scan (schema v16), so cost
is bounded by index size rather than heap size. Retention bounds the table,
the cadences above are deliberately spaced, and each open dashboard
multiplies the load. The task list's `total` is a planner estimate on the
unfiltered view and exact under any active filter.

If the server's listener fails and cannot reconnect, the stream emits a
`degraded` event and closes; the client falls back to polling and keeps
retrying. The server never fabricates events.

## Actions

Actions are off by default. When a deployment enables them, five are available:

| Entity | Actions |
|---|---|
| Task | Cancel, Retry |
| Workflow run | Pause, Resume, Cancel |

Semantics are exact and, in two places, surprising: cancelling a RUNNING task
does not stop the process, and cancelling a workflow leaves executing nodes
draining. [Action Semantics](action-semantics) states each one as a
condition-to-result table. The UI states the same caveats in its confirmation
dialogs.

Three rules govern how actions behave in the interface:

- **Status is never optimistic.** A status chip changes only when refetched
  server data says so.
- **One action in flight per entity.** While an action is pending, every action
  button for that entity is disabled.
- **Actions live in detail panels only.** Table rows carry no action buttons.

When actions are disabled — by configuration, by the auth policy, or by a schema
mismatch — the affordances are absent rather than shown-and-disabled.

## Limits

- One horsies app, one schema, one dashboard. The UI has no notion of multiple
  deployments.
- No date-range filters.
- No node-level workflow actions, and no workflow restart: no such primitive
  exists.
- No way to kill a running task process. See [Action Semantics](action-semantics).
- The response models are exported and typed, and unstable before 1.0.
