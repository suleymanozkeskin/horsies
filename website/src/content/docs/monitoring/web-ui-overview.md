---
title: Web UI Overview
section: shared
summary: Browser dashboard for tasks, workflows, and workers, served by the monitoring app of each client.
related: [web-ui-deployment, web-ui-mount, action-semantics, worker-health]
tags: [monitoring, web, dashboard, ui, sse]
---

The web UI is a browser dashboard for a single horsies deployment. The
monitoring app serves both the JSON API under `/api` and the single-page app
that consumes it.

The UI is read-only unless a deployment enables actions. See
[Deployment & Authentication](/horsies/monitoring/web-ui-deployment/) for how to turn them on and
[Action Semantics](/horsies/monitoring/action-semantics/) for what each action does.

## Installation

Each client ships the monitoring app as an option of its package: the
`horsies[web]` extra of the Python package, and the `web` feature of the Rust
crate. The install is on the client pages:
[Install the Extra](/horsies/monitoring/web-ui-mount/#install-the-extra)
(Python) and [Add the Feature](/horsies/rust/monitoring/web-ui-mount/#add-the-feature)
(Rust).

## Surfaces

Three surfaces, one per route.

### Tasks

Server-paginated, server-sorted, server-filtered task list, plus:

- Status cards for the seven `TaskStatus` values, always rendered in fixed order
  and zero-filled. Clicking a card toggles that status in the filter. The
  counts are exact when loaded and can be up to 10 seconds old: all clients
  of one monitoring app share one cached answer per filter scope.
- Scoped facet filters for worker, task name, queue, and error code. Values OR
  within a dimension and AND across dimensions.
- An error-code rollup by taxonomy family (`OPERATIONAL`, `CONTRACT`,
  `RETRIEVAL`, `OUTCOME`, `DOMAIN`). It counts tasks carrying an error code,
  which includes completed tasks that returned a domain error, so it is not a
  failure count.
- A grouping pivot by worker, task name, or queue, with a `TOTAL` rollup row.
- A per-task detail panel with timings, worker, and full attempt history.
  Each attempt shows its phase times in record order (due, first claim,
  claim, placement on a child, start, finalizing marker, last sign of life,
  recorded), the time from each to the next, its claim count and its writer
  (the worker or stale recovery). An attempt recorded before schema migration
  1001 says that its phase times were not captured.

Filters, sort, pagination, and the open task are held in the URL, so a filtered
view is shareable and survives a reload.

### Workflows

Run picker plus two views of the selected run:

- **Graph** — the run's DAG. Click a node for its detail, double-click a
  subworkflow node to open the child run inside the same panel. Failure
  navigation steps through failed nodes; node search jumps by name. Each node
  card keeps its status color and adds its wall time and a thin phase strip:
  the time of each phase as a part of the node total, summed over retries and
  tasks, in lifecycle order.
- **Timeline** — one row per node, in task-index order, on one time axis from
  the creation of the run. It shows where each node spent its time, which
  shows queue waits, serialization and stragglers that the DAG cannot.

The node detail panel adds a phase timing section: the coverage label, the
wall time, the end of the node, the time of each phase, and the segments
grouped by task and attempt.

The selected run and open node are URL search parameters (`?run=&node=`).

#### Phase timing

The run read `GET /api/workflows/{id}` holds the phase timing of the run and
of each node (the `timing` field). The core composes it from the lifecycle
record; the UI draws it and does not compute phases of its own. A read-only
legend under the toolbar names each phase and its meaning.

The timeline draws, on each row:

- One segment per phase interval of the node, in time order:
  `pending`, `ready`, `enqueued`, `retry_wait`, `claimed`, `running`,
  `finalizing`, `advancing` and `unknown` (hatched). A retry repeats the
  segments of an attempt, and a new task of the node after a pause abandon
  repeats them again. A segment shorter than one pixel stays visible; its
  times do not change.
- The terminal status of the node as a marker at its completion time. The
  marker is not a duration: a task that runs on after a workflow cancel keeps
  its segments past the marker.
- The wall time of the node in the right column: from its first segment to
  the latest of its segment ends and its marker.

A pause of the run is a band over every row, from the pause to the resume,
to the end of the run (a cancel or a paused expiry), or to now.

Each node has a coverage label:

| Label | Row on the timeline | Graph phase strip |
|---|---|---|
| `exact` | Segments, marker and wall time | Phase shares |
| `partial` | The same; the `unknown` segments are hatched, and a banner names their reasons | Phase shares with hatched `unknown` time |
| `unavailable` | A hatched row with the text "Timing unavailable"; its tooltip gives the reason | A hatched strip; the card shows the execution time |
| `inconsistent` | A hatched error row with the text "Timing inconsistent"; its tooltip names the two times in the wrong order, the missing record or the unexpected event | A hatched error strip; the card shows the execution time |

A run that began before schema migration 1001, or that has more than 100,000
events, is unavailable as a whole: a banner gives the reason and the start of
the lifecycle record, and every row is hatched. A run with an inconsistent
node or pause record shows a banner that names each conflict.

Hover a segment for its phase, its task and attempt, its claim (earlier
claims, the recorded claim with its placement, or a claim with no attempt
row), its start, its end and its duration. The tooltip is shared by all rows.
With the keyboard, the up and down arrows (and Home, End) move between rows,
the left and right arrows step through the segments of the focused row in the
same tooltip, and Escape closes it. The accessible name of each row and each
graph card holds its status, coverage, time per phase, wall time and end.

An open segment or pause grows on a local clock of one second, from the read
time of the database (`as_of`) plus the browser time since the response
arrived. The clock runs only while a segment or a pause is open, and it sends
no request: the next read replaces the timing.

The timeline renders only the rows in view and 8 rows above and below them,
so a run of 10,000 nodes renders the same number of rows as a short run.

### Workers

- Liveness banner: database reachability with round-trip latency, and the count
  of workers that answered the last ping. The ping is an active round trip over
  LISTEN/NOTIFY, so it runs on demand only — never on a timer — and the banner
  states the age of the last one.
- Pending by execution class: one row per (queue, class) with `PENDING` rows,
  with the wait of its oldest due row and the live workers that serve it. A
  fleet warning above the table names each (queue, class) that no live worker
  serves (red) or whose serving workers have no child in service (amber), and
  what to start or check. See
  [Does a live worker serve every pending class?](/horsies/monitoring/worker-health/#does-a-live-worker-serve-every-pending-class)
- Per-worker cards with running/claimed counts, queues, CPU and memory, and
  uptime. A worker whose snapshot is older than 120 seconds is marked stale.
  Stale workers collapse into a separate group. A live worker's card shows
  each claim budget with its free capacity (`ISOLATED 3/8 free`), and a card
  shows the number of down slots.
- For the focused worker: each budget with its capacity in service, rows in
  flight, free capacity and the classes it serves; each down slot with its
  cause, failed starts and error text; and load and resource timeseries.
- The schedule table: last run, next run, and run count per registered schedule.

## Freshness

Freshness is events-first. The web app subscribes on the process listener of
its broker to the monitoring NOTIFY channels (`horsies_task_status`,
`horsies_workflow_status`, `horsies_worker_state`) and fans coalesced
invalidation events out over SSE at `GET /api/events`. Payloads are ids, never data: an event says which surface
changed, and the browser refetches through the normal endpoints.

Each stream queues at most 256 events (`SUBSCRIBER_QUEUE_CAPACITY`). When a
browser reads slower than events arrive and its queue is full, the queued
events are dropped and the stream sends one event with no ids per topic
(`tasks`, `workflows`, `workers`): the browser re-reads every surface. A slow
browser therefore cannot grow the memory of the web process.

A task sends a `horsies_task_status` event when it is created, when its status
changes, and when it ends. Its start (RUNNING) sends none: the start commit is
one transaction per task, and a NOTIFY would make Postgres commit those one at
a time. The task list and the task detail show RUNNING at their next re-read.

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
| Execution classes | 10 s, always | Reads the whole fleet; event-decoupled |
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
| Workflow run detail | 4 s | only while the run is PENDING, RUNNING, READY, or ENQUEUED, or its timing has an open segment or pause |
| Node detail | 4 s | only while the node is PENDING, RUNNING, READY, or ENQUEUED |

On reconnect the client invalidates every event-covered query once — and the
timer-driven aggregates once as well, because an arbitrary number of changes
were missed while it was down.

Stats, facets, and the grouping breakdown count every live row in
`horsies_tasks`, and the finished rows in `horsies_task_history` inside the
selected window. Facets read each table in one scan. Each of the three answers
through a cache in the web process, one entry per set of filters and window.
One load serves every open dashboard with the same filters, and an answer is
kept for 10 seconds after its load ends. The task list's `total` is a planner estimate on the
unfiltered view and exact under any active filter.

When the listener loses its connection, it reconnects, and each stream then
gets one event with no ids per topic: notifications sent during the gap may
be lost, so the browser re-reads every surface. If the app's broker has no
listener (a broker built with `assume_initialized=True`) or the listener
stopped, the stream emits a `degraded` event and closes; the client falls
back to polling and keeps retrying. The server never fabricates events.

## Actions

Actions are off by default. When a deployment enables them, four are available:

| Entity | Actions |
|---|---|
| Task | Cancel |
| Workflow run | Pause, Resume, Cancel |

Semantics are exact and, in two places, surprising: cancelling a RUNNING task
does not stop the process, and cancelling a workflow leaves executing nodes
draining. [Action Semantics](/horsies/monitoring/action-semantics/) states each one as a
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
- No way to kill a running task process. See [Action Semantics](/horsies/monitoring/action-semantics/).
- The response models are exported and typed, and unstable before 1.0.
