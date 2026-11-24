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
- A per-task detail panel with the status, the error code, the queue and the
  attempt timing of the task. See [Attempt timing](#attempt-timing).

Filters, sort, pagination, and the open task are held in the URL, so a filtered
view is shareable and survives a reload.

### Workflows

Run picker plus two views of the selected run:

- **Graph** — the run's DAG. Click a node for its detail, double-click a
  subworkflow node to open the child run inside the same panel. Failure
  navigation steps through failed nodes; node search jumps by name. Each node
  card keeps its status color and adds its wall time and a thin phase strip:
  the time of each phase as a part of the node total, summed over retries and
  tasks, in lifecycle order. The card over the canvas names the node with the
  longest wall time, the same time as its timeline row. The edges of the
  critical path are bold.
- **Timeline** — one row per node, in task-index order, on one time axis from
  the creation of the run. It shows where each node spent its time, which
  shows queue waits, serialization and stragglers that the DAG cannot.

A run summary is above both views. It shows:

- The wall time of the run: from its creation to its last recorded time.
- The critical path: the chain of nodes whose end times decided the end of
  the run. The chain ends at the node that ended last. Each step goes back to
  the dependency that ended last, which is the dependency that released the
  node. On a tie (a skip cascade ends many nodes at one instant), the node
  with more time that is not dependency wait comes first, then the lower task
  index. The length of the path is the time of its nodes without dependency
  wait. Its tooltip names the chain and splits the length by phase.
- "Where the time went": one bar of the time of each phase, summed over every
  node of the run. Parallel nodes add up, so the total is node time, not wall
  time.
- The one legend of the views: each phase of the run with its total time,
  then the marks of the view (the pause band, the end marker and the critical
  path line on the timeline; the bold critical edges and the tolerant join
  border on the graph). The legend wraps; it does not clip when the detail
  panel opens. Each item gives its meaning in its tooltip.
- The node count of each status in the run. Click a count to fade the nodes of
  the other statuses; click it again to clear the filter.

The node detail panel shows the status, the task name, one node timing summary
and the attempts of the task (see [Attempt timing](#attempt-timing)). The
summary holds the wall time and the end of the node, one bar of its segments
in time order, and its time split into its own phases and its task (for
example `dependency wait 7.1s · 3 attempts 32.1s · advancing 1ms`). A node on
the critical path has a "critical path" badge. A node whose coverage is not
`exact` names its reasons or its conflict.

The selected run and open node are URL search parameters (`?run=&node=`).

#### Phase timing

The run read `GET /api/workflows/{id}` holds the phase timing of the run and
of each node (the `timing` field). The core composes it from the lifecycle
record; the UI draws it and does not compute phases of its own. The run
summary, the critical path and the longest wall time are sums and choices
over these phases.

The UI names each phase by the time that it is:

| Phase in the API | Name in the UI | Drawing |
|---|---|---|
| `pending` | dependency wait | A thin track |
| `ready` | ready | Striped (a wait) |
| `enqueued` | queue wait | Striped (a wait) |
| `retry_wait` | retry wait | Striped (a wait) |
| `claimed` | slot wait | Striped (a wait): from the claim to the start of the task code, with the placement on a child in it |
| `running` | running | Solid (work) |
| `finalizing` | finalizing | Solid (work) |
| `advancing` | advancing | Solid (work) |
| `unknown` | unknown | Hatched |

Two phases that touch on a row differ in hue and in lightness in both themes,
and a wait is striped while work is solid. A reader who does not see the hues
(protan, deutan or tritan color vision) still sees each boundary.

The timeline draws, on each row:

- One segment per phase interval of the node, in time order. Dependency wait
  is a thin track, so queue wait, slot wait, running and finalizing stand
  out. A retry repeats the segments of an attempt, and a new task of the node
  after a pause abandon repeats them again. A segment shorter than one pixel
  stays visible; its times do not change. A segment that is wide enough
  shows its duration in it.
- A line under the active part of each node on the critical path, from the
  end of its dependency wait to its end. The name of the row has a mark. The
  lines of the path form a chain of steps across the rows.
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

Hover a segment for its phase and its duration, its claim (earlier claims,
the recorded claim with its placement after the claim, or a claim with no
attempt row), its task and attempt, and its start and end as times of day. The tooltip is shared by all rows.
With the keyboard, the up and down arrows (and Home, End) move between rows,
the left and right arrows step through the segments of the focused row in the
same tooltip, and Escape closes it. The accessible name of each row and each
graph card holds its status, coverage, time per phase, wall time and end; a
row on the critical path says so.

An open segment or pause grows on a local clock of one second, from the read
time of the database (`as_of`) plus the browser time since the response
arrived. The clock runs only while a segment or a pause is open, and it sends
no request: the next read replaces the timing.

The timeline renders only the rows in view and 8 rows above and below them,
so a run of 10,000 nodes renders the same number of rows as a short run.

#### Attempt timing

The node detail panel and the task detail panel show the attempts of a task
with one view. The view reads the attempt rows (`attempts` of
`GET /api/workflows/{id}/tasks/{index}` and of `GET /api/tasks/{id}`) and the
task row.

- A date line shows the date of the first attempt and the local time zone
  once. Every other time is a time of day with milliseconds. A time on
  another day has its day offset (`+1d`).
- Each attempt starts with the answer: its number, its outcome, its error
  code and its run time (the time the task code ran), then its first and last
  recorded times and its worker host.
- A bar shows the phases of the attempt in the timeline fills. A phase of a
  few milliseconds keeps a minimum width; each part has a tooltip.
- A table lists each phase: its duration first, its name, and its time span.
  The slot wait shows the placement on a child as an offset from the claim
  (`placed +1ms`).
- The error message of the attempt stays under the table.
- Facts that the normal case does not have show as notes: more than one claim
  (a lease expired or a claim was released), claims not counted, a worker
  lost (stale recovery recorded the attempt; the note gives the last sign of
  life), a cancel of the running task, a placement that was not observed, and
  an attempt recorded before schema migration 1001 (its phase times were not
  captured). One claim recorded by the worker shows no note.
- With more than one attempt, a summary bar shows every attempt and every
  retry wait in time order, with the total time, the running time and the
  retry wait. Between two attempts, a retry wait row gives the time from the
  record of one attempt to the due time of the next.
- The count of attempts and the allowed count (`3 of 5 allowed`) are in the
  section title.
- A task that is not terminal ends with its attempt in progress: a retry that
  is not due yet, a queue wait, a claim, or a run, with its time so far on a
  one-second clock.

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
gives the cancel to its code but does not kill its process (an `ISOLATED` task
with `cancel_grace_ms` is the exception), and cancelling a workflow leaves
executing nodes draining. [Action Semantics](/horsies/monitoring/action-semantics/) states each one as a
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
