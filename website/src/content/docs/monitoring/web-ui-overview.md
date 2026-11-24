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
  card keeps its status color and shows the node time: the time of the node
  without its dependency wait. A card states in words what the normal case
  does not have: a long engine step (`queue wait 7.2s`), more than one attempt
  (`3 attempts`), and timing that is not exact. The card over the canvas names
  the node with the longest node time and holds the key to the marks of the
  graph. The edges of the critical path are bold.
- **Timeline** — one row per node, in task-index order, on one linear time
  axis from the creation of the run. It shows when each node ran: the order,
  the overlap, and the waits that are long at the scale of the run.

A run summary is above both views, in one line:

- The duration of the run: from its creation to its last recorded time.
- The split of the duration: the time of each phase on the critical path.
  The critical path is the chain of nodes whose end times decided the end of
  the run. The chain ends at the node that ended last. Each step goes back to
  the dependency that ended last, which is the dependency that released the
  node. On a tie (a skip cascade ends many nodes at one instant), the node
  with more time that is not dependency wait comes first, then the lower task
  index. Each node of the chain starts when the node before it ends, so the
  phases of the chain without its dependency wait add up to the duration of a
  run that had no pause. The label `critical path` names the chain on hover.
- The pause time of a paused run.
- The long engine steps of the nodes that are not on the critical path,
  after the words `other nodes` (for example
  `queue wait up to 7.0s in 2 nodes`). These nodes did not decide the end of
  the run; the item is absent when no such step exists.
- The node count of each status in the run. Click a count to fade the nodes of
  the other statuses; click it again to clear the filter.

The node detail panel shows the status, the task name, one node timing summary
and the attempts of the task (see [Attempt timing](#attempt-timing)). The
summary holds:

- The node time, and the end of the node as a time of day.
- The wall time of the node as named numbers: its dependency wait, its task
  (`task`, `3 attempts` or `2 tasks`) or its child run, and its own engine
  steps (for example `dependency wait 7.1s`, `3 attempts 32.1s`,
  `advancing 1ms`). The attempts split the task.
- The dependency at whose end the dependency wait ended.
- A note for a long engine step of the node itself, and for coverage that is
  not `exact`: its reasons or its conflict.

A node on the critical path has a "critical path" badge. A skipped node has no
attempts section. A node error that an attempt shows as its message is shown
once.

The selected run and open node are URL search parameters (`?run=&node=`).

#### Phase timing

The run read `GET /api/workflows/{id}` holds the phase timing of the run and
of each node (the `timing` field). The core composes it from the lifecycle
record; the UI draws it and does not compute phases of its own. The run
summary, the critical path and the node time are sums and choices over these
phases.

The UI names each phase by the time that it is:

| Phase in the API | Name in the UI | Measures | Drawing on the timeline |
|---|---|---|---|
| `pending` | dependency wait | The node waits for its dependencies. A node also waits here while its run is paused | A thin track |
| `ready` | ready | The dependencies are met and the node has no task: after a pause or a recovery pass | Striped (a wait) |
| `enqueued` | queue wait | From the due time of the attempt to its claim by a worker | Striped (a wait) |
| `retry_wait` | retry wait | The delay of the retry policy: from the record of an attempt to the due time of the next | Striped (a wait) |
| `claimed` | slot wait | From the claim to the start of the task code, with the placement on a child in it | Striped (a wait) |
| `running` | running | The task code runs. A sub-workflow node: the child run runs | Solid (work) |
| `finalizing` | finalizing | From the end of the task code to the record of the attempt | Solid (work) |
| `advancing` | advancing | From the end of the task, or of the child run, to the end of the node in the workflow | Solid (work) |
| `unknown` | unknown | A known interval whose phase the record does not show | Hatched |

The phases are of four kinds. The kinds differ in scale: an engine step is a
few database round trips (milliseconds), and the task code and the planned
waits are seconds or more. A proportional bar gives a phase of 3 ms beside a
task code of 4 s less than one pixel. Thus the detail panels show each phase
time as a named number, and the weight of the number follows its kind:

| Kind | Phases | Controlled by | Weight of the number |
|---|---|---|---|
| Task code | `running` | The task | Bold |
| Planned wait | `pending`, `retry_wait`, a pause | The workflow definition, the retry policy, the operator | Plain |
| Engine step | `ready`, `enqueued`, `claimed`, `finalizing`, `advancing` | The queue, the workers and the workflow steps | Quiet below 250 ms; marked at 250 ms or more |
| Record gap | `unknown` | The record | Quiet below 250 ms; marked at 250 ms or more |

A marked number has a warning icon and its own color. A sum of phase times
(a node, a run, all attempts of a task) is marked when one of its intervals is
250 ms or more: three queue waits of 100 ms are not a long step. The UI says
what a long time of each phase tells:

| Phase | A time of 250 ms or more tells |
|---|---|
| queue wait | No worker claimed the task in this time. The usual cause is capacity: the queue is at its `max_concurrency`, or no worker has a free slot. The [Workers](#workers) surface shows the free capacity |
| slot wait | A worker held the claim and the task code did not start: no child was free, or a child was starting. The attempt shows the time after which a child took the task, and the time of the start on the child |
| finalizing | The outcome write waited for the database |
| advancing | The worker did not apply the end of the task to the workflow at once, so a recovery pass did |
| ready | A pause took the task of the node, or a recovery pass repaired the node |
| unknown | The record does not show the phase: a lost worker, claims that were not counted, an attempt with no phase record, or a history row that retention removed |

A dependency wait and a retry wait are never marked: the first is the shape of
the workflow, and the second is the delay of the retry policy.

The timeline keeps a linear axis, because it shows when each node ran. Two
phases that touch on a row differ in hue and in lightness in both themes, and
a wait is striped while work is solid. A reader who does not see the hues
(protan, deutan or tritan color vision) still sees each boundary.

The timeline draws, on each row:

- One segment per phase interval of the node, in time order, as wide as its
  time. Dependency wait is a thin track. A retry repeats the segments of an
  attempt, and a new task of the node after a pause abandon repeats them
  again. A segment below one pixel is not drawn: the tooltip of the row gives
  its time. A marked segment keeps a width of 3 pixels. A segment that is wide
  enough shows its duration in it.
- A line under the active part of each node on the critical path, from the
  end of its dependency wait to its end. The lines of the path form a chain of
  steps across the rows.
- The terminal status of the node as a marker at its completion time. The
  marker is not a duration: a task that runs on after a workflow cancel keeps
  its segments past the marker.
- The node time in the right column.

A pause of the run is a band over every row, from the pause to the resume,
to the end of the run (a cancel or a paused expiry), or to now.

The key above the rows lists the fill of each phase that the timeline draws at
its scale, then its marks. The key of the graph is on the card over the
canvas.

Each node has a coverage label:

| Label | Row on the timeline | Graph card |
|---|---|---|
| `exact` | Segments, marker and node time | The node time |
| `partial` | The same; the `unknown` segments are hatched, and a banner names their reasons | The node time and the text `partial timing` |
| `unavailable` | A hatched row with the text "Timing unavailable"; the tooltip of the row gives the reason | The text `timing unavailable`; the card shows the execution time |
| `inconsistent` | A hatched error row with the text "Timing inconsistent"; the tooltip of the row names the two times in the wrong order, the missing record or the unexpected event | The marked text `timing inconsistent`; the card shows the execution time |

A run that began before schema migration 1001, or that has more than 100,000
events, is unavailable as a whole: a banner gives the reason and the start of
the lifecycle record, and every row is hatched. A run with an inconsistent
node or pause record shows a banner that names each conflict.

Each mark explains itself on hover, with no wait:

- The track of a row shows the tooltip of the node: its status, its node
  time, each phase of the node as a number (also the phases too short to
  draw), its end, and whether it is on the critical path.
- The tooltip names the mark under the pointer first. A segment: its phase,
  its time, its start and end as times of day, its task and attempt, its
  claim (earlier claims, or the placement of a long slot wait), and what a
  long time tells. The end marker: the status and the time of the end; a
  failed node adds the error code of its task. The critical path line. A
  pause of the run at that time.
- A graph card has the same tooltip, with the meaning of its marks: the
  dashed border of a tolerant join, the icon and the count of a sub-workflow
  node. An edge names its two nodes, and a bold edge its part in the critical
  path.
- Each number and each item of a key gives its meaning on hover and as its
  accessible description.

The tooltip is shared by all rows. With the keyboard, the up and down arrows
(and Home, End) move between rows, the left and right arrows step through the
segments of the focused row in the same tooltip, and Escape closes it. The
accessible name of each row and each graph card holds its status, coverage,
time per phase, node time and end; a row on the critical path says so.

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
task row. It has no bar: it shows each phase time as a named number (see
[Phase timing](#phase-timing)).

- A date line shows the date of the first attempt and the local time zone
  once. Every other time is a time of day with milliseconds. A time on
  another day has its day offset (`+1d`).
- Each attempt starts with the answer: its number, its outcome, its error
  code and its time (from its due time to its record).
- The phases of the attempt follow in record order: queue wait, slot wait,
  running, finalizing. The time from the first claim of an attempt to the
  claim that ran it is `earlier claims`. The time after the last sign of life
  of a lost worker is `worker lost`.
- A long engine step has a note with its time and what it tells. The note of
  a long slot wait gives the time after which a child took the task and the
  time of the start on the child. A slot wait in its normal range shows no
  placement.
- The other facts that the normal case does not have show as notes: more than
  one claim (a lease expired or a claim was released), claims not counted, a
  worker lost (recovery recorded the attempt; the note gives the last sign of
  life), a cancel of the running task (the end of the attempt is the time of
  the cancel), and an attempt recorded before schema migration 1001 (its
  phase times were not captured). One claim recorded by the worker shows no
  note.
- The error message of the attempt follows the notes.
- The last line gives the time span of the attempt and its worker host. The
  host wraps; it is not cut.
- With more than one attempt, a box above the attempts gives the time from
  the first due time to the last record, and the time of each phase over all
  attempts and retry waits. Between two attempts, a retry wait row gives the
  time from the record of one attempt to the due time of the next.
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
