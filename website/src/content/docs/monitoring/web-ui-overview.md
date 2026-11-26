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
- The `Queued` and `Exec` columns write their durations with the rule in
  [Durations and Times](#durations-and-times).
- A task detail panel. It shows the [attempt panel](#attempt-panel) of the
  task, and the task actions when actions are on.

Filters, sort, pagination, and the open task are held in the URL, so a filtered
view is shareable and survives a reload.

### Workflows

The page has a run list and two views of the selected run: **Graph** and
**Timeline**.

- The run list has a filter by workflow name and a filter by status. The
  newest run is first. Each run shows its name, its status, its duration and
  its definition key. A button in the toolbar collapses the list.
- The toolbar holds the switch between **Graph** and **Timeline**, and the
  workflow actions when actions are on.
- The selected run and the open node are URL search parameters
  (`?run=&node=`).

The views use two terms:

- **Node time**: the time of a node without its dependency wait, from the
  time that the node could start to its end. The UI calls it `duration` in
  the timeline column and in the node detail panel.
- **Longest chain**: the chain of nodes whose end times decided the end of
  the run. The chain ends at the node that ended last. Each step goes back to
  the dependency that ended last, which is the dependency that released the
  node. On a tie (a skip cascade ends many nodes at one instant), the node
  with more time that is not dependency wait comes first, then the lower task
  index. Each node of the chain starts when the node before it ends. Thus the
  phases of the chain without its dependency wait add up to the duration of a
  run that had no pause.

#### Run summary

The run summary is above the two views. Its first line shows:

- The status of the run, its date, and its start and end to the second. The
  end of an open run is `now`.
- The node count and the count of each node status. Click a count to fade the
  nodes of the other statuses. Click it again to show all nodes.

Its second line is a key with totals:

- `duration`: the time from the creation of the run to its last recorded
  time.
- The time of each phase on the longest chain, with the fill of the phase.
- `paused`: the pause time of a paused run.

Each item has a tooltip. The tooltip of `duration` gives the status of the
run, its start and end, the time of each phase on the longest chain, and the
nodes of the chain (`validate_order → reserve_stock_1 → …`). Put the pointer
on a phase item to highlight the marks of that phase on the timeline. The
marks of the other phases fade.

#### Graph

The graph shows the DAG of the run.

- Click a node to open its detail panel. Double-click a sub-workflow node to
  open the child run in the same panel. The back button of the panel goes
  back one step.
- Failure navigation (`1 failed`, `1/1`) steps through the failed nodes.
- Node search (`Find node…`) jumps to a node by name. Enter goes to the next
  match, and Shift+Enter goes to the match before.
- A card over the canvas shows the node count, the node with the longest node
  time (`Longest node: generate_invoice 8.03s`), and the key to the marks of
  the graph. The key has `longest chain` (bold edges) and `tolerant join`
  (dashed border), each only when the graph has that mark. In a sub-workflow
  graph, the card also counts the done nodes, and the running and failed
  nodes when there are some.

Each node card:

- Has the color of its status: a bar on its left edge, a dot, and a tint.
- Shows its name, its task name and its node time. A node with no phase
  timing shows its execution time.
- States in words what a node that ran once does not have: `3 attempts`,
  `2 tasks`, `partial timing`, `timing unavailable` and `timing inconsistent`.
  `timing inconsistent` has a warning icon and the warning color.
- For a sub-workflow node: shows a layers icon and the count of its child
  nodes. When child nodes failed, the count is `1/3 failed` in the error
  color.
- For a node that runs on under a cancelled or paused run: shows the badge
  `draining` or `finishing` in place of its task name.

The edges of the longest chain are bold.

#### Timeline

The timeline is a table. Its first row is the run, and then it has one row
for each node, in task-index order. A banner above the table names the
coverage of the run when it is not `exact` (see [Coverage](#coverage)). The
key of the table is below the banner.

| Column | Header | Content |
|---|---|---|
| Name | `node` | The node name. `sub` marks a sub-workflow node, `×3` marks three attempts, and `2 tasks` marks a node whose task was replaced |
| Linear cell | `timeline` | The phases of the node on one linear time axis from the creation of the run. It shows the order, the overlap, and the waits that are long at the scale of the run |
| Log cell | `phases · log scale` | One thin bar for the total time of each phase of the node, on a log axis of duration. It shows the size of each time: a phase of 3 ms and a task code of 4 s both get a visible bar |
| Time | `duration` | The node time. A node that did not start shows `—` |

The run row (`run`) shows the phases of the nodes on the longest chain, at
their times, without their dependency wait. A thin line in the color of the
window separates two nodes. Its log cell has the total of each phase along
the chain. Its last column has the duration of the run.

The linear cell of a node row has:

- One segment for each phase interval of the node, in time order, as wide as
  its time. The dependency wait is a thin line. A retry repeats the segments
  of an attempt. A new task of the node after a pause abandon repeats them
  again.
- No segment for an interval narrower than one pixel. The log cell and the
  tooltip of the row give its time.
- A diamond at the time of the terminal status of the node, in the color of
  that status. The diamond is a point in time, not a duration. A task that
  runs on after a workflow cancel keeps its segments past the diamond.

The log cell of a node row has one lane for each phase of the run, except
dependency wait. The lanes are in the order of the key and at the same height
in every row. A row draws a bar in the lane of each phase that it has. The
axis starts at 0.1ms. Each labeled line is 10 times the line before it, up to
10s, then 1m, 10m and 1h. The axis ends at the first labeled or minor line
above the longest bar, and at 1s or later. The minor ticks between two labeled
lines become closer toward the next labeled line.

Over the rows, the table draws:

- A grid line at each labeled time of the two axes.
- A vertical line at the end of each node of the longest chain, from its row
  to the row of the next node of the chain. The next node starts at that time.
- A band over every row for each pause of the run: from the pause to the
  resume, to the end of the run (a cancel or a paused expiry), or to now.

The key of the table names what the table draws and the run summary does not
name:

- The phases that are not on the longest chain, with no totals.
- `dependency wait` (the thin line).
- `end of a node` (the diamond), `longest chain` (the vertical line) and
  `run paused` (the band), each only when the table has that mark.
- `wait` (a striped fill is a wait) and `work` (a solid fill is work).

Click a row to open the node detail panel. The table renders only the rows in
view and 8 rows above and below them, so a run of 10,000 nodes renders the
same number of rows as a short run.

#### Node detail panel

The panel opens on the right. Drag its left edge, or use the left and right
arrow keys on that edge, to change its width. It shows:

- The status, a `longest chain` badge for a node on the longest chain, a
  `draining` or `finishing` badge, and a `tolerant join` badge. The task
  name is on this line when the panel has no attempt panel.
- The node timing:
  - `duration` and the node time. A node that ended with no time after its
    dependency wait shows `did not run`. A node that did not start shows
    `not started`. A node with no timing shows `no timing`.
  - The end of the node: a diamond and `completed at 18:46:58.708`. Its hover
    text gives the date and the time zone.
  - The parts of the wall time of the node, each with its time: the
    dependency wait, the task (`task`, `3 attempts` or `2 tasks`) or the child
    run of a sub-workflow node (`running`), and the steps of the node itself
    (for example `ready` and `advancing`). The attempt panel below splits the
    task.
  - `dependency wait ended with <node>`: the dependency at whose end the
    dependency wait ended.
  - A note for coverage that is not `exact`, with its reasons or its
    conflict.
- An `Open subworkflow` button for a sub-workflow node.
- The node error, when no attempt shows the same text as its message.
- The [attempt panel](#attempt-panel) of the task of the node. A node with no
  task shows `no attempt recorded`. A sub-workflow node and a skipped node
  with no attempt have no attempt panel.

#### Phase timing

The run read `GET /api/workflows/{id}` holds the phase timing of the run and
of each node (the `timing` field). The core composes it from the lifecycle
record. The UI draws it and does not compute phases of its own. The run
summary, the longest chain and the node time are sums and choices over these
phases.

The UI names each phase by the time that it is. The keys, the lanes and the
bars use the order of this table.

| Phase in the API | Name in the UI | Measures | Fill |
|---|---|---|---|
| `pending` | dependency wait | The node waits for its dependencies. A node also waits here while its run is paused | A thin line |
| `ready` | ready | The dependencies are met and the node has no task: after a pause or a recovery pass | Striped (a wait) |
| `enqueued` | queue wait | From the due time of the attempt to its claim by a worker | Striped (a wait) |
| `claimed` with an earlier claim | earlier claims | From the first claim of the attempt to the claim that ran it | Striped, the fill of slot wait |
| `claimed` | slot wait | From the claim to the start of the task code | Striped (a wait) |
| `running` | running | The task code runs. A sub-workflow node: the child run runs | Solid (work) |
| `finalizing` | finalizing | From the end of the task code to the record of the attempt | Solid (work) |
| `advancing` | advancing | From the end of the task, or of the child run, to the end of the node | Solid (work) |
| `unknown` with `WORKER_LOST` | worker lost | From the last sign of life of a lost worker to the record by recovery | Hatched |
| `unknown` | unknown | A known interval whose phase the record does not show | Hatched |
| `retry_wait` | retry wait | The delay of the retry policy: from the record of a failed attempt to the due time of the next | Striped (a wait) |

Two phases that touch on a row differ in hue and in lightness in both themes.
A wait is striped and work is solid. Thus a reader who does not see the hues
(protan, deutan or tritan color vision) still sees each boundary.

An open segment or pause grows on a local clock of one second, from the read
time of the database (`as_of`) plus the browser time since the response
arrived. The clock runs only while a segment or a pause is open, and it sends
no request. The next read replaces the timing.

#### Coverage

Each node has a coverage label:

| Label | Timeline row | Graph card |
|---|---|---|
| `exact` | Segments, diamond and node time | The node time |
| `partial` | The same. The `unknown` segments are hatched, and the banner above the table names their reasons | The node time and the text `partial timing` |
| `unavailable` | A hatched row with the text `Timing unavailable`. The tooltip of the row gives the reason | The text `timing unavailable`. The card shows the execution time |
| `inconsistent` | A hatched row with the text `Timing inconsistent`. The tooltip of the row names the two times in the wrong order, the missing record or the unexpected event | The text `timing inconsistent` with a warning icon. The card shows the execution time |

A run that began before schema migration 1001, or that has more than 100,000
events, is unavailable as a whole. The run summary and a banner give the
reason, the banner gives the start of the lifecycle record, and every row is
hatched. A run with an inconsistent node or pause record shows a banner that
names each conflict.

#### Tooltips and keyboard

Each mark of the graph and the timeline has a tooltip. The tooltip opens when
the pointer is on the mark, and when the mark has the keyboard focus. It names
the mark first. The tooltip of a time also gives its duration, and its start
and end as times of day with milliseconds.

| Mark | Tooltip |
|---|---|
| Graph card, or a timeline row off its marks | The name, status and node time of the node; the time of each phase; its attempt or task count (or `sub-workflow`) and `on the longest chain`; its end (`end: completed at 18:46:58.708`), with the error code of the task of a failed node. A graph card adds its task name when the node has a name of its own, and the meaning of a tolerant join, a sub-workflow (`Double-click to open it.`) and the `draining` or `finishing` badge |
| Graph edge | Its two nodes and `<node> waits for <node>.` A bold edge adds that the first node ended last of the dependencies of the second, so its end decided when the second could start |
| Segment | Its phase, its node and source (`task 1 · attempt 2`), its duration, its start and end, and what the phase measures. Also what it holds beyond its phase: `2 earlier claims`, `a child took the task after 273ms`, or the reason of an `unknown` time |
| Diamond | `end of <node>`, the status and the time. A failed node adds the error code of its task |
| Bar of a log cell | The phase, the node and the total; its start and end for one interval, or the interval count and the longest interval for more than one; and what the phase measures |
| Segment or bar of the run row | The same, along the longest chain. A segment highlights the row of its node |
| Longest chain line | The two nodes, the time, and `<node> starts when <node> ends`. It highlights the two rows |
| Pause band | `run paused`, its time, its start and end, under the tooltip of the mark below the pointer |
| Column header | What the axis or the column shows |
| Key item | The fill or the mark, its name, its total when it has one, and what it measures or means |

One tooltip box serves all rows of the timeline. Each node row is a button.
With the keyboard:

- The up and down arrows, Home and End move between rows.
- The left and right arrows step through the segments of the focused row in
  the tooltip.
- Escape closes the tooltip.

The accessible name of a graph card holds its name, status, coverage and time
per phase. The accessible name of a timeline row also holds its node time, its
end, and `on the longest chain` for a node on the chain.

#### Attempt panel

The task detail and the node detail panel use one panel for a task: the
attempt panel. It reads `GET /api/tasks/{id}`: the task row and its attempt
rows. From top to bottom, it shows:

1. The task name, the status, and the error code in the color of its
   category. The task detail has its close button on this line.
2. `queue`, `priority`, and `attempt n of m`, where m is the allowed attempt
   count (`max_retries` + 1).
3. The task actions, in the task detail. See [Actions](#actions).
4. The errors, grouped by error code. Each message shows once, with the
   attempts that had it (`attempt 1–4`, `attempt 1, 3`). A message longer than
   3 lines is cut, and `more` opens it. The words
   `Unhandled exception in task <name>:` are muted. A failure of the task that
   no attempt shows (for example an expiry) shows under `task`.
5. The timing: the duration, the strip, the key and the bars. See below.
6. `details`, closed. It holds a table of exact times: each attempt with its
   outcome or error code, then each of its intervals with its start, its end
   and its duration, and the retry wait after the attempt. It also holds the
   worker hosts, the task id, the trace id (a link when the server has a
   trace link template), `good until` (the expiry of the task), and the date
   and time zone of the times.

The timing has two scales, each with one job. The linear strip shows the
order and the share of each time. The log bars show the size of each time.

- `duration`: the time from the first due time to the last record, with the
  start and end to the second.
- The strip: the task on a linear axis from the first due time. The attempts
  and the retry waits are in time order. The width of each interval is its
  share of the total. Above each attempt are its number, the sign of its
  outcome, and a bracket over its part of the strip. An interval too narrow to
  see is not drawn; the bars show its time.
- The key: each phase of the task, the outcome signs, `wait` and `work`. With
  more than one attempt, each phase has its total over all attempts. Its
  tooltip names the interval count (`total of 4`).
- The bars: one bar for each phase of each attempt, and one bar for each retry
  wait, on a log axis of duration (`duration · log scale`). The duration is
  written at the end of each bar. The axis follows the rule of the log cell of
  the timeline.
- The grouping switch of the bars:
  - `by phase` (the default): one group for each phase, with a row for each
    attempt (`1`, `2`) and for each retry wait (`1→2`).
  - `by attempt`: one group for each attempt with its phases in time order,
    then the retry wait to the next attempt.
  - The browser keeps the choice for every task. With one attempt, the two
    groupings show the same bars: the switch is off, and its tooltip says why.

The phases of an attempt are, in record order: queue wait, earlier claims,
slot wait, running, finalizing, and worker lost. An attempt row with no phase
record (an attempt recorded before schema migration 1001) has one `unknown`
interval from its start to its record. When the claims of an attempt were not
counted, an `unknown` interval takes the place of its queue wait. A retry
wait is between two attempts.

A task that is not terminal ends with a line for its attempt in progress. The
line changes on a one-second clock:

- `attempt 3 · retry wait · due 09:14:26.244 · in 4.00s`
- `attempt 3 · queue wait · since 09:14:26.244 · 1.20s so far`
- `attempt 3 · claimed`
- `attempt 3 · running · since 09:14:26.250 · 5.00s so far`

A task with no recorded time shows `no attempt recorded` or
`no time recorded`.

The times of day in the tooltips, in the details and in the line of the
attempt in progress have milliseconds. A time on another day than the first
due time has its day offset (`+1d`).

Each mark of the panel has a tooltip:

- An interval (a segment of the strip or a bar): its phase, its attempt when
  the task has more than one, its duration, its start and end, and what the
  phase measures. Also what it holds beyond its phase: `2 earlier claims`,
  `a child took the task after 273ms`, `claims not counted`, or
  `attempt row with no phase record`.
- An attempt (its number, its bracket, or its group label): its number, its
  outcome and error code, its duration, its start and end, the time of each
  of its phases, and its worker host. An attempt that the worker did not
  record adds `recorded by recovery` or `ended by a cancel`.
- `duration`, the two axes, and the grouping switch when it is off.

The mark under the pointer highlights its related marks, and the other marks
fade. An interval highlights itself. An attempt highlights its intervals. A
phase item of the key highlights the intervals of that phase. Escape closes
the tooltip.

### Workers

- The page header counts the workers: `3 live`, or `1 live · 4 stale`.
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
- Workers by profile: one panel for each
  [worker profile](/horsies/monitoring/worker-health/#which-profile-does-a-worker-run)
  that a live worker runs, in name order. Live workers whose snapshot holds
  no profile name are one last panel, "Profile not recorded".
  - The header of a panel shows the profile name, the number of workers, the
    seats in service and the down slots.
  - A line states what the profile gives a worker: its number of children
    and its budget classes (`12 children per worker · ISOLATED budget`). These
    two facts do not change while a worker runs. When the workers of one
    profile do not have the same two facts, the panel lists each shape with
    its number of workers: these workers do not run the same definition of
    the profile (for example, the profile changed and only a part of the
    workers started again).
  - Each budget of a profile has a meter of the capacity in service that
    holds a row, the rows in flight and the free capacity (summed over the
    workers of the profile), the capacity in service (children for
    `ISOLATED`, seats for a shared class) and the classes that the budget
    serves. A budget shows the rows above its capacity when a worker holds
    more rows than its capacity in service.
  - A line names the classes that the workers of the profile do not claim.
  - The budgets come from the class report. A panel counts the workers that
    the report does not hold, and its sums leave them out. A panel with no
    worker in the report says why its budgets are not known.
- Per-worker cards, inside the panel of the worker's profile, with
  running/claimed counts, the CPU and the memory of the worker process, and
  uptime. A worker whose snapshot is older than 120 seconds is marked stale.
  Stale workers collapse into a separate list, outside the panels; the card
  of a stale worker names its profile. A live worker's card shows each claim
  budget with its free capacity (`ISOLATED 3/8 free`), and a card shows the
  number of down slots.
- For the focused worker: its profile and its queues; each budget with its
  capacity in service, rows in flight, free capacity and the classes it
  serves; each down slot with its cause, failed starts and error text; and
  two charts of its snapshots.
  - Load: the `RUNNING` and the `CLAIMED` tasks that the worker held.
  - Worker process: the CPU and the memory of the worker process, in percent.
    The task children of the worker are not in these numbers. See
    [Snapshot Fields](/horsies/monitoring/worker-health/#snapshot-fields).
  - The legend is under each chart. Each legend item has the mark of its
    series: an area for `running` and `claimed`, and a line for `CPU` and
    `memory`.
  - Each legend item is a button. A click draws that series alone, and the
    value axis then fits that series. A click on a series that is alone draws
    the two series again. The icon of a series that is not drawn is dim, and
    its name is struck through.
  - The tooltip of a chart gives the time of the snapshot and one row for
    each series that is drawn.
- The schedule table: last run, next run, and run count per registered
  schedule. A click on a column header sorts the table by that column, and a
  second click reverses the order; Enter and Space do the same when the
  header has the keyboard focus. The header shows the sorted column and its
  direction. The first order is the soonest next run. Schedules with equal
  values are in name order. A schedule that has not run shows `never` and is
  last in a sort by last run, in the two directions.

## Durations and Times

Every duration in the UI follows one rule:

| Duration | Text | Examples |
|---|---|---|
| Less than 1 s | Milliseconds with 3 significant digits | `0.821ms`, `7.05ms`, `482ms` |
| 1 s to less than 1 min | Seconds with 3 significant digits | `4.82s`, `11.4s` |
| 1 min or more | Minutes and seconds to 0.1 s. From 1 h, also hours | `1m 0.9s`, `2m 3.9s`, `1h 2m 3.4s` |

- A value that rounds up to the next unit takes that unit: `999.6ms` is
  `1.00s`, and `59.96s` is `1m 0.0s`.
- Zero is `0ms`.
- A time with no end yet has the suffix `so far` (`5m 43.9s so far`).
- The monitoring API sends its durations (`queue_s`, `exec_s`, `wall_s`,
  `uptime_s`, `snapshot_age_s`, `wait_seconds`) as seconds with microsecond
  detail. The digits that the UI shows come from the record.
- An axis label is a round time: `0`, `0.1ms`, `100ms`, `1s`, `30s`,
  `1m 30s`, `1h`.
- A relative time stays short: `5m ago`, `in 8s`. The `Enqueued` column of the
  task list and the schedule table use relative times.
- The timing views write a time of day in local time. The tooltips, the ends
  of nodes and the details have milliseconds (`09:14:26.244`). The start and
  end of the run summary and of the attempt panel have seconds (`09:14:26`).

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
listener (a broker of `SchemaMode.ASSUME_INITIALIZED`) or the listener
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
- **Actions are on the selected entity only.** The task actions are in the
  task detail panel. The workflow actions are in the toolbar of the selected
  run. Table rows and list items carry no action buttons.

When actions are disabled — by configuration, by the auth policy, or by a schema
mismatch — the affordances are absent rather than shown-and-disabled.

After the request, a message in the lower right corner gives the outcome. The
HTTP answers are on
[Action Semantics](/horsies/monitoring/action-semantics/#conflict-handling).

| Answer | Message | Retry button | The UI then |
|---|---|---|---|
| Success | The result of the action, for example `Workflow paused. Executing nodes will finish.` | No | Changes the status when a read shows the change |
| No answer (a network error) | The UI reads the entity first. `<Action> applied.` when the read shows the change. Else `<Action> could not be confirmed. Nothing appears to have changed.` | On the second message | — |
| 400 | `This task is managed by its workflow; task-level actions do not apply.` | No | Reads the entity again |
| 403 | `Not authorized to perform actions.` | No | Removes the action buttons |
| 404 | `Task no longer exists (retention may have removed it).`, or the same for a workflow run | No | Closes the task detail of a task |
| 409 | `Cannot <action>: the state is now <status>.` | No | Reads the entity again |
| 409 `SCHEMA_INCOMPATIBLE` | `Actions are disabled: the database schema does not match this UI version.` | No | Removes the action buttons |
| 500 | `<Action> failed: <detail>. A retry gives the same result.` | No | Reads the entity again |
| 503, and every other status | `<Action> failed: <detail>. The broker may be unreachable.` | Yes | — |

A 500 answer is a permanent failure: the same request gets the same answer
until the stored row, the schema or the connection settings change. A 503
answer is a failure that may clear, and the action may or may not have
committed. A message with a Retry button and the message of a 500 answer stay
until you close them. The other messages close after 6 seconds.

## Limits

- One horsies app, one schema, one dashboard. The UI has no notion of multiple
  deployments.
- No date-range filters.
- No node-level workflow actions, and no workflow restart: no such primitive
  exists.
- No way to kill a running task process. See [Action Semantics](/horsies/monitoring/action-semantics/).
- The response models are exported and typed, and unstable before 1.0.
