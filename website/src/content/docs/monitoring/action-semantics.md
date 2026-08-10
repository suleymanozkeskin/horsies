---
title: Action Semantics
summary: Exactly what task cancel, task rerun, and workflow pause, resume, and cancel do to the rows and the processes behind them.
related: [web-ui-overview, web-ui-deployment, ../concepts/task-lifecycle, ../concepts/workflows/workflow-api]
tags: [monitoring, actions, cancel, rerun, pause, resume, workflow]
---

Five actions are available from the monitoring API and the web UI: cancel and
rerun a task, and pause, resume, and cancel a workflow run. Each acts on
database rows. Two of them leave a process running afterwards, and this page
states which and why.

Every action is a compare-and-swap against the row's current status. Two
operators acting at once do not corrupt anything: one CAS matches and the other
returns 409 with the status it actually found.

Actions are disabled unless the deployment enables them. See
[Deployment & Authentication](web-ui-deployment).

## Task cancel

Marks a task `CANCELLED` with `error_code='TASK_CANCELLED'` and a failure
timestamp. Waiting result handles unblock and receive `TASK_CANCELLED`.

| Task status | Result |
|---|---|
| `PENDING` | Cancelled. The task never runs |
| `CLAIMED` | Cancelled. The child's pre-start ownership check fails, it aborts with `CLAIM_LOST`, and writes nothing |
| `RUNNING` | Cancelled **only when the caller opts in**. See below |
| `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED` | Refused, 409 `TASK_NOT_CANCELLABLE` with the current status |
| Row is a workflow node | Refused, 400 `TASK_IS_WORKFLOW_TASK` |

A committed `CANCELLED` is final. Every lifecycle transition is a CAS with an
expected status, so claim, finalize, auto-retry, and the reaper all no-op
against a cancelled row and cannot overwrite it.

### Cancelling a RUNNING task

**horsies does not kill running processes.** Cancelling a `RUNNING` task flips
the row durably, and:

- The task's code **keeps executing** on its worker until the function returns.
- Its **side effects still happen** — writes, HTTP calls, file operations.
- Its result is discarded. The row stays `CANCELLED`; finalize finds no
  `RUNNING` row to update.
- **No attempt row is recorded** for that run. Attempt history will show a gap
  where this execution would have been.
- A pool slot stays occupied until the function returns.

There is no safe cross-process kill, so the API offers none. The
`include_running` flag exists so this is opted into rather than stumbled into;
the UI gates it behind a checkbox stating the same facts.

```bash
curl -X POST "$BASE/api/tasks/$TASK_ID/cancel" \
  -H 'Content-Type: application/json' \
  -H 'X-Horsies-Intent: action' \
  -d '{"include_running": true}'
```

Without `include_running`, a `RUNNING` task is refused with 409 like any other
ineligible state.

**Settled when:** the task reads `CANCELLED`. That is immediate. A worker
process that was executing may keep running for arbitrarily long afterwards, and
that is not a failure of the cancel.

## Task rerun

**The manual retry action is removed.** It reset a terminal row to `PENDING`
in place; a terminal record is now immutable and lives in the history
archive, so there is no row to reset. Re-execution is a **new request**:
rerun mints a new task with a new id and records its lineage back to the
source (`rerun_of_task_id`, and `rerun_root_task_id` for a chain).

| Source task | Result |
|---|---|
| `FAILED`, `CANCELLED`, `EXPIRED` with a retained rerun input | New task enqueued (`RerunEnqueued` carries `new_task_id`) |
| `COMPLETED` | Refused, `RerunNotEligible(COMPLETED_SOURCE)` — a succeeded request is not re-executed by replay |
| Still live (`PENDING`, `CLAIMED`, `RUNNING`) | Refused, `RerunSourceLive` — its own lifecycle owns retry |
| Input not retained or undecodable | Refused, `RerunInputUnavailable` or `RerunInputCorrupt` |
| Not found in live or history | Refused, `RerunSourceAbsent` |
| Row is a workflow node | Refused, `RerunNotEligible(WORKFLOW_TASK)` |
| Caller key already used | `RerunKeyReplay` (same request) or `RerunKeyConflict` (different request under that key) |

Whether a task can be rerun is decided **at enqueue**, not at rerun time: the
`retain_rerun_input_default` broker setting (with a per-task override)
decides whether the enqueue input is preserved with the record. A task
enqueued without a retained input cannot be rerun later.

Programmatic callers use the same contract:

```python
from datetime import timedelta

from horsies import rerun_task, RerunTask, RerunEnqueuePolicy, RerunEnqueued

outcome = await rerun_task(
    connection,
    RerunTask(source_task_id=task_id, deadline=None),
    RerunEnqueuePolicy(
        retention_class_key='standard_30d',
        retain_rerun_input=True,
        reservation_window=timedelta(hours=24),
    ),
)
match outcome:
    case RerunEnqueued(new_task_id=new_id):
        ...
```

The policy is supplied by the caller because enqueue policy lives in
configuration, not in the rerun contract: the **new** request gets its own
retention class, its own rerun-input retention, and its own key-reservation
window.

`RerunOutcome` is an exhaustive union — every refusal is a distinct typed
variant, so a caller matching over it cannot silently miss one.

**Settled when:** the response carries the new task id. The source record is
unchanged: rerun never mutates history.

## Workflow pause

Compare-and-swap `RUNNING` → `PAUSED`.

| Run status | Result |
|---|---|
| `RUNNING` | Paused |
| Anything else, including `PAUSED` | 409 `STATE_CONFLICT` with the current status |

Pausing is not only a status flip. It also **rewinds claimed-but-not-started
work**: every node task that a worker has claimed but not begun executing is
cancelled (`TASK_CANCELLED`), and its node returns to `READY` with its task id
cleared. Those nodes get fresh task rows on resume.

- **Nodes already executing finish.** Running user code is never interrupted.
- **New nodes stop being scheduled.**
- A cancelled claimed-but-unstarted task is terminal, so its record **moves to
  the history archive** like any other terminal task, carrying a message
  naming the pause. A node still `PENDING` moves nothing. The run row itself
  is untouched beyond its status.
- Unclaimed enqueued rows are left claimable; a worker's post-claim filter
  releases them sub-second.
- Only `status` and `updated_at` change on the run row. `completed_at`,
  `result`, and `error` stay as they were.

**Settled when:** the run reads `PAUSED` and no node is still executing with a
claimed or running backing task. The status flip is immediate; the drain is not.

## Workflow resume

Compare-and-swap `PAUSED` → `RUNNING`, then re-enqueue synchronously.

| Run status | Result |
|---|---|
| `PAUSED` | Resumed. `READY` nodes get **fresh** task rows, child workflows cascade, and completion is re-checked |
| Anything else | 409 `STATE_CONFLICT` with the current status |

Resume runs a scoped recovery pass in a second transaction after the resume
itself has committed. That pass can fail on its own. When it does, the resume
has still happened, so the API re-reads the run: if it is `RUNNING`, the response
is 200 with `warning: "post_resume_recovery_failed"`. Treat that as *resumed,
check worker logs* — not as a failed resume.

Resume does not clear a stale `error` left on the run by an `on_error='pause'`
policy.

**Settled when:** the run reads `RUNNING`. All the work happens before the call
returns.

## Workflow cancel

Compare-and-swap `PENDING`, `RUNNING`, or `PAUSED` → `CANCELLED`, recursively
through every descendant workflow, in a single commit.

| Run status | Result |
|---|---|
| `PENDING`, `RUNNING`, `PAUSED` | Cancelled, together with all sub-workflows |
| `COMPLETED`, `FAILED` | 409 `STATE_CONFLICT` with the current status |
| `CANCELLED` | 200. The operation is idempotent |

Per node:

- `PENDING` and `READY` nodes are **skipped**. The node vocabulary has no
  cancelled state; cancel expresses as `SKIPPED`. Those nodes keep
  `completed_at` null, so they show no duration.
- Backing tasks of enqueued nodes are cancelled, whether `PENDING`, `CLAIMED`,
  or `RUNNING`.
- **Executing nodes keep running to completion.** As with task cancel, no
  process is killed.

### Draining

A node that was executing when its run was cancelled is *draining*: it finishes,
its side effects happen, and its result does not advance the workflow. The UI
badges it `draining` rather than treating it as an error, because it is not one.

A draining node can persist indefinitely if the worker executing it crashed: the
reaper ignores nodes under non-running workflows, so nothing reclaims it. That
is a visible, explainable state, not a stuck action.

**Settled when:** the run reads `CANCELLED` and no node remains `PENDING`,
`READY`, or `ENQUEUED`. Nodes still `RUNNING` do not block this — draining is
the expected steady state after cancelling a run with work in flight.

## No workflow restart

There is no restart or retry action for a workflow run, because no such
primitive exists. `retry_start` retries a failed *start call*, not a run. To
re-execute a workflow, send it again.

## What the reads see

Task reads resolve across both sides of the lifecycle: a task that has
terminalized is answered from the history archive, so detail, result, and
attempt history stay available after the row leaves the live table.

List and aggregate reads are **window-scoped**. They cover live rows plus a
bounded slice of terminal history: the last 24 hours by default, with
optional `since` / `until`. Bounds must be timezone-aware and increasing,
and the span may not exceed 30 days — a request over the maximum is refused
with the bound named, never silently clamped.

The page total follows a stated contract: **exact when any filter is
active, a planner estimate on the unfiltered view.** An unfiltered total is
therefore approximate by design on both sides of the split; narrow with any
filter to get an exact count.

## Conflict handling

There is no optimistic-concurrency token on any action. The CAS is the
concurrency control.

| Response | Meaning | What to do |
|---|---|---|
| 200 | The CAS matched and committed | Observe the entity |
| 400 | The row is a workflow node; task actions do not apply | Use the workflow actions |
| 404 | The row no longer exists | Retention may have removed it |
| 409 | Another change won the race, or the schema does not match this build | Re-read the entity; the body carries `current_status` |
| 503 | Broker or database failure | The action may or may not have committed; re-read before retrying |

A failed request is not proof that nothing happened. A network error in
particular leaves the outcome unknown — the request may have committed and the
response been lost. Re-read the entity before concluding the action failed. The
web UI does this automatically and reports success when it observes the effect.
