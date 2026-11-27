---
title: Action Semantics
section: shared
summary: Exactly what task cancel, task rerun, and workflow pause, resume, and cancel do to the rows and the processes behind them.
related: [web-ui-overview, web-ui-deployment, ../concepts/task-lifecycle, ../concepts/workflows/workflow-api, ../operations/rerun-in-your-transaction]
tags: [monitoring, actions, cancel, rerun, pause, resume, workflow]
---

Four actions are available from the web UI: task cancel, and workflow pause,
resume and cancel. Each acts on database rows. Task rerun is a client call,
not a web UI action. Two of them can leave task code running afterwards, and this
page states when and why.

Every action is a compare-and-swap against the row's current status. Two
operators acting at once do not corrupt anything: one CAS matches and the other
returns 409 with the status it actually found.

Actions are disabled unless the deployment enables them. See
[Deployment & Authentication](/horsies/monitoring/web-ui-deployment/).

## Task cancel

Marks a task `CANCELLED` with `error_code='TASK_CANCELLED'` and a failure
timestamp. Waiting result handles unblock and receive `TASK_CANCELLED`.

| Task status | Result |
|---|---|
| `PENDING` | Cancelled. The task never runs |
| `CLAIMED` | Cancelled. The child's pre-start ownership check fails, it aborts with `CLAIM_LOST`, and writes nothing |
| `RUNNING` | Cancelled **only when the caller opts in**. The attempt is recorded as `CANCELLED`, and the worker gives the cancel to the task code. See below |
| `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED` | Refused, 409 `TASK_NOT_CANCELLABLE` with the current status |
| Row is a workflow node | Refused, 400 `TASK_IS_WORKFLOW_TASK` |
| Stored row does not parse, for example a history status that is not a task status | Refused, 500 with a `detail` message. Nothing changes |

A committed `CANCELLED` is final. Every lifecycle transition is a CAS with an
expected status, so claim, finalize, auto-retry, and the reaper all no-op
against a cancelled row and cannot overwrite it.

### Cancelling a RUNNING task

With the policy `INCLUDE_RUNNING`, one statement does these steps in one transaction:

- It writes one attempt row for the run: outcome `CANCELLED`, `will_retry`
  false, the start time of the run, `finished_at` at the time of the cancel,
  error code `TASK_CANCELLED`, and `recorded_by` `CANCEL` in its phase record.
- It moves the task to history as `CANCELLED`, with that attempt in its
  attempt archive. Waiting result handles receive `TASK_CANCELLED` at once.
- It sends a notice on the channel `horsies_task_cancel` to the worker that
  holds the run. PostgreSQL sends the notice only when the cancel commits.

The worker then gives the cancel to the task code. The task code decides when
to stop:

| Execution and body | The task code gets | When the code does not stop |
|---|---|---|
| `ISOLATED`, async (Python `async def`, Rust `#[task]`) | Python: `asyncio.CancelledError` at its next `await`. Rust: `TaskRuntime::cancelled()` completes. Both: the cancel state of `TaskRuntime` | The code runs until it returns, or until its `timeout_ms` kill. With `cancel_grace_ms` on the task, the worker kills the child `cancel_grace_ms` after the cancel |
| `ISOLATED`, sync (Python `def`, Rust `#[blocking_task]`) | The cancel state of `TaskRuntime`: the code reads it | As for an async body |
| `SHARED_ASYNC` | As for an `ISOLATED` async body | After `cancel_grace_ms`, a second cancel and the cancel rounds, as at the deadline. A run that does not stop then keeps its seat; the worker kills the child only when no task in it has time left |
| `SHARED_THREAD` | The cancel state of `TaskRuntime` | The thread runs until it returns: a thread cannot be stopped |

- A cancel that comes after the start of the run and before the task code
  starts: the code does not run.
- What the code returns after the cancel is dropped. The task stays
  `CANCELLED`, and no result is stored. A result that reached the worker
  before the child got the cancel is dropped too: the row is in history.
- The side effects that the code made before it stopped stay.
- The seat or the child slot stays in use until the code stops.
- An `ISOLATED` task with no `cancel_grace_ms` is never killed for a cancel:
  the cancel is cooperative only. Its `timeout_ms` still kills it. A task
  with no `timeout_ms` and no `cancel_grace_ms` that does not read the cancel
  runs until it returns.
- When no worker holds the run (the worker stopped or crashed), nothing
  gets the notice. The attempt row is recorded in all cases.

How the worker gets the cancel:

- The notice comes on the listener connection of the worker, when the cancel
  commits.
- A notice can be lost: at a reconnect of the listener, or when the notice
  queue of the worker is full. The worker then reads the cancels of the runs
  that it holds, with one statement. It also does this read when a runner
  heartbeat of a run that it holds does not land. So a lost notice reaches
  the task code at most one `runner_heartbeat_interval_ms` (default 30 s)
  later, while the heartbeat connection of the worker works.

`finished_at` of the `CANCELLED` attempt is the time of the cancel: from then
on, no write of the run lands. The time when the code stopped, and how it
stopped, are in the `AttemptEnded` event, in the attempt span and in
`horsies.task.run.duration`. See
[Lifecycle Events](/horsies/observability/events/#the-end-of-an-attempt).

The task API is on the client pages:
[Cancellation](/horsies/tasks/defining-tasks/#cancellation) (Python) and
[Task Runtime](/horsies/rust/tasks/defining-tasks/#task-runtime) (Rust).

```bash
curl -X POST "$BASE/api/tasks/$TASK_ID/cancel" \
  -H 'Content-Type: application/json' \
  -H 'X-Horsies-Intent: action' \
  -d '{"policy": "INCLUDE_RUNNING"}'
```

The body key `policy` is `PENDING_OR_CLAIMED_ONLY` or `INCLUDE_RUNNING`. An
empty body, and a body with no `policy` key, cancel with
`PENDING_OR_CLAIMED_ONLY`: a `RUNNING` task is then refused with 409 like any
other ineligible state. The policy keeps a cancel of running code deliberate;
the UI gates it behind a checkbox. Python: `cancel_task(app, task_id,
policy=CancelPolicy.INCLUDE_RUNNING)`. Rust: `cancel_task(&app, task_id,
CancelPolicy::IncludeRunning)`. A key other than `policy` is refused with
422, and nothing changes (see [Request bodies](#request-bodies)).

**Settled when:** the task reads `CANCELLED` with its `CANCELLED` attempt. That
is immediate. The task code stops when it reads the cancel, or at the kill of an
`ISOLATED` task with `cancel_grace_ms`. Code that does not read the cancel can
run on up to its `timeout_ms`, and that is not a failure of the cancel.

## Task rerun

**The manual retry action is removed.** It reset a terminal row to `PENDING`
in place; a terminal record is now immutable and lives in the history
archive, so there is no row to reset. Re-execution is a **new request**:
rerun mints a new task with a new id and records its lineage back to the
source (`rerun_of_task_id`, and `rerun_root_task_id` for a chain).

| Source task | Outcome |
|---|---|
| `FAILED`, `CANCELLED`, `EXPIRED` with a retained rerun input | Enqueued: a new task, with its id |
| `COMPLETED` | Refused, not eligible (completed source): a succeeded request is not re-executed by replay |
| Still live (`PENDING`, `CLAIMED`, `RUNNING`) | Refused, source live: its own lifecycle owns retry |
| Input not retained, or does not decode | Refused, input unavailable (with the stored reason) or input corrupt |
| History row does not decode (for example a stored status that is not a task status) | Refused, source corrupt; nothing is written |
| Not found in live or history | Refused, source absent, with a `TaskAbsenceReason` that says whether retention can explain the absence |
| Row is a workflow node | Refused, not eligible (workflow task) |
| Idempotency key already used | Key replay (same request: the id of the task it enqueued) or key conflict (a different request under that key) |
| The stored `retry_for` names a custom code that the task does not declare in the calling app | Refused with the error `RETRY_CODE_NOT_DECLARED`; nothing is written |
| The stored `retry_for` names a custom code, and the calling app does not register the task | Refused with the error `TASK_NOT_REGISTERED`; nothing is written |

Whether a task can be rerun is decided **at enqueue**, not at rerun time: the
`retain_rerun_input_default` broker setting (with a per-task override)
decides whether the enqueue input is preserved with the record. A task
enqueued without a retained input cannot be rerun later.

Every outcome other than enqueued writes nothing.

The calling app supplies the declared codes of its tasks. The rerun checks
the stored retry list of the source against them: a built-in code passes for
every task, and a custom code passes when the app registers the task and the
task declares the code. A stored list of built-in codes only, or no retry
policy, needs no registered task.

The caller supplies the enqueue policy of the new request, because enqueue
policy lives in configuration, not in the rerun contract: the **new** request
gets its own retention class, its own rerun-input retention, and its own
key-reservation window. The caller also states the deadline of the new task,
or no deadline.

Each refusal is a distinct outcome, so a caller that matches over the
outcomes cannot miss one.

The API is on the client pages: [Rerun in Your Transaction](/horsies/operations/rerun-in-your-transaction/) (Python) and [Rerun in Your Transaction](/horsies/rust/operations/rerun-in-your-transaction/) (Rust).

**Settled when:** the call returns the new task id. The source record is
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

On the timeline, the pause is a band over every row, from the pause to the
resume or to the end of the run. A node whose claimed task the pause cancelled
shows `ready` from the move of that task to history until the resume; the
task of the resume is a new task of the node (`task 2` in the tooltip).

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
has still happened: `resume_workflow()` returns `WorkflowHandleResumeRecoveryFailed`,
and the response is 200 with `warning: "post_resume_recovery_failed"`. Treat
that as *resumed, check worker logs* — not as a failed resume. The worker
reaper runs the same recovery pass over all workflows. A database failure
before the commit is 503 or 500, by its cause (see
[Conflict handling](#conflict-handling)): the resume rolled back, or, on a
503 only, its commit outcome is unknown.

Resume does not clear a stale `error` left on the run by an `on_error=OnError.PAUSE`
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
  cancelled state; cancel expresses as `SKIPPED`. The cancel sets their
  `completed_at`: the timeline shows their `pending` or `ready` time up to
  the cancel, then the `skipped` marker.
- Backing tasks of enqueued nodes are cancelled, whether `PENDING`, `CLAIMED`,
  or `RUNNING`.
- **Executing nodes keep running to completion.** As with task cancel, no
  process is killed.

### Draining

A node that was executing when its run was cancelled is *draining*: it finishes,
its side effects happen, and its result does not advance the workflow. The UI
badges it `draining` rather than treating it as an error, because it is not one.
On the timeline, the task of a draining node keeps its segments after the end
of the run, and the time axis grows with them.

A draining node can persist indefinitely if the worker executing it crashed: the
reaper ignores nodes under non-running workflows, so nothing reclaims it. That
is a visible, explainable state, not a stuck action.

**Settled when:** the run reads `CANCELLED` and no node remains `PENDING`,
`READY`, or `ENQUEUED`. Nodes still `RUNNING` do not block this — draining is
the expected steady state after cancelling a run with work in flight.

## No workflow restart

There is no restart or retry action for a workflow run, because no such
primitive exists. `resend_start` repeats a failed *start call*, not a run. To
re-execute a workflow, send it again.

## What the reads see

Task reads resolve across both sides of the lifecycle: a task that has
terminalized is answered from the history archive, so detail, result, and
attempt history stay available after the row leaves the live table.

List and aggregate reads are **window-scoped**. They cover live rows plus a
bounded slice of terminal history: the last 24 hours by default, with
optional `since` / `until`. Bounds must be timezone-aware and increasing,
and the span may not exceed 30 days — a request over the maximum is refused
with the bound named, never silently clamped. The default and the maximum are
elapsed time: each bound is converted to a UTC instant first. Across a
daylight saving change, the wall-clock span and the elapsed span differ by the
change, and the elapsed span counts.

The page total follows a stated contract: **exact when any filter is
active, a planner estimate on the unfiltered view.** An unfiltered total is
therefore approximate by design on both sides of the split; narrow with any
filter to get an exact count.

## Request bodies

Each action route reads its JSON body before the path id and before any
statement of the action. The body is optional.

| Route | Body | Keys |
|---|---|---|
| `POST /api/tasks/{id}/cancel` | No body, or a JSON object | `policy`: `PENDING_OR_CLAIMED_ONLY` (the default) or `INCLUDE_RUNNING` |
| `POST /api/workflows/{id}/pause`, `/resume`, `/cancel` | No body, or `{}` | None |

- The route reads a body only when the `Content-Type` starts with
  `application/json`.
- A body over 1 MiB is refused with 413 before any other check of the body.
- A key that the route does not know is refused with 422, and the action does
  not run. The response names the first unknown key in body order, its value,
  and the known keys:

  ```json
  {"detail": [{
    "type": "extra_forbidden",
    "loc": ["body", "include_running"],
    "msg": "Unknown key \"include_running\". Known keys: \"policy\".",
    "input": true,
    "ctx": {"known_keys": ["policy"]}
  }]}
  ```

- A body that is not JSON, JSON that is not an object, and a known key with a
  value of the wrong type or a value that is not one of its texts are refused
  with 422 `value_error` at `["body"]`.

## Query parameters

A read route that takes query parameters refuses a parameter that it does not
know with 422, before it reads any parameter. A misspelled filter therefore
never reads as "not given". The response names the first unknown parameter in
query order, the value of its first occurrence, and the known parameters:

```json
{"detail": [{
  "type": "extra_forbidden",
  "loc": ["query", "retried_onyl"],
  "msg": "Unknown parameter \"retried_onyl\". Known parameters: \"since\", \"until\", \"task_name\", \"queue\", \"worker\", \"error_code\", \"error_category\", \"retried_only\".",
  "input": "true",
  "ctx": {"known_keys": ["since", "until", "task_name", "queue", "worker", "error_code", "error_category", "retried_only"]}
}]}
```

| Route | Known parameters |
|---|---|
| `GET /api/tasks` | `since`, `until`, `status`, `task_name`, `queue`, `worker`, `error_code`, `error_category`, `retried_only`, `sort_field`, `sort_direction`, `offset`, `limit` |
| `GET /api/tasks/status-counts` | `since`, `until`, `task_name`, `queue`, `worker`, `error_code`, `error_category`, `retried_only` |
| `GET /api/tasks/facets` | `since`, `until`, `status`, `error_category`, `retried_only` |
| `GET /api/tasks/breakdown` | `since`, `until`, `group_by`, `status`, `task_name`, `queue`, `worker`, `error_code`, `error_category`, `retried_only`, `limit` |
| `GET /api/workflows` | `workflow_name`, `status`, `limit` |
| `GET /api/workers/ping` | `timeout_seconds` |
| `GET /api/workers/{worker_id}/history` | `limit` |

A route that is not in the table reads no query parameter and ignores any.

## Conflict handling

There is no optimistic-concurrency token on any action. The CAS is the
concurrency control.

| Response | Meaning | What to do |
|---|---|---|
| 200 | The CAS matched and committed | Observe the entity |
| 400 | The row is a workflow node; task actions do not apply | Use the workflow actions |
| 400 | A task id in the path of a task route, or a workflow id in the path of a workflow action (`pause`, `resume`, `cancel`), is not a UUID (`INVALID_INPUT`, with a `detail`); nothing was read or changed | Send the id as UUID text |
| 404 | The row no longer exists. On the workflow read routes also a workflow id that is not a UUID, and a node index outside the 32-bit node index column; nothing was read | Retention may have removed it |
| 409 | Another change won the race, or the schema does not match this build | Re-read the entity; the body carries `current_status` |
| 413 | The request body is over 1 MiB; the action did not run | Send a smaller body |
| 422 | A query parameter that the route does not know (`extra_forbidden`, with the parameter and the known parameters; see [Query parameters](#query-parameters)). Or the request body is not valid: not JSON, not a JSON object, a known key with a value of the wrong type, or an unknown key (`extra_forbidden`, with the key and the known keys). The action did not run | Send only the keys of [Request bodies](#request-bodies) |
| 500 | Stored data that does not parse (`STORED_DATA_INVALID`): a stored value of the workflow run, or, on a task cancel, a history status that is not a task status. Or a database failure that stays (see below). Or a server defect | Check the server logs and re-read the entity. A repeat gets the same answer until the stored row, the schema or the connection settings change. For `STORED_DATA_INVALID`, nothing was committed |
| 503 | A database failure that may clear (see below) | The action may or may not have committed; re-read before retrying |

A failed database operation answers by the core's classification of its
cause: the classification that sets `retryable` on the error types of both
clients. The read routes answer by the same rule.

| Cause | Status |
|---|---|
| A connect that the network refused or that did not answer; a connection that closed; a pool checkout that timed out; a statement whose reply was lost (its outcome is unknown) | 503 |
| SQLSTATE class 08 (connection exception), 40 (serialization failure, deadlock), 53 (insufficient resources), 55 (object not in prerequisite state, for example 55P03 lock not available) or 57 (operator intervention, for example 57014 statement timeout) | 503 |
| Every other SQLSTATE class, also a class that this list does not name: for example 22 (data exception), 23 (integrity violation, for example 23514 a CHECK constraint), 28 (a password that the server refuses), 42 (syntax error or undefined object, for example a database with no horsies schema) and XX (internal error) | 500 |
| A connection setting that the core refuses; a server certificate that the client refuses; a broker with no listener (`/api/workers/ping`) | 500 |

A failed request is not proof that nothing happened. A network error in
particular leaves the outcome unknown — the request may have committed and the
response been lost. Re-read the entity before concluding the action failed. The
web UI does this automatically and reports success when it observes the effect.
