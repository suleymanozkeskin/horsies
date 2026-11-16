---
title: Action Semantics
summary: State rules and failure responses for monitoring actions.
related: [./web-ui-overview, ./web-ui-deployment, ../tasks/retrieving-results]
tags: [monitoring, actions, tasks, workflows]
---

The dashboard can cancel tasks and pause, resume, or cancel workflows. Each
action uses the same authorization and schema guards.

## Common guards

An action runs only when all conditions pass:

1. The authorization policy permits the request to view the dashboard.
2. The deployment enables actions.
3. The authorization policy permits actions.
4. The request includes `X-Horsies-Intent: action`.
5. The database schema matches this Horsies build.
6. The task-history cutover attestation exists.

A missing or wrong intent header returns HTTP 403. An incompatible schema
returns HTTP 409 with `SCHEMA_INCOMPATIBLE`. A failed schema probe with no
cached result returns HTTP 409 with `SCHEMA_UNKNOWN`.

## Task cancellation

The cancel endpoint is:

```text
POST /api/tasks/{task_id}/cancel
```

The request body contains `include_running`.

| Current state | `include_running=false` | `include_running=true` |
| --- | --- | --- |
| `PENDING` | Cancel | Cancel |
| `CLAIMED` | Cancel | Cancel |
| `RUNNING` | Refuse | Cancel |
| Terminal history row | Refuse | Refuse |
| Missing task | Not found | Not found |

Cancellation moves the task to retained history in the terminalization
transaction. It does not leave a terminal row in `horsies_tasks`.

The committed `CANCELLED` state is final. Claim, finalize, automatic retry,
and recovery transitions use guarded status updates. They cannot overwrite it.

A workflow-bound task returns HTTP 400 with `TASK_IS_WORKFLOW_TASK`. Use a
workflow action instead.

A state conflict returns HTTP 409 with `TASK_NOT_CANCELLABLE` and the current
status when it can be determined. A missing task returns HTTP 404. A database
failure returns HTTP 503.

### Cancelling a running task

Horsies does not kill the worker process. The task function keeps running until
it returns. Its database, network, and file side effects still happen.

The function result is discarded. Finalize cannot replace the committed
`CANCELLED` state. No attempt row is recorded for that execution. The worker
pool slot remains occupied until the function returns.

`include_running` makes these effects an explicit choice. The action settles
when the task reads `CANCELLED`. The task function can keep running after that
point.

## Workflow pause

The pause endpoint is:

```text
POST /api/workflows/{workflow_id}/pause
```

Only a running workflow can win the pause transition. The operation pauses the
workflow and its running descendants.

Pause also rewinds claimed work that has not started. Each matching backing
task moves to retained history as `CANCELLED`. Its workflow node returns to
`READY` and clears the old task ID. Resume creates a fresh backing task.

Nodes that are already executing keep running. Their side effects still
happen. New dependent nodes are not scheduled while the workflow is paused.
Unclaimed enqueued rows remain claimable. A worker releases them during its
post-claim workflow check.

The workflow status changes immediately. Running nodes then drain. The pause
action does not kill or wait for their user code.

A lost state race returns HTTP 409 with `STATE_CONFLICT` and the current
workflow status. A missing workflow returns HTTP 404.

## Workflow resume

The resume endpoint is:

```text
POST /api/workflows/{workflow_id}/resume
```

Resume normally changes a paused workflow to `RUNNING`. It also supports an
idempotent recovery call for a workflow that is already `RUNNING`.

| Current state | Result |
| --- | --- |
| `PAUSED` | Resume and return success |
| `RUNNING` with stranded children or ready nodes | Repair the stranded work and return success |
| Consistent `RUNNING` | Refuse with `STATE_CONFLICT` |
| `PENDING`, `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED` | Refuse with `STATE_CONFLICT` |

Resume starts paused descendants. It re-evaluates pending nodes and creates
fresh backing tasks for ready nodes. It also checks whether the workflow can
complete.

Resume does not restore old claimed rows. It does not clear an error stored by
an `on_error="pause"` policy.

A committed resume can return this warning:

```text
post_resume_recovery_failed
```

The warning means the workflow is already `RUNNING`, but a recovery step
failed. The state change can have committed in this request, or an earlier
request can have committed it. The response still reports
`outcome="resumed"`.

Standalone database-URL mode has no compiled workflow registry. Mount the
router with the application when resume must resolve registered workflow nodes
or `args_from` links.

## Workflow cancellation

The cancel endpoint is:

```text
POST /api/workflows/{workflow_id}/cancel
```

| Current state | Result |
| --- | --- |
| `PENDING` | Cancel |
| `RUNNING` | Cancel |
| `PAUSED` | Cancel |
| `CANCELLED` | Return success without changing it |
| `COMPLETED`, `FAILED`, `EXPIRED` | Refuse with `STATE_CONFLICT` |

Cancellation cascades to every non-terminal descendant workflow. Pending and
ready nodes become `SKIPPED`. Their `completed_at` value remains null.

Backing tasks for enqueued nodes move to retained history as `CANCELLED`.
Executing nodes keep running. Their side effects still happen. Their results
do not advance the cancelled workflow.

An executing node is draining after workflow cancellation. The workflow can
already read `CANCELLED` while that node is still running. A crashed worker can
leave the draining node visible because recovery does not reclaim work below a
non-running workflow.

The cancel action settles when the workflow reads `CANCELLED` and no node is
`PENDING`, `READY`, or `ENQUEUED`. A `RUNNING` node can remain while it drains.

`COMPLETED`, `FAILED`, `CANCELLED`, and `EXPIRED` are terminal workflow states.
They cannot be resumed or paused.

## Success body

All four endpoints return the same success shape.

```json
{
  "outcome": "cancelled",
  "was_status": "PENDING",
  "next_attempt_number": null,
  "warning": null
}
```

Unused fields are `null`. Workflow actions do not return a task retry number.

## Refusal summary

| Condition | HTTP status | Body field |
| --- | --- | --- |
| Malformed task action UUID | 404 | `detail` |
| Malformed workflow action UUID | 503 | `detail` |
| Workflow-bound task cancel | 400 | `code=TASK_IS_WORKFLOW_TASK` |
| Not authorized | 403 | `detail` |
| Missing intent header | 403 | `detail` |
| Task or workflow missing | 404 | `detail` |
| Task state conflict | 409 | `code=TASK_NOT_CANCELLABLE` |
| Workflow state conflict | 409 | `code=STATE_CONFLICT` |
| Schema mismatch, absence, or incomplete cutover | 409 | `code=SCHEMA_INCOMPATIBLE` |
| Schema probe unavailable | 409 | `code=SCHEMA_UNKNOWN` |
| Database or internal operation failed | 503 | `detail` |

The task malformed-ID response states that the task does not exist. The
workflow malformed-ID response states that the workflow identity is invalid.

A transport error leaves the commit outcome unknown. Re-read the entity before
retrying an action.

## Rerun outside the dashboard

Manual retry is not part of the monitoring API. Use `horsies::rerun_task` for
an eligible retained task.

The rerun operation creates a new UUID. It records the source and root lineage.
It never mutates the retained source row.

The dashboard does not expose a rerun button or endpoint.
