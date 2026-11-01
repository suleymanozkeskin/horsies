---
title: Deploying Code Changes
summary: What happens to stored task rows and workflows when task signatures, task names or return types change, and the order of a safe deploy.
related: [../workers/worker-architecture, ../tasks/errors, ../internals/serialization, ../concepts/workflows/subworkflows, ../concepts/workflows/workflow-api]
tags: [operations, deploy, rolling-deploy, rename, WORKER_SERIALIZATION_ERROR, WORKER_RESOLUTION_ERROR, SUBWORKFLOW_LOAD_FAILED, RESULT_DESERIALIZATION_ERROR]
---

# Deploying Code Changes

A task row stores the task name and the encoded kwargs. It does not store the code. The worker that claims the row resolves the task name and decodes the kwargs against the code that it has loaded at that time. The same rule holds for old and new workers that run side by side during a deploy.

## Why and When

Read this page before a deploy that changes any of these:

- a task name
- a task parameter (added, removed, renamed or retyped)
- the return type of a task
- a workflow definition or a `build_with` signature

Rows that exist before the deploy are not changed by it. They run with the code that is loaded when they are claimed.

## Which Code Reads Which Stored Data

| Stored data | Read against | When |
| ----------- | ------------ | ---- |
| Task name of a task row | The task registry of the claiming worker | At claim and execution |
| Kwargs of a task row | The current signature of the task | At execution |
| Workflow DAG (nodes, edges, node kwargs) | Nothing. The DAG is stored when the workflow starts and is not rebuilt | - |
| Subworkflow definition | The workflow registry, by `definition_key`. The child is built from the current code | When the subworkflow node becomes ready |
| Stored result of an upstream task | The current return type of the upstream task | When a later node, a workflow context or a handle reads it |

## Failure Modes

| Change after the row was stored | Error code | Where it appears |
| ------------------------------- | ---------- | ---------------- |
| A parameter was removed or renamed. The row has a kwarg that the signature does not declare | `WORKER_SERIALIZATION_ERROR` | The result of the task. The task function does not run |
| A parameter changed type, and the stored value does not fit the new type | `WORKER_SERIALIZATION_ERROR` | The result of the task. The task function does not run |
| A required parameter was added. The row has no value for it | `UNHANDLED_EXCEPTION` | The result of the task. Python raises `TypeError` at the call. This is the default `default_unhandled_error_code` |
| The task name is not registered in the worker | `WORKER_RESOLUTION_ERROR` | The result of the task. The message names the task |
| A subworkflow `definition_key` is not registered | `SUBWORKFLOW_LOAD_FAILED` | The subworkflow node ends `FAILED`, and failure handling of the parent runs |
| The kwargs of a subworkflow node do not fit the current `build_with` signature | `WORKER_SERIALIZATION_ERROR` | The subworkflow node ends `FAILED` |
| The return type of an upstream task changed, and a stored result does not fit | `RESULT_DESERIALIZATION_ERROR` | A `TaskResult` with this error replaces the stored result where it is read: downstream nodes, `WorkflowContext` and handles |
| An upstream task name was removed or renamed | `RESULT_DESERIALIZATION_ERROR` | Same place. The error data has `stage: 'source_not_registered'` |

An old worker follows the same rules. During a rolling deploy, a new producer can send a row that an old worker claims. An added kwarg that the old signature does not declare gives `WORKER_SERIALIZATION_ERROR` on the old worker.

## How To

### Add a Parameter

1. Add the parameter with a default value. Deploy all workers.
2. Deploy the producers that send the new kwarg.

A default value lets old rows decode and run. A required parameter fails every old row with `UNHANDLED_EXCEPTION`.

```python
from horsies import TaskError, TaskResult

from myapp.instance import app


@app.task('send_email')
def send_email(
    *,
    to: str,
    subject: str,
    locale: str = 'en',
) -> TaskResult[None, TaskError]:
    return TaskResult(ok=None)
```

### Remove or Rename a Parameter

1. Deploy the producers so that they stop sending the old kwarg.
2. Wait until no live row carries it (see [Check for Remaining Rows](#check-for-remaining-rows)).
3. Deploy the workers with the new signature.

A rename is an add and a remove. Add the new parameter with a default, move the producers, drain, then remove the old parameter.

### Rename or Remove a Task

Keep the old name registered until no row uses it. Register two tasks that call the same function body.

```python
from horsies import TaskError, TaskResult

from myapp.instance import app


def _deliver_report(*, report_id: str) -> TaskResult[None, TaskError]:
    return TaskResult(ok=None)


@app.task('build_report')
def build_report(*, report_id: str) -> TaskResult[None, TaskError]:
    return _deliver_report(report_id=report_id)


# Old name. Remove it after no row with this name remains.
@app.task('make_report')
def make_report(*, report_id: str) -> TaskResult[None, TaskError]:
    return _deliver_report(report_id=report_id)
```

1. Deploy workers that register both names.
2. Deploy producers that send the new name.
3. Wait until no row has the old name.
4. Remove the old task.

A scheduled task (see [Schedule Config](../scheduling/schedule-config)) sends by name. Change the schedule to the new name in step 2.

### Change a Return Type

A task result that a workflow reads later is decoded with the type that the code declares at read time. Keep the new type able to decode the old stored shape, or wait until no workflow holds a stored result of the old shape. Add new fields with defaults.

### Change a Workflow Definition

A started workflow keeps its stored DAG. Changing the class does not change a running workflow.

- Give a new shape a new `definition_key`, for example `myapp.pipeline.v2`.
- Keep the old `definition_key` registered for as long as a running workflow can start a subworkflow with it.
- A subworkflow node that is not ready yet is built from the current code when it becomes ready. If the `build_with` signature changed, its stored kwargs can fail to decode.

`retry_start` is best-effort idempotent by `workflow_id` and does not compare the spec. After a deploy, it can return the handle of a workflow that has the old shape. See [Workflow API](../concepts/workflows/workflow-api).

### Check for Remaining Rows

The live table holds only live rows. Query it by task name:

```sql
SELECT task_name, status, count(*)
FROM horsies_tasks
WHERE task_name = 'make_report'
GROUP BY task_name, status;
```

Workflow nodes that are not enqueued yet are in `horsies_workflow_tasks` with status `PENDING` or `READY`. They read the code when they are enqueued:

```sql
SELECT task_name, status, count(*)
FROM horsies_workflow_tasks
WHERE status IN ('PENDING', 'READY')
GROUP BY task_name, status;
```

A `PAUSED` workflow keeps its unstarted nodes until it resumes or ends.

## Worker Shutdown

On SIGTERM or SIGINT a worker stops new claims and waits for running tasks. See [Worker Architecture](../workers/worker-architecture). A task that is already running keeps the code that it loaded. A row that is still `PENDING` is claimed later by any worker, with the code of that worker.

## Things to Avoid

**Don't add a required parameter in one step.** Old rows have no value for it, and each one fails with `UNHANDLED_EXCEPTION`.

**Don't remove a task name while a row or a schedule uses it.** Each row fails with `WORKER_RESOLUTION_ERROR`.

**Don't deploy a new producer before the workers when the producer sends a new kwarg.** An old worker rejects the kwarg with `WORKER_SERIALIZATION_ERROR`.
