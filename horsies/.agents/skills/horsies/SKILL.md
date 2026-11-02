---
name: horsies
description: Guidance for the horsies Python task queue and workflow engine (PostgreSQL broker). Use when code has `import horsies`, `from horsies import ...`, `@app.task`, `TaskResult`, `TaskError`, `WorkflowSpec` or `WorkflowDefinition`, or when the user works on horsies tasks, workflows, configuration, scheduling, or monitoring.
---

# horsies — Quick Reference

PostgreSQL-backed background task queue and workflow engine for Python.

This is an **introductory quick reference** — it covers core concepts and
patterns at a glance. For production-level guidance, see the dedicated
reference files in this skill folder (open one only when the task needs it):

| File | When to open |
|---|---|
| `tasks.md` | `@app.task`, `TaskResult`, `send()`, `with_options()`, `RetryPolicy`, `ExceptionMapper`, serialization |
| `workflows.md` | `WorkflowSpec`, `TaskNode`, `WorkflowHandle`, DAG construction, failure semantics |
| `configs.md` | `AppConfig`, `PostgresConfig`, queues, recovery, recurring scheduling including `CronSchedule`, CLI commands |
| `monitoring.md` | `horsies.monitoring` query API, `cancel_task`, the `horsies[web]` dashboard, `create_monitoring_app`, `horsies web`, auth policies, schema states |

All public symbols: `from horsies import <name>`

For the full API reference, run `horsies get-docs` to download the docs of the installed version,
or read `llms.txt` on the docs site:
`https://suleymanozkeskin.github.io/horsies/llms.txt`.

## Deployment facts

- Use a dedicated Postgres instance for the broker. Ownership, finalization, and workflow transitions are transactional state changes in that database.
- Pre-1.0 is an API-contract statement: breaking API changes may land in minor releases (see CHANGELOG).
- The Q&A page of the docs explains how to describe the library: `questions-and-answers`.

Honest boundary: fire-hose throughput and streaming fan-out belong to
Redis/Kafka-class systems. Below that ceiling, throughput scales with the
Postgres instance. Reference shape: separate machines, same region,
entry-tier managed Postgres, ~2.5M statements/day — no top-20 statement
exceeds p99 2 ms (claim function: p50 1 ms / p99 2 ms at 150k claims/day).

Note that these are measurements at a reference shape on an entry-tier instance, not throughput limits; headroom scales with the instance tier, and per-statement latency stays flat until the instance saturates.


## Define a Task

```python
from pydantic import SecretStr

from horsies import Horsies, AppConfig, PostgresConfig, TaskResult, TaskError

app = Horsies(config=AppConfig(
    broker=PostgresConfig(database_url=SecretStr("postgresql+psycopg://...")),
))

@app.task("add_numbers")
def add_numbers(*, a: int, b: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=a + b)
```

Every task must return `TaskResult[T, TaskError]`.
Task functions are sync (`def`). `@app.task()` refuses an `async def` function with `HRS-106`. Async task support is not available; use `.send_async()` and `.get_async()` on the producer side.
Use `TaskResult(ok=value)` for success, `TaskResult(err=TaskError(...))` for failure.

Register task modules for worker discovery:

```python
app.discover_tasks(["myapp.tasks", "myapp.jobs.tasks"])
```

Only records paths — actual imports happen when the worker starts.

## Send a Task

`send()` returns `TaskSendResult[TaskHandle[T]]` — always handle both branches:

```python
from horsies import Ok, Err

match add_numbers.send(a=5, b=3):
    case Ok(handle):
        result = handle.get(timeout_ms=5000)
    case Err(send_err):
        print(f"Send failed: {send_err.code} - {send_err.message}")
```

Async: `send_async()` / `handle.get_async()`. Inside a coroutine or a Jupyter cell, use only the `*_async` forms: every sync call there returns `ASYNC_CONTEXT` and runs nothing.
Delayed: `add_numbers.schedule(60, a=5, b=3)` dispatches after 60 seconds.
Deadline: `add_numbers.with_options(good_until=deadline).send(a=5, b=3)` expires if not executed in time.

### Retry a Failed Send

`TaskSendError` is a union with one variant per code; match on the variant. Only `TaskSendEnqueueFailed` has `retryable` (`True` for a transient cause). The retry methods take a `TaskSendRetryable` (`TaskSendEnqueueFailed`, `TaskSendAsyncContext`, `TaskSendIdempotencyKeyConflict`, `TaskSendLoopRunnerFailed`); another variant is a type error and fails an assertion at run time. For a key conflict, `owner_task_id` names the task that holds the key; a retry is accepted after the reservation ends:

<!-- check: skip partial -- uses names that the page does not define -->
```python
match my_task.send(**kwargs):
    case Ok(handle):
        ...
    case Err(TaskSendEnqueueFailed(retryable=True) as err):
        match my_task.retry_send(err):
            case Ok(handle):
                ...
            case Err(retry_err):
                ...  # permanent failure
    case Err(err):
        ...  # permanent failure
```

`retry_send` / `retry_send_async` replay the exact stored payload (SHA-verified).

## Define a Workflow

Workflows are DAGs of tasks. Two approaches:

### Functional — `app.workflow()` with `.node()`

`.node()` returns a `NodeFactory`; the second `()` call passes task kwargs.
First call sets workflow options, second call sets task arguments:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import OnError, from_node

fetch = fetch_data.node()()
process = process_data.node()(data=from_node(fetch))
save = save_result.node()(result=from_node(process))

spec = app.workflow(
    name="etl_pipeline",
    tasks=[fetch, process, save],
    definition_key="myapp.etl_pipeline.v1",
    on_error=OnError.FAIL,
    output=save,
)
```

Best for dynamic workflows where node kwargs depend on runtime inputs.
`definition_key` is required for top-level workflows built with `app.workflow()` and is used for persistence/runtime identity.

### Class-based — `WorkflowDefinition`

Nodes are class attributes; `node_id` is auto-assigned from the attribute name:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import TaskNode, WorkflowDefinition, WorkflowDefinitionConfig, OnError

class ETLPipeline(WorkflowDefinition[SaveResult]):
    name = "etl_pipeline"
    definition_key = "myapp.etl_pipeline.v1"

    fetch = TaskNode(fn=fetch_data)
    process = TaskNode(fn=process_data, waits_for=[fetch], args_from={"data": fetch})
    save = TaskNode(fn=save_result, waits_for=[process], args_from={"result": process})

    config = WorkflowDefinitionConfig(
        output=save,
        on_error=OnError.FAIL,
    )

spec = ETLPipeline.build(app)
```

`config = WorkflowDefinitionConfig(...)` sets `output`, `on_error` and `success_policy`, and can name the nodes of the same class body. A `Meta` attribute raises HRS-033 when the class is created.

Best for static, reusable DAGs. Use `build_with(app, **params)` for parameterized builds.

### Node options

`waits_for`, `args_from`, `workflow_ctx_from`, `queue`, `priority`,
`allow_failed_deps`, `join` (`"all"` | `"any"` | `"quorum"`),
`min_success`, `good_until`, `node_id`.

## Start a Workflow

`start()` returns `WorkflowStartResult[WorkflowHandle[T]]`:

```python
from horsies import Ok, Err

match spec.start():
    case Ok(handle):
        status = handle.status()
        result = handle.get(timeout_ms=30000)
    case Err(start_err):
        print(f"Start failed: {start_err.code}")
```

Async: `spec.start_async()`.

### Retry a Failed Start

`retry_start` takes `WorkflowStartRetryable`: `WorkflowStartEnqueueFailed`, `WorkflowStartAsyncContext` or `WorkflowStartLoopRunnerFailed`. It starts again with the error's `workflow_id`:

```python
match spec.start():
    case Ok(handle):
        ...
    case Err(WorkflowStartEnqueueFailed(retryable=True) as err):
        match spec.retry_start(err):
            case Ok(handle):
                ...
            case Err(retry_err):
                ...
    case Err(err):
        ...
```

`retry_start` is best-effort idempotent by `workflow_id` (not payload-verified).

### Auto-retry ( this is not execution retry, only for sending tasks / starting workflows, for task execution retry see `RetryPolicy` )

Set `resend_on_transient_err=True` on `AppConfig` to auto-retry transient
`ENQUEUE_FAILED` errors (up to 3 times, exponential backoff) for both
task sends and workflow starts.

## WorkflowHandle

<!-- check: skip partial -- uses names that the page does not define -->
```python
handle.status()          # HandleResult[WorkflowStatus] — unwrap before use
handle.get(timeout_ms=N) # TaskResult (blocks for output node)
handle.results()         # HandleResult[dict[str, TaskResult]] — unwrap before use
handle.tasks()           # HandleResult[list[WorkflowTaskInfo]] — unwrap before use
handle.cancel()          # cancel workflow
handle.pause()           # pause (RUNNING -> PAUSED)
handle.resume()          # resume (PAUSED -> RUNNING)
```

All methods have `_async()` variants.

### Reconnect to an Existing Workflow

```python
from horsies import WorkflowHandle

handle = WorkflowHandle(workflow_id="known-uuid", broker=app.get_broker())
result = handle.get(timeout_ms=30_000)
```

### Direct Result Access by Task ID

Fetch a task result by ID without a `TaskHandle` (e.g. in HTTP endpoints). Use
the app-level typed API — `broker.get_result(_async)` was removed in 0.1.2.

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
from horsies.core.types.result import is_err

# Returns BrokerResult[TaskResult[Any, TaskError]] — unwrap the outer result.
outer = await app.get_result_async("task-uuid", timeout_ms=5000)
if is_err(outer):
    ...  # infrastructure failure (INVALID_JSON_PAYLOAD / NO_TYPE_AVAILABLE)
else:
    task_result = outer.ok_value  # typed TaskResult[Any, TaskError]

# Task metadata with typed-decoded result: the app-level wrapper decodes
# when the task is registered locally (decoded_result / result_decoded set).
info = await app.get_task_info_async("task-uuid", include_result=True)

# Broker-level variants are RAW envelope only — decoded_result is always
# empty there. Reach for them for cross-process monitoring/dashboards:
# app.get_broker().get_task_info_async(...) or
# broker.get_raw_result_record_async(...)
```

Result reads resolve across both sides of the lifecycle: a task that has
terminalized is answered from the task-history archive, so
`get_result`/`get_task_info` keep working after the row leaves the live
table.

See `configs.md` for all broker methods. See website docs `monitoring/broker-methods` for full reference.

### Re-executing a Terminal Task

A terminal record is immutable. Manual in-place retry is removed — re-execution
is a **new request** with a new id and recorded lineage:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from datetime import timedelta

from horsies import (
    rerun_task, RerunTask, RerunEnqueuePolicy,
    RerunEnqueued, RerunSourceLive, RerunSourceAbsent, RerunSourceCorrupt,
    RerunNotEligible, RerunInputUnavailable, RerunInputCorrupt,
    RerunKeyConflict, RerunKeyReplay,
)

outcome = await rerun_task(
    connection,
    RerunTask(source_task_id=task_id, deadline=None),
    RerunEnqueuePolicy(
        # 'standard_30d', 'forever', a class declared in
        # AppConfig.retention.retention_classes, or one derived from
        # AppConfig.retention.queue_retention (q_<queue>_<duration>). A rerun
        # states its class outright -- there is no queue mapping to fall back
        # on here. The rerun path checks the key against the registry and
        # refuses an unregistered one.
        retention_class_key='standard_30d',
        retain_rerun_input=True,
        reservation_window=timedelta(hours=24),
    ),
)
match outcome:
    case RerunEnqueued(new_task_id=new_id):
        ...
    case RerunSourceLive() | RerunSourceAbsent() | RerunNotEligible():
        ...
    case RerunInputUnavailable() | RerunInputCorrupt():
        ...
    case RerunSourceCorrupt(exception=cause):
        ...  # the source history row does not decode; nothing was written
    case RerunKeyConflict() | RerunKeyReplay():
        ...
```

`RerunOutcome` is an exhaustive union — match it exhaustively rather than
checking one variant. Eligibility is decided **at enqueue** by
`retain_rerun_input_default` (per-task override available): a task whose input
was not retained cannot be rerun. A `COMPLETED` source is not eligible.
`RerunSourceAbsent.reason` is a `TaskAbsenceReason`: it says whether
retention can explain that no record has the id (see `monitoring.md`).

## Result Types

| Operation | Result type | Ok | Err |
|---|---|---|---|
| Task execution | `TaskResult[T, TaskError]` | value `T` | `TaskError` |
| `send()` / `schedule()` | `TaskSendResult[TaskHandle[T]]` | `TaskHandle` | `TaskSendError` |
| `start()` | `WorkflowStartResult[WorkflowHandle[T]]` | `WorkflowHandle` | `WorkflowStartError` |
| Broker infra | `BrokerResult[T]` | value `T` | `BrokerOperationError` |
| Handle ops | `HandleResult[T]` | value `T` | `HandleOperationError` |

Use `is_ok(result)` / `is_err(result)` type guards, or `match Ok / Err`.

### Error Code Enums

- `TaskSendErrorCode`: `SEND_SUPPRESSED`, `ASYNC_CONTEXT`, `VALIDATION_FAILED`, `ENQUEUE_FAILED`, `PAYLOAD_MISMATCH`, `IDEMPOTENCY_KEY_CONFLICT`, `PAYLOAD_TOO_LARGE`, `EVENT_LOOP_LIMIT`, `LOOP_RUNNER_FAILED`
- `WorkflowStartErrorCode`: `BROKER_NOT_CONFIGURED`, `VALIDATION_FAILED`, `WORKFLOW_ID_CONFLICT`, `ENQUEUE_FAILED`, `LOOP_RUNNER_FAILED`, `EVENT_LOOP_LIMIT`, `ASYNC_CONTEXT`
- `HandleErrorCode`: `WORKFLOW_NOT_FOUND`, `DB_OPERATION_FAILED`, `STORED_DATA_INVALID`, `RESUME_RECOVERY_FAILED`, `LOOP_RUNNER_FAILED`, `EVENT_LOOP_LIMIT`, `ASYNC_CONTEXT`

`retryable=True` means transient (DB connection blip). `retryable=False` means permanent. On send errors only `TaskSendEnqueueFailed` has `retryable`.

`HandleOperationError` is a union with one variant per `HandleErrorCode` member (`HandleWorkflowNotFound`, `HandleDbOperationFailed`, `HandleStoredDataInvalid`, `HandleResumeRecoveryFailed`, `HandleLoopRunnerFailed`, `HandleEventLoopLimit`, `HandleAsyncContext`); match on the variant. `HandleResumeRecoveryFailed` means `resume()` committed and only its recovery pass failed: the workflow is `RUNNING`.

`EVENT_LOOP_LIMIT` (every catalog above, `BrokerErrorCode`, `RetrievalCode`, monitoring codes): a broker keeps one resource set per open event loop, up to 5 loops plus its sync runner loop. The next open loop gets this error and nothing runs; it clears when one of those loops closes.

`ASYNC_CONTEXT` (every catalog above, `BrokerErrorCode`, `RetrievalCode`): a sync call was made on a thread that runs an event loop (a coroutine, a Jupyter cell). Its wait would stall that loop, so nothing ran. Call the `*_async` form. `retryable=False` where the error has the field; `RetrievalCode.ASYNC_CONTEXT` is not cached; `await spec.retry_start_async(err)` completes a start that returned it. `app.check(live=True)` there reports `HRS-217` (`CHECK_LIVE_ASYNC_CONTEXT`).

## CLI

```bash
horsies worker myapp.config:app          # start worker
horsies scheduler myapp.config:app       # start scheduler
horsies check myapp.config:app [--live]  # validate before deploy
horsies web myapp.config:app             # serve the monitoring web UI (needs horsies[web])
horsies web --database-url URL           # monitor a database without an app module
horsies get-docs                         # download the docs of the installed version
horsies get-docs --ref main              # download the main branch docs (needed for a development version)
horsies skills install --target DIR      # copy this skill folder to DIR/horsies/ (--force replaces a changed copy)

# Recycle each child after N tasks to bound memory (default 100, per-child,
# staggered). Tune per deployment; 0 disables.
horsies worker myapp.config:app --max-tasks-per-child=500
```

`horsies check` runs phased validation (config, imports, DAG, builders, policies,
optional live DB check). Worker and scheduler also run check at startup.

Two knobs recycle worker children to bound memory, with **OR** semantics (a
child exits when either limit is reached). `--max-tasks-per-child` (default
`100`) recycles after N tasks; `--max-memory-per-child-mb` (default
off) recycles once a child's own RSS reaches N MB after a task — the
primary guard for memory-quota deployments, since it maps to the RSS the quota
charges. A threshold at or below the warmed child baseline fails startup.
Recommended default: memory recycling on, count high as a
backstop, tuned against the `children_memory_mb` field on `WorkerStateSnapshot`.
See `configs.md` and website docs `workers/concurrency`.
