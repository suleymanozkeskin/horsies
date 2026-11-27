---
name: horsies
description: Guidance for the horsies Python task queue and workflow engine (PostgreSQL broker). Use when code has `import horsies`, `from horsies import ...`, `@app.task`, `TaskError`, `TaskOutcome`, `ReadResult`, `WorkflowSpec` or `WorkflowDefinition`, or when the user works on horsies tasks, workflows, configuration, scheduling, or monitoring.
---

# horsies — Quick Reference

PostgreSQL-backed background task queue and workflow engine for Python.

This is an **introductory quick reference** — it covers core concepts and
patterns at a glance. For production-level guidance, see the dedicated
reference files in this skill folder (open one only when the task needs it):

| File | When to open |
|---|---|
| `tasks.md` | `@app.task`, `TaskRuntime` and the fencing token, `Result[T, TaskError]`, `ReadResult` and `ReadError`, `send()`, `with_options()`, `RetryPolicy`, `ExceptionMapper`, serialization, the cancel of a running task (`TaskRuntime.cancel_state()`) |
| `workflows.md` | `WorkflowSpec`, `TaskNode`, `WorkflowHandle`, DAG construction, failure semantics |
| `configs.md` | `AppConfig`, `PostgresConfig`, queues, recovery, recurring scheduling including `CronSchedule`, child hooks, event handlers (`@app.on_event`), CLI commands |
| `monitoring.md` | `horsies.monitoring` query API, `cancel_task`, the `horsies[web]` dashboard, `create_monitoring_app`, `horsies web`, auth policies, schema states |

All public symbols: `from horsies import <name>`

For the full API reference, run `horsies get-docs` to download the docs of the installed version,
or read `llms.txt` on the docs site:
`https://suleymanozkeskin.github.io/horsies/llms.txt`.

## Deployment facts

- Use a dedicated Postgres instance for the broker. Ownership, finalization, and workflow transitions are transactional state changes in that database.
- A failover under asynchronous replication can lose committed sends, claims, task ends and workflow progress, so a task body can run again. Use synchronous replication for the horsies instance, or make each effect outside horsies idempotent. The reasons, the connections of each URL and the failover limits are on the docs page `operations/deployment-topology`.
- Pre-1.0 is an API-contract statement: breaking API changes may land in minor releases (see CHANGELOG).
- The Q&A page of the docs explains how to describe the library: `questions-and-answers`.
- Changing a model that a task takes or returns: add a field with a default; rename with `Field(validation_alias=AliasChoices('new_name', 'old_name'))`, the new name first; never rename a field and give it a default with no alias, because old rows then read the default with no error. Stored data is read strictly. Unknown keys are ignored on read and refused at send.

Honest boundary: fire-hose throughput and streaming fan-out belong to
Redis/Kafka-class systems. Below that ceiling, throughput scales with the
Postgres instance. Reference shape: separate machines, same region,
entry-tier managed Postgres, ~2.5M statements/day — no top-20 statement
exceeds p99 2 ms (claim function: p50 1 ms / p99 2 ms at 150k claims/day).

Note that these are measurements at a reference shape on an entry-tier instance, not throughput limits; headroom scales with the instance tier, and per-statement latency stays flat until the instance saturates.


## Define a Task

```python
from pydantic import SecretStr

from horsies import Horsies, AppConfig, PostgresConfig, Ok, Result, TaskError

app = Horsies(config=AppConfig(
    database=PostgresConfig(database_url=SecretStr("postgresql://...")),
))

@app.task("add_numbers")
def add_numbers(*, a: int, b: int) -> Result[int, TaskError]:
    return Ok(a + b)
```

Every task must return `Result[T, TaskError]`.
A task function is `def` or `async def`. With `execution=Execution.ISOLATED` (the default), a worker child runs one task at a time, and an `async def` task runs on the one event loop that the child keeps. `Execution.SHARED_ASYNC` (`async def`) and `Execution.SHARED_THREAD` (`def`) run many tasks in one child, as coroutines or on threads, and need `timeout_ms`. See `tasks.md`, Shared async tasks and Shared thread tasks, and the docs page `workers/execution-modes` to choose. In an `async def` task, send with `.send_async()`: the sync `.send()` returns `Err(TaskSendAsyncContext)` in a running event loop. See `tasks.md`, Async tasks.
Return `Ok(value)` for success and `Err(TaskError(...))` for failure.

Register task modules for worker discovery:

```python
app.discover_tasks(["myapp.tasks", "myapp.jobs.tasks"])
```

Only records paths — actual imports happen when the worker starts.

## Send a Task

`send()` returns `TaskSendResult[TaskHandle[T]]` — always handle both branches:

```python
from horsies import Err, Failed, Ok, Succeeded

match add_numbers.send(a=5, b=3):
    case Ok(handle):
        match handle.get(wait_ms=5000):
            case Ok(Succeeded(total)):
                print(f"Sum: {total}")
            case Ok(Failed(task_error)):
                print(f"Task failed: {task_error.code} - {task_error.message}")
            case Err(read_error):
                print(f"Read failed: {read_error.code} - {read_error.message}")
    case Err(send_err):
        print(f"Send failed: {send_err.code} - {send_err.message}")
```

`handle.get()` returns `ReadResult[T, TaskError]`. `Ok(Succeeded(value))` holds the value of the task. `Ok(Failed(task_error))` holds the error that the task returned or that the library recorded (`TASK_TIMEOUT`, `WORKER_CRASHED`, ...). `Err(read_error)` means the read gave no outcome: `read_error` is one class of the `ReadError` union, for example `ReadWaitTimeout`.

Async: `send_async()` / `handle.get_async()`. Inside a coroutine or a Jupyter cell, use only the `*_async` forms: every sync call there returns `ASYNC_CONTEXT` and runs nothing.
Delayed: `add_numbers.with_options(delay_seconds=60).send(a=5, b=3)` dispatches after 60 seconds.
Deadline: `add_numbers.with_options(good_until=deadline).send(a=5, b=3)` expires if not executed in time.

### Resend a Failed Send

`TaskSendError` is a union with one variant per code; match on the variant. `TaskSendEnqueueFailed` has the field `retryable` (`True` for a transient cause); `TaskSendIdempotencyKeyConflict` has the class value `retryable = False` (a repeat of the same send does not complete it). The resend methods take a `TaskSendResendable` (`TaskSendEnqueueFailed`, `TaskSendAsyncContext`); another variant, the key conflict included, is a type error and fails an assertion at run time. For a key conflict, `owner_task_id` names the task that holds the key; send the same command, or use another key:

<!-- check: skip partial -- uses names that the page does not define -->
```python
match my_task.send(**kwargs):
    case Ok(handle):
        ...
    case Err(TaskSendEnqueueFailed(retryable=True) as err):
        match my_task.resend(err):
            case Ok(handle):
                ...
            case Err(resend_err):
                ...  # permanent failure
    case Err(err):
        ...  # permanent failure
```

`resend` / `resend_async` replay the exact stored payload (SHA-verified), with the delay of the first send when it had one.

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

    fetch = TaskNode(function=fetch_data)
    process = TaskNode(function=process_data, waits_for=[fetch], args_from={"data": fetch})
    save = TaskNode(function=save_result, waits_for=[process], args_from={"result": process})

    config = WorkflowDefinitionConfig(
        output=save,
        on_error=OnError.FAIL,
    )

spec = ETLPipeline.build(app)
```

`config = WorkflowDefinitionConfig(...)` sets `output`, `on_error` and `success_policy`, and can name the nodes of the same class body. A `Meta` attribute raises HRS-033 when the class is created.

Best for static, reusable DAGs. Use `build_with(app, **params)` for parameterized builds.

### Node options

`waits_for`, `args_from`, `workflow_context_from`, `queue`, `priority`,
`allow_failed_dependencies`, `join` (`"all"` | `"any"` | `"quorum"`),
`min_success`, `good_until`, `node_id`.

## Start a Workflow

`start()` returns `WorkflowStartResult[WorkflowHandle[T]]`:

```python
from horsies import Ok, Err

match spec.start():
    case Ok(handle):
        status = handle.status()
        result = handle.get(wait_ms=30000)
    case Err(start_err):
        print(f"Start failed: {start_err.code}")
```

Async: `spec.start_async()`.

### Retry a Failed Start

`retry_start` takes `WorkflowStartResendable`: `WorkflowStartEnqueueFailed` or `WorkflowStartAsyncContext`. It starts again with the error's `workflow_id`:

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

A start with an existing `workflow_id` compares its start command with the stored one. The same command returns the existing handle; another command is `WorkflowStartCommandConflict` (`WORKFLOW_COMMAND_CONFLICT`, never retryable, nothing written). `retry_start` builds the same spec again, so a first attempt that committed comes back as the existing handle. Runs started before this release are not checked.

### Auto-retry ( this is not execution retry, only for sending tasks / starting workflows, for task execution retry see `RetryPolicy` )

Set `resend_on_transient_error=True` on `AppConfig` to auto-retry transient
`ENQUEUE_FAILED` errors (up to 3 times, exponential backoff) for both
task sends and workflow starts.

## WorkflowHandle

<!-- check: skip partial -- uses names that the page does not define -->
```python
handle.status()          # HandleResult[WorkflowStatus] — unwrap before use
handle.get(wait_ms=N) # ReadResult[T, TaskError] (blocks for output node)
handle.results()         # HandleResult[dict[str, TaskOutcome[Any, TaskError]]] — unwrap before use
handle.tasks()           # HandleResult[list[WorkflowTaskInfo]] — unwrap before use
handle.cancel()          # cancel workflow
handle.pause()           # pause (RUNNING -> PAUSED)
handle.resume()          # resume (PAUSED -> RUNNING)
```

All methods have `_async()` variants.

### Reconnect to an Existing Workflow

`WorkflowHandle.for_id(text, broker)` parses a workflow id text once, at the
boundary: `Ok(handle)`, or `Err(WorkflowHandleInvalidWorkflowId)` for text that is not
a UUID (no handle is built).

```python
from horsies import Err, Ok, WorkflowHandle

match WorkflowHandle.for_id("3c9e1f4a-6b2d-4c58-8e71-0a1b2c3d4e5f", app.get_broker()):
    case Ok(ok_value=handle):
        result = handle.get(wait_ms=30_000)
    case Err(err_value=invalid):
        print(invalid.message)  # the text is not a UUID
```

### Direct Result Access by Task ID

Fetch a task result by ID without a `TaskHandle` (e.g. in HTTP endpoints). Use
the app-level typed API — `broker.get_result(_async)` was removed in 0.1.2.

<!-- check: skip partial -- task_id comes from the caller -->
```python
from horsies import Err, Failed, Ok, Succeeded

# Returns ReadResult[Any, TaskError]: one match reads it.
match await app.get_result_async(task_id, wait_ms=5000):
    case Ok(Succeeded(value)):
        ...  # the task returned this value
    case Ok(Failed(task_error)):
        ...  # the task failed: task_error.code
    case Err(read_error):
        ...  # no outcome: ReadWaitTimeout, ReadNoTypeAvailable, ReadResultDeserializationError, ReadDatabaseError, ...

# Task metadata with typed-decoded result: the app-level wrapper decodes
# when the task is registered locally (decoded_result / result_decoded set).
info = await app.get_task_info_async(task_id, include_result=True)

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
    rerun_task_in_async, RerunCommand, RerunEnqueuePolicy,
    RerunEnqueued, RerunSourceLive, RerunSourceAbsent, RerunSourceCorrupt,
    RerunNotEligible, RerunInputUnavailable, RerunInputCorrupt,
    RerunKeyConflict, RerunKeyReplay,
)

outcome = await rerun_task_in_async(
    connection,
    RerunCommand(source_task_id=task_id, good_until=None),
    RerunEnqueuePolicy(
        # A period in whole days, or None for forever. A rerun states its
        # period outright -- there is no queue mapping to fall back on here.
        # The rerun path checks the period's class against the registry and
        # refuses an unregistered one.
        retention=timedelta(days=30),
        retain_rerun_input=True,
        reservation_window=timedelta(hours=24),
    ),
    # The declared codes of the app's tasks: the stored retry list of the
    # source is checked against them before the write.
    registered_task_codes=app.registered_task_codes(),
)
match outcome:
    case RerunEnqueued(new_task_id=new_id):
        ...
    case RerunSourceLive() | RerunSourceAbsent() | RerunNotEligible():
        ...
    case RerunInputUnavailable() | RerunInputCorrupt():
        ...
    case RerunSourceCorrupt(detail=detail):
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

Before the write, the rerun checks the stored `retry_for` of the source
against `registered_task_codes` (`app.registered_task_codes()`: each
registered task by name, with its `error_codes`). A built-in code
passes for every task; a custom code passes when the app registers the task
and the task declares the code. Otherwise `CorePreconditionFailed` (subject
`rerun_task`, not retryable, nothing written): `RETRY_CODE_NOT_DECLARED`
for a code that the task does not declare, `TASK_NOT_REGISTERED` for a task
that the app does not register. `app.rerun_task_in_async` and `app.rerun_task_in`
pass the app's codes themselves.

`app.rerun_task_in_async(connection, command, policy)` is the same rerun with
the tracing setting of the app (blocking: `app.rerun_task_in` on a SQLAlchemy
`Connection`). With `OtelTracing()` the new task stores the trace carrier of
the caller's current context, so its attempts continue the caller's trace.
The free `rerun_task_in_async` stores no carrier.

## Result Types

| Operation | Result type | Ok | Err |
|---|---|---|---|
| Task execution | `Result[T, TaskError]` | value `T` | `TaskError` |
| `get()` / `get_result()` / `result_for()` | `ReadResult[T, TaskError]` | `Succeeded(value)` or `Failed(task_error)` | `ReadError` |
| `send()` / `resend()` | `TaskSendResult[TaskHandle[T]]` | `TaskHandle` | `TaskSendError` |
| `start()` | `WorkflowStartResult[WorkflowHandle[T]]` | `WorkflowHandle` | `WorkflowStartError` |
| Broker infra | `DatabaseResult[T]` | value `T` | `DatabaseOperationError` |
| Handle ops | `HandleResult[T]` | value `T` | `WorkflowHandleError` |

Use `is_ok(result)` / `is_err(result)` type guards, or `match Ok / Err`. Read a `ReadResult` with one `match`: `case Ok(Succeeded(value))`, `case Ok(Failed(task_error))`, `case Err(read_error)`. A workflow node input (`args_from`) and `WorkflowContext.result_for()` give a `TaskOutcome[T, TaskError]`: `Succeeded(value)` or `Failed(task_error)`.

### Error Code Enums

- `TaskSendErrorCode`: `SEND_SUPPRESSED`, `ASYNC_CONTEXT`, `VALIDATION_FAILED`, `ENQUEUE_FAILED`, `PAYLOAD_MISMATCH`, `IDEMPOTENCY_KEY_CONFLICT`, `PAYLOAD_TOO_LARGE`
- `WorkflowStartErrorCode`: `BROKER_NOT_CONFIGURED`, `VALIDATION_FAILED`, `WORKFLOW_ID_CONFLICT`, `WORKFLOW_COMMAND_CONFLICT`, `ENQUEUE_FAILED`, `ASYNC_CONTEXT`
- `WorkflowHandleErrorCode`: `WORKFLOW_NOT_FOUND`, `DATABASE_OPERATION_FAILED`, `STORED_DATA_INVALID`, `RESUME_RECOVERY_FAILED`, `ASYNC_CONTEXT`, `INVALID_WORKFLOW_ID`
- `ReadError` (a union, one class per read failure; each has `code` and `message`): `ReadWaitTimeout`, `ReadTaskNotFound`, `ReadWorkflowNotFound`, `ReadResultNotAvailable`, `ReadResultNotReady`, `ReadWaitInsideTask`, `ReadAsyncContext`, `ReadInvalidInput`, `ReadResultDeserializationError`, `ReadUnknownCustomCode` (a stored custom code that the declared `Enum` does not name; adds `custom_code`), `ReadDatabaseError` (adds `retryable` and `cause`), `ReadNoTypeAvailable`, `ReadWorkflowContextMissingId`

`retryable=True` means transient (DB connection blip). `retryable=False` means permanent. On send errors, `TaskSendEnqueueFailed` has the field `retryable` and `TaskSendIdempotencyKeyConflict` the class value `retryable = False`; the other send errors have no `retryable`.

`WorkflowHandleError` is a union with one variant per `WorkflowHandleErrorCode` member (`WorkflowHandleNotFound`, `WorkflowHandleDatabaseOperationFailed`, `WorkflowHandleStoredDataInvalid`, `WorkflowHandleResumeRecoveryFailed`, `WorkflowHandleAsyncContext`); match on the variant. `WorkflowHandleResumeRecoveryFailed` means `resume()` committed and only its recovery pass failed: the workflow is `RUNNING`.

Every call runs on the core of the process: one pool for every thread and event loop, so no call has an event loop limit.

`ASYNC_CONTEXT` (every catalog above, `DatabaseErrorCode`, and the read error `ReadAsyncContext`): a sync call was made on a thread that runs an event loop (a coroutine, a Jupyter cell). Its wait would stall that loop, so nothing ran. Call the `*_async` form. `retryable=False` where the error has the field; a handle does not keep an `ReadAsyncContext` read error; `await spec.retry_start_async(err)` completes a start that returned it. `app.check(live=True)` there reports `HRS-217` (`CHECK_LIVE_ASYNC_CONTEXT`).

### Raised, never returned: `ForkedAfterCoreStart`, `CoreStopped`

These two core errors (import them from `horsies`) are states of the process, not of one call: the core refuses every later call in the process. Every call that runs on the core raises them, sync and async forms alike: the sends and their resend methods, the task, result and health reads, `check(live=True)`, the workflow start, `WorkflowHandle`, `horsies.monitoring` and the `horsies web` schema probe. No call returns them as a result, and no result type has a variant for them. Nothing ran, and a retry in the same process raises again.

| Raised | Cause | Fix |
|---|---|---|
| `ForkedAfterCoreStart` | The process was forked without exec after the core started in its parent, for example a `multiprocessing` pool with the `fork` start method inside a task | Start child processes that call horsies with the `spawn` or `forkserver` start method, or start the core after the fork: fork before the parent's first call |
| `CoreStopped` | A background task of the core panicked: a defect in horsies, not a failure of the database or of your code. `args` are the `(subject, cause)` of the first panic | Restart the process: the core does not start again in it. Report the panic's subject and cause as a defect |

A worker child ends itself at its next core call (exit code 80; its last log line names the cause), and the worker replaces it. At the start of a task, before task code ran, the claim is released with no attempt charged. After a body that finished, the child reports the result first, and the task keeps it. A body that the refusal ended (a core call of the body raised it), and a body of a shared child that still runs at the exit, take the crashed-child path (`WORKER_CRASHED`) and the retry policy.

## CLI

```bash
horsies worker myapp.config:app --profile default  # start worker (--profile is required)
horsies worker myapp.config:app --profile default --metrics-listen 0.0.0.0:9464  # + /metrics, /livez, /readyz
horsies scheduler myapp.config:app       # start scheduler
horsies check myapp.config:app [--live]  # validate before deploy
horsies web myapp.config:app             # serve the monitoring web UI (needs horsies[web])
horsies web --database-url URL           # monitor a database without an app module
horsies metrics myapp.config:app --metrics-listen 0.0.0.0:9465  # serve the fleet metrics at GET /metrics
horsies get-docs                         # download the docs of the installed version
horsies get-docs --ref main              # download the main branch docs (needed for a development version)
horsies skills install --target DIR      # copy this skill folder to DIR/horsies/ (--force replaces a changed copy)
```

Worker settings live in named profiles in `AppConfig.worker_profiles`, not in
flags. `--profile` is required, with no default; a missing or unknown name
fails before anything starts and lists the defined names. The removed flags
(`--processes`, `--max-claim-batch`, `--max-claim-per-worker`,
`--max-tasks-per-child`, `--max-memory-per-child-mb`, `--drain-timeout`,
`--child-preload`) exit 2 and name their profile field. See `configs.md`
"Worker profiles".

```python
from horsies import (
    IsolatedChildren, IsolatedRecycle, MemoryLimit, TaskLimit, WorkerProfile,
)

profiles = {
    'default': WorkerProfile(
        isolated=IsolatedChildren(
            count=8,
            recycle=IsolatedRecycle(after=TaskLimit(tasks=500), above=MemoryLimit(mb=1024)),
        ),
    ),
}
```

`horsies check` runs phased validation (config and every worker profile, imports,
DAG, builders, policies, optional live DB check and worker connection check).
Worker and scheduler also run check at startup.

A worker owns one child and one socket per slot. An isolated child holds one
task, so a crash or a timeout affects only that task; a crash of a shared child
ends every task in it. On Linux a preload parent forks each child: a new child takes
milliseconds, and children share memory. Before you change `WorkerProfile.preload`,
read the traps under "Child process model" in `configs.md`: `app` mode changes
what runs at import, and a recycle no longer reloads your code.

Two limits recycle worker children to bound memory, with **OR** semantics (a
child exits when either limit is reached). `IsolatedRecycle.after` (default
`TaskLimit(tasks=100)`) recycles after N tasks; `IsolatedRecycle.above` (default
`NoMemoryLimit()`) recycles once a child's own RSS reaches N MB after a task — the
primary guard for memory-quota deployments, since it maps to the RSS the quota
charges. A threshold at or below the warmed child baseline fails startup.
Recommended default: memory recycling on, count high as a
backstop, tuned against the `children_memory_mb` field on `WorkerStateSnapshot`.
See `configs.md` and website docs `workers/concurrency`.
