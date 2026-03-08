---
name: horsies-quick-reference
description: Quick orientation for the horsies Python task queue and workflow engine. Use when users need a concise overview and routing to detailed guidance for tasks, workflows, and configuration.
---

# horsies — Quick Reference

PostgreSQL-backed background task queue and workflow engine for Python.

This is an **introductory quick reference** — it covers core concepts and
patterns at a glance. For production-level guidance, see the dedicated
skill files in this directory:

| File | When to open |
|---|---|
| `tasks.md` | `@app.task`, `TaskResult`, `send()`, `RetryPolicy`, `ExceptionMapper`, serialization |
| `workflows.md` | `WorkflowSpec`, `TaskNode`, `WorkflowHandle`, DAG construction, failure semantics |
| `configs.md` | `AppConfig`, `PostgresConfig`, queues, recovery, scheduling, CLI commands |

All public symbols: `from horsies import <name>`

For the full API reference, run `horsies get-docs` to download docs locally,
or read `website/public/llms.txt` in this repository.

## Define a Task

```python
from horsies import Horsies, AppConfig, PostgresConfig, TaskResult, TaskError

app = Horsies(config=AppConfig(
    broker=PostgresConfig(database_url="postgresql+psycopg://..."),
))

@app.task("add_numbers")
def add_numbers(a: int, b: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=a + b)
```

Every task must return `TaskResult[T, TaskError]`.
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

Async: `send_async()` / `handle.get_async()`.
Delayed: `add_numbers.schedule(60, a=5, b=3)` dispatches after 60 seconds.

### Retry a Failed Send

Only `ENQUEUE_FAILED` (transient) errors are retryable:

```python
match my_task.send(arg1, arg2):
    case Ok(handle):
        ...
    case Err(err) if err.retryable:
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

```python
from horsies import OnError, from_node

fetch = fetch_data.node()()
process = process_data.node()(data=from_node(fetch))
save = save_result.node()(result=from_node(process))

spec = app.workflow(
    name="etl_pipeline",
    tasks=[fetch, process, save],
    on_error=OnError.FAIL,
    output=save,
)
```

Best for dynamic workflows where node kwargs depend on runtime inputs.

### Class-based — `WorkflowDefinition`

Nodes are class attributes; `node_id` is auto-assigned from the attribute name:

```python
from horsies import TaskNode, WorkflowDefinition, OnError

class ETLPipeline(WorkflowDefinition[SaveResult]):
    name = "etl_pipeline"

    fetch = TaskNode(fn=fetch_data)
    process = TaskNode(fn=process_data, waits_for=[fetch], args_from={"data": fetch})
    save = TaskNode(fn=save_result, waits_for=[process], args_from={"result": process})

    class Meta:
        output = save
        on_error = OnError.FAIL

spec = ETLPipeline.build(app)
```

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

Only `ENQUEUE_FAILED` errors are retryable:

```python
match spec.start():
    case Ok(handle):
        ...
    case Err(err) if err.retryable:
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

```python
handle.status()          # WorkflowStatus
handle.get(timeout_ms=N) # TaskResult (blocks for output node)
handle.results()         # HandleResult[dict[str, TaskResult]] — unwrap before use
handle.tasks()           # list[WorkflowTaskInfo]
handle.cancel()          # cancel workflow
handle.pause()           # pause (RUNNING -> PAUSED)
handle.resume()          # resume (PAUSED -> RUNNING)
```

All methods have `_async()` variants.

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

- `TaskSendErrorCode`: `SEND_SUPPRESSED`, `VALIDATION_FAILED`, `ENQUEUE_FAILED`, `PAYLOAD_MISMATCH`
- `WorkflowStartErrorCode`: `BROKER_NOT_CONFIGURED`, `VALIDATION_FAILED`, `ENQUEUE_FAILED`, `INTERNAL_FAILED`
- `HandleErrorCode`: `WORKFLOW_NOT_FOUND`, `DB_OPERATION_FAILED`, `LOOP_RUNNER_FAILED`, `INTERNAL_FAILED`

`retryable=True` means transient (DB connection blip). `retryable=False` means permanent.

## CLI

```bash
horsies worker myapp.config:app          # start worker
horsies scheduler myapp.config:app       # start scheduler
horsies check myapp.config:app [--live]  # validate before deploy
horsies get-docs                         # download docs locally
```

`horsies check` runs phased validation (config, imports, DAG, builders, policies,
optional live DB check). Worker and scheduler also run check at startup.
