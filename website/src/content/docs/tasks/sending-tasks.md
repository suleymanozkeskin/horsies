---
title: Sending Tasks
summary: How to enqueue tasks for background execution.
related: [defining-tasks, retrieving-results]
tags: [tasks, send, async, scheduling]
---

# Sending Tasks

Enqueue tasks with `.send()`, `.send_async()`, `.schedule()`, or `.schedule_async()`. All return a `TaskSendResult[TaskHandle[T]]` -- a `Result` type that is either `Ok(TaskHandle)` on success or `Err(TaskSendError)` on failure.

## How To

### Send a Task (Sync)

```python
from horsies import Ok, Err
from instance import my_task

match my_task.send(name="alice", count=3):
    case Ok(handle):
        print(f"Task submitted: {handle.task_id}")
    case Err(send_err):
        print(f"Send failed: {send_err.code} - {send_err.message}")
```

### Send a Task (Async)

```python
from horsies import Ok, Err

async def my_endpoint():
    match await my_task.send_async(name="alice", count=3):
        case Ok(handle):
            return {"task_id": handle.task_id}
        case Err(send_err):
            return {"error": send_err.message}
```
`send_async()` only enqueues the task. Use `handle.get_async()` if you want to wait for completion.

The sync variants are blocking database round trips, so calling them from a
running event loop would stall every coroutine on it. They fail closed
instead: `.send()`, `.schedule()`, `.retry_send()`, and `.retry_schedule()`
called inside a running loop return `Err(TaskSendAsyncContext)`
without touching the broker. The error carries the prepared `task_id` and
payload — complete the dispatch with `.retry_send_async(err)` /
`.retry_schedule_async(err)`, or call `.send_async` / `.schedule_async` with
the original arguments.

### Set a Task Deadline

Use `.with_options(good_until=...)` to set a per-send start deadline. If no worker starts the task before the deadline, the task transitions to `EXPIRED` and never runs. A task that has started is not stopped by the deadline.

Set a deadline when a task loses its value after a known time, such as a ride request, a one-time login code, or a price quote. After an outage or a backlog, workers skip expired tasks, so worker capacity goes to tasks that are still useful. See [Task Expiry](/horsies/concepts/task-lifecycle/#task-expiry).

```python
from datetime import datetime, timedelta, timezone
from horsies import Ok, Err

deadline = datetime.now(timezone.utc) + timedelta(minutes=5)

match my_task.with_options(good_until=deadline).send(name="alice", count=3):
    case Ok(handle):
        print(f"Task submitted with 5-minute deadline: {handle.task_id}")
    case Err(err):
        print(f"Send failed: {err.code}")
```

`good_until` must be a timezone-aware `datetime`. Naive datetimes return `Err(VALIDATION_FAILED)`.

`with_options()` works with all send methods:

```python
opts = my_task.with_options(good_until=deadline)

opts.send(name="alice", count=3)                        # sync
await opts.send_async(name="alice", count=3)            # async
opts.schedule(60, name="alice", count=3)                # delayed
await opts.schedule_async(60, name="alice", count=3)    # delayed, async
```

For workflow nodes, use `.node(good_until=...)` instead — see [Typed Node Builder](../concepts/workflows/typed-node-builder).

### Keep a Terminal Record Forever

Every task's terminal record ages by its retention class. Which class a
send gets, highest precedence first: what the send asks for, then the
queue's [`queue_retention`](../../configuration/retention-config#retention-per-queue)
mapping if it has one, then the immutable 30-day default. Omitting the
argument therefore takes the queue's mapping where one exists — and
naming `standard_30d` explicitly is a choice that overrides it.

Pass `retention_class_key=None` to keep the record forever:

<!-- check: skip partial -- uses names that the page does not define -->
```python
match await audit_task.with_options(retention_class_key=None).send_async(entry=entry):
    case Ok(handle):
        print(f"Audit task {handle.task_id}: terminal record kept forever")
    case Err(err):
        print(f"Send failed: {err.code}")
```

Forever is reached by `None`, not by the word: `retention_class_key='forever'`
is refused, because the accepted values are the ones this process knows.

### Send Into a Declared Retention Class

Beyond the 30-day default and forever, a deployment can declare its own
finite classes in
[`AppConfig.retention.retention_classes`](../../configuration/retention-config#declaring-your-own)
and send into them by key:

<!-- check: skip partial -- uses names that the page does not define -->
```python
match await audit_task.with_options(retention_class_key='audit_1y').send_async(entry=entry):
    case Ok(handle):
        print(f"Audit task {handle.task_id}: kept a year")
    case Err(err):
        print(f"Send failed: {err.code}")  # VALIDATION_FAILED if undeclared
```

An unknown class is refused at the send call with `VALIDATION_FAILED`,
naming the class and listing what this deployment accepts — nothing is
written. The check reads this process's config, so declare a class in every
process that sends into it.

The class is fixed at enqueue. An unknown class name fails the send with
`Err(VALIDATION_FAILED)` naming the class — nothing is written.

### Delay Execution

```python
from horsies import Ok, Err

match my_task.schedule(60, name="alice", count=3):
    case Ok(handle):
        print(f"Scheduled: {handle.task_id}")
    case Err(err):
        print(f"Schedule failed: {err.code}")
```

From async code, use `.schedule_async()`:

```python
match await my_task.schedule_async(60, name="alice", count=3):
    case Ok(handle):
        print(f"Scheduled: {handle.task_id}")
    case Err(err):
        print(f"Schedule failed: {err.code}")
```

### Wait for Result

```python
from horsies import Ok, Err

match my_task.send(name="alice", count=3):
    case Ok(handle):
        # Blocking wait
        result = handle.get()

        # With timeout (milliseconds)
        result = handle.get(timeout_ms=5000)

        # Async wait
        result = await handle.get_async(timeout_ms=5000)
    case Err(err):
        print(f"Send failed: {err.code}")
```
`get_async()` waits via broker notifications (LISTEN/NOTIFY) with a polling fallback.

### Fire and Forget

```python
# Send without waiting for result -- discard the TaskSendResult
my_task.send(name="alice", count=3)
```

### Pass Complex Arguments

Arguments must be keyword-only and JSON-serializable. Positional `.send(arg1, arg2)` is rejected with `Err(VALIDATION_FAILED)`. Pydantic models and dataclass instances are supported directly; the worker decodes them using the registered task's parameter type via `pydantic.TypeAdapter`.

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Ok, Err

match process.send(data={"key": "value", "nested": {"a": 1}}, items=[1, 2, 3]):
    case Ok(handle):
        result = handle.get()
    case Err(err):
        print(f"Send failed: {err.code}")

# Pydantic models - pass the instance to preserve type metadata
order = Order(id=123, items=["a", "b"])
match process_order.send(order=order):
    case Ok(handle):
        result = handle.get()
    case Err(err):
        print(f"Send failed: {err.code}")
```

Pydantic models and dataclasses must be defined in importable modules (not `__main__` and not inside functions) so the worker can resolve the declared parameter type.

### Execute Directly (Skip Queue)

```python
# Runs immediately in current process (plain Python call; bypasses the queue)
result = my_task(name="alice", count=3)
```

Direct calls bypass the queue entirely. Library features do not apply:

- No retries (`retry_policy`)
- No persistence (task not recorded in database)
- No worker distribution
- No scheduling

Use only for **unit testing**. For production, always use `.send()` or `.send_async()`.

## Things to Avoid

**Don't call `.send()` at module level.**

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong - returns Err(TaskSendSuppressed) during worker import
# tasks.py
result = my_task.send(name="test")  # Err(TaskSendSuppressed)

# Correct - call from functions/endpoints
def process():
    match my_task.send(name="test"):
        case Ok(handle):
            ...
        case Err(err):
            ...
```

**Don't pass non-serializable objects.**

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong
my_task.send(connection=db_connection)

# Correct
my_task.send(connection_url=str(db_connection.url))
```

## Retrying Failed Sends

When `.send()` fails with `TaskSendEnqueueFailed` (a broker or database error), use the retry methods to replay the exact same payload without re-supplying arguments. The `enqueue_sha` on the stored `TaskSendPayload` guarantees the retry carries the identical serialized payload.

```python
from horsies import Err, Ok, TaskSendEnqueueFailed, TaskSendIdempotencyKeyConflict

match my_task.send(name="alice", count=3):
    case Ok(handle):
        result = handle.get()
    case Err(TaskSendEnqueueFailed(retryable=True) as err):
        match my_task.retry_send(err):
            case Ok(handle):
                result = handle.get()
            case Err(retry_err):
                print(f"Retry failed: {retry_err.code}")
    case Err(TaskSendIdempotencyKeyConflict(owner_task_id=owner)):
        print(f"Key is held by task {owner}")
    case Err(err):
        print(f"Permanent failure: {err.code}")
```

The retry methods take a `TaskSendRetryable`: `TaskSendEnqueueFailed`, `TaskSendAsyncContext`, `TaskSendIdempotencyKeyConflict` or `TaskSendLoopRunnerFailed`. Passing another variant is a type error; at run time it fails an assertion that names the variant. A retry of a `TaskSendIdempotencyKeyConflict` before the key's reservation ends returns the same error. A sync retry of a `TaskSendLoopRunnerFailed` returns the same error, because a stopped runner does not start again; `retry_send_async(err)` or `retry_schedule_async(err)` completes the send.

### Automatic Retry via Config

Set `resend_on_transient_err=True` in `AppConfig` to have the library automatically retry transient enqueue failures before returning the error:

<!-- check: skip defect -- does not type-check: reportCallIssue -->
```python
config = AppConfig(
    resend_on_transient_err=True,
    # ...
)
```

## API Reference

### `.send(**kwargs) -> TaskSendResult[TaskHandle[T]]`

Enqueue task for immediate execution. Keyword-only — positional arguments are rejected with `Err(VALIDATION_FAILED)`.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `**kwargs` | task kwargs | Keyword arguments matching the task's signature |

**Returns:** `TaskSendResult[TaskHandle[T]]` -- `Ok(TaskHandle)` on success, `Err(TaskSendError)` on failure.

### `.send_async(**kwargs) -> TaskSendResult[TaskHandle[T]]`

Async variant of `.send()`. Use in async code (FastAPI, etc.).
This does not execute the task locally; it only enqueues. Keyword-only.

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.schedule(delay, **kwargs) -> TaskSendResult[TaskHandle[T]]`

Enqueue task for delayed execution. Task arguments are keyword-only.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `delay` | `int` | Seconds to wait before task becomes claimable |
| `**kwargs` | task kwargs | Keyword arguments matching the task's signature |

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.schedule_async(delay, **kwargs) -> TaskSendResult[TaskHandle[T]]`

Async variant of `.schedule()`. Use in async code (FastAPI, etc.).
Same `delay` validation as `.schedule()`.

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.with_options(*, good_until=None, idempotency_key=None, retention_class_key=<unset>) -> TaskSendOptions[P, T]`

Return a per-send options builder. The returned object exposes `.send()`, `.send_async()`, `.schedule()`, and `.schedule_async()` with the overridden options applied.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `good_until` | `datetime \| None` | Task expiry deadline (must be timezone-aware) |
| `idempotency_key` | `str \| None` | Caller-supplied enqueue-deduplication key, scoped per task name |
| `retention_class_key` | `str \| None` | Terminal-record retention: omit to take the queue's mapping (else the 30-day default), `None` for forever |

**Returns:** `TaskSendOptions[P, T]` — a builder with `.send()`, `.send_async()`, `.schedule()`, and `.schedule_async()`.

Passing `good_until=None` explicitly clears any internally inherited deadline.

`retention_class_key` accepts `None` (keep the terminal record forever),
`'standard_30d'` (the immutable 30-day default), any class declared in
`AppConfig.retention.retention_classes`, and any class derived from
`AppConfig.retention.queue_retention` (`q_<queue>_<duration>`). Any other
string returns `Err(VALIDATION_FAILED)` at the send call, naming the class,
before anything is written.

Naming `'standard_30d'` is **not** the same as omitting the parameter.
Omitting it takes the queue's mapping where one exists; naming the default
class overrides that mapping for this send.

The precedence is the same on `.schedule()` and `.schedule_async()` as on
`.send()`, and the same for a `TaskSchedule` cron fire and a workflow
node's backing task. `with_options(...)` applies its options to the
delayed methods as well as the immediate ones.

The class is snapshotted on the row at enqueue and decides which history
partition the record moves to at terminalization; it cannot be changed
afterwards. A retry replays the class its original send chose, so editing
`queue_retention` governs later sends and never reaches a task in flight.

### `.retry_send(error) -> TaskSendResult[TaskHandle[T]]`

Retry a failed send using the stored payload from the error.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `error` | `TaskSendRetryable` | The error from a previous `.send()` call |

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.retry_send_async(error) -> TaskSendResult[TaskHandle[T]]`

Async variant of `.retry_send()`.

### `.retry_schedule(error) -> TaskSendResult[TaskHandle[T]]`

Retry a failed schedule using the stored payload. The error must come from `.schedule()` or `.schedule_async()`.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `error` | `TaskSendRetryable` | The error from a previous `.schedule()` call |

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.retry_schedule_async(error) -> TaskSendResult[TaskHandle[T]]`

Async variant of `.retry_schedule()`.

### `TaskSendResult[T]`

Type alias: `Result[T, TaskSendError]`. The `Ok` side is `TaskHandle[T]` when returned from send methods.

| Property/Method | Type | Description |
| --------------- | ---- | ----------- |
| `.is_ok()` | `bool` | True if send succeeded |
| `.is_err()` | `bool` | True if send failed |
| `.ok_value` | `T` | The `TaskHandle`; raises `ValueError` if error |
| `.err_value` | `TaskSendError` | The error; raises `ValueError` if success |

Use `is_ok(result)` / `is_err(result)` from `horsies` as type-narrowing guards.

### `TaskSendError`

A union with one frozen variant per `TaskSendErrorCode` member; each variant has only the fields of its failure. The variants, their fields and `NoCause` are in the [Errors Reference](./errors#tasksenderror).

### `TaskSendErrorCode`

The codes, and which of them the retry methods accept, are in the [Errors Reference](./errors#tasksenderrorcode).

### `TaskHandle[T]`

| Property/Method | Type | Description |
| --------------- | ---- | ----------- |
| `.task_id` | `str` | Unique task identifier |
| `.get(timeout_ms=None)` | `TaskResult[T, TaskError]` | Wait for result (blocking) |
| `.get_async(timeout_ms=None)` | `TaskResult[T, TaskError]` | Wait for result (async) |
| `.info(include_result=False, include_failed_reason=False)` | `BrokerResult[TaskInfo \| None]` | Fetch task metadata from broker |
| `.info_async(include_result=False, include_failed_reason=False)` | `BrokerResult[TaskInfo \| None]` | Async variant of `.info()` |
