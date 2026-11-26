---
title: Sending Tasks
section: python
summary: How to enqueue tasks for background execution.
related: [defining-tasks, retrieving-results]
tags: [tasks, send, async, scheduling]
---

# Sending Tasks

Enqueue tasks with `.send()` or `.send_async()`. For a delay, a deadline, an idempotency key or a retention, send through `.with_options(...)`. All sends return a `TaskSendResult[TaskHandle[T]]` -- a `Result` type that is either `Ok(TaskHandle)` on success or `Err(TaskSendError)` on failure.

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
            return {"task_id": str(handle.task_id)}
        case Err(send_err):
            return {"error": send_err.message}
```
`send_async()` only enqueues the task. Use `handle.get_async()` if you want to wait for completion.

The sync variants are blocking database round trips, so calling them from a
running event loop would stall every coroutine on it. They fail closed
instead: `.send()` and `.resend()` called inside a running loop return
`Err(TaskSendAsyncContext)` without touching the broker. The error carries
the prepared `task_id` and payload — complete the dispatch with
`.resend_async(err)`, or call `.send_async` with the original arguments.

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

`good_until` must be a timezone-aware `datetime`. Naive datetimes return `Err(VALIDATION_FAILED)`. Compute the deadline at send time, not at module load time.

`with_options()` works with all send methods:

```python
opts = my_task.with_options(good_until=deadline)

opts.send(name="alice", count=3)                        # sync
await opts.send_async(name="alice", count=3)            # async
```

For workflow nodes, use `.node(good_until=...)` instead — see [Typed Node Builder](/horsies/concepts/workflows/typed-node-builder/).

### Keep a Terminal Record Forever

Every task's terminal record ages by its retention class. Which class a
send gets, highest precedence first: what the send asks for, then the
queue's [`queue_retention`](../../configuration/retention-config#retention-per-queue)
mapping if it has one, then the immutable 30-day default. Omitting the
argument therefore takes the queue's mapping where one exists — and
choosing 30 days explicitly is a choice that overrides it.

Pass `retention=None` to keep the record forever:

<!-- check: skip partial -- uses names that the page does not define -->
```python
match await audit_task.with_options(retention=None).send_async(entry=entry):
    case Ok(handle):
        print(f"Audit task {handle.task_id}: terminal record kept forever")
    case Err(err):
        print(f"Send failed: {err.code}")
```

### Send With a Listed Retention Period

Beyond the 30-day default, the mapped periods and forever, a deployment
can list finite periods in
[`AppConfig.retention.retention_periods`](../../configuration/retention-config#periods-a-send-chooses)
and send with them:

<!-- check: skip partial -- uses names that the page does not define -->
```python
match await audit_task.with_options(retention=timedelta(days=365)).send_async(entry=entry):
    case Ok(handle):
        print(f"Audit task {handle.task_id}: kept a year")
    case Err(err):
        print(f"Send failed: {err.code}")  # VALIDATION_FAILED if not listed
```

A period this process does not register is refused at the send call with
`VALIDATION_FAILED`, naming the period and listing the periods this
deployment accepts — nothing is written. A fractional day is refused the
same way. The check reads this process's config, so list a period in
every process that sends with it.

The class is fixed at enqueue.

### Delay Execution

Pass `delay_seconds` to `with_options()`. The task becomes claimable after that number of seconds.

```python
from horsies import Ok, Err

match my_task.with_options(delay_seconds=60).send(name="alice", count=3):
    case Ok(handle):
        print(f"Sent with a delay: {handle.task_id}")
    case Err(err):
        print(f"Send failed: {err.code}")
```

From async code, use `.send_async()`:

```python
match await my_task.with_options(delay_seconds=60).send_async(name="alice", count=3):
    case Ok(handle):
        print(f"Sent with a delay: {handle.task_id}")
    case Err(err):
        print(f"Send failed: {err.code}")
```

`delay_seconds` is a whole number of seconds, `0` or more. `0` sends now. A negative value, a `bool`, a `float` or a `str` returns `Err(VALIDATION_FAILED)` before anything is written.

### Wait for Result

```python
from horsies import Err, Failed, Ok, Succeeded

match my_task.send(name="alice", count=3):
    case Ok(handle):
        # Blocking wait, with a timeout in milliseconds
        match handle.get(wait_ms=5000):
            case Ok(Succeeded(value)):
                print(f"Result: {value}")
            case Ok(Failed(task_error)):
                print(f"Task failed: {task_error.code}")
            case Err(read_error):
                print(f"No result: {read_error.code}")

        # Blocking wait with no timeout
        result = handle.get()

        # Async wait
        result = await handle.get_async(wait_ms=5000)
    case Err(err):
        print(f"Send failed: {err.code}")
```
`get()` and `get_async()` return `ReadResult[T, TaskError]`. `Ok(Succeeded(value))` holds the value of the task. `Ok(Failed(task_error))` holds the error of the task. `Err(read_error)` means the read gave no outcome, for example `ReadWaitTimeout`. See [Retrieving Results](/horsies/tasks/retrieving-results/).

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
# Runs immediately in current process (plain Python call; bypasses the queue).
# Returns the Result[T, TaskError] of the body: Ok(value) or Err(TaskError).
result = my_task(name="alice", count=3)
```

Direct calls bypass the queue entirely. Library features do not apply:

- No retries (`retry_policy`)
- No persistence (task not recorded in database)
- No worker distribution
- No scheduling

Use only for **unit testing**. For production, always use `.send()` or `.send_async()`.

### Send After a Commit

Commit the application transaction, then send, and handle the `Err` result of the send. No send takes the caller's connection; the rule and its failure windows are on [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/).

```python
from sqlalchemy import Engine, text

from horsies import Err, Ok, Result, TaskError, TaskSendEnqueueFailed

from myapp.instance import app


@app.task('send_receipt')
def send_receipt(*, order_id: str) -> Result[None, TaskError]:
    return Ok(None)


def place_order(engine: Engine, order_id: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text('INSERT INTO orders (id) VALUES (:id)'),
            {'id': order_id},
        )
    # The application transaction is committed here. Send after the commit.
    options = send_receipt.with_options(idempotency_key=f'receipt:{order_id}')
    match options.send(order_id=order_id):
        case Ok(handle):
            print(f'receipt task {handle.task_id}')
        case Err(TaskSendEnqueueFailed(retryable=True) as error):
            # The order exists. Repeat the same send with the same key.
            match send_receipt.resend(error):
                case Ok(handle):
                    print(f'receipt task {handle.task_id}')
                case Err(resend_error):
                    print(f'receipt not queued: {resend_error.code}')
        case Err(error):
            print(f'receipt not queued: {error.code}')
```

In an async function, use `send_async()` and `resend_async()`.

### Make a Send Repeatable

Pass `idempotency_key` to `with_options()`, as in the example above. A repeat of the same send returns `Ok(TaskHandle)` for the first task. A repeat with another command returns `Err(TaskSendIdempotencyKeyConflict)`, whose `owner_task_id` names the first task. A key outside 1 to 255 UTF-8 bytes returns `Err(TaskSendEnqueueFailed)` with `retryable=False`. The key rules and the reservation window are on [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/#make-the-send-repeatable-with-an-idempotency-key).

The reservation window is the `idempotency_reservation_window` argument of `PostgresBroker`. `AppConfig` has no field for it, so `app.get_broker()` uses the default of 24 hours.

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

**Don't send before the commit.** A rollback leaves a task for a write that does not exist.

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong
with engine.begin() as connection:
    connection.execute(text('INSERT INTO orders (id) VALUES (:id)'), {'id': order_id})
    send_receipt.send(order_id=order_id)  # runs before the commit

# Correct
with engine.begin() as connection:
    connection.execute(text('INSERT INTO orders (id) VALUES (:id)'), {'id': order_id})
send_receipt.send(order_id=order_id)
```

## Resending Failed Sends

When `.send()` fails with `TaskSendEnqueueFailed` (a broker or database error), use the resend methods to replay the exact same payload without re-supplying arguments. The stored `TaskSendPayload` carries the task id, the serialized arguments, the `sent_at` and the delay of the first send; if that send was written, the resend finds the same command on the same task id and returns it instead of writing a second row. This holds after the task finished too: a resend reads the task history and does not run the task a second time.

```python
from horsies import Err, Ok, TaskSendEnqueueFailed, TaskSendIdempotencyKeyConflict

match my_task.send(name="alice", count=3):
    case Ok(handle):
        result = handle.get()
    case Err(TaskSendEnqueueFailed(retryable=True) as err):
        match my_task.resend(err):
            case Ok(handle):
                result = handle.get()
            case Err(resend_err):
                print(f"Resend failed: {resend_err.code}")
    case Err(TaskSendIdempotencyKeyConflict(owner_task_id=owner)):
        print(f"Key is held by task {owner}")
    case Err(err):
        print(f"Permanent failure: {err.code}")
```

The resend methods take a `TaskSendResendable`: `TaskSendEnqueueFailed`, `TaskSendAsyncContext` or `TaskSendIdempotencyKeyConflict`. Passing another variant is a type error; at run time it fails an assertion that names the variant. A resend of a `TaskSendIdempotencyKeyConflict` before the key's reservation ends returns the same error. A resend of a send with a delay keeps the delay.

### Automatic Resend via Config

Set `resend_on_transient_err=True` in `AppConfig` to resend a transient enqueue failure before the send returns the error. The default is `False`. The resend rules are on [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/#resend-transient-failures).

```python
from horsies import AppConfig, Horsies, QueueMode

from myapp.config import broker_config

app = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=broker_config,
        resend_on_transient_err=True,
    )
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

`.send()` refuses a call that cannot run, with nothing stored: a missing required argument, a wrong value, an unknown key, and a value that the worker could not read back. Each gives `TaskSendValidationFailed`.

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.with_options(*, good_until=None, idempotency_key=None, retention=<unset>, delay_seconds=None) -> TaskFunctionWithOptions[P, T]`

Return a per-send options builder. The returned object exposes `.send()` and `.send_async()` with the overridden options applied.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `good_until` | `datetime \| None` | Task expiry deadline (must be timezone-aware) |
| `idempotency_key` | `str \| None` | Caller-supplied enqueue-deduplication key, scoped per task name |
| `retention` | `SendRetention` | Terminal-record retention: omit to take the queue's mapping (else the 30-day default), a period in whole days (`timedelta`), or `None` for forever |
| `delay_seconds` | `int \| None` | Seconds to wait before the task becomes claimable: `0` or more. `None` (the default) sends now |

**Returns:** `TaskFunctionWithOptions[P, T]` — a builder with `.send()` and `.send_async()`.

Passing `good_until=None` explicitly clears any internally inherited deadline.

`retention` accepts `None` (keep the terminal record forever), or a
period in whole days that this process registers: 30 days (the default),
a period in `AppConfig.retention.queue_retention`, or a period in
`AppConfig.retention.retention_periods`. Any other period, a fractional
day, and a value that is not a `timedelta` (such as a 0.6.0 class key
string) return `Err(VALIDATION_FAILED)` at the send call before anything
is written.

Choosing 30 days is **not** the same as omitting the parameter. Omitting
it takes the queue's mapping where one exists; choosing 30 days overrides
that mapping for this send.

The precedence is the same for a send with a delay as for a send now,
and the same for a `TaskSchedule` cron fire and a workflow node's backing
task.

The class is snapshotted on the row at enqueue and decides which history
partition the record moves to at terminalization; it cannot be changed
afterwards. A resend replays the class its original send chose, so editing
`queue_retention` governs later sends and never reaches a task in flight.

### `PostgresBroker(config, *, idempotency_reservation_window: timedelta | None = None, ...)`

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `idempotency_reservation_window` | `timedelta \| None` | 24 hours | Time that a key stays reserved after its task is terminal. Must be greater than zero and at most 30 days. The constructor raises `ValueError` outside this range |

### `.resend(error) -> TaskSendResult[TaskHandle[T]]`

Send again the task of a failed send, with the stored payload from the error: the same task id, the same payload and `sent_at`, and the delay of the first send when it had one.

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `error` | `TaskSendResendable` | The error from a previous send of this task |

**Returns:** `TaskSendResult[TaskHandle[T]]`

### `.resend_async(error) -> TaskSendResult[TaskHandle[T]]`

Async variant of `.resend()`.

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

A union with one frozen variant per `TaskSendErrorCode` member; each variant has only the fields of its failure. The variants, their fields and `NoCause` are in [Error Types](/horsies/tasks/error-types/#tasksenderror).

### `TaskSendErrorCode`

The codes, and which of them the resend methods accept, are in the [Errors Reference](/horsies/tasks/errors/#send-error-codes).

### `TaskHandle[T]`

| Property/Method | Type | Description |
| --------------- | ---- | ----------- |
| `.task_id` | `uuid.UUID` | Unique task identifier |
| `.get(wait_ms=None)` | `ReadResult[T, TaskError]` | Wait for result (blocking) |
| `.get_async(wait_ms=None)` | `ReadResult[T, TaskError]` | Wait for result (async) |
| `.info(include_result=False, include_failed_reason=False, include_attempts=False)` | `BrokerResult[TaskInfo \| None]` | Fetch task metadata from broker |
| `.info_async(include_result=False, include_failed_reason=False, include_attempts=False)` | `BrokerResult[TaskInfo \| None]` | Async variant of `.info()` |
