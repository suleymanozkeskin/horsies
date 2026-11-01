---
title: Atomic Enqueue and Topology
summary: Why a send cannot join the caller's database transaction, and the pattern that replaces it - send after commit, automatic resend, and an idempotency key.
related: [../tasks/sending-tasks, ../tasks/errors, ../configuration/app-config, ../configuration/broker-config, ../concepts/workflows/workflow-api]
tags: [operations, topology, enqueue, idempotency, idempotency_key, resend_on_transient_err, transaction]
---

# Atomic Enqueue and Topology

No send or start API takes the caller's database connection. A send opens its own connection from the broker pool and commits on its own. The application write and the enqueue are always two transactions, also when both use the same PostgreSQL database.

## Why and When

Read this page when a task must exist if, and only if, an application write is committed. Examples: a receipt after an order row, a sync job after a status change.

Two failure windows exist:

| Order | Failure | Result |
| ----- | ------- | ------ |
| Send, then commit | The commit fails | A task exists for a write that did not happen |
| Commit, then send | The send fails, or the process stops between the two | A write exists without its task |

Horsies closes the first window by one rule: send after commit. Horsies narrows the second window with `resend_on_transient_err` and `idempotency_key`.

## What Joins a Caller Transaction

One function takes the caller's connection: `rerun_task(connection, command, policy)`. It runs in the transaction that the caller owns, and it enqueues a rerun of a terminal task. See [Action Semantics](../monitoring/action-semantics).

No other function does this:

- `.send()`, `.send_async()`, `.schedule()` and `.schedule_async()` take task keyword arguments only.
- `spec.start()` and `spec.start_async()` take an optional `workflow_id` only.

## How To

### Send After Commit

Commit the application transaction. Then send. Handle the `Err` result of the send.

```python
from sqlalchemy import Engine, text

from horsies import Err, Ok, TaskError, TaskResult, TaskSendEnqueueFailed

from myapp.instance import app


@app.task('send_receipt')
def send_receipt(*, order_id: str) -> TaskResult[None, TaskError]:
    return TaskResult(ok=None)


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
            match send_receipt.retry_send(error):
                case Ok(handle):
                    print(f'receipt task {handle.task_id}')
                case Err(retry_error):
                    print(f'receipt not queued: {retry_error.code}')
        case Err(error):
            print(f'receipt not queued: {error.code}')
```

In an async function, use `send_async()` and `retry_send_async()`.

### Resend Transient Failures

Set `resend_on_transient_err=True` in `AppConfig`. The default is `False`.

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

Rules:

- A send makes at most 4 broker calls: 1 first call and 3 resends.
- The wait before a resend starts at 200 ms and doubles. The maximum wait is 2000 ms.
- Only `TaskSendEnqueueFailed` with `retryable=True` is resent. Every other error returns at once.
- All resends of one send use the same task id. A resend of an enqueue that did commit returns the same task.
- The setting applies to `.send()`, `.send_async()`, `.schedule()` and `.schedule_async()`.

A resend lives inside one send call. It does not survive a process stop. If the process stops after the commit and before the send, no task exists.

### Make the Send Repeatable with an Idempotency Key

Pass `idempotency_key` to `with_options()`. Derive the key from the application write, for example `receipt:{order_id}`. A repeat of the same send, from a job rerun or a second process, then returns the first task and does not enqueue a second one.

Rules:

| Property | Value |
| -------- | ----- |
| Scope | The task name. Two task names can use the same key |
| Content | An opaque string. Horsies does not trim, fold case or normalize it |
| Size | 1 to 255 UTF-8 bytes. A key outside this range returns `Err(TaskSendEnqueueFailed)` with `retryable=False`, and nothing is written |
| Same key, same command | `Ok(TaskHandle)` for the first task. The handle does not show that it was a repeat |
| Same key, different command | `Err(TaskSendIdempotencyKeyConflict)`. The `owner_task_id` field names the first task |

The key holds as long as its task is live (`PENDING`, `CLAIMED` or `RUNNING`). When the task reaches a terminal state, a reservation window starts. During the window, the key still returns the first task or a conflict. After the window, the key is free for a new request.

| Window | Value |
| ------ | ----- |
| Default | 24 hours |
| Maximum | 30 days (inclusive) |
| Where it is set | The `idempotency_reservation_window` argument of `PostgresBroker`. `AppConfig` has no field for it, so `app.get_broker()` uses the default |

The command compared on a repeat includes the task name, queue, priority, kwargs, `good_until`, delay and task options. A repeat with changed kwargs is a conflict, not a replay.

A workflow start has no `idempotency_key`. `spec.start(workflow_id=...)` with a stable id is best-effort idempotent by id and does not compare the payload. See [Workflow API](../concepts/workflows/workflow-api).

## Things to Avoid

**Don't send before the commit.** A rollback leaves a task for a write that does not exist.

<!-- check: skip -- shows a wrong form next to the right one -->
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

**Don't use a random key.** A key from `uuid4()` differs on each attempt, so it never matches a repeat. Derive the key from data that the attempts share.

**Don't retry `TaskSendIdempotencyKeyConflict` in a tight loop.** The retry returns the same error until the reservation ends.

## API Reference

### `with_options(*, idempotency_key: str | None = None, ...) -> TaskSendOptions[P, T]`

See [Sending Tasks](../tasks/sending-tasks) for the full signature.

### `PostgresBroker(config, *, idempotency_reservation_window: timedelta | None = None, ...)`

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `idempotency_reservation_window` | `timedelta \| None` | 24 hours | Time that a key stays reserved after its task is terminal. Must be greater than zero and at most 30 days. The constructor raises `ValueError` outside this range |

### `AppConfig.resend_on_transient_err: bool`

Default `False`. See [App Config](../configuration/app-config).
