---
title: Producing Tasks
section: python
summary: Define tasks and send them for execution.
related: [01-configuring-horsies, 03-defining-workflows]
tags: [quickstart, tasks]
---

## Defining a Task

Decorate a function with `@app.task()` to register it. Provide a task name and the target queue:

<!-- check: skip partial -- imports a placeholder module -->
```python
from horsies import Err, Ok, Result, TaskError
from .config import app

@app.task("validate_order", queue="standard")
def validate_order(
    *,
    order: Order,
) -> Result[ValidatedOrder, TaskError]:
    if not order.order_id:
        return Err(TaskError(
            code="INVALID_ORDER_ID",
            message="Order ID is required",
        ))

    if not order.items:
        return Err(TaskError(
            code="EMPTY_ORDER",
            message="Order must contain at least one item",
        ))

    validated = ValidatedOrder(
        order_id=order.order_id,
        customer_email=order.customer_email,
        items=order.items,
        shipping_address=order.shipping_address,
        shipping_method=order.shipping_method,
        validated_at=datetime.now(),
    )
    return Ok(validated)
```

Every task returns `Result[T, TaskError]`. Use `Ok(value)` for success and `Err(TaskError(...))` for failure.

## Sending a Task

`send()` dispatches the task to its queue and returns a `TaskSendResult[TaskHandle[T]]` — a `Result` type that is either `Ok(TaskHandle)` on success or `Err(TaskSendError)` on failure:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Err, Failed, Ok, ReadWaitTimeout, Succeeded

match validate_order.send(order=order):
    case Ok(handle):
        match handle.get(wait_ms=5000):  # blocks up to 5 seconds
            case Ok(Succeeded(validated)):
                print(f"Validated: {validated.order_id}")
            case Ok(Failed(task_error)):
                print(f"Validation failed: {task_error.code}")
            case Err(ReadWaitTimeout()):
                print("Not done after 5 seconds; the task may still run")
            case Err(read_error):
                print(f"Read failed: {read_error.code} - {read_error.message}")
    case Err(send_err):
        print(f"Send failed: {send_err.code} - {send_err.message}")
```

`get()` returns `ReadResult[T, TaskError]`. `Ok(Succeeded(value))` holds the value of the task. `Ok(Failed(task_error))` holds the error of the task. `Err(read_error)` means the read gave no outcome.

`wait_ms` controls the maximum wait time. If the task does not complete within the timeout, `get()` returns `Err(ReadWaitTimeout(...))`. The task may still be running. `wait_ms` is None (no limit) or an int from 0 to `MAX_TASK_TIMEOUT_MS` (4,294,967,295). 0 checks once with no wait. Any other value returns `Err(ReadInvalidInput(...))` before any read.

## Async Usage

`send_async()` and `get_async()` are the async equivalents:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Ok, Err

match await validate_order.send_async(order=order):
    case Ok(handle):
        result = await handle.get_async(wait_ms=5000)
    case Err(send_err):
        print(f"Send failed: {send_err.code}")
```

## Delayed Execution

`with_options(delay_seconds=...)` dispatches the task after a delay (in seconds):

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Ok, Err

match validate_order.with_options(delay_seconds=5).send(order=order):
    case Ok(handle):
        print(f"Sent with a delay: {handle.task_id}")
    case Err(err):
        print(f"Send failed: {err.code}")
```
