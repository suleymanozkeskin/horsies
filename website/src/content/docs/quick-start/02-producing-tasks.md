---
title: Producing Tasks
summary: Define tasks and send them for execution.
related: [01-configuring-horsies, 03-defining-workflows]
tags: [quickstart, tasks]
---

## Defining a Task

Decorate a function with `@app.task()` to register it. Provide a task name and the target queue:

```python
from horsies import TaskResult, TaskError
from .config import app

@app.task("validate_order", queue_name="standard")
def validate_order(
    order: Order,
) -> TaskResult[ValidatedOrder, TaskError]:
    if not order.order_id:
        return TaskResult(err=TaskError(
            error_code="INVALID_ORDER_ID",
            message="Order ID is required",
        ))

    if not order.items:
        return TaskResult(err=TaskError(
            error_code="EMPTY_ORDER",
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
    return TaskResult(ok=validated)
```

Every task returns `TaskResult[T, TaskError]`. Use `TaskResult(ok=value)` for success and `TaskResult(err=TaskError(...))` for failure.

## Sending a Task

`send()` dispatches the task to its queue and returns a `TaskSendResult[TaskHandle[T]]` — a `Result` type that is either `Ok(TaskHandle)` on success or `Err(TaskSendError)` on failure:

```python
from horsies import Ok, Err

match validate_order.send(order):
    case Ok(handle):
        result = handle.get(timeout_ms=5000)  # blocks up to 5 seconds
    case Err(send_err):
        print(f"Send failed: {send_err.code} - {send_err.message}")
```

`timeout_ms` controls the maximum wait time. If the task does not complete within the timeout, `get()` returns `TaskResult(err=TaskError(error_code=LibraryErrorCode.WAIT_TIMEOUT, ...))`. The task may still be running.

## Async Usage

`send_async()` and `get_async()` are the async equivalents:

```python
from horsies import Ok, Err

match await validate_order.send_async(order):
    case Ok(handle):
        result = await handle.get_async(timeout_ms=5000)
    case Err(send_err):
        print(f"Send failed: {send_err.code}")
```

## Delayed Execution

`schedule()` dispatches the task after a delay (in seconds):

```python
from horsies import Ok, Err

match validate_order.schedule(5, order):
    case Ok(handle):
        print(f"Scheduled: {handle.task_id}")
    case Err(err):
        print(f"Schedule failed: {err.code}")
```
