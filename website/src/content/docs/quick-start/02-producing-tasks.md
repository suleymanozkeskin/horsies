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

`send()` dispatches the task to its queue and returns a `TaskSendResult[TaskHandle[T]]`. Unwrap the result to get the handle, then call `TaskHandle.get()` to block until the task result is available:

```python
handle = validate_order.send(order).unwrap()  # raises on send failure
result = handle.get(timeout_ms=5000)  # blocks up to 5 seconds
```

For production code, check the result explicitly:

```python
from horsies import is_ok

send_result = validate_order.send(order)
if is_ok(send_result):
    handle = send_result.ok_value
    result = handle.get(timeout_ms=5000)
```

`timeout_ms` controls the maximum wait time. If the task does not complete within the timeout, `get()` raises a `TimeoutError`.

## Async Usage

`send_async()` and `get_async()` are the async equivalents:

```python
send_result = await validate_order.send_async(order)
handle = send_result.unwrap()
result = await handle.get_async(timeout_ms=5000)
```

## Delayed Execution

`schedule()` dispatches the task after a delay (in seconds):

```python
handle = validate_order.schedule(5, order).unwrap()  # dispatched after 5 seconds
```
