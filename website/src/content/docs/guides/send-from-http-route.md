---
title: Send from an HTTP Route
section: python
summary: Send a task from a FastAPI route, answer 202 with the task id, map each send error to a status, read the task by its id, and see what resend_on_transient_error changes.
related: [../tasks/sending-tasks, ../tasks/error-types, ../operations/atomic-enqueue-and-topology, ./async-frameworks]
tags: [guides, fastapi, send, idempotency, TaskSendError, resend_on_transient_error]
---

# Send from an HTTP Route

A route sends a task and answers at once. The task runs later in a worker. The route answers 202 with the task id. A second route reads the task by that id.

A send returns a result. It does not raise. The route must handle the `Err` side.

This page uses the Acme Clothing tasks `authorize_payment` and `send_shipping_sms`. The files are `shop/horsies_app.py`, `shop/tasks/payments.py`, `shop/tasks/notify.py` and the new file `shop/api.py`.

## Why and When

Read this page when a web route starts work that must not block the request.

- The route sends the task and does not wait for it.
- The caller gets the task id and asks for the outcome later.
- A failed send becomes an HTTP status. The route never hides it.

For the lifespan of the app and for the sync forms in a plain `def` route, see [Async Frameworks](/horsies/guides/async-frameworks/).

## How To

### Send and Answer 202

An `async def` route uses `send_async`. A blocking `send` in an `async def` route returns `Err(TaskSendAsyncContext)` with the code `ASYNC_CONTEXT`. Nothing is written.

Create `shop/api.py`:

<!-- check: skip partial -- imports the task of the layout of the example as a module -->
```python
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from horsies import (
    Err,
    Ok,
    TaskSendAsyncContext,
    TaskSendEnqueueFailed,
    TaskSendError,
    TaskSendIdempotencyKeyConflict,
    TaskSendPayloadMismatch,
    TaskSendPayloadTooLarge,
    TaskSendSuppressed,
    TaskSendValidationFailed,
)
from pydantic import BaseModel

from shop.tasks.payments import authorize_payment

api = FastAPI()


class AuthorizePaymentRequest(BaseModel):
    amount_cents: int


def send_error_response(send_error: TaskSendError) -> JSONResponse:
    error = {"error": send_error.code.value}
    match send_error:
        case TaskSendEnqueueFailed():
            return JSONResponse(error, status_code=503)
        case TaskSendIdempotencyKeyConflict(owner_task_id=owner_task_id):
            return JSONResponse(
                {**error, "owner_task_id": str(owner_task_id)},
                status_code=409,
            )
        case TaskSendValidationFailed():
            return JSONResponse(error, status_code=422)
        case TaskSendPayloadTooLarge():
            return JSONResponse(error, status_code=413)
        case TaskSendSuppressed() | TaskSendAsyncContext() | TaskSendPayloadMismatch():
            return JSONResponse(error, status_code=500)


@api.post("/orders/{order_id}/payment", status_code=202)
async def authorize(order_id: str, body: AuthorizePaymentRequest) -> JSONResponse:
    match await authorize_payment.send_async(
        order_id=order_id,
        amount_cents=body.amount_cents,
    ):
        case Ok(handle):
            return JSONResponse({"task_id": str(handle.task_id)}, status_code=202)
        case Err(send_error):
            return send_error_response(send_error)
```

Run the service in a process that is not the worker:

```bash
uvicorn shop.api:api
```

- `send_async` returns `Ok(handle)` or `Err(send_error)`. `handle.task_id` is the id of the stored task.
- `TaskSendError` is a closed union. The `match` in `send_error_response` is exhaustive. Pyright in strict mode refuses a `match` that leaves a variant out.
- The response holds the code of the error and no input value.

### What Each Send Error Means for the Queue

The status of a failed send tells the caller what to do. It also says what the queue holds.

| Send error | Status | Queue state | Caller action |
|------------|--------|-------------|---------------|
| `TaskSendEnqueueFailed` | 503 | The row may exist or not. The database failed during the send | If `retryable` is true, repeat the request with the same idempotency key. If it is false, a repeat does not clear the cause |
| `TaskSendIdempotencyKeyConflict` | 409 | Nothing written | Send the same command, or use another key |
| `TaskSendValidationFailed` | 422 | Nothing written | Change the input |
| `TaskSendPayloadTooLarge` | 413 | Nothing written | Send a smaller input |
| `TaskSendSuppressed`, `TaskSendAsyncContext`, `TaskSendPayloadMismatch` | 500 | Nothing written | None. These are defects of the service or its setup |

A 503 does not mean that nothing is queued. The row may exist whatever the value of `retryable`. A send whose commit reply was lost has a stored row. A repeat of the request without a key makes a second task. The next section removes that risk.

### Take the Idempotency Key from the Request

A client that repeats a request after a 503 sends the same `Idempotency-Key` header each time. The route passes the header to the send:

<!-- check: skip partial -- replaces the route of the block above -->
```python
from typing import Annotated

from fastapi import Header


@api.post("/orders/{order_id}/payment", status_code=202)
async def authorize(
    order_id: str,
    body: AuthorizePaymentRequest,
    idempotency_key: Annotated[str, Header()],
) -> JSONResponse:
    sender = authorize_payment.with_options(idempotency_key=idempotency_key)
    match await sender.send_async(
        order_id=order_id,
        amount_cents=body.amount_cents,
    ):
        case Ok(handle):
            return JSONResponse({"task_id": str(handle.task_id)}, status_code=202)
        case Err(send_error):
            return send_error_response(send_error)
```

- A repeat with the same key and the same command returns `Ok(handle)` of the first task. The route answers 202 with the same task id.
- A send with the same key and another command is `Err(TaskSendIdempotencyKeyConflict)`. The route answers 409. `owner_task_id` names the task that holds the key.
- The conflict is not retryable. A repeat of the same call returns the same conflict while the other task holds the key. `resend` refuses it. The caller sends the same command, or uses another key.
- The reservation of a key ends `reservation_window` after its task finishes. A send with the key after that time starts a new task.
- A key must have 1 to 255 UTF-8 bytes. FastAPI answers 422 when the header is missing. A key outside the size is `Err(TaskSendValidationFailed)`.

### Send Without Reading the Result

Some sends have no result that the route needs. The route logs the failed send and answers as usual. The send of a shipping text is an example.

Create the second route in `shop/api.py`:

<!-- check: skip partial -- adds a route to the file of the blocks above -->
```python
import logging

from shop.tasks.notify import send_shipping_sms

logger = logging.getLogger(__name__)


class ShippedRequest(BaseModel):
    tracking_code: str


@api.post("/orders/{order_id}/shipped")
async def mark_shipped(order_id: str, body: ShippedRequest) -> dict[str, str]:
    # Stand-in: the order row is updated here, in the shop database.
    match await send_shipping_sms.send_async(
        order_id=order_id,
        tracking_code=body.tracking_code,
    ):
        case Ok():
            pass
        case Err(send_error):
            logger.warning(
                "shipping sms not queued: order=%s code=%s",
                order_id,
                send_error.code.value,
            )
    return {"status": "shipped"}
```

- The `Ok` arm drops the handle. No code reads the task.
- The `Err` arm logs the order id and the code. The route still answers 200, because the order is shipped.
- A logged `ENQUEUE_FAILED` can still have a stored task. The text can be sent. Do not use this form for a send that must happen exactly one time. Use a key for that send.

### Read the Task by Its Id

The status route reads the task from the database. It does not need the handle of the send. Any process of the service can answer it.

<!-- check: skip partial -- adds a route to the file of the blocks above -->
```python
from uuid import UUID

from horsies import Failed, ReadTaskNotFound, ReadWaitTimeout, Succeeded

from shop.horsies_app import app


@api.get("/payments/{task_id}")
async def payment_status(task_id: UUID) -> JSONResponse:
    match await app.get_result_async(task_id, wait_ms=0):
        case Ok(Succeeded(authorization)):
            return JSONResponse(
                {"state": "succeeded", "authorization": authorization.model_dump(mode="json")},
            )
        case Ok(Failed(task_error)):
            return JSONResponse({"state": "failed", "code": f"{task_error.code}"})
        case Err(ReadWaitTimeout()):
            return JSONResponse({"state": "pending"})
        case Err(ReadTaskNotFound()):
            return JSONResponse({"error": "TASK_NOT_FOUND"}, status_code=404)
        case Err(read_error):
            return JSONResponse({"error": read_error.code.value}, status_code=503)
```

- `wait_ms=0` reads one time and does not wait. A task that has not ended gives `Err(ReadWaitTimeout)`. The route answers `pending`.
- The three arms of a read are `Ok(Succeeded(value))`, `Ok(Failed(task_error))` and `Err(read_error)`. The last arm names the two read errors that this route answers on its own.
- `Failed` holds the error that the task returned, or a code that the library recorded, for example `TASK_TIMEOUT`.
- `ReadTaskNotFound` means that no stored task has this id. A task from a 503 send may not exist.
- The route needs the task module in the process, so that the result is read with the type `PaymentAuthorization`. The import of `authorize_payment` does that.

### Resend Transient Errors in the Library

`resend_on_transient_error` in `AppConfig` is off by default. Turn it on to let the library send again before the route sees an error:

<!-- check: skip partial -- shows the one changed field of the app config -->
```python
config = AppConfig(
    database=PostgresConfig(
        database_url=SecretStr("postgresql://user:password@localhost:5432/mydb"),
    ),
    resend_on_transient_error=True,
)
```

The setting changes what the route sees:

- Only `TaskSendEnqueueFailed` with `retryable=True` is sent again. Every other error returns at once.
- The library sends at most 3 more times. The waits are 200 ms, 400 ms and 800 ms.
- Each resend uses the same task id and payload, and the same idempotency key. A first attempt that was stored answers with its task id. The task is not queued two times.
- The route gets `Ok(handle)` after one of the attempts, or the `TaskSendEnqueueFailed` of the last attempt. The route still maps that error to 503.
- A send that must resend waits at least 1,400 ms in total, plus the time of each attempt. In `send_async` the wait does not block the event loop. Set the timeout of the HTTP client and of the proxy above this time.

## Things to Avoid

**Do not call the blocking `send` in an `async def` route.** It returns `Err(TaskSendAsyncContext)` and writes nothing.

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong
authorize_payment.send(order_id=order_id, amount_cents=amount_cents)

# Correct
await authorize_payment.send_async(order_id=order_id, amount_cents=amount_cents)
```

**Do not answer 503 and tell the caller that nothing is queued.** Only `TaskSendEnqueueFailed` leaves the queue state open. Use a key to make a repeat safe.

**Do not repeat a send after a key conflict.** The same call returns the same conflict.

**Do not wait for the task in the send route.** A route that waits holds the request for the time of the task. Answer 202 and read in the status route.
