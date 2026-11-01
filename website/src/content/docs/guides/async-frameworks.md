---
title: Async Frameworks
summary: Own the broker in a FastAPI lifespan, call the async forms from coroutines, and stay inside the event-loop limit.
related: [../configuration/broker-config, ../quick-start/getting-started, ../tasks/sending-tasks]
tags: [guides, async, fastapi, asgi, broker, event-loop]
---

# Async Frameworks

An ASGI application runs endpoint code on an event loop. Horsies has a sync form and an async form of each producer call. On such a loop, use the async form.

## Why and When

Read this page when a web framework (FastAPI, Starlette, or another ASGI framework) sends tasks or starts workflows.

The sync forms block their thread. On a thread that runs an event loop, the block would stop every coroutine on that loop. For this reason the sync forms refuse to run there. See [Async Code and Jupyter](/horsies/configuration/broker-config/#async-code-and-jupyter) for the full table of sync calls and their `ASYNC_CONTEXT` results.

## How To

### Own the Broker in the Lifespan

`app.get_broker()` creates the broker once per process. A configuration error raises at that call, so a call in the lifespan fails the start of the server. The call opens no connection. Connections open on the first database call.

Close the broker at shutdown with `close_async()`. It returns a `BrokerResult`. Handle the `Err` side.

```python
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from horsies import Err, Ok

from instance import app as horsies_app
from tasks import send_welcome_email

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_api: FastAPI) -> AsyncIterator[None]:
    broker = horsies_app.get_broker()
    try:
        yield
    finally:
        match await broker.close_async():
            case Ok():
                pass
            case Err(close_error):
                logger.error("broker close failed: %s", close_error.message)


api = FastAPI(lifespan=lifespan)
```

### Call the Async Forms in Coroutines

An `async def` endpoint uses `send_async()`, `start_async()` and `get_async()`.

```python
@api.post("/welcome/{address}")
async def welcome(address: str) -> dict[str, str]:
    match await send_welcome_email.send_async(address=address):
        case Ok(handle):
            return {"task_id": handle.task_id}
        case Err(send_error):
            raise HTTPException(status_code=503, detail=send_error.code.value)
```

A sync call in this endpoint returns `Err` with `ASYNC_CONTEXT`. Nothing is enqueued. The error keeps the payload, so `retry_send_async()` can send it.

### Use the Sync Forms Only in Plain `def` Endpoints

FastAPI runs a plain `def` endpoint in a thread pool. That thread has no event loop, so the sync form is allowed. The sync form runs on the broker's own runner loop.

```python
@api.post("/welcome-blocking/{address}")
def welcome_blocking(address: str) -> dict[str, str]:
    match send_welcome_email.send(address=address):
        case Ok(handle):
            return {"task_id": handle.task_id}
        case Err(send_error):
            raise HTTPException(status_code=503, detail=send_error.code.value)
```

Prefer one form per application. A mix of both forms opens one more connection set. See [Connection Ceiling](/horsies/configuration/broker-config/#connection-ceiling).

## The Event-Loop Limit

A broker keeps one resource set (pools and a listener) for each event loop that calls it. It serves at most 5 open caller loops, plus its own sync runner loop (`MAX_CALLER_EVENT_LOOPS = 5`).

A call from one more open loop returns `EVENT_LOOP_LIMIT` in the error type of that call. Nothing runs. The error clears when one of the open loops closes.

An ASGI server such as uvicorn runs one loop for each process, so a server stays far below the limit. The limit matters for code that creates loops itself, and for test runs. See [Event Loops](/horsies/configuration/broker-config/#event-loops).

## Things to Avoid

**Don't call sync `close()` in a coroutine.** It returns `ASYNC_CONTEXT` and closes nothing.

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong
broker.close()

# Correct
await broker.close_async()
```

**Don't create an event loop for each call.** Each open loop keeps its resource set. The sixth open loop gets `EVENT_LOOP_LIMIT`.

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong: each call leaves one more open loop
loop = asyncio.new_event_loop()
loop.run_until_complete(horsies_app.get_broker().ping_database_async())

# Correct: asyncio.run() closes its loop
asyncio.run(horsies_app.get_broker().ping_database_async())
```
