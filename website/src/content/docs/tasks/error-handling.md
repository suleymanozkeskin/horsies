---
title: Error Handling
section: python
summary: Patterns for handling task errors and read errors with explicit control flow.
related: [errors, retrieving-results, retry-policy]
tags: [tasks, errors, Result, TaskOutcome, ReadError, patterns]
---

# Error Handling

Handle errors explicitly through dedicated handlers or explicit returns. Avoid raising exceptions - they break control flow and obscure error paths.

For exhaustive error code reference, see [errors](/horsies/tasks/errors/).

This document means to be a pattern guide rather than definitive way to handle your errors.
Adjust to your needs.

## Why and When

### Error Categories

| Category | Source | Examples | Auto-Retry? |
| -------- | ------ | -------- | ----------- |
| Read errors | `handle.get()` gave no outcome | `ReadWaitTimeout`, `ReadDatabaseError` | No |
| Execution errors | Task raised an exception, or its worker process died | `UNHANDLED_EXCEPTION`, `TASK_EXCEPTION`, `WORKER_CRASHED` | If in `retry_for` |
| Domain errors | User returned error | `"RATE_LIMITED"`, `"VALIDATION_FAILED"` | If in `retry_for` |

A read gives an execution error or a domain error as `Ok(Failed(task_error))`. It gives a read error as `Err(read_error)`. The read errors are on [ReadError](/horsies/concepts/result-handling/#readerror).

### When to Handle Errors

Handle errors when:

- The error is a **read error** (task may still complete)
- The error is **not in `retry_for`** (won't be retried)
- The task has **exhausted retries** (final failure)

## How To

### Configure Automatic Retries

The library handles retries automatically when `retry_for` matches the error. The entries are code members: built-in members, and members of the `Enum` class that the task declares with `error_codes=`:

```python
from enum import Enum

from horsies import OperationalCode, RetryPolicy, Result, TaskError, JsonValue


class FetchCode(str, Enum):
    RATE_LIMITED = "RATE_LIMITED"
    TIMEOUT = "TIMEOUT"


@app.task(
    "fetch_api_data",
    error_codes=FetchCode,
    retry_policy=RetryPolicy.exponential(
        base_seconds=30,
        max_retries=3,
        retry_for=[OperationalCode.UNHANDLED_EXCEPTION, FetchCode.RATE_LIMITED],
    ),
)
def fetch_api_data(*, url: str) -> Result[dict[str, JsonValue], TaskError]:
    # If this returns a RATE_LIMITED error or raises an unhandled exception,
    # the worker automatically retries up to 3 times
    ...
```

To avoid try/except boilerplate for mapping exceptions to error codes, use `exception_mapper`:

> **Exact-class matching.** The mapper looks up `type(exc)` exactly —
> subclasses do **not** inherit a parent's mapping. Mapping
> `ConnectionError` will not catch `ConnectionResetError`; map each
> concrete class you expect (this matters when retry policies key on
> the mapped error code).

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task(
    "fetch_api_data",
    error_codes=FetchCode,
    retry_policy=RetryPolicy.exponential(
        base_seconds=30,
        max_retries=3,
        retry_for=[FetchCode.RATE_LIMITED, FetchCode.TIMEOUT],
    ),
    exception_mapper={
        RateLimitError: "RATE_LIMITED",
        TimeoutError: "TIMEOUT",
    },
)
def fetch_api_data(*, url: str) -> Result[dict[str, JsonValue], TaskError]:
    # Unhandled RateLimitError/TimeoutError automatically mapped and retried
    ...
```

The `RetryPolicy` API is in [Configure Retries](/horsies/tasks/defining-tasks/#configure-retries). The retry rules are on [Retry Policy](/horsies/tasks/retry-policy/).

### Handling Upstream Failures in Chained Tasks

Tasks wired with `args_from` receive the upstream `TaskOutcome`: `Succeeded(value)` or `Failed(task_error)`, not just the raw success value. Use this workflow example as the canonical guard pattern:

```python
from pydantic import SecretStr
from dataclasses import dataclass

from horsies import (
    Horsies,
    AppConfig,
    PostgresConfig,
    Result,
    TaskError,
    TaskNode,
    TaskOutcome,
    Succeeded,
    Failed,
    OnError,
    Ok,
    Err,
)

config = AppConfig(
    database=PostgresConfig(
        database_url=SecretStr("postgresql://user:password@localhost:5432/mydb"),
    ),
)
app = Horsies(config)


@dataclass
class ProductRecord:
    product_id: str
    name: str
    price_cents: int


@app.task("fetch_product_page")
def fetch_product_page(*, url: str) -> Result[str, TaskError]:
    if "missing" in url:
        return Err(TaskError(
            code="FETCH_FAILED",
            message=f"Could not fetch {url}",
        ))
    return Ok("<html>...</html>")


@app.task("parse_product_page")
def parse_product_page(
    *,
    page_result: TaskOutcome[str, TaskError],
) -> Result[ProductRecord, TaskError]:
    match page_result:
        case Failed():
            return Err(TaskError(
                code="UPSTREAM_FAILED",
                message="Cannot parse product page: fetch failed",
            ))
        case Succeeded():
            return Ok(ProductRecord(
                product_id="product-123",
                name="Widget Pro",
                price_cents=1999,
            ))


@app.task("save_product_record")
def save_product_record(
    *,
    product_result: TaskOutcome[ProductRecord, TaskError],
) -> Result[None, TaskError]:
    match product_result:
        case Failed():
            return Err(TaskError(
                code="UPSTREAM_FAILED",
                message="Cannot save product: parse failed",
            ))
        case Succeeded(product):
            _ = product  # persist to storage
            return Ok(None)


fetch: TaskNode[str] = TaskNode(
    function=fetch_product_page,
    kwargs={'url': "https://example.com/missing"},
    node_id="fetch_product_page",
)
parse: TaskNode[ProductRecord] = TaskNode(
    function=parse_product_page,
    waits_for=[fetch],
    args_from={"page_result": fetch},
    allow_failed_dependencies=True,
    node_id="parse_product_page",
)
save: TaskNode[None] = TaskNode(
    function=save_product_record,
    waits_for=[parse],
    args_from={"product_result": parse},
    allow_failed_dependencies=True,
    node_id="save_product_record",
)

spec = app.workflow(
    name="product_ingest",
    nodes=[fetch, parse, save],
    output=save,
    on_error=OnError.FAIL,
    definition_key="myapp.product_ingest.v1",
)
match spec.start():
    case Ok(handle):
        handle.get(wait_ms=30000)
    case Err(start_err):
        raise RuntimeError(f"Start failed: {start_err.code}")
```

This pattern only runs when the downstream task actually executes. With `join="all"` and `allow_failed_dependencies=False` (default), any failed or skipped dependency causes the task to be SKIPPED, so this guard code never runs. With `join="any"` or `join="quorum"`, the task can still run if the success threshold is met. See [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/) for default join rules and failure propagation.

### Handle Errors with Explicit Returns

Prefer explicit returns over raising exceptions:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Err, Failed, Ok, ReadTaskNotFound, ReadWaitTimeout, Succeeded
from instance import my_task

def process_task() -> str | None:
    match my_task.send(x=10, y=20):
        case Err(send_err):
            print(f"Send failed: {send_err.code} - {send_err.message}")
            return None
        case Ok(handle):
            pass

    match handle.get(wait_ms=5000):
        case Ok(Succeeded(value)):
            return value
        case Ok(Failed(task_error)):
            # Pass to centralized handler
            handle_error(task_error)
            return None
        case Err(ReadWaitTimeout()):
            # Task may still be running
            # check status again using task id if you need absolute decisions
            return None
        case Err(ReadTaskNotFound()):
            return None
        case Err(read_error):
            # Pass to centralized handler
            handle_read_error(read_error)
            return None
```

### Centralized Error Handler

Create a handler that **returns actions**, not just logs.

## Things to Avoid

**Don't raise exceptions for flow control.**

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong - exceptions break control flow
match handle.get():
    case Ok(Failed(task_error)):
        raise RuntimeError(f"Task failed: {task_error.code}")
    case _:
        ...  # the other arms

# Correct - explicit return and handling
match handle.get():
    case Ok(Failed(task_error)):
        handle_error(task_error)
        return None
    case _:
        ...  # the other arms
```

**Don't ignore errors after logging.**

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong - logs but continues as if nothing happened
match handle.get():
    case Ok(Failed(task_error)):
        print(task_error.message)
    case _:
        pass
# Code continues...

# Correct - handle and return explicitly
match handle.get():
    case Ok(Failed(task_error)):
        handle_error(task_error)
        return task_error.code
    case _:
        ...  # the other arms
```

**Don't manually retry errors that should be auto-retried.**

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong - duplicates library retry logic
if error.code == "RATE_LIMITED":
    manually_schedule_retry(task_id)

# Correct - configure retry_for on the retry policy
@app.task(
    "my_task",
    error_codes=FetchCode,
    retry_policy=RetryPolicy.fixed([60, 120, 300], retry_for=[FetchCode.RATE_LIMITED]),
)
def my_task() -> Result[str, TaskError]:
    ...
```
