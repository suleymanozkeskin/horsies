---
title: Error Handling
summary: Patterns for handling TaskResult errors with explicit control flow.
related: [errors, retrieving-results, retry-policy]
tags: [tasks, errors, TaskResult, patterns]
---

# Error Handling

Handle errors explicitly through dedicated handlers or explicit returns. Avoid raising exceptions - they break control flow and obscure error paths.

For exhaustive error code reference, see [errors](errors.md).

This document means to be a pattern guide rather than definitive way to handle your errors.
Adjust to your needs.

## Why and When

### Error Categories

| Category | Source | Examples | Auto-Retry? |
| -------- | ------ | -------- | ----------- |
| Retrieval errors | `handle.get()` | `WAIT_TIMEOUT`, `BROKER_ERROR` | No |
| Execution errors | Task raised exception | `TASK_EXCEPTION`, `WORKER_CRASHED` | If in `auto_retry_for` |
| Domain errors | User returned error | `"RATE_LIMITED"`, `"VALIDATION_FAILED"` | If in `auto_retry_for` |

### When to Handle Errors

Handle errors when:

- The error is a **retrieval error** (task may still complete)
- The error is **not in `auto_retry_for`** (won't be retried)
- The task has **exhausted retries** (final failure)

## How To

### Configure Automatic Retries

The library handles retries automatically when `auto_retry_for` matches the error:

```python
from horsies import RetryPolicy, TaskResult, TaskError

@app.task(
    "fetch_api_data",
    retry_policy=RetryPolicy.exponential(base_seconds=30, max_retries=3),
    auto_retry_for=["TASK_EXCEPTION", "RATE_LIMITED", "ConnectionError"],
)
def fetch_api_data(url: str) -> TaskResult[dict, TaskError]:
    # If this raises ConnectionError or returns RATE_LIMITED error,
    # the worker automatically retries up to 3 times
    ...
```

See [retry-policy](../retry-policy) for configuration details.

### Handling Upstream Failures in Chained Tasks

Tasks wired with `args_from` receive the upstream `TaskResult`, not the unwrapped value. Use this workflow example as the canonical guard pattern:

```python
from dataclasses import dataclass

from horsies import (
    Horsies,
    AppConfig,
    PostgresConfig,
    TaskResult,
    TaskError,
    TaskNode,
    OnError,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:password@localhost:5432/mydb",
    ),
)
app = Horsies(config)


@dataclass
class ProductRecord:
    product_id: str
    name: str
    price_cents: int


@app.task("fetch_product_page")
def fetch_product_page(url: str) -> TaskResult[str, TaskError]:
    if "missing" in url:
        return TaskResult(err=TaskError(
            error_code="FETCH_FAILED",
            message=f"Could not fetch {url}",
        ))
    return TaskResult(ok="<html>...</html>")


@app.task("parse_product_page")
def parse_product_page(
    page_result: TaskResult[str, TaskError],
) -> TaskResult[ProductRecord, TaskError]:
    if page_result.is_err():
        return TaskResult(err=TaskError(
            error_code="UPSTREAM_FAILED",
            message="Cannot parse product page: fetch failed",
        ))
    return TaskResult(ok=ProductRecord(
        product_id="product-123",
        name="Widget Pro",
        price_cents=1999,
    ))


@app.task("save_product_record")
def save_product_record(
    product_result: TaskResult[ProductRecord, TaskError],
) -> TaskResult[None, TaskError]:
    if product_result.is_err():
        return TaskResult(err=TaskError(
            error_code="UPSTREAM_FAILED",
            message="Cannot save product: parse failed",
        ))
    product = product_result.ok_value
    _ = product  # persist to storage
    return TaskResult(ok=None)


fetch: TaskNode[str] = TaskNode(
    fn=fetch_product_page,
    args=("https://example.com/missing",),
    node_id="fetch_product_page",
)
parse: TaskNode[ProductRecord] = TaskNode(
    fn=parse_product_page,
    waits_for=[fetch],
    args_from={"page_result": fetch},
    allow_failed_deps=True,
    node_id="parse_product_page",
)
save: TaskNode[None] = TaskNode(
    fn=save_product_record,
    waits_for=[parse],
    args_from={"product_result": parse},
    allow_failed_deps=True,
    node_id="save_product_record",
)

spec = app.workflow(
    name="product_ingest",
    tasks=[fetch, parse, save],
    output=save,
    on_error=OnError.FAIL,
)
handle = spec.start()
handle.get(timeout_ms=30000)
```

This pattern only runs when the downstream task actually executes. With `join="all"` and `allow_failed_deps=False` (default), any failed or skipped dependency causes the task to be SKIPPED, so this guard code never runs. With `join="any"` or `join="quorum"`, the task can still run if the success threshold is met. See [Workflow Semantics](../concepts/workflows/workflow-semantics) for default join rules and failure propagation.

### Handle Errors with Explicit Returns

Prefer explicit returns over raising exceptions:

```python
from horsies import LibraryErrorCode
from instance import my_task

def process_task() -> str | None:
    handle = my_task.send(10, 20)
    result = handle.get(timeout_ms=5000)

    if result.is_err():
        error = result.err
        match error.error_code:
            case LibraryErrorCode.WAIT_TIMEOUT:
                # Task may still be running
                # check status again using task id if you need absolute decisions
            case LibraryErrorCode.TASK_NOT_FOUND:
                return None
            case _:
                # Pass to centralized handler
                handle_error(error)
                return None

    return result.ok
```

### Centralized Error Handler

Create a handler that **returns actions**, not just logs.

## Things to Avoid

**Don't raise exceptions for flow control.**

```python
# Wrong - exceptions break control flow
if result.is_err():
    raise RuntimeError(f"Task failed: {result.err.error_code}")

# Correct - explicit return and handling
if result.is_err():
    handle_error(result.err)
    return None
```

**Don't ignore errors after logging.**

```python
# Wrong - logs but continues as if nothing happened
if result.is_err():
    print(result.err.message)
# Code continues...

# Correct - handle and return explicitly
if result.is_err():
    handle_error(result.err)
    return result.err.error_code
```

**Don't manually retry errors that should be auto-retried.**

```python
# Wrong - duplicates library retry logic
if error.error_code == "RATE_LIMITED":
    manually_schedule_retry(task_id)

# Correct - configure auto_retry_for on the task
@app.task(
    "my_task",
    retry_policy=RetryPolicy.fixed([60, 120, 300]),
    auto_retry_for=["RATE_LIMITED"],
)
def my_task() -> TaskResult[str, TaskError]:
    ...
```
