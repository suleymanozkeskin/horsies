---
title: Retry Policy
summary: Automatic retry behavior with fixed or exponential backoff.
related: [defining-tasks, ../../concepts/task-lifecycle]
tags: [tasks, retry, backoff]
---

## Basic Usage

```python
from horsies import RetryPolicy

@app.task("flaky_task", retry_policy=RetryPolicy.fixed([60, 300, 900]))
def flaky_task() -> TaskResult[str, TaskError]:
    # Will retry up to 3 times with delays: 1min, 5min, 15min
    ...
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | `int` | 3 | Number of retry attempts (1-20) |
| `intervals` | `list[int]` | [60, 300, 900] | Delay intervals in seconds |
| `backoff_strategy` | `str` | "fixed" | "fixed" or "exponential" |
| `jitter` | `bool` | `True` | Add random variation to delays |

## Backoff Strategies

### Fixed Backoff

Uses exact intervals from the list. The list length must match `max_retries`.

```python
# Retry 3 times: wait 1min, then 5min, then 15min
RetryPolicy.fixed([60, 300, 900])

# Equivalent to:
RetryPolicy(
    max_retries=3,
    intervals=[60, 300, 900],
    backoff_strategy='fixed',
)
```

### Exponential Backoff

Uses a base interval multiplied by 2^(attempt-1).

```python
# Base 30s: retry at 30s, 60s, 120s, 240s, 480s
RetryPolicy.exponential(base_seconds=30, max_retries=5)

# Equivalent to:
RetryPolicy(
    max_retries=5,
    intervals=[30],  # Single base interval
    backoff_strategy='exponential',
)
```

## Jitter

When `jitter=True` (default), delays are randomized by +/-25%:

- 60 second base -> 45-75 seconds actual
- Prevents thundering herd when many tasks retry simultaneously

```python
# Disable jitter for predictable delays
RetryPolicy.fixed([60, 300, 900], jitter=False)
```

## Auto-Retry Triggers

Retries only happen when specific conditions are met. Configure via `auto_retry_for`:

```python
@app.task(
    "api_call",
    retry_policy=RetryPolicy.fixed([30, 60, 120]),
    auto_retry_for=["RATE_LIMITED", "SERVICE_UNAVAILABLE"],
)
def api_call() -> TaskResult[dict, TaskError]:
    ...
```

`auto_retry_for` accepts:

- Error codes from `TaskError`: `"RATE_LIMITED"`, `"SERVICE_UNAVAILABLE"`
- Library error codes: `"UNHANDLED_EXCEPTION"`, `"WORKER_CRASHED"`
- Codes must use `UPPER_SNAKE_CASE` (exception class names like `"TimeoutError"` are rejected)

## Exception Mapper

Map unhandled exceptions to error codes without try/except boilerplate. When a task raises an exception, the mapper matches the exact exception class (`type(exc)`).

### Per-Task Mapper

```python
@app.task(
    "call_api",
    retry_policy=RetryPolicy.fixed([30, 60, 120]),
    auto_retry_for=["TIMEOUT", "CONNECTION_ERROR"],
    exception_mapper={
        TimeoutError: "TIMEOUT",
        ConnectionError: "CONNECTION_ERROR",
    },
)
def call_api() -> TaskResult[dict, TaskError]:
    # No try/except needed — TimeoutError becomes "TIMEOUT" automatically
    response = requests.get("https://api.example.com", timeout=10)
    return TaskResult(ok=response.json())
```

### Global Mapper

Set a global mapper on `AppConfig` to apply to all tasks:

```python
config = AppConfig(
    broker=PostgresConfig(database_url="postgresql+psycopg://..."),
    exception_mapper={
        TimeoutError: "TIMEOUT",
        ConnectionError: "CONNECTION_ERROR",
        PermissionError: "PERMISSION_DENIED",
    },
    default_unhandled_error_code="UNHANDLED_EXCEPTION",
)
```

### Resolution Order

When an unhandled exception is caught:

1. Per-task `exception_mapper` (exact class lookup)
2. Global `AppConfig.exception_mapper` (exact class lookup)
3. Per-task `default_unhandled_error_code`
4. Global `AppConfig.default_unhandled_error_code` (defaults to `"UNHANDLED_EXCEPTION"`)

Per-task mapper entries take priority over global. If the task function returns `TaskResult(err=...)` explicitly, the mapper is never invoked.

Only exact class matches count — subclasses are not matched. If you need to handle a subclass, map it explicitly.

## How Retries Work

1. Task fails with matching error code
2. Worker checks `retry_count < max_retries`
3. If retries remaining, task status set to PENDING
4. `next_retry_at` calculated from retry policy
5. Task not claimable until `next_retry_at` passes
6. Worker sends delayed notification to trigger claiming

## Retry Count Tracking

| Field | Description |
|-------|-------------|
| `retry_count` | Current number of retry attempts |
| `max_retries` | Maximum attempts allowed |
| `next_retry_at` | When task becomes claimable again |

Access via database or result:

```python
result = handle.get()
if result.is_err():
    error = result.unwrap_err()
    # Check if retries exhausted
    if "retry" in str(error.data):
        print("All retries exhausted")
```

## Validation

The policy validates consistency:

```python
# This raises ValueError:
RetryPolicy(
    max_retries=3,
    intervals=[60, 300],  # Only 2 intervals for 3 retries
    backoff_strategy='fixed',
)

# This also raises ValueError:
RetryPolicy(
    max_retries=3,
    intervals=[60, 300, 900],  # Multiple intervals
    backoff_strategy='exponential',  # Exponential needs exactly 1 interval
)
```

## Examples

### API with Rate Limiting

```python
@app.task(
    "call_external_api",
    retry_policy=RetryPolicy.exponential(base_seconds=60, max_retries=5),
    auto_retry_for=["RATE_LIMITED", "SERVICE_UNAVAILABLE"],
)
def call_external_api() -> TaskResult[dict, TaskError]:
    try:
        response = requests.get("https://api.example.com")
        if response.status_code == 429:
            return TaskResult(err=TaskError(error_code="RATE_LIMITED"))
        return TaskResult(ok=response.json())
    except requests.Timeout:
        return TaskResult(err=TaskError(error_code="TIMEOUT"))
```

### Database Transaction with Deadlock Retry

```python
@app.task(
    "update_inventory",
    retry_policy=RetryPolicy.fixed([1, 2, 5]),  # Quick retries
    auto_retry_for=["DEADLOCK"],
)
def update_inventory(item_id: int, delta: int) -> TaskResult[None, TaskError]:
    try:
        db.update_stock(item_id, delta)
        return TaskResult(ok=None)
    except DeadlockDetected:
        return TaskResult(err=TaskError(error_code="DEADLOCK", message="Deadlock detected"))
```
