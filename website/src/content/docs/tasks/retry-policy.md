---
title: Retry Policy
summary: Automatic retry behavior with fixed or exponential backoff.
related: [defining-tasks, ../../concepts/task-lifecycle]
tags: [tasks, retry, backoff]
---

## Basic Usage

```python
from horsies import RetryPolicy, TaskResult, TaskError, JsonValue

@app.task(
    "flaky_task",
    retry_policy=RetryPolicy.fixed([60, 300, 900], auto_retry_for=["TRANSIENT_ERROR"]),
)
def flaky_task() -> TaskResult[str, TaskError]:
    # Will retry up to 3 times with delays: 1min, 5min, 15min
    ...
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_retry_for` | `list[str \| BuiltInTaskCode]` | (required) | Error codes that trigger automatic retries |
| `max_retries` | `int` | 3 | Number of retry attempts (1-20) |
| `intervals` | `list[int]` | [60, 300, 900] | Delay intervals in seconds |
| `backoff_strategy` | `str` | "fixed" | "fixed" or "exponential" |
| `jitter` | `bool` | `True` | Add random variation to delays |
| `max_delay_seconds` | `int \| None` | `None` | Optional upper bound on the computed delay (positive int); `None` = uncapped |

## Backoff Strategies

### Fixed Backoff

Uses exact intervals from the list. The list length must match `max_retries`.

```python
# Retry 3 times: wait 1min, then 5min, then 15min
RetryPolicy.fixed([60, 300, 900], auto_retry_for=["TRANSIENT_ERROR"])

# Equivalent to:
RetryPolicy(
    max_retries=3,
    intervals=[60, 300, 900],
    backoff_strategy='fixed',
    auto_retry_for=["TRANSIENT_ERROR"],
)
```

### Exponential Backoff

Uses a base interval multiplied by 2^(attempt-1).

```python
# Base 30s: retry at 30s, 60s, 120s, 240s, 480s
RetryPolicy.exponential(base_seconds=30, max_retries=5, auto_retry_for=["TRANSIENT_ERROR"])

# Equivalent to:
RetryPolicy(
    max_retries=5,
    intervals=[30],  # Single base interval
    backoff_strategy='exponential',
    auto_retry_for=["TRANSIENT_ERROR"],
)
```

## Jitter

When `jitter=True` (default), the computed delay is floored at 1 second and then
randomized **upward** by 0–25%:

- 60 second base -> 60-75 seconds actual
- Applied after the floor, so 1.0s is the true bottom of the range (no lower-half
  collapse at small base delays)
- Prevents thundering herd when many tasks retry simultaneously

```python
# Disable jitter for predictable delays
RetryPolicy.fixed([60, 300, 900], auto_retry_for=["TRANSIENT_ERROR"], jitter=False)
```

## Maximum Delay Cap

Exponential backoff is otherwise unbounded (`base * 2^(attempt-1)`), so a high
`max_retries` can produce very large delays. Set `max_delay_seconds` to cap the
final delay. The cap is opt-in (`None` = uncapped) and applied **last** — after
backoff, the 1s floor, and jitter:

```python
# Exponential backoff, but never wait longer than 10 minutes between retries
RetryPolicy.exponential(
    base_seconds=30,
    max_retries=10,
    auto_retry_for=["TRANSIENT_ERROR"],
    max_delay_seconds=600,
)

# Equivalent to:
RetryPolicy(
    max_retries=10,
    intervals=[30],
    backoff_strategy='exponential',
    max_delay_seconds=600,
    auto_retry_for=["TRANSIENT_ERROR"],
)
```

## Auto-Retry Triggers

Retries only happen when specific conditions are met. Configure via `auto_retry_for` on `RetryPolicy`:

```python
@app.task(
    "api_call",
    retry_policy=RetryPolicy.fixed([30, 60, 120], auto_retry_for=["RATE_LIMITED", "SERVICE_UNAVAILABLE"]),
)
def api_call() -> TaskResult[dict[str, JsonValue], TaskError]:
    ...
```

`auto_retry_for` accepts:

- Error codes from `TaskError`: `"RATE_LIMITED"`, `"SERVICE_UNAVAILABLE"`
- Library error codes: `"UNHANDLED_EXCEPTION"`, `"WORKER_CRASHED"`, `"TASK_TIMEOUT"`
- Codes must use `UPPER_SNAKE_CASE` (exception class names like `"TimeoutError"` are rejected)

## Exception Mapper

Map unhandled exceptions to error codes without try/except boilerplate. When a task raises an exception, the mapper matches the exact exception class (`type(exc)`).

### Per-Task Mapper

```python
@app.task(
    "call_api",
    retry_policy=RetryPolicy.fixed([30, 60, 120], auto_retry_for=["TIMEOUT", "CONNECTION_ERROR"]),
    exception_mapper={
        TimeoutError: "TIMEOUT",
        ConnectionError: "CONNECTION_ERROR",
    },
)
def call_api() -> TaskResult[dict[str, JsonValue], TaskError]:
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

Each step writes an immutable attempt row to `horsies_task_attempts`. A retried failure creates an attempt with `will_retry=True` and `outcome='FAILED'`. The final attempt (whether success or terminal failure) has `will_retry=False`. During retries, `horsies_tasks.error_code` remains `NULL` — it is only set when the task reaches its final terminal state.

Use `handle.info(include_attempts=True)` to inspect the full attempt timeline. See [Retrieving Results](retrieving-results#task-metadata-and-attempt-history) for details.

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
    error = result.err_value
    # Check if retries exhausted
    if "retry" in str(error.data):
        print("All retries exhausted")
```

## Retry vs Rerun

`RetryPolicy` governs **automatic retry of the same request**: the task row
stays live, `retry_count` advances, and every attempt is recorded against one
task id. That is unchanged in 0.5.0.

**Rerun is a different operation.** A task that has reached a terminal status
has left the live table for the immutable history archive — there is no row to
reset, and manual in-place retry of a terminal task is removed. Re-executing
that work is a **new request**: `rerun_task` mints a new task with a new id and
records lineage back to the source.

| | Retry (`RetryPolicy`) | Rerun (`rerun_task`) |
|---|---|---|
| Applies to | A live task that failed an attempt | A terminal task record |
| Task id | Unchanged | New, with recorded lineage |
| Triggered by | `auto_retry_for` matching the error code | An explicit call or the dashboard action |
| Attempt history | Appended to the same task | Belongs to the new task |
| Requires | A retry policy with attempts remaining | The enqueue input to have been retained |

```python
from horsies import rerun_task, RerunTask, RerunEnqueuePolicy
```

Whether a task is rerunnable is decided at enqueue by
`retain_rerun_input_default` (with a per-task override), not at rerun time.
See [Action Semantics](../monitoring/action-semantics#task-rerun).

## Validation

The policy validates consistency:

```python
# This raises ValueError:
RetryPolicy(
    max_retries=3,
    intervals=[60, 300],  # Only 2 intervals for 3 retries
    backoff_strategy='fixed',
    auto_retry_for=["TRANSIENT_ERROR"],
)

# This also raises ValueError:
RetryPolicy(
    max_retries=3,
    intervals=[60, 300, 900],  # Multiple intervals
    backoff_strategy='exponential',  # Exponential needs exactly 1 interval
    auto_retry_for=["TRANSIENT_ERROR"],
)
```

## Examples

### API with Rate Limiting

```python
@app.task(
    "call_external_api",
    retry_policy=RetryPolicy.exponential(
        base_seconds=60,
        max_retries=5,
        auto_retry_for=["RATE_LIMITED", "SERVICE_UNAVAILABLE"],
    ),
)
def call_external_api() -> TaskResult[dict[str, JsonValue], TaskError]:
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
    retry_policy=RetryPolicy.fixed([1, 2, 5], auto_retry_for=["DEADLOCK"]),  # Quick retries
)
def update_inventory(*, item_id: int, delta: int) -> TaskResult[None, TaskError]:
    try:
        db.update_stock(item_id, delta)
        return TaskResult(ok=None)
    except DeadlockDetected:
        return TaskResult(err=TaskError(error_code="DEADLOCK", message="Deadlock detected"))
```
