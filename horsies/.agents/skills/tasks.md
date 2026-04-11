---
name: horsies-tasks
description: Task authoring and execution guidance for horsies, including @app.task, TaskResult, send and schedule APIs, retry behavior, exception mapping, and serialization pitfalls. Use when implementing, debugging, or reviewing task-related code.
---

# horsies — Tasks

Detailed reference for defining, sending, and handling tasks.
Covers all parameters, error codes, serialization rules, retry patterns, and pitfalls.

## `@app.task` Decorator

```python
@app.task(
    task_name: str,
    *,
    queue_name: str | None = None,
    retry_policy: RetryPolicy | None = None,
    exception_mapper: dict[type[BaseException], str] | None = None,
    default_unhandled_error_code: str | None = None,
)
def my_task(...) -> TaskResult[T, TaskError]: ...
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `task_name` | `str` | required | Unique identifier. Duplicate names raise `RegistryError(E301)`. |
| `queue_name` | `str \| None` | `None` | Must be `None` in DEFAULT mode (raises `E200`); must match a configured queue in CUSTOM mode (unrecognized raises `E103`). |
| `retry_policy` | `RetryPolicy \| None` | `None` | See RetryPolicy section. |
| `exception_mapper` | `dict[type[BaseException], str] \| None` | `None` | Per-task exception-to-error-code mapping. See ExceptionMapper section. |
| `default_unhandled_error_code` | `str \| None` | `None` | Override global default for unmapped exceptions. Must be `UPPER_SNAKE_CASE`. |

`good_until` is **not** accepted at definition time — it is a per-send concern. Passing it raises `E102`. Use `.with_options(good_until=...)` at send time instead. For workflow nodes, use `.node(good_until=...)`.

### Validated at definition time

- Return type annotation present and is `TaskResult[T, TaskError]` — missing raises `E100`, wrong type raises `E101`.
- Pre-decorated functions (with `__wrapped__` chain) are rejected — `E104`.
- `exception_mapper` keys must be `BaseException` subclasses, values must be `UPPER_SNAKE_CASE`.
- `queue_name` cross-validated against `AppConfig.queue_mode`.
- `good_until` rejected — raises `E102` with help text directing to `.with_options()` or `.node()`.
- Top-level `auto_retry_for` kwarg is rejected — it belongs inside `RetryPolicy`.

Returns `TaskFunction[P, T]`.

## `TaskResult[T, TaskError]`

The only valid return type for task functions.

### Construction

```python
TaskResult(ok=value)              # success (value can be None)
TaskResult(err=TaskError(...))    # failure
# Passing both or neither raises ValueError
```

### Methods and properties

| Name | Returns | Behavior |
|---|---|---|
| `.is_ok()` | `bool` | `True` when success |
| `.is_err()` | `bool` | `True` when error |
| `.ok` | `T \| None` | Success value or `None` — safe, never raises |
| `.err` | `E \| None` | Error value or `None` — safe, never raises |
| `.ok_value` | `T` | Success value — **raises `ValueError`** if err |
| `.err_value` | `E` | Error value — **raises `ValueError`** if ok |

**`.ok` vs `.ok_value`**: `.ok` is safe (returns `None` on wrong side). `.ok_value` raises. Always guard with `is_ok()` / `is_err()` before using `_value` forms.

### Runtime return-type validation

The task wrapper validates the returned value against declared `T` using Pydantic's `TypeAdapter`. A mismatch produces `TaskResult(err=TaskError(error_code=ContractCode.RETURN_TYPE_MISMATCH))` — does not raise. Returning `None` instead of `TaskResult` produces `TASK_EXCEPTION`.

## `TaskError`

```python
class TaskError(BaseModel):
    error_code: BuiltInTaskCode | str | None = None
    message: str | None = None
    data: Any | None = None
    exception: dict[str, Any] | BaseException | None = None
```

All fields optional. Domain errors typically set `error_code` + `message`. `data` carries structured context. `exception` populated by the library for unhandled exceptions.

**Construction rule**: reserved built-in code values must be passed as enum members, not raw strings. User-defined codes must be plain `str`, not `str, Enum` subclasses.

```python
TaskError(error_code=OperationalErrorCode.BROKER_ERROR)  # OK
TaskError(error_code="MY_CUSTOM_CODE")                   # OK
TaskError(error_code="BROKER_ERROR")                     # ValueError — reserved
```

**Serialization**: built-in codes serialize as tagged dicts `{"__builtin_task_code__": "BROKER_ERROR"}` so round-trip identity is preserved. User strings serialize as plain strings.

**Deserialization**: use standard `model_validate()` / `model_validate_json()` everywhere. No special rehydration methods needed — works for nested models too.

### `SubWorkflowError`

Extends `TaskError` with `sub_workflow_id: str` and `sub_workflow_summary: SubWorkflowSummary[Any]`. Distinguish via pattern matching:

```python
match result.err:
    case SubWorkflowError() as e:
        e.sub_workflow_id  # available
    case TaskError() as e:
        ...  # regular task error
```

## Error Code Families

The old flat `LibraryErrorCode` enum has been split into 4 families. The umbrella type alias is `BuiltInTaskCode`.

### `OperationalErrorCode`

Infra-level failures during task execution, brokering, or worker processing:

- `UNHANDLED_EXCEPTION` — uncaught exception wrapped by library
- `TASK_EXCEPTION` — task returned `None` or raised a checked exception
- `WORKER_CRASHED` — worker process died during execution
- `BROKER_ERROR` — DB/broker failure during result retrieval
- `WORKER_RESOLUTION_ERROR` — task name not found in registry
- `WORKER_SERIALIZATION_ERROR` — failed to serialize/deserialize arguments
- `RESULT_DESERIALIZATION_ERROR` — stored result JSON is corrupt
- `WORKFLOW_ENQUEUE_FAILED` — workflow node failed during enqueue
- `SUBWORKFLOW_LOAD_FAILED` — sub-workflow definition could not be loaded

### `ContractCode`

Violations of the declared API contract between task author and library:

- `RETURN_TYPE_MISMATCH` — returned value doesn't match declared type
- `PYDANTIC_HYDRATION_ERROR` — return value could not be rehydrated to declared type
- `WORKFLOW_CTX_MISSING_ID` — workflow context missing required ID

### `RetrievalCode`

Results from `handle.get()` / `handle.result_for()` when data is not (yet) available:

- `WAIT_TIMEOUT` — `get()` timed out; task may still be running, not cached, re-pollable
- `TASK_NOT_FOUND` — task ID does not exist
- `WORKFLOW_NOT_FOUND` — workflow ID does not exist
- `RESULT_NOT_AVAILABLE` — result cache never set
- `RESULT_NOT_READY` — result not ready yet

### `OutcomeCode`

Terminal lifecycle outcomes for tasks and workflows:

- `TASK_CANCELLED` — task was cancelled
- `TASK_EXPIRED` — task's `good_until` deadline passed before execution started (PENDING or CLAIMED)
- `WORKFLOW_PAUSED` — workflow is paused
- `WORKFLOW_FAILED` — workflow has failed
- `WORKFLOW_CANCELLED` — workflow was cancelled
- `UPSTREAM_SKIPPED` — upstream task was skipped
- `SUBWORKFLOW_FAILED` — sub-workflow has failed
- `WORKFLOW_SUCCESS_CASE_NOT_MET` — success condition not satisfied

### Reserved Built-In Codes

All built-in code string values are globally reserved. User-defined error codes must be plain `str` and must not collide with any built-in value.

- `BUILTIN_CODE_REGISTRY` (from `horsies.core.models.tasks`) maps every reserved string to its canonical enum member
- `horsies check` reports `E212` (`CHECK_RESERVED_CODE_COLLISION`) when `exception_mapper` values or `default_unhandled_error_code` collide with a reserved code
- The library default `UNHANDLED_EXCEPTION` for `default_unhandled_error_code` is intentionally built-in and is not flagged
- User-defined `str, Enum` types degrade to plain `str` after JSON round-trip — only built-in families preserve enum identity

## `TaskFunction[P, T]`

All methods available after `@app.task(...)`:

```python
task_name: str

# Direct call — bypasses queue, use only in tests
def __call__(*args: P.args, **kwargs: P.kwargs) -> TaskResult[T, TaskError]

# Enqueue for background execution
def send(*args: P.args, **kwargs: P.kwargs) -> TaskSendResult[TaskHandle[T]]
async def send_async(*args: P.args, **kwargs: P.kwargs) -> TaskSendResult[TaskHandle[T]]

# Enqueue with delay
def schedule(delay: int, *args: P.args, **kwargs: P.kwargs) -> TaskSendResult[TaskHandle[T]]

# Per-send options builder (returns TaskSendOptions with .send/.send_async/.schedule)
def with_options(*, good_until: datetime | None = None) -> TaskSendOptions[P, T]

# Replay failed send with stored payload (ENQUEUE_FAILED only)
def retry_send(error: TaskSendError) -> TaskSendResult[TaskHandle[T]]
async def retry_send_async(error: TaskSendError) -> TaskSendResult[TaskHandle[T]]

# Replay failed schedule with stored payload (ENQUEUE_FAILED only)
def retry_schedule(error: TaskSendError) -> TaskSendResult[TaskHandle[T]]

# Workflow node builder (returns NodeFactory, call with task kwargs to get TaskNode)
def node(
    *,
    waits_for: Sequence[TaskNode | SubWorkflowNode] | None = None,
    workflow_ctx_from: Sequence[TaskNode | SubWorkflowNode] | None = None,
    args_from: dict[str, TaskNode | SubWorkflowNode] | None = None,
    queue: str | None = None,
    priority: int | None = None,
    allow_failed_deps: bool = False,
    join: Literal['all', 'any', 'quorum'] = 'all',
    min_success: int | None = None,
    good_until: datetime | None = None,
    node_id: str | None = None,
) -> NodeFactory[P, T]
```

**Direct call vs `.send()`**: Direct call runs synchronously in current process, no queue, no retry, no persistence. `.send()` enqueues for background execution by a worker.

## `TaskSendOptions[P, T]` — Per-Send Options

Returned by `task.with_options(...)`. Exposes `.send()`, `.send_async()`, and `.schedule()` with overridden options applied to this specific send.

```python
# Set a per-send expiry deadline
deadline = datetime.now(timezone.utc) + timedelta(minutes=5)
opts = my_task.with_options(good_until=deadline)

opts.send(arg1, arg2)              # sync
await opts.send_async(arg1, arg2)  # async
opts.schedule(60, arg1, arg2)      # delayed
```

**`good_until`**: timezone-aware `datetime` or `None`. Naive datetimes return `Err(VALIDATION_FAILED)`. Passing `good_until=None` explicitly clears any inherited deadline.

**When to use `with_options()` vs `.node(good_until=...)`**: `with_options()` is for ad-hoc sends (`.send()`, `.send_async()`, `.schedule()`). For workflow nodes, use `.node(good_until=...)` — the deadline is evaluated at workflow start time, not at definition time.

### Task Expiry Lifecycle

When `good_until` is set on a send:

1. **Claim-time guard**: The claim SQL filters `AND (good_until IS NULL OR good_until > now())` — expired tasks are not claimed.
2. **PENDING expiry**: The reaper transitions unclaimed expired tasks to `EXPIRED` with `TASK_EXPIRED` outcome code.
3. **CLAIMED expiry**: If claimed but `good_until` passes before execution starts, the worker marks it `EXPIRED` directly (takes precedence over workflow PAUSED/CANCELLED handling).
4. **Retry cap**: A retry scheduled at or past the deadline is rejected — `good_until` caps the entire retry window.

## `from_node()` — Ergonomic Data Flow for `.node()()`

```python
from horsies import from_node

def from_node(upstream: TaskNode[Any] | SubWorkflowNode[Any]) -> Any
```

When used as a kwarg value in the second call of `.node()()`, the `NodeFactory` will:
1. Add the upstream node to `args_from` under this kwarg's key.
2. Auto-add the upstream node to `waits_for` if not already present.

```python
producer = produce.node()(value=42)
consumer = consume.node()(data=from_node(producer))
# Equivalent to:
# TaskNode(fn=consume, args_from={"data": producer}, waits_for=[producer])
```

Raises `WorkflowValidationError(E008)` if argument is not a `TaskNode` or `SubWorkflowNode`.

## `TaskHandle[T]`

Returned by successful `.send()` / `.send_async()` / `.schedule()`.

```python
task_id: str  # UUID, stable across retries of the same send

def get(timeout_ms: int | None = None) -> TaskResult[T, TaskError]
async def get_async(timeout_ms: int | None = None) -> TaskResult[T, TaskError]

def info(*, include_result: bool = False, include_failed_reason: bool = False, include_attempts: bool = False) -> BrokerResult[TaskInfo | None]
async def info_async(*, include_result: bool = False, include_failed_reason: bool = False, include_attempts: bool = False) -> BrokerResult[TaskInfo | None]
```

### Caching semantics

- `WAIT_TIMEOUT` is **not** cached — subsequent `get()` calls re-poll from broker.
- All other results **are** cached — repeated `get()` returns cached value without DB query.

### `TaskInfo` fields

```python
task_id: str, task_name: str, status: TaskStatus
queue_name: str, priority: int
retry_count: int, max_retries: int, next_retry_at: datetime | None
sent_at: datetime | None, enqueued_at: datetime
claimed_at: datetime | None, started_at: datetime | None
completed_at: datetime | None, failed_at: datetime | None
worker_hostname: str | None, worker_pid: int | None, worker_process_name: str | None
error_code: str | None                      # always returned (final TaskError.error_code, NULL if not failed)
result: TaskResult[Any, TaskError] | None    # opt-in: include_result=True
failed_reason: str | None                    # opt-in: include_failed_reason=True
attempts: list[TaskAttemptInfo] | None       # opt-in: include_attempts=True (None when not requested, [] when empty)
```

### `TaskAttemptInfo` fields

```python
task_id: str, attempt: int
outcome: TaskAttemptOutcome  # COMPLETED, FAILED, WORKER_FAILURE
will_retry: bool
started_at: datetime, finished_at: datetime
error_code: str | None, error_message: str | None, failed_reason: str | None
worker_id: str | None, worker_hostname: str | None, worker_pid: int | None, worker_process_name: str | None
```

`TaskAttemptOutcome`: `COMPLETED` (success), `FAILED` (domain/library error with TaskResult), `WORKER_FAILURE` (worker crash without TaskResult).

`TaskStatus`: `PENDING -> CLAIMED -> RUNNING -> COMPLETED | FAILED | CANCELLED | EXPIRED`. `is_terminal` is `True` for `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`. `EXPIRED` means the task's `good_until` deadline passed before execution started (either PENDING or CLAIMED).

## `TaskSendResult` / `TaskSendError`

```python
type TaskSendResult[T] = Result[T, TaskSendError]
```

### `TaskSendError`

```python
@dataclass(slots=True, frozen=True)
class TaskSendError:
    code: TaskSendErrorCode
    message: str
    retryable: bool
    task_id: str | None         # None for SEND_SUPPRESSED; may be set or None for VALIDATION_FAILED
    payload: TaskSendPayload | None  # None when no serialization happened
    exception: BaseException | None
```

### `TaskSendErrorCode`

| Code | Retryable | When |
|---|---|---|
| `SEND_SUPPRESSED` | No | `.send()` called during worker import/discovery |
| `VALIDATION_FAILED` | No | Serialization failed, invalid queue, or cross-method retry attempted |
| `ENQUEUE_FAILED` | Yes | Broker/DB failure. `TaskSendPayload` with `enqueue_sha` available for retry |
| `PAYLOAD_MISMATCH` | No | Retry payload SHA doesn't match — payload was altered between send and retry |

### `TaskSendPayload`

```python
@dataclass(slots=True, frozen=True)
class TaskSendPayload:
    task_name: str, queue_name: str, priority: int
    args_json: str | None, kwargs_json: str | None
    sent_at: datetime, good_until: datetime | None
    enqueue_delay_seconds: int | None, task_options: str | None
    enqueue_sha: str  # SHA-256 hex digest for same-payload guarantee
```

Pre-serialized at send time. Never construct manually — created by the library internally.

### Retry guard pattern

```python
match my_task.send(arg1, arg2):
    case Ok(handle):
        result = handle.get(timeout_ms=5000)
    case Err(err) if err.retryable:
        match my_task.retry_send(err):
            case Ok(handle):
                result = handle.get(timeout_ms=5000)
            case Err(retry_err):
                log_permanent_failure(retry_err)
    case Err(err):
        log_permanent_failure(err)
```

Cross-method retry is blocked: `retry_send` on a scheduled-task error returns `VALIDATION_FAILED`. `retry_schedule` on a non-scheduled error also returns `VALIDATION_FAILED`.

## `RetryPolicy`

Always use the class methods:

```python
# Fixed backoff — intervals length defines max_retries
RetryPolicy.fixed(
    intervals: list[int],                              # seconds, e.g. [60, 300, 900]
    *,
    auto_retry_for: list[str | BuiltInTaskCode],        # required
    jitter: bool = True,
)

# Exponential backoff — delay = base * 2^(attempt-1)
RetryPolicy.exponential(
    base_seconds: int,
    *,
    max_retries: int,
    auto_retry_for: list[str | BuiltInTaskCode],        # required
    jitter: bool = True,
)
```

### Validation rules

- `fixed`: `len(intervals)` must equal `max_retries`
- `exponential`: `len(intervals)` must be exactly `1`
- `auto_retry_for` entries must be `UPPER_SNAKE_CASE` — exception class names like `"TimeoutError"` are rejected
- Each interval: 1–86400 seconds. Max retries: 1–20.

### Jitter

`jitter=True` (default) randomizes each delay by ±25%. Prevents thundering herd. Disable only in tests.

### `auto_retry_for` valid values

- User-defined domain codes: `"RATE_LIMITED"`, `"DEADLOCK"`, `"SERVICE_UNAVAILABLE"`
- `BuiltInTaskCode` members: `OperationalErrorCode.UNHANDLED_EXCEPTION`, `RetrievalCode.WAIT_TIMEOUT`, etc.
- Plain strings matching `^[A-Z][A-Z0-9_]*$`

Retries only trigger when the error code on the stored result matches and `retry_count < max_retries`.

### Examples

```python
# 3 fixed retries: 1min, 5min, 15min
RetryPolicy.fixed([60, 300, 900], auto_retry_for=["RATE_LIMITED"])

# 5 exponential retries: 30s, 60s, 120s, 240s, 480s
RetryPolicy.exponential(base_seconds=30, max_retries=5, auto_retry_for=["TIMEOUT"])

# Quick deadlock retries, no jitter
RetryPolicy.fixed([1, 2, 5], auto_retry_for=["DEADLOCK"], jitter=False)
```

## `ExceptionMapper`

```python
ExceptionMapper = dict[type[BaseException], str]
```

When a task raises an unhandled exception, the library resolves the error code in this order:

1. Per-task `exception_mapper` — exact class lookup (`type(exc) in mapper`)
2. Global `AppConfig.exception_mapper` — exact class lookup
3. Per-task `default_unhandled_error_code`
4. Global `AppConfig.default_unhandled_error_code` (defaults to `"UNHANDLED_EXCEPTION"`)

**Exact class match only** — subclasses are not matched. Map subclasses explicitly:

```python
exception_mapper={
    requests.exceptions.Timeout: "TIMEOUT",
    requests.exceptions.ReadTimeout: "READ_TIMEOUT",  # subclass — map explicitly
}
```

Only invoked for unhandled exceptions. If the task returns `TaskResult(err=...)` explicitly, the mapper is never called.

### Validation

- Keys must be `BaseException` subclasses (not instances, not strings)
- Values must be non-empty `UPPER_SNAKE_CASE` matching `^[A-Z][A-Z0-9_]*$`
- Values that look like exception class names (e.g., `"TimeoutError"`) are rejected
- Invalid entries raise `ConfigurationError(E209)`

## Serialization Rules

Serialization happens at `.send()` time, before enqueueing. Failure returns `Err(TaskSendError(VALIDATION_FAILED))` — task is never enqueued.

### Supported types

| Type | Rehydration |
|---|---|
| `None`, `bool`, `int`, `float`, `str` | Direct |
| `datetime`, `date`, `time` | ISO 8601 round-trip |
| `pydantic.BaseModel` subclass | `model_dump(mode="json")` → `model_validate()` |
| `@dataclass` instance | Field extraction → `cls(**init_kwargs)` |
| `dict` / `list` | Recursive rehydration |

### Not supported — causes `VALIDATION_FAILED`

- Custom class instances (not Pydantic/dataclass)
- `bytes`, `bytearray`
- Enum instances (convert to `.value` first)
- Objects defined in `__main__` or inside functions (`<locals>` in qualname)
- `dict` keys that collide after `str()` coercion
- Non-finite floats (`nan`, `inf`)

### Importability requirement

Pydantic models and dataclasses must be importable from their `__module__` via `importlib.import_module`. Moving a model to a different file breaks rehydration of arguments serialized before the move → `PYDANTIC_HYDRATION_ERROR`.

### Return type serialization

Worker serializes the return value after execution with the same codec. Non-serializable return → `WORKER_SERIALIZATION_ERROR`. Schema changes between serialization and retrieval → `PYDANTIC_HYDRATION_ERROR` (not a crash — `handle.get()` always returns `TaskResult`).

## Common Pitfalls

### `.send()` at module level → `SEND_SUPPRESSED`

```python
# WRONG — runs during worker import
result = my_task.send("test")  # Err(SEND_SUPPRESSED)

# CORRECT — call from functions/endpoints
def process():
    match my_task.send("test"):
        case Ok(handle): ...
        case Err(err): ...
```

### Non-serializable arguments → `VALIDATION_FAILED`

```python
# WRONG
my_task.send(conn=db_connection)

# CORRECT
my_task.send(conn_url=str(db_connection.url))
```

### Classes defined in `__main__` or inside functions

```python
# WRONG — worker cannot import this
class Config:
    ...
my_task.send(cfg=Config())  # SerializationError

# CORRECT — define in an importable module
from myapp.models import Config  # Pydantic model or dataclass
my_task.send(cfg=Config(value=42))
```

### Forgetting `TaskResult` wrapper

```python
# WRONG — returns raw value, causes TASK_EXCEPTION at runtime
@app.task("bad")
def bad(x: int) -> TaskResult[dict, TaskError]:
    return {"x": x}

# CORRECT
@app.task("good")
def good(x: int) -> TaskResult[dict, TaskError]:
    return TaskResult(ok={"x": x})
```

### `auto_retry_for` at top level

```python
# WRONG — rejected at definition time
@app.task("t", auto_retry_for=["TIMEOUT"])
def t() -> TaskResult[str, TaskError]: ...

# CORRECT — goes inside RetryPolicy
@app.task("t", retry_policy=RetryPolicy.fixed([60], auto_retry_for=["TIMEOUT"]))
def t() -> TaskResult[str, TaskError]: ...
```

### Decorator ordering — `E104`

```python
# WRONG — @app.task sees already-wrapped function
@app.task("t")
@some_other_decorator
def t() -> TaskResult[str, TaskError]: ...

# CORRECT — @app.task must be applied to the raw function
@app.task("t")
def t() -> TaskResult[str, TaskError]: ...
```

### `WAIT_TIMEOUT` is not a terminal failure

```python
result = handle.get(timeout_ms=5000)
if result.is_err() and result.err_value.error_code == RetrievalCode.WAIT_TIMEOUT:
    # Task may still be running. NOT cached — calling get() again will re-poll.
    check_again_later(handle.task_id)
```

### `queue_name` in DEFAULT mode

```python
# WRONG — raises E200 at definition time if app uses QueueMode.DEFAULT
@app.task("t", queue_name="critical")
def t() -> TaskResult[str, TaskError]: ...
```

### `good_until` is a per-send concern, not a decorator option

```python
# WRONG — rejected at definition time (raises E102)
@app.task("t", good_until=datetime.now(timezone.utc) + timedelta(hours=1))

# CORRECT — set at send time via with_options()
deadline = datetime.now(timezone.utc) + timedelta(hours=1)
my_task.with_options(good_until=deadline).send()

# CORRECT — for workflow nodes, use .node(good_until=...)
node = my_task.node(good_until=deadline)(arg=value)
```

`good_until` must be timezone-aware. Naive datetimes return `Err(VALIDATION_FAILED)`.

## All Public Imports

```python
from horsies import (
    # App and config
    Horsies, AppConfig, PostgresConfig,
    # Task result types
    TaskResult, TaskError, SubWorkflowError, TaskInfo, TaskAttemptInfo,
    # Error code families
    BuiltInTaskCode, OperationalErrorCode, ContractCode, RetrievalCode, OutcomeCode,
    # Retry
    RetryPolicy,
    # Send types
    TaskHandle, TaskSendOptions, TaskSendResult, TaskSendError, TaskSendErrorCode, TaskSendPayload,
    # Exception mapper
    ExceptionMapper,
    # Status
    TaskStatus, TaskAttemptOutcome, TASK_TERMINAL_STATES,
    # Broker result (for handle.info())
    BrokerResult, BrokerOperationError, BrokerErrorCode,
    # Node data flow helper
    from_node,
    # Result primitives
    Result, Ok, Err, is_ok, is_err,
)
```
