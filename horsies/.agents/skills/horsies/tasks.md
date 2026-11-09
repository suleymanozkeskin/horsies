# horsies — Tasks

Detailed reference for defining, sending, and handling tasks.
Covers all parameters, error codes, serialization rules, retry patterns, and pitfalls.

## `@app.task` Decorator

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
@app.task(
    task_name: str,
    *,
    queue_name: str | None = None,
    retry_policy: RetryPolicy | None = None,
    exception_mapper: dict[type[BaseException], str] | None = None,
    default_unhandled_error_code: str | None = None,
    timeout_ms: int | None = None,
)
def my_task(*, ...) -> TaskResult[T, TaskError]: ...
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `task_name` | `str` | required | Unique identifier. An empty name raises `TaskDefinitionError(HRS-102)`. Duplicate names raise `RegistryError(HRS-301)`. |
| `queue_name` | `str \| None` | `None` | Must be `None` in DEFAULT mode (raises `HRS-200`); must match a configured queue in CUSTOM mode (unrecognized raises `HRS-103`). |
| `retry_policy` | `RetryPolicy \| None` | `None` | See RetryPolicy section. |
| `exception_mapper` | `dict[type[BaseException], str] \| None` | `None` | Per-task exception-to-error-code mapping. See ExceptionMapper section. |
| `default_unhandled_error_code` | `str \| None` | `None` | Override global default for unmapped exceptions. Must be `UPPER_SNAKE_CASE`. |
| `timeout_ms` | `int \| None` | `None` | Execution time limit in ms, from 1000 to `MAX_TASK_TIMEOUT_MS` (4,294,967,295, about 49.7 days, the Rust port's `u32` limit), measured from the moment a child takes the task; time waiting for a free child does not count. On expiry the child and the subprocesses in its session are SIGKILLed and the task fails with `TASK_TIMEOUT`; retry is opt-in via `auto_retry_for`. A deadline that fires before user code starts requeues the task instead. Other tasks on the worker continue. Use it as a hang backstop, not as routine control flow. |

`good_until` is **not** accepted at definition time — it is a per-send concern. Passing it raises `HRS-102`. Use `.with_options(good_until=...)` at send time instead. For workflow nodes, use `.node(good_until=...)`.

### Validated at definition time

- Return type annotation present and is `TaskResult[T, TaskError]` — missing raises `HRS-100`, wrong type raises `HRS-101`.
- Pre-decorated functions (with `__wrapped__` chain) are rejected — `HRS-104`.
- The function is `def` or `async def`. A function that yields is rejected — `HRS-101`, for a generator and for an async generator.
- `exception_mapper` keys must be `BaseException` subclasses, values must be `UPPER_SNAKE_CASE`.
- `queue_name` cross-validated against `AppConfig.queue_mode`.
- `good_until` rejected — raises `HRS-102` with help text directing to `.with_options()` or `.node()`.
- Top-level `auto_retry_for` kwarg is rejected — it belongs inside `RetryPolicy`.

Returns `TaskFunction[P, T]` for a `def` function, and `AsyncTaskFunction[P, T]` for an `async def` function.

### Async tasks

- A worker child runs one task at a time, `def` or `async def`. The same RUNNING update, heartbeats, `timeout_ms` kill, recovery and retry policy apply.
- Each child keeps one event loop until the child stops. An `async def` task runs on it, so an async client that a module keeps stays open across the tasks of one child.
- The loop runs only while an `async def` task runs. A `def` task runs with no running event loop and can call `asyncio.run()`.
- An `async def` task must await the work that it starts. When it ends and asyncio tasks that it started (`asyncio.create_task()`) are still on the loop, the worker replaces the child after that task, and the next task runs in a new child. The old child cancels that work when it stops, within 5 seconds. A client that keeps its own background tasks on the loop (a keepalive, a consumer) causes a replacement after each async task.
- A child creates its event loop before it marks its first async task RUNNING. When the OS refuses it (for example EMFILE), the task returns to the queue unrun, with no attempt or retry used, and the worker replaces the child. When that child had run no task, the worker also stops with a non-zero exit, so its supervisor restarts it.
- A thread of `asyncio.to_thread()` that outlives its task does not cause a replacement. At the stop, the child does not wait for it past the 5-second close: it exits, and `atexit` handlers do not run.
- In an `async def` task, send with `.send_async()` / `.schedule_async()`. The sync `.send()` / `.schedule()` return `Err(TaskSendAsyncContext)` there and enqueue nothing.
- An exception, a wrong return, and the exception mappers give the same `TaskResult` as in a `def` task.

## `TaskResult[T, TaskError]`

The only valid return type for task functions.

### Construction

<!-- check: skip partial -- uses names that the page does not define -->
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

### Strict values

Every boundary validates in strict mode: `"7"`, `7.0` and `True` are refused for an `int`, and a str is refused for a `datetime`, `Enum`, `UUID` or `Decimal`. `.send()`, schedules, workflow definitions and task results read a value back before it is stored. An unknown key is refused at any depth at send, and ignored on read. A subclass with fields that the declared type lacks is refused.

`@app.task` and each `SubWorkflowNode` refuse `Field(exclude=True)`, `exclude_if`, `extra='allow'`, `PlainValidator`, `WrapValidator`, `SkipValidation`, a schema hook, `Json[T]`, `Literal` with a plain `Enum`, an alias whose written key does not read back, and a default that does not hold its type. Use `AfterValidator` or `BeforeValidator`.

`TaskError.data` must be JSON-native. It takes `JsonInput`, so a `list[str]` fits, and it gives `JsonValue`, an exact `list` or `dict`. A bad value becomes `{'data_refused': <cause>}`, and the code and message stay. A `TaskError` subclass with extra fields does not cross the wire: use `data`, and match on `error_code`.

Changing a model that a task takes or returns: see `SKILL.md`, Deployment facts.

### Runtime return-type validation

The task wrapper validates the returned value against declared `T` using Pydantic's `TypeAdapter` in strict mode. A mismatch produces `TaskResult(err=TaskError(error_code=ContractCode.RETURN_TYPE_MISMATCH))` — does not raise. The error stores the actual type and the failing fields (`data`: `variant`, `expected_type`, `actual_type`, `validation_errors`, `validation_error_count`), not the returned value. Returning `None` instead of `TaskResult` produces `TASK_EXCEPTION`.

## `TaskError`

<!-- check: skip partial -- uses names that the page does not define -->
```python
class TaskError(BaseModel):
    error_code: BuiltInTaskCode | str
    message: str | None = None
    data: JsonValue = None  # takes JsonInput
    exception: dict[str, Any] | BaseException | None = None
```

`error_code` is required and must not be null: `auto_retry_for` and outcome routing read it. `message` is optional, and domain errors typically set it. `data` carries structured context. `exception` populated by the library for unhandled exceptions.

**Construction rule**: reserved built-in code values must be passed as enum members, not raw strings. User-defined codes must be plain `str`, not `str, Enum` subclasses.

<!-- check: skip partial -- uses a name before the page defines it -->
```python
TaskError(error_code=OperationalErrorCode.BROKER_ERROR, message="db down")  # OK
TaskError(error_code="MY_CUSTOM_CODE", message="card declined")             # OK
TaskError(error_code="BROKER_ERROR", message="db down")                     # ValueError — reserved
TaskError(message="card declined")                                          # ValueError — no code
```

**Serialization**: built-in codes serialize as tagged dicts `{"__builtin_task_code__": "BROKER_ERROR"}` so round-trip identity is preserved. User strings serialize as plain strings.

**Deserialization**: use standard `model_validate()` / `model_validate_json()` everywhere. No special rehydration methods needed — works for nested models too.

**Stored data**: `TaskError` refuses a key that it does not declare, at construction and at decode. A stored error with an unknown key, or without `error_code`, is stored data that does not parse: `TaskHandle.get()` and `WorkflowHandle.get()` return `RESULT_DESERIALIZATION_ERROR` with the cause, and `app.get_result()` returns `Err(BrokerInvalidJsonPayload)`.

### `SubWorkflowError`

Extends `TaskError` with `sub_workflow_id: str` and `sub_workflow_summary: SubWorkflowSummary[Any]`. Distinguish via pattern matching:

<!-- check: skip partial -- uses a name before the page defines it -->
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

<!-- members: OperationalErrorCode -->
- `UNHANDLED_EXCEPTION` — uncaught exception wrapped by library
- `TASK_EXCEPTION` — the worker caught an exception outside the task wrapper, or a validator raised on a default
- `WORKER_CRASHED` — worker process died during execution
- `BROKER_ERROR` — DB/broker failure during result retrieval
- `WORKER_RESOLUTION_ERROR` — task name not found in registry
- `WORKER_SERIALIZATION_ERROR` — failed to serialize/deserialize arguments
- `RESULT_DESERIALIZATION_ERROR` — a stored result or a stored workflow error is corrupt or does not decode
- `WORKFLOW_ENQUEUE_FAILED` — workflow node failed during enqueue
- `SUBWORKFLOW_LOAD_FAILED` — sub-workflow definition could not be loaded
- `WORKFLOW_RECOVERY_LIMIT_REACHED` — workflow recovery raised a persistent error in `workflow_recovery_failure_limit` reaper passes, so the workflow failed closed
- `SUBWORKFLOW_DEPTH_LIMIT_REACHED` — the engine did not start a child workflow, because its depth would pass 32

### `ContractCode`

Violations of the declared API contract between task author and library:

<!-- members: ContractCode -->
- `RETURN_TYPE_MISMATCH` — returned value doesn't fit the declared type, or is not a `TaskResult`, including `None`
- `WORKFLOW_CTX_MISSING_ID` — workflow context missing required ID
- `NO_TYPE_AVAILABLE` — a typed decode needs the success type, but the task or workflow is not registered locally (for example a handle built from an id)

### `RetrievalCode`

Results from `handle.get()` / `handle.result_for()` when data is not (yet) available:

<!-- members: RetrievalCode -->
- `WAIT_TIMEOUT` — `get()` timed out; task may still be running, not cached, re-pollable
- `TASK_NOT_FOUND` — task ID does not exist
- `WORKFLOW_NOT_FOUND` — workflow ID does not exist, or retention removed the workflow after `get()` read its terminal status
- `RESULT_NOT_AVAILABLE` — result cache never set
- `RESULT_NOT_READY` — result not ready yet
- `EVENT_LOOP_LIMIT` — the broker serves its limit of open event loops and this is a new one; not cached, clears when one of those loops closes
- `ASYNC_CONTEXT` — sync `get()` / `result_for()` called on a thread that runs an event loop; nothing ran, not cached — call `get_async()` / `result_for_async()`
- `INVALID_INPUT` — an argument of `get()` / `get_async()` was refused before any read: `timeout_ms` must be None or an int from 0 to `MAX_TASK_TIMEOUT_MS` (4,294,967,295 ms); not cached
- `WAIT_INSIDE_TASK` — a result wait (`TaskHandle.get()` / `get_async()`, `app.get_result()` / `get_result_async()`, `WorkflowHandle.get()` / `get_async()`) was called inside a worker child, also from a thread that the task started; it waited for nothing, not cached. It is the first check of the call, before `timeout_ms` and `ASYNC_CONTEXT`. The raw reads `app.get_raw_result()` and `broker.get_raw_result_record()` (and `*_async`) return `Err(BrokerWaitInsideTask)` there. A task never waits for another task: compose them as a workflow. `send()`, starting a workflow, `info()` and `workflow_ctx.result_for()` still work in a task

### `OutcomeCode`

Terminal lifecycle outcomes for tasks and workflows:

<!-- members: OutcomeCode -->
- `TASK_CANCELLED` — task was cancelled
- `TASK_EXPIRED` — task's `good_until` deadline passed before execution started (PENDING or CLAIMED)
- `TASK_TIMEOUT` — task exceeded its `timeout_ms`; child process killed, retry opt-in via `auto_retry_for`
- `WORKFLOW_PAUSED` — workflow is paused
- `WORKFLOW_EXPIRED` — a paused workflow passed `paused_workflow_auto_cancel_after` and expired; `data` holds the policy name and the age
- `WORKFLOW_FAILED` — workflow has failed
- `WORKFLOW_CANCELLED` — workflow was cancelled
- `UPSTREAM_SKIPPED` — upstream task was skipped
- `SUBWORKFLOW_FAILED` — sub-workflow has failed
- `WORKFLOW_SUCCESS_CASE_NOT_MET` — success condition not satisfied
- `WORKFLOW_OUTPUT_SKIPPED` — a `COMPLETED` workflow has no output: its output node, or a terminal node of an outputless workflow, is SKIPPED, and a success case does not require that node

### Reserved Built-In Codes

All built-in code string values are globally reserved. User-defined error codes must be plain `str` and must not collide with any built-in value.

- `BUILTIN_CODE_REGISTRY` (from `horsies.core.models.tasks`) maps every reserved string to its canonical enum member
- `horsies check` reports `HRS-212` (`CHECK_RESERVED_CODE_COLLISION`) when `exception_mapper` values or `default_unhandled_error_code` collide with a reserved code
- The library default `UNHANDLED_EXCEPTION` for `default_unhandled_error_code` is intentionally built-in and is not flagged
- User-defined `str, Enum` types degrade to plain `str` after JSON round-trip — only built-in families preserve enum identity

## `TaskFunction[P, T]`

All methods available after `@app.task(...)`:

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
task_name: str

# Direct call — bypasses queue, use only in tests
def __call__(*args: P.args, **kwargs: P.kwargs) -> TaskResult[T, TaskError]
# AsyncTaskFunction[P, T] (an async def task) differs only here:
# def __call__(*args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, TaskResult[T, TaskError]]

# Enqueue for background execution
def send(*args: P.args, **kwargs: P.kwargs) -> TaskSendResult[TaskHandle[T]]
async def send_async(*args: P.args, **kwargs: P.kwargs) -> TaskSendResult[TaskHandle[T]]

# Enqueue with delay
def schedule(delay: int, *args: P.args, **kwargs: P.kwargs) -> TaskSendResult[TaskHandle[T]]
async def schedule_async(delay: int, *args: P.args, **kwargs: P.kwargs) -> TaskSendResult[TaskHandle[T]]

# Per-send options builder (returns TaskSendOptions with .send/.send_async/.schedule/.schedule_async)
def with_options(*, good_until: datetime | None = None, idempotency_key: str | None = None, retention: timedelta | None = <unset>) -> TaskSendOptions[P, T]
# retention's default is absence, not 30 days: omitting it takes the
# queue's mapping, while choosing timedelta(days=30) overrides one.

# Replay failed send with stored payload; another variant is a type error
# (and fails an assertion at run time)
def retry_send(error: TaskSendRetryable) -> TaskSendResult[TaskHandle[T]]
async def retry_send_async(error: TaskSendRetryable) -> TaskSendResult[TaskHandle[T]]

# Replay failed schedule with stored payload; another variant is a type error
# (and fails an assertion at run time)
def retry_schedule(error: TaskSendRetryable) -> TaskSendResult[TaskHandle[T]]
async def retry_schedule_async(error: TaskSendRetryable) -> TaskSendResult[TaskHandle[T]]

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

**Direct call vs `.send()`**: Direct call runs in current process, no queue, no retry, no persistence. For an `async def` task, the direct call returns a coroutine: `await` it. `.send()` enqueues for background execution by a worker.

## `TaskSendOptions[P, T]` — Per-Send Options

Returned by `task.with_options(...)`. Exposes `.send()`, `.send_async()`, `.schedule()`, and `.schedule_async()` with overridden options applied to this specific send.

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Set a per-send start deadline: a task not started by then becomes EXPIRED and never runs
deadline = datetime.now(timezone.utc) + timedelta(minutes=5)
opts = my_task.with_options(good_until=deadline)

opts.send(**kwargs)              # sync
await opts.send_async(**kwargs)  # async
opts.schedule(60, **kwargs)      # delayed
await opts.schedule_async(60, **kwargs)  # delayed, async
```

**`good_until`**: timezone-aware `datetime` or `None`. Naive datetimes return `Err(VALIDATION_FAILED)`. Passing `good_until=None` explicitly clears any inherited deadline.

**`idempotency_key`**: optional caller-supplied key, scoped per task name. The
enqueue reserves the key together with a canonical fingerprint of the request:
a resend under the same key with the same request is a replay and creates zero
rows; a resend under the same key with a *different* request returns
`Err(TaskSendIdempotencyKeyConflict)`. Its `owner_task_id` names the task
that holds the key. While the owner task is
live, the key stays reserved. After the owner task finishes, the key clears
when the reservation window (24 h by default) has passed. The retry methods
accept the error. A retry that changes the key of a sent task id returns
`PAYLOAD_MISMATCH`. Keys are not a rerun contract — they deduplicate enqueues.

**`retention`**: how long the terminal record is kept. Precedence,
highest first:

1. What you pass here — a period in whole days, or `None` for forever.
   Choosing 30 days explicitly counts as a choice and beats a queue mapping.
2. The queue's entry in `AppConfig.retention.queue_retention`, if it has one.
3. The immutable 30-day default class.

So omitting the argument means "the queue's mapping if there is one, and
30 days otherwise" — not 30 days unconditionally.

This precedence holds on EVERY enqueue path — `.send()`, `.send_async()`,
`.schedule()`, `.schedule_async()`, a `TaskSchedule` firing on its cron,
and a workflow node's backing task. A cron fire resolves from the
scheduler process's own config; a workflow node from the queue its node
runs on. `.schedule*()` also carries `idempotency_key`.

A retention class is its period: the key is `standard_<days>d`
(`standard_7d`, `standard_30d`). Valid values are `None` and a whole-day
period this process registers: 30 days, a period in `queue_retention`, or
a period in `AppConfig.retention.retention_periods`. Any other period, a
fractional day, and a value that is not a `timedelta` (such as a 0.6.0
class key string) return `Err(VALIDATION_FAILED)` at the send call before
anything is written. The class is snapshotted on the row at enqueue and
decides which history partition the record moves to when the task
terminalizes — so a later edit to `queue_retention` governs later sends
and leaves records already enqueued under the promise they were given.

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Keep this task's terminal record forever
await my_task.with_options(retention=None).send_async(**kwargs)

# Opt one send back to the 30-day default on a queue that is mapped
await my_task.with_options(retention=timedelta(days=30)).send_async(**kwargs)
```

**When to use `with_options()` vs `.node(good_until=...)`**: `with_options()` is for ad-hoc sends (`.send()`, `.send_async()`, `.schedule()`, `.schedule_async()`). For workflow nodes, use `.node(good_until=...)` — the deadline is evaluated at workflow start time, not at definition time.

### Task Expiry Lifecycle

When `good_until` is set on a send:

1. **Claim-time guard**: The claim SQL filters `AND (good_until IS NULL OR good_until > now())` — expired tasks are not claimed.
2. **PENDING expiry**: The reaper transitions unclaimed expired tasks to `EXPIRED` with `TASK_EXPIRED` outcome code.
3. **CLAIMED expiry**: If claimed but `good_until` passes before execution starts, the worker marks it `EXPIRED` directly (takes precedence over workflow PAUSED/CANCELLED handling).
4. **Retry cap**: A retry scheduled at or past the deadline is rejected — `good_until` caps the entire retry window.

## `from_node()` — Ergonomic Data Flow for `.node()()`

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
from horsies import from_node

def from_node(upstream: TaskNode[Any] | SubWorkflowNode[Any]) -> Any
```

When used as a kwarg value in the second call of `.node()()`, the `NodeFactory` will:
1. Add the upstream node to `args_from` under this kwarg's key.
2. Auto-add the upstream node to `waits_for` if not already present.

<!-- check: skip partial -- uses names that the page does not define -->
```python
producer = produce.node()(value=42)
consumer = consume.node()(data=from_node(producer))
# Equivalent to:
# TaskNode(fn=consume, args_from={"data": producer}, waits_for=[producer])
```

Raises `WorkflowValidationError(HRS-008)` if argument is not a `TaskNode` or `SubWorkflowNode`.

## `TaskHandle[T]`

Returned by successful `.send()` / `.send_async()` / `.schedule()` / `.schedule_async()`.

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
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

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
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

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
task_id: str, attempt: int
outcome: TaskAttemptOutcome  # COMPLETED, FAILED, WORKER_FAILURE
will_retry: bool
started_at: datetime, finished_at: datetime
error_code: str | None, error_message: str | None, failed_reason: str | None
worker_id: str | None, worker_hostname: str | None, worker_pid: int | None, worker_process_name: str | None
```

`TaskAttemptOutcome`: `COMPLETED` (success), `FAILED` (domain/library error or recoverable worker/process failure represented as a TaskResult), `WORKER_FAILURE` (legacy/rare worker failure path without a usable TaskResult).

`TaskStatus`: `PENDING -> CLAIMED -> RUNNING -> COMPLETED | FAILED | CANCELLED | EXPIRED`. `is_terminal` is `True` for `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`. `EXPIRED` means the task's `good_until` deadline passed before execution started (either PENDING or CLAIMED).

## `TaskSendResult` / `TaskSendError`

<!-- check: skip elided -- shows the library definition, not user code -->
```python
type TaskSendResult[T] = Result[T, TaskSendError]
```

### `TaskSendError`

One frozen variant per `TaskSendErrorCode` member. Each variant has only the fields of its failure; `err.code` (a class variable) names its member. `payload`, `message` and `exception` are not in the repr.

<!-- check: skip elided -- shows the library definition, not user code -->
```python
type TaskSendError = (
    TaskSendSuppressed               # message
    | TaskSendAsyncContext           # task_id, payload, message
    | TaskSendValidationFailed       # message, exception
    | TaskSendEnqueueFailed          # task_id, payload, message, retryable, exception
    | TaskSendPayloadMismatch        # task_id, payload, message
    | TaskSendPayloadTooLarge        # message, size_bytes, limit_bytes
    | TaskSendIdempotencyKeyConflict # task_id, payload, message, owner_task_id
    | TaskSendEventLoopLimit         # message
    | TaskSendLoopRunnerFailed       # task_id, payload, message, exception
)
type TaskSendRetryable = (
    TaskSendEnqueueFailed
    | TaskSendAsyncContext
    | TaskSendIdempotencyKeyConflict
    | TaskSendLoopRunnerFailed
)
```

`exception` is `BaseException | NoCause` on `TaskSendValidationFailed` and `TaskSendEnqueueFailed`. `NoCause()` means a check found the failure without a Python exception. On `TaskSendLoopRunnerFailed` it is always the `LoopRunnerError`. `NoCause` is not an exception: match on `exception` before `raise ... from`, `exc_info=` or an error tracker call.

Validation failure text never contains the refused value: each failure is `<loc>: <msg> [<type>]`, at most 10, and a message part that comes from the input is `<input>`. Text that your own validators write is kept as written. A kwarg that fails its declared type gives `TaskSendValidationFailed.exception` as a `ValueValidationError` (`horsies.core.codec`), not a pydantic `ValidationError`. A key from the input can be part of `<loc>` for a dict or mapping type, and for a key that `extra='forbid'` refuses.

### `TaskSendErrorCode`

<!-- members: TaskSendErrorCode -->
| Code | Retry methods | When |
|---|---|---|
| `SEND_SUPPRESSED` | Refused | `.send()` called during worker import/discovery |
| `ASYNC_CONTEXT` | Accepted | Sync `.send()` / `.schedule()` (or a sync retry) called inside a running event loop; nothing was enqueued — the error keeps the payload, so `retry_send_async(err)` / `retry_schedule_async(err)` completes the same send |
| `VALIDATION_FAILED` | Refused | Serialization failed, invalid queue, the broker refused the input before the write (`INVALID_INPUT`, `INVALID_TASK_OPTIONS`), or cross-method retry attempted |
| `ENQUEUE_FAILED` | Accepted | Broker/DB failure. `TaskSendPayload` with `enqueue_sha` available for retry; `retryable` is the driver classification |
| `PAYLOAD_MISMATCH` | Refused | The task id exists, and the command or the idempotency key differs from the original send — the payload was altered between send and retry |
| `IDEMPOTENCY_KEY_CONFLICT` | Accepted | The idempotency key is reserved by another task with a different command; nothing was enqueued. `owner_task_id` names the owner task. A retry after the reservation ends is accepted |
| `PAYLOAD_TOO_LARGE` | Refused | Serialized kwargs exceeded `payload.reject_bytes` (off by default); nothing was enqueued — shrink the payload (pass a reference to external storage) or raise the limit |
| `EVENT_LOOP_LIMIT` | Refused | The broker serves its limit of open event loops (5, plus its sync runner loop and, in a worker child, the task loop) and this send ran on a new one; nothing was enqueued — close loops that are done |
| `LOOP_RUNNER_FAILED` | Accepted | Sync `.send()` / `.schedule()` (or a sync retry) could not run the enqueue on the broker's sync runner (stopped, thread did not start, or scheduling failed); nothing was enqueued. A stopped runner does not start again, so a sync retry returns the same error — `retry_send_async(err)` / `retry_schedule_async(err)` completes the same send |

### `TaskSendPayload`

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
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

<!-- check: skip partial -- uses names that the page does not define -->
```python
match my_task.send(**kwargs):
    case Ok(handle):
        result = handle.get(timeout_ms=5000)
    case Err(TaskSendEnqueueFailed(retryable=True) as err):
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

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
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
    max_delay_seconds: int | None = None,              # cap on each delay; None = no cap
)
```

### Validation rules

- `fixed`: `intervals` must have 1–20 entries. Each entry is 1–86400 seconds. The number of entries is the number of retries.
- `exponential`: `base_seconds` is 1–86400. `max_retries` is 1–20.
- `max_delay_seconds` (`exponential` only): `None` or an integer >= 1. The cap applies last, after backoff and jitter.
- `auto_retry_for` must have at least 1 entry. Each entry must be `UPPER_SNAKE_CASE` — exception class names like `"TimeoutError"` are rejected.
- A rule violation raises `pydantic.ValidationError` (a `ValueError` subclass) at the constructor call.

### Jitter

`jitter=True` (default) adds a random 0–25% to each delay; it never shortens a delay. Prevents thundering herd. Disable only in tests.

### `auto_retry_for` valid values

- User-defined domain codes: `"RATE_LIMITED"`, `"DEADLOCK"`, `"SERVICE_UNAVAILABLE"`
- `BuiltInTaskCode` members that a task failure produces, for example `OperationalErrorCode.UNHANDLED_EXCEPTION`, `OperationalErrorCode.WORKER_CRASHED`, `OutcomeCode.TASK_TIMEOUT`
- Plain strings matching `^[A-Z][A-Z0-9_]*$`

Retries only trigger when the error code on the stored result matches and `retry_count < max_retries`.

### Examples

<!-- check: skip partial -- uses a name before the page defines it -->
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
- Invalid entries raise `ConfigurationError(HRS-209)`

## Serialization Rules

Serialization happens at `.send()` time, before enqueueing. Failure returns `Err(TaskSendValidationFailed)` — task is never enqueued.

Serialized size is checked against `AppConfig.payload` (`PayloadPolicy`): above `warn_bytes` (default 1 MiB) a rate-limited warning fires; above `reject_bytes` (off by default) the send returns `Err(TaskSendPayloadTooLarge)` (with `size_bytes` and `limit_bytes`) and nothing is enqueued. See `configs.md`.

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
- PEP 695 `type X = ...` aliases (`TypeAliasType`) anywhere in a model that
  reaches a `TaskResult` ok slot — rejected by strict-serde at app startup
  (HRS-105), which surfaces far from the alias itself; use a plain assignment
  (`X = Literal[...]`) instead
- `bytes`, `bytearray`
- Enum instances (convert to `.value` first)
- Objects defined in `__main__` or inside functions (`<locals>` in qualname)
- `dict` keys that collide after `str()` coercion
- Non-finite floats (`nan`, `inf`)
- A lone surrogate in a string or a dict key, for example a file name that `os.listdir()` decoded with `surrogateescape`
- A value that holds itself, or a value nested too deep: pydantic refuses at 255 levels of lists and dicts, and `loads_json` refuses text past 300 levels

### Return type serialization

Worker serializes the return value after execution with the strict-serde
codec. Non-serializable return → `WORKER_SERIALIZATION_ERROR`.

### Stored-result decode failure

Decode failures surface differently depending on which retrieval API the
caller uses:

- `app.get_result(task_id, ...)` returns
  `Err(BrokerInvalidJsonPayload(...))` (code `INVALID_JSON_PAYLOAD`) when
  the stored envelope is malformed, missing the `__h_task_result__`
  marker, or fails typed decode against the registered `task_ok_type`.
  Cross-process monitoring and debugging callers see the infrastructure
  failure as an outer `Err`, distinct from any domain `TaskError`.

- `TaskHandle.get(...)` returns
  `TaskResult(err=TaskError(error_code=RESULT_DESERIALIZATION_ERROR))`
  when the stored payload cannot be decoded — `handle.get()` always
  returns a `TaskResult`, so the decode failure is folded into the
  domain error surface.

## Common Pitfalls

### `.send()` at module level → `SEND_SUPPRESSED`

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — runs during worker import
result = my_task.send(value="test")  # Err(SEND_SUPPRESSED)

# CORRECT — call from functions/endpoints
def process():
    match my_task.send(value="test"):
        case Ok(handle): ...
        case Err(err): ...
```

### Non-serializable arguments → `VALIDATION_FAILED`

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG
my_task.send(conn=db_connection)

# CORRECT
my_task.send(conn_url=str(db_connection.url))
```

### Classes defined in `__main__` or inside functions

<!-- check: skip elided -- shows a wrong form next to the right one -->
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

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — returns raw value, fails with UNHANDLED_EXCEPTION (AttributeError) at runtime
@app.task("bad")
def bad(*, x: int) -> TaskResult[dict[str, int], TaskError]:
    return {"x": x}

# CORRECT
@app.task("good")
def good(*, x: int) -> TaskResult[dict[str, int], TaskError]:
    return TaskResult(ok={"x": x})
```

### `auto_retry_for` at top level

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — rejected at definition time
@app.task("t", auto_retry_for=["TIMEOUT"])
def t() -> TaskResult[str, TaskError]: ...

# CORRECT — goes inside RetryPolicy
@app.task("t", retry_policy=RetryPolicy.fixed([60], auto_retry_for=["TIMEOUT"]))
def t() -> TaskResult[str, TaskError]: ...
```

### Decorator ordering — `HRS-104`

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — @app.task sees already-wrapped function
@app.task("t")
@some_other_decorator
def t() -> TaskResult[str, TaskError]: ...

# CORRECT — @app.task must be applied to the raw function
@app.task("t")
def t() -> TaskResult[str, TaskError]: ...
```

### Open event loops that are never closed → `EVENT_LOOP_LIMIT`

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — every call leaves one more open loop; the sixth gets EVENT_LOOP_LIMIT
loop = asyncio.new_event_loop()
loop.run_until_complete(my_task.send_async(value="test"))

# CORRECT — asyncio.run() closes its loop, so the next call releases its set
asyncio.run(my_task.send_async(value="test"))
```

### Sync calls inside a coroutine or a Jupyter cell → `ASYNC_CONTEXT`

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — on a thread that runs an event loop, each sync call returns ASYNC_CONTEXT
async def report(handle: TaskHandle[str]) -> None:
    send_r = my_task.send(value="test")      # Err(ASYNC_CONTEXT), nothing enqueued
    result = handle.get(timeout_ms=5000)     # TaskError(ASYNC_CONTEXT), not cached

# CORRECT — use the *_async forms
async def report() -> None:
    handle = (await my_task.send_async(value="test")).unwrap()
    result = await handle.get_async(timeout_ms=5000)
```

### `WAIT_TIMEOUT` is not a terminal failure

<!-- check: skip partial -- uses names that the page does not define -->
```python
result = handle.get(timeout_ms=5000)
if result.is_err() and result.err_value.error_code == RetrievalCode.WAIT_TIMEOUT:
    # Task may still be running. NOT cached — calling get() again will re-poll.
    check_again_later(handle.task_id)
```

### `queue_name` in DEFAULT mode

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — raises HRS-200 at definition time if app uses QueueMode.DEFAULT
@app.task("t", queue_name="critical")
def t() -> TaskResult[str, TaskError]: ...
```

### `good_until` is a per-send concern, not a decorator option

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
# WRONG — rejected at definition time (raises HRS-102)
@app.task("t", good_until=datetime.now(timezone.utc) + timedelta(hours=1))

# CORRECT — set at send time via with_options()
deadline = datetime.now(timezone.utc) + timedelta(hours=1)
my_task.with_options(good_until=deadline).send()

# CORRECT — for workflow nodes, use .node(good_until=...)
node = my_task.node(good_until=deadline)(arg=value)
```

`good_until` must be timezone-aware. Naive datetimes return `Err(VALIDATION_FAILED)`.

## All Public Imports

<!-- check: alone -->
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
    TaskSendRetryable, NoCause, TaskSendSuppressed, TaskSendAsyncContext, TaskSendValidationFailed,
    TaskSendEnqueueFailed, TaskSendPayloadMismatch, TaskSendPayloadTooLarge,
    TaskSendIdempotencyKeyConflict, TaskSendEventLoopLimit, TaskSendLoopRunnerFailed,
    # Exception mapper
    ExceptionMapper,
    # Status
    TaskStatus, TaskAttemptOutcome, TASK_TERMINAL_STATES,
    # Broker result (for handle.info()); match on the variant
    BrokerResult, BrokerOperationError, BrokerErrorCode,
    BrokerOperationFailed, BrokerEnqueueFailed, BrokerInvalidInput,
    broker_error_cause,
    # Node data flow helper
    from_node,
    # Rerun (re-execute a terminal task as a new request)
    rerun_task, RerunTask, RerunEnqueuePolicy, RerunOutcome,
    RerunEnqueued, RerunSourceLive, RerunSourceAbsent, RerunSourceCorrupt,
    RerunNotEligible, RerunInputUnavailable, RerunInputCorrupt,
    RerunKeyConflict, RerunKeyReplay, NotEligibleReason,
    # Result primitives
    Result, Ok, Err, is_ok, is_err,
)
```

## Terminal Records Are Immutable

A task that reaches a terminal status leaves `horsies_tasks` for the
`horsies_task_history` archive in the same transaction. Consequences for task
code and callers:

- **Manual in-place retry is removed.** `RetryPolicy` still governs automatic
  retry of a live task; re-executing a *terminal* task is a new request via
  `rerun_task`, with a new task id and recorded lineage.
- Result and info reads still work after the move — they resolve across live
  and history.
- Rerun eligibility is decided **at enqueue**: `retain_rerun_input_default`
  (per-task override available) decides whether the enqueue input is preserved
  with the record. A `COMPLETED` source is not eligible for rerun.
- Every task carries a retention class assigned at enqueue (the send's own
  choice, else the queue's `queue_retention` mapping, else the immutable
  30-day class); records age by dropping whole partitions.
