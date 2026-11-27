---
title: Defining Tasks
section: python
summary: How to define tasks with the @app.task decorator, and how a task reads the fencing token of its run through the runtime parameter.
related: [sending-tasks, retrieving-results, ../../concepts/result-handling, ../../concepts/fencing-token]
tags: [tasks, decorator, definition, TaskRuntime, FencingToken, fencing token]
---

# Defining Tasks

Define tasks by decorating functions with `@app.task()` and returning `Result[T, TaskError]`. A task function is a `def` or an `async def` function. For argument serialization rules, see `sending-tasks`.

## How To

### Basic Task

```python
from pydantic import SecretStr
from horsies import Horsies, AppConfig, PostgresConfig, Err, Ok, Result, TaskError, JsonValue

config = AppConfig(
    database=PostgresConfig(database_url=SecretStr("postgresql://...")),
)
app = Horsies(config)

@app.task("process_order")
def process_order(*, order_id: int) -> Result[str, TaskError]:
    # Do work
    return Ok(f"Order {order_id} processed")
```

### Async Tasks

Define a task with `async def` to await I/O in its body. `@app.task()` checks it as it checks a `def` task, and it returns a `Result` in the same way.

```python
import asyncio

@app.task("fetch_price", timeout_ms=30_000)
async def fetch_price(*, symbol: str) -> Result[float, TaskError]:
    await asyncio.sleep(0.1)  # stands in for an awaited client call
    return Ok(101.5)
```

With `Execution.ISOLATED` (the default), a worker child runs one task at a time, `def` or `async def`. A `SHARED_ASYNC` task runs next to other tasks on the event loop of a shared async child instead: see [Shared Async Tasks](/horsies/workers/shared-async-tasks/). An isolated child keeps one event loop until the child stops, and an async task runs on that loop. An async client that a module keeps stays open across the tasks of one child.

The loop runs only while an async task runs. A `def` task runs with no running event loop, so it can call `asyncio.run()`. While the child runs a `def` task or waits for its next task, the timers of a module-level async client, for example a keepalive or a pool maintenance task, do not run.

An async task must await the work that it starts. When the body of an async task returns (a value, or an exception that the task maps to a result) and asyncio tasks that it started, for example with `asyncio.create_task()`, are still on the loop:

1. The child waits up to 1 second for that work, also for the asyncio tasks that it starts meanwhile.
2. Work that is still alive gets a cancel, and two more cancels 0.1 seconds apart.
3. When the work has ended, the worker writes what the task returned. Only then do the retry and the next workflow node start, so they never run next to that work. The worker then replaces the child, and the next task runs in a new child: cancelled code can leave broken state in the process.
4. Work that is still alive 0.3 seconds after the first cancel, for example a task that catches every cancel, is stuck. The worker kills the child, and writes what the task returned after it saw the exit. A child that reports neither the end of the work nor that it is stuck within 26.3 seconds of the return, for example because that work blocks the event loop, is killed the same way. When the exit is not seen within 30 seconds of the kill, the rule of a timeout kill applies (see [Set an Execution Timeout](#set-an-execution-timeout)).

The worker writes runner heartbeats for the row while the child waits for that work: the child marks the row finalizing only after the work ended, and not at all when it did not stop. So [stale RUNNING recovery](/horsies/workers/heartbeats-recovery/#stale-running-recovery) does not fail the row while the work runs.

The return ends the deadline of the task: `timeout_ms` bounds the body, and what the task returned stands while the child waits for the work. The outcome is never `TASK_TIMEOUT` then. A deadline that fires before the worker has read the return still kills the child, as for a result that arrives after the deadline. A crash of the child in that wait also keeps what the task returned, once the worker saw the exit.

- The worker sees asyncio tasks only. A callback that `loop.call_soon()` or `loop.call_later()` scheduled, and work that `loop.run_in_executor()` started and the task did not await, are not waited for and cause no replacement. They can still run during a later task of the same child.
- A client that keeps its own background tasks on the loop, for example a keepalive or a consumer task, delays the outcome of each async task by about 1 second, and causes a child replacement after it.
- A thread that `asyncio.to_thread()` starts can outlive its task, for example when a timeout ends the wait for it. The thread is not an asyncio task, so it is not waited for and does not cause a replacement. When the worker stops the child, the child does not wait for that thread past the close, which takes at most 7 seconds: it exits, and its `atexit` handlers do not run. An idle executor thread does not count: the close of the async generators and the shutdown of the executor each get at least 1 second.
- The worker logs one line for each task that left work: the task, the coroutine of each asyncio task that was left, for up to 10 tasks, and how that work ended, for example `Task watch_price (<id>) returned and left 1 task(s) on the event loop of child pid=4242 (refresh_session); that work ended with no cancel; the outcome is what the task returned, and the child is replaced`.

A task that cancels a helper task in `finally` must also await it. `cancel()` only asks the helper to stop. A helper that awaits while it stops, for example to close a session, is still on the loop when the task returns: the outcome waits for it, and the worker replaces the child. Await the helper after the cancel:

```python
async def refresh_session() -> None:
    try:
        while True:
            await asyncio.sleep(30)  # stands in for a keepalive call
    finally:
        await asyncio.sleep(0)  # stands in for closing the session


@app.task("watch_price")
async def watch_price(*, symbol: str) -> Result[float, TaskError]:
    helper = asyncio.create_task(refresh_session())
    try:
        await asyncio.sleep(0.1)  # stands in for the work of the task
        return Ok(101.5)
    finally:
        helper.cancel()
        # gather returns the CancelledError of the helper instead of raising it.
        await asyncio.gather(helper, return_exceptions=True)
```

A child creates its event loop at boot, before it reports ready, when the app has at least one `async def` task. A child of an app with no `async def` task creates no loop. When the loop cannot be created:

- The child fails its boot, and no task reaches it.
- An `OSError` with `EMFILE`, `ENFILE`, `ENOMEM` or `EAGAIN` (the process has no file descriptor left, for example) can clear, so the slot is down: it takes no task, and the worker starts a new child for it after a growing wait. Other children continue. When every slot is down, the worker exits non-zero after three failed attempts, so its supervisor restarts it. The log names the slot and the cause, for example `Slot 1 is down: the OS ran short of a resource (EMFILE): OSError: [Errno 24] Too many open files`.
- Any other failure, such as an event loop policy that raises, is the same on every start, so it repeats (`TaskLoopCreateFailed`): each failed start counts one of three attempts, also while other children serve, and the third stops the worker. See [Child Start Failures](/horsies/workers/worker-architecture/#child-start-failures).

In an async task, send with `.send_async()`. The sync `.send()` refuses a running event loop: it returns `Err(TaskSendAsyncContext)` and enqueues nothing. See [Sending Tasks](/horsies/tasks/sending-tasks/).

```python
from horsies import Ok, Err

@app.task("refresh_price")
async def refresh_price(*, symbol: str) -> Result[str, TaskError]:
    match await fetch_price.send_async(symbol=symbol):
        case Ok(handle):
            return Ok(str(handle.task_id))
        case Err(send_err):
            return Err(TaskError(code="SEND_FAILED", message=send_err.message))
```

`timeout_ms` works as for a `def` task: on expiry, the worker kills the child. An `async def` function that uses `yield` is refused with `HRS-101`.

### Parameters are keyword-only

Every task parameter must be keyword-only: declare them after a bare `*,`. The leading `*` forces callers to pass arguments by name.

```python
@app.task("send_email")
def send_email(*, to: str, subject: str) -> Result[None, TaskError]:
    ...
```

The queue transmits arguments by name only — the signature is the wire contract. A positionally-bindable parameter (`def f(order_id: int)`) is rejected at registration, so the error surfaces when the app loads, not at send time. With keyword-only parameters, a positional call (`process_order.send(1)`) also fails the type checker at the call site rather than at runtime.

One parameter comes before the `*`: `runtime: TaskRuntime`, which the worker passes. See [Task Runtime](#task-runtime).

### Task Runtime

A task whose first parameter is `runtime: TaskRuntime` gets the runtime of its run. The worker builds one runtime for each run and passes it as the first argument.

```python
from horsies import FencingToken, TaskRuntime


@app.task('reserve_stock')
def reserve_stock(runtime: TaskRuntime, *, sku: str) -> Result[FencingToken, TaskError]:
    return Ok(runtime.fencing_token)
```

The rules of the parameter:

- It is the first parameter, before the `*`. Its name is free; the pages use `runtime`.
- Its annotation is exactly `TaskRuntime`. A run always has a runtime, so `TaskRuntime | None` is refused.
- It has no default.
- It is not part of the wire contract. `.send()`, a workflow node and a schedule do not supply it, and a kwarg with its name is refused.

`TaskRuntime` in any other place (a keyword-only parameter, a second positional parameter, a field of a model, the ok type of the result) is refused at registration with `HRS-105` (`TASK_STRICT_SIGNATURE_VIOLATION`).

The decorator gives a `RuntimeTaskFunction` for a `def` function and an `AsyncRuntimeTaskFunction` for an `async def` function. Their sends and `node()` take the parameters after the runtime. A direct call takes the runtime first: in a test, pass `TaskRuntime.for_test(...)` ([Testing](/horsies/operations/testing/#pass-a-task-runtime)).

| Member | Type | Description |
|--------|------|-------------|
| `fencing_token` | `FencingToken` | The fencing token of this attempt. See [Fencing Token](#fencing-token) |
| `cancel_state()` | `RunCancelState` | Whether a cancel of this run was requested, and why. See [Cancellation](#cancellation) |

### Fencing Token

An attempt can run on after the worker retried its task, for example when the worker process is paused. horsies refuses the late writes of that attempt to its own tables. It cannot refuse its writes to your systems. Write the fencing token with each such write, and make the store refuse a write whose token is lower than the token that it holds for the task id. The pattern and its limits are on [Fencing Token](/horsies/concepts/fencing-token/).

`runtime.fencing_token` is a frozen `FencingToken`:

| Attribute | Type | Description |
|-----------|------|-------------|
| `task_id` | `uuid.UUID` | The id of the task. The token orders the attempts of this task only |
| `value` | `int` | From 1. Strictly greater for each attempt that runs after another attempt of the task |

- Two reads in one attempt give the same token.
- The value depends on no clock.
- Today the value is the attempt number, 1 to 21. Compare the values of one task id; do not depend on the number itself.

The task below writes a payment with one conditional statement. The row changes only when no later attempt of the task wrote it.

<!-- check: skip partial -- uses a database connection that the page does not define -->
```python
from horsies import FencingToken, TaskRuntime

UPSERT_PAYMENT = """
INSERT INTO payments (task_id, fence, order_id, cents)
VALUES (%(task_id)s, %(fence)s, %(order_id)s, %(cents)s)
ON CONFLICT (task_id) DO UPDATE
SET fence = EXCLUDED.fence, order_id = EXCLUDED.order_id, cents = EXCLUDED.cents
WHERE payments.fence <= EXCLUDED.fence
"""


@app.task('record_payment')
def record_payment(runtime: TaskRuntime, *, order_id: str, cents: int) -> Result[str, TaskError]:
    token = runtime.fencing_token
    with connection.cursor() as cursor:
        cursor.execute(
            UPSERT_PAYMENT,
            {'task_id': token.task_id, 'fence': token.value, 'order_id': order_id, 'cents': cents},
        )
        written = cursor.rowcount
    connection.commit()
    match written:
        case 0:
            return Err(TaskError(
                code='LATER_ATTEMPT',
                message='a later attempt of this task holds the fence',
            ))
        case _:
            return Ok(order_id)
```

Limits:

- A rerun, and a workflow node that a resume starts again, get a new task id. Their value starts again at 1.
- A stale attempt with no later attempt (the task failed, was cancelled or expired) is not refused.
- A stale attempt that writes before the later attempt took the fence is not refused. Take the fence at the start of the body.
- The runtime is a plain value. Work that the body starts (a thread, an asyncio task, an executor call) reads the token only when the body passes `runtime` or the token on as an argument.

### Return Success

```python
@app.task("compute")
def compute(*, x: int) -> Result[int, TaskError]:
    return Ok(x * 2)
```

For void tasks:

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task("fire_and_forget")
def fire_and_forget() -> Result[None, TaskError]:
    do_something()
    return Ok(None)
```

### Return Errors

Domain errors (expected failures):

```python
@app.task("validate_input")
def validate_input(*, data: dict[str, JsonValue]) -> Result[dict[str, JsonValue], TaskError]:
    if not data.get("email"):
        return Err(TaskError(
            code="MISSING_EMAIL",
            message="Email is required",
            data={"field": "email"},
        ))
    return Ok(data)
```

Unhandled exceptions are wrapped automatically:

```python
@app.task("might_crash")
def might_crash() -> Result[str, TaskError]:
    raise ValueError("Something went wrong")
    # Becomes Err(TaskError(
    #     code=OperationalCode.UNHANDLED_EXCEPTION,
    #     message="Unhandled exception in task might_crash: ...",
    # ))
```

### Configure Retries

`retry_policy` takes a `RetryPolicy`. A failure retries when its error code is in `retry_for` and retries remain. The entries of `auto_retry_for` are code members: members of the built-in code enums, or members of the `Enum` class that the task declares with `error_codes=` ([Declare Error Codes](#declare-error-codes)). The policy fields, the delay steps and the retry rules are on [Retry Policy](/horsies/tasks/retry-policy/).

```python
from enum import Enum

from horsies import RetryPolicy, Err, Ok, Result, TaskError, JsonValue


class SyncCode(str, Enum):
    TRANSIENT_ERROR = "TRANSIENT_ERROR"


@app.task(
    "flaky_task",
    error_codes=SyncCode,
    retry_policy=RetryPolicy.fixed([60, 300, 900], retry_for=[SyncCode.TRANSIENT_ERROR]),
)
def flaky_task() -> Result[str, TaskError[SyncCode]]:
    # Retries up to 3 times, after 1 min, 5 min and 15 min
    ...
```

#### RetryPolicy Constructors

| Constructor | Policy |
| ----------- | ------ |
| `RetryPolicy.fixed(intervals_seconds, *, retry_for, jitter=True)` | `fixed` backoff. `max_retries` is `len(intervals_seconds)` |
| `RetryPolicy.exponential(base_seconds, *, max_retries, retry_for, jitter=True, max_delay_seconds=None)` | `exponential` backoff from one base interval |
| `RetryPolicy(...)` | Every field, with the defaults below |

```python
# Fixed: wait 1 min, then 5 min, then 15 min
RetryPolicy.fixed([60, 300, 900], retry_for=[SyncCode.TRANSIENT_ERROR])

# Equivalent to:
RetryPolicy(
    max_retries=3,
    intervals_seconds=[60, 300, 900],
    backoff_strategy='fixed',
    retry_for=[SyncCode.TRANSIENT_ERROR],
)

# Exponential with base 30 s: retry after 30 s, 60 s, 120 s, 240 s, 480 s
RetryPolicy.exponential(base_seconds=30, max_retries=5, retry_for=[SyncCode.TRANSIENT_ERROR])

# Equivalent to:
RetryPolicy(
    max_retries=5,
    intervals_seconds=[30],  # one base interval
    backoff_strategy='exponential',
    retry_for=[SyncCode.TRANSIENT_ERROR],
)

# No jitter, for delays that are the same on each run
RetryPolicy.fixed([60, 300, 900], retry_for=[SyncCode.TRANSIENT_ERROR], jitter=False)

# Exponential, but never wait longer than 10 minutes between retries
RetryPolicy.exponential(
    base_seconds=30,
    max_retries=10,
    retry_for=[SyncCode.TRANSIENT_ERROR],
    max_delay_seconds=600,
)

# Equivalent to:
RetryPolicy(
    max_retries=10,
    intervals_seconds=[30],
    backoff_strategy='exponential',
    max_delay_seconds=600,
    retry_for=[SyncCode.TRANSIENT_ERROR],
)
```

`RetryPolicy.fixed()` has no `max_delay_seconds` parameter. For a `fixed` policy with a cap, use `RetryPolicy(...)`.

| Field | Type | Default |
| ----- | ---- | ------- |
| `retry_for` | `list[BuiltInTaskCode \| Enum]` | (required) |
| `max_retries` | `int` | `3` |
| `intervals_seconds` | `list[int]` | `[60, 300, 900]` |
| `backoff_strategy` | `Literal['fixed', 'exponential']` | `'fixed'` |
| `jitter` | `bool` | `True` |
| `max_delay_seconds` | `int \| None` | `None` (no cap) |

A `retry_for` entry is a code member: a `BuiltInTaskCode` member, or a member of the `Enum` class of the task's `error_codes=`. Pyright refuses a plain `str`. The constructor does not check the entries: `@app.task` checks them against the codes of the task. It refuses a plain `str` (also a misspelled code such as `"BROKR_ERROR"`), a member of another `Enum` class, and a custom member of a task that declares a tuple or no `error_codes`, with `TaskDefinitionError` `TASK_INVALID_OPTIONS` (`HRS-102`). The notes name the task and the entry, and the task is not registered. A task that retries on a code of its own declares the code in an `Enum` class. The stored policy holds the text of each code.

A policy that breaks a rule of [Policy Fields](/horsies/tasks/retry-policy/#policy-fields) raises `ValueError` when it is built:

```python
# This raises ValueError:
RetryPolicy(
    max_retries=3,
    intervals_seconds=[60, 300],  # Only 2 intervals for 3 retries
    backoff_strategy='fixed',
    retry_for=[SyncCode.TRANSIENT_ERROR],
)

# This also raises ValueError:
RetryPolicy(
    max_retries=3,
    intervals_seconds=[60, 300, 900],  # Multiple intervals
    backoff_strategy='exponential',  # Exponential needs exactly 1 interval
    retry_for=[SyncCode.TRANSIENT_ERROR],
)
```

#### Return a Retryable Error

Return a `TaskError` with a code that `retry_for` lists:

```python
class ApiCode(str, Enum):
    RATE_LIMITED = "RATE_LIMITED"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    TIMEOUT = "TIMEOUT"


@app.task(
    "call_external_api",
    error_codes=ApiCode,
    retry_policy=RetryPolicy.exponential(
        base_seconds=60,
        max_retries=5,
        retry_for=[ApiCode.RATE_LIMITED, ApiCode.SERVICE_UNAVAILABLE],
    ),
)
def call_external_api() -> Result[dict[str, JsonValue], TaskError[ApiCode]]:
    try:
        response = requests.get("https://api.example.com")
        if response.status_code == 429:
            return Err(TaskError(
                code=ApiCode.RATE_LIMITED,
                message="the API answered 429",
            ))
        return Ok(response.json())
    except requests.Timeout:
        return Err(TaskError(
            code=ApiCode.TIMEOUT,
            message="the API did not answer in time",
        ))
```

`retry_for` reads `code` only. `message` is optional: `TaskError(code=ApiCode.RATE_LIMITED)` retries in the same way.

#### Map Exceptions to Codes

`exception_mapper` maps an exception that the task raises to an error code, so the task needs no try/except. The mapper matches the exact exception class (`type(exc)`).

```python
class CallCode(str, Enum):
    TIMEOUT = "TIMEOUT"
    CONNECTION_ERROR = "CONNECTION_ERROR"


@app.task(
    "call_api",
    error_codes=CallCode,
    retry_policy=RetryPolicy.fixed([30, 60, 120], retry_for=[CallCode.TIMEOUT, CallCode.CONNECTION_ERROR]),
    exception_mapper={
        TimeoutError: "TIMEOUT",
        ConnectionError: "CONNECTION_ERROR",
    },
)
def call_api() -> Result[dict[str, JsonValue], TaskError]:
    # No try/except: a TimeoutError becomes "TIMEOUT"
    response = requests.get("https://api.example.com", timeout=10)
    return Ok(response.json())
```

A mapper on `AppConfig` applies to all tasks:

```python
from pydantic import SecretStr

config = AppConfig(
    database=PostgresConfig(database_url=SecretStr("postgresql://...")),
    exception_mapper={
        TimeoutError: "TIMEOUT",
        ConnectionError: "CONNECTION_ERROR",
        PermissionError: "PERMISSION_DENIED",
    },
    default_unhandled_error_code="UNHANDLED_EXCEPTION",
)
```

When the task raises an exception, the code comes from the first match in this order:

1. The task's `exception_mapper` (exact class lookup)
2. `AppConfig.exception_mapper` (exact class lookup)
3. The task's `default_unhandled_error_code`
4. `AppConfig.default_unhandled_error_code` (default `"UNHANDLED_EXCEPTION"`)

A task entry has priority over a global entry. A task that returns `Err(...)` does not use the mapper.

Only the exact class matches; a subclass does not. Map each subclass that you expect.

#### Declare Error Codes

`error_codes` declares the task's own error codes: the values of the `error_code` label of its worker metrics besides the built-in codes and `undeclared` ([Declared Error Codes](/horsies/tasks/errors/#declared-error-codes)). Pass an `Enum` class, whose member values are the codes, or a tuple of strings:

```python
from enum import Enum

from horsies import Err, Horsies, Ok, Result, TaskError


class ChargeCode(Enum):
    CARD_DECLINED = "CARD_DECLINED"
    RATE_LIMITED = "RATE_LIMITED"


def register(app: Horsies) -> None:
    @app.task("charge_card", error_codes=ChargeCode)
    def charge_card(*, amount_cents: int) -> Result[str, TaskError[ChargeCode]]:
        if amount_cents > 100_000:
            return Err(TaskError(code=ChargeCode.CARD_DECLINED))
        return Ok("charged")

    @app.task("send_receipt", error_codes=("BOUNCED",))
    def send_receipt(*, email: str) -> Result[str, TaskError]:
        return Ok(email)
```

`TaskError` is generic in the type of its custom code. `TaskError` with no argument is `TaskError[str]`: the code is a `str`. A task that declares an `Enum` with `error_codes=` and returns `Result[T, TaskError[E]]` has typed codes:

- The body passes members of `E` (`ChargeCode.CARD_DECLINED`). A `str` code is a `RETURN_TYPE_MISMATCH`.
- The stored text is the value of the member. It does not change.
- These reads give members of `E` back: `TaskHandle.get()`; `WorkflowHandle.result_for(node)` and `WorkflowContext.result_for(node)` for a node of the task (`task.node()(...)` gives a `TaskNode[T, E]`); an `args_from` parameter annotated `TaskOutcome[T, TaskError[E]]`; the entries of `results()`, `tasks()` and the map of an outputless workflow's `get()`; and `app.get_result()`, typed `TaskError[Any]`.
- A stored code that `E` does not name (a row of another version of the task) is the read failure `ReadUnknownCustomCode` (`UNKNOWN_CUSTOM_CODE`). In a map of node outcomes and in a workflow context, it is a failed entry with `RESULT_DESERIALIZATION_ERROR`; for an `args_from` input, the node fails with `WORKER_SERIALIZATION_ERROR`.
- The error of a workflow that `WorkflowHandle.get()` gives keeps its custom code as text. That error is the error of the failed node, which can be any node of the workflow, with the codes of its own task, so no one `Enum` names it.
- A `SubWorkflowNode` gives custom codes as text: a child workflow holds the codes of many tasks.
- A `match` on `task_error.code` that leaves out a member of `E` fails pyright's exhaustiveness check.

A task that returns `TaskError` keeps `str` codes with any declaration. Declaring is optional, and the default is `()`. A custom code that the task does not declare still works everywhere; its metric label is `undeclared`. An `auto_retry_for` entry of a code of the task's own is a member of the declared `Enum` class, so a task that retries on its own code declares an `Enum` class ([Configure Retries](#configure-retries)).

The codes that the exception mappers can produce are declared with no second declaration: the values of the task's `exception_mapper` and of `AppConfig.exception_mapper` (as the app config holds it when the task is defined). A code that a task body returns directly needs `error_codes=`. For a task with `TaskError[E]`, a mapped exception gives the member of `E`, so each custom code of both mappers and both `default_unhandled_error_code` values must be the value of a member of `E`.

The decorator refuses:

- a value that is neither an `Enum` class nor a tuple (a list, a string, an `Enum` member): `HRS-102`;
- a code that is empty, not `UPPER_SNAKE_CASE`, or an exception class name: `HRS-112`;
- a built-in code or `WORKFLOW_STOPPED`: `HRS-212`;
- a code given twice: `HRS-113`;
- `TaskError[E]` in the return annotation without `error_codes=E`: `HRS-105`;
- for a task with `TaskError[E]`, a mapper code or a default code that `E` does not name: `HRS-209`;
- a `retry_for` entry that is not a built-in member or a member of the declared `Enum` class: `HRS-102`.

The task function keeps the codes in `error_codes`: the declared codes in the order of the declaration, then each mapper code that is not in the list yet, first the task's mapper, then the app's. `horsies check` lists them.

### Assign to Queue (CUSTOM Mode)

```python
@app.task("urgent_task", queue="critical")
def urgent_task() -> Result[str, TaskError]:
    ...
```

### Set an Execution Timeout

`timeout_ms` bounds task execution time, measured from the moment a worker gives the task to a child process. Time that a task waits for a free child does not count. The value is from 1000 (1 second) to `MAX_TASK_TIMEOUT_MS` (4,294,967,295, about 49.7 days), which is also the limit of the Rust port. `@app.task` refuses a value outside that range with `TASK_INVALID_OPTIONS`. A queued row that holds a value outside it fails with `WORKER_SERIALIZATION_ERROR` when a worker claims it, and no user code runs.

```python
@app.task("resize_video", timeout_ms=120_000)
def resize_video(*, video_id: str) -> Result[str, TaskError]:
    ...
```

On expiry, the worker kills the child process, waits for it to exit, and then fails the task with `TASK_TIMEOUT`. A result that reaches the worker after the deadline does not change this. An `async def` body that returned before the deadline is not timed out while the child waits for the asyncio tasks that it left (see [Async Tasks](#async-tasks)). If the deadline elapses before user code starts, the task is requeued instead. Timeouts are terminal by default; opt into retries by adding `OutcomeCode.TASK_TIMEOUT` to `retry_for`:

```python
@app.task(
    "resize_video",
    timeout_ms=120_000,
    retry_policy=RetryPolicy.fixed([60, 300], retry_for=[OutcomeCode.TASK_TIMEOUT]),
)
def resize_video(*, video_id: str) -> Result[str, TaskError]:
    ...
```

The kill goes to the child and to the subprocesses in its session. Other tasks on the same worker continue. A subprocess that calls `setsid` itself leaves the session, and the kill does not reach it. The worker starts a new child in that slot, so size `timeout_ms` as a hang backstop well above normal runtime, not as routine control flow.

The worker writes the timeout only after the child has exited. So a retry never runs next to the attempt that it replaces, and a workflow node that runs after a failure does not start while that attempt runs. If the child does not exit within 30 seconds of the kill, the worker writes nothing and logs at critical level. Then [stale-task recovery](/horsies/workers/heartbeats-recovery/) handles the task: a started task as a crashed worker's task, and a task that had not started when its claim lease ends. With stale RUNNING recovery off (no `RecoveryConfig`, or `fail_stale_running=False`), nothing else would move the row, so the worker waits for the exit and writes the row once it sees it; a stop of the worker ends that wait.

A task with `Execution.SHARED_ASYNC` on a shared async child is different. On expiry its child cancels it: the task gets `asyncio.CancelledError` at its next `await`. The task fails with `TASK_TIMEOUT` when it has stopped, with the work that it started, also when it returns a value after the cancel. If it has not stopped `cancel_grace_ms` after the cancel (default 1 second), the child cancels it a second time, and takes no new task. A task that still has not stopped is stuck: the worker kills the child when every task left in it is stuck, so no other task in that child loses time that it still had. Each of those tasks then fails with `TASK_TIMEOUT`. A task that blocks the event loop of its child for 5 seconds makes the worker kill the child at once: that task fails with `TASK_BLOCKED_LOOP`, each other task before its deadline with `NEIGHBOUR_BLOCKED_LOOP`. See [Shared Async Tasks](/horsies/workers/shared-async-tasks/#the-timeout).

A `SHARED_THREAD` task on a shared thread child cannot be stopped from outside. On expiry the cancel state of its run gets the cause `DEADLINE` (see [Cancellation](#cancellation)), and its child closes: it takes no new task. When no task in the child has time left, the worker kills the child, and each task that was still in it fails with `TASK_TIMEOUT`. A task alone in its child is killed at its deadline. See [Shared Thread Tasks](/horsies/workers/shared-thread-tasks/#the-timeout).

### Cancellation

An operator can cancel a running task: `cancel_task(broker, task_id, include_running=True)`, or the web UI. The cancel moves the task to history as `CANCELLED` at once, and records its attempt as `CANCELLED`. Then the worker gives the cancel to the task code, and the code decides when to stop. What the task returns after the cancel is dropped. The rules of each task status are on [Action Semantics](/horsies/monitoring/action-semantics/#cancelling-a-running-task).

An `async def` task gets `asyncio.CancelledError` at its next `await`, in an isolated child and in a shared async child. Let it propagate. Catch it only to clean up, then raise it again:

```python
import asyncio


async def write_rows(rows: list[str]) -> None:
    await asyncio.sleep(0.1)  # stands in for an awaited database write


async def close_writer() -> None:
    await asyncio.sleep(0)  # stands in for closing the writer


@app.task('copy_rows', timeout_ms=600_000)
async def copy_rows(*, rows: list[str]) -> Result[int, TaskError]:
    try:
        await write_rows(rows)
    except asyncio.CancelledError:
        await close_writer()
        raise
    return Ok(len(rows))
```

A `def` task reads the cancel state of its run. Its first parameter takes the `TaskRuntime` of the run, before the `*`, and `runtime.cancel_state()` returns `CancelNotRequested()` or `CancelRequested(cause)`. Read it between the steps of the work:

```python
from horsies import CancelNotRequested, CancelRequested, TaskRuntime


def import_batch(batch_id: str) -> None:
    ...  # stands in for one step of the work


@app.task('import_batches', timeout_ms=600_000, cancel_grace_ms=5_000)
def import_batches(runtime: TaskRuntime, *, batch_ids: list[str]) -> Result[int, TaskError]:
    done = 0
    for batch_id in batch_ids:
        match runtime.cancel_state():
            case CancelRequested():
                break  # what the task returns after a cancel is dropped
            case CancelNotRequested():
                import_batch(batch_id)
                done += 1
    return Ok(done)
```

`CancelRequested.cause` is a `CancelCause`:

<!-- members: CancelCause -->
| `CancelCause` | The cancel came from |
| ------------- | -------------------- |
| `OPERATOR` | An operator cancel of the task: `cancel_task` with `include_running=True`, or the web UI |
| `DEADLINE` | The deadline of the run: a `SHARED_ASYNC` task or a `SHARED_THREAD` task at its `timeout_ms` |

The first cause stays: a deadline after an operator cancel does not change it. An `async def` task can read `cancel_state()` too, for example to tell the two causes apart while it cleans up.

When the task code does not stop:

<!-- members: Execution -->
- `ISOLATED`: the code runs until it returns, or until its `timeout_ms` kill. For a hard stop, set `cancel_grace_ms` on the task: the worker kills the child `cancel_grace_ms` after the cancel when the task has not stopped, with the subprocesses in its session. The task stays `CANCELLED`.
- `SHARED_ASYNC`: after `cancel_grace_ms`, the child cancels the task a second time, as at the deadline, and takes no new task. A task that still does not stop keeps its seat; the worker kills the child only when no task in it has time left.
- `SHARED_THREAD`: the thread runs until it returns. A thread cannot be stopped.

When an `async def` task in an isolated child leaves asyncio tasks on the loop after a cancel, the child does not wait for them, and the worker replaces the child.

A test that calls a task function directly passes a runtime with a fixed cancel state:

```python
import uuid

from horsies import CancelCause

cancelled_run = TaskRuntime.for_test(
    task_id=uuid.uuid4(),
    cancel_state=CancelRequested(cause=CancelCause.OPERATOR),
)
assert import_batches(cancelled_run, batch_ids=['b-1', 'b-2']) == Ok(0)
```

### Choose the Execution Class

`execution` declares how a worker runs the task. The value is an `Execution` member, and the default is `Execution.ISOLATED`. Every task row stores the class. To choose a class for the work of a task, see [Choosing an Execution Mode](/horsies/workers/execution-modes/).

| `execution` | Runs | Function | `timeout_ms` | `cancel_grace_ms` |
| ----------- | ---- | -------- | ------------ | ----------------- |
| `Execution.ISOLATED` (default) | one task at a time in its own child process | `def` or `async def` | optional | optional: the kill after an operator cancel |
| `Execution.SHARED_ASYNC` | many tasks as coroutines in one child process | `async def` | required | optional |
| `Execution.SHARED_THREAD` | many tasks on threads in one child process | `def` | required | not allowed |

```python
@app.task(
    "fetch_quote",
    execution=Execution.SHARED_ASYNC,
    timeout_ms=10_000,
    cancel_grace_ms=2_000,
)
async def fetch_quote(*, symbol: str) -> Result[str, TaskError]:
    ...


@app.task("render_thumbnail", execution=Execution.SHARED_THREAD, timeout_ms=30_000)
def render_thumbnail(*, image_id: str) -> Result[str, TaskError]:
    ...
```

A `SHARED_THREAD` task runs next to itself and to the other thread tasks of its child. Guard module state with a lock:

```python
import threading

_seen: set[str] = set()
_seen_lock = threading.Lock()


@app.task('mark_seen', execution=Execution.SHARED_THREAD, timeout_ms=10_000)
def mark_seen(*, key: str) -> Result[bool, TaskError]:
    with _seen_lock:
        first = key not in _seen
        _seen.add(key)
    return Ok(first)
```

A `SHARED_ASYNC` task runs on a seat of a shared async child when the worker profile has `shared_async=SharedAsyncChildren(...)`, and in an isolated child otherwise. See [Shared Async Tasks](/horsies/workers/shared-async-tasks/). A `SHARED_THREAD` task runs on a seat thread of a shared thread child when the profile has `shared_thread=SharedThreadChildren(...)`, and in an isolated child otherwise. See [Shared Thread Tasks](/horsies/workers/shared-thread-tasks/).

`cancel_grace_ms` is the time that a task gets to stop after a cancel. The value is from `MIN_CANCEL_GRACE_MS` (100) to `MAX_CANCEL_GRACE_MS` (10,000).

<!-- members: Execution -->
- `SHARED_ASYNC`: after the grace, the child cancels the task a second time, after the cancel at the deadline and after an operator cancel. A task that sets none gets `DEFAULT_CANCEL_GRACE_MS` (1,000).
- `ISOLATED`: after the grace, the worker kills the child of a task that an operator cancelled and that has not stopped. A task that sets none gets no such kill: the cancel is cooperative only. The deadline kills the child at `timeout_ms` with no grace, as before.
- `SHARED_THREAD`: not allowed. A thread cannot be stopped.

The stored options of the task hold the value in effect when the task is sent, so a later change of the default does not change a queued task.

`@app.task` refuses a declaration that breaks a rule with a `TaskDefinitionError`, one code per rule:

<!-- members: HorsiesErrorCode subset -->
| Code | Name | Rule |
| ---- | ---- | ---- |
| HRS-107 | `TASK_SHARED_ASYNC_REQUIRES_ASYNC_DEF` | `SHARED_ASYNC` needs an `async def` function |
| HRS-108 | `TASK_SHARED_THREAD_REQUIRES_DEF` | `SHARED_THREAD` needs a `def` function |
| HRS-109 | `TASK_SHARED_EXECUTION_REQUIRES_TIMEOUT` | `SHARED_ASYNC` and `SHARED_THREAD` need `timeout_ms` |
| HRS-110 | `TASK_CANCEL_GRACE_REQUIRES_SHARED_ASYNC` | `SHARED_THREAD` does not take `cancel_grace_ms` |
| HRS-111 | `TASK_CANCEL_GRACE_OUT_OF_RANGE` | `cancel_grace_ms` is from 100 to 10,000 |

When a declaration breaks more than one rule, the first rule in this order is reported. A type checker refuses the same declarations when `execution` is written as a member: the overloads require `timeout_ms` for a shared class, and the decorator that they return accepts only the function kind of the class. With `cancel_grace_ms` and `Execution.SHARED_THREAD`, the decorator accepts no function. A class that is known only at run time requires `timeout_ms` and takes `cancel_grace_ms`, and the decorator checks the other rules at run time.

### Register Tasks for Workers

```python
# Dotted module paths (recommended)
app.discover_tasks(["myapp.tasks", "myapp.jobs.worker_tasks"])

# Or file paths
app.discover_tasks(["tasks.py", "more_tasks.py"])
```

`discover_tasks` imports the **exact entries listed**. Dotted module paths use `importlib.import_module()`, while `.py` entries are imported by file path — it does not recursively scan submodules. To discover tasks in `myapp.tasks.scraping`, either list it explicitly or export the decorated functions from `myapp.tasks.__init__.py`.

## Things to Avoid

**Don't omit the return type annotation.**

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong - raises TypeError at definition time
@app.task("bad_task")
def bad_task():
    return {"status": "done"}

# Correct
@app.task("good_task")
def good_task() -> Result[dict[str, JsonValue], TaskError]:
    return Ok({"status": "done"})
```

**Don't block the event loop in a `SHARED_ASYNC` task.** A blocking call stops every task of the child for that time.

```python
import time

# Wrong: stops every task of the child for 2 seconds
@app.task('wait_wrong', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def wait_wrong() -> Result[None, TaskError]:
    time.sleep(2)
    return Ok(None)


# Correct
@app.task('wait_right', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def wait_right() -> Result[None, TaskError]:
    await asyncio.sleep(2)
    return Ok(None)
```

**Don't reuse task names.**

```python
@app.task("duplicate_name")
def task_one() -> Result[str, TaskError]:
    ...

@app.task("duplicate_name")  # Raises error
def task_two() -> Result[str, TaskError]:
    ...
```

## API Reference

### `@app.task(name, **options)`

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `name` | `str` | Yes | Unique task identifier. An empty name raises `TaskDefinitionError` (`HRS-102`) |
| `queue` | `str` | No | Target queue (CUSTOM mode only) |
| `retry_policy` | `RetryPolicy` | No | Retry timing, backoff, and auto-retry triggers |
| `exception_mapper` | `dict[type[BaseException], str]` | No | Maps exception classes to error codes |
| `default_unhandled_error_code` | `str` | No | Error code for unmapped exceptions (overrides global) |
| `error_codes` | `type[Enum] \| tuple[str, ...]` | No | The task's own error codes: an `Enum` class (its member values) or a tuple of strings. Default `()`. See [Declare Error Codes](#declare-error-codes) |
| `timeout_ms` | `int` | No | Execution time limit in milliseconds, from 1000 to `MAX_TASK_TIMEOUT_MS` (4,294,967,295), measured from the moment a child takes the task. On expiry the child process is killed and the task fails with `TASK_TIMEOUT` |
| `execution` | `Execution` | No | How a worker runs the task: `ISOLATED` (default), `SHARED_ASYNC` or `SHARED_THREAD`. See [Choose the Execution Class](#choose-the-execution-class) |
| `cancel_grace_ms` | `int` | No | `ISOLATED` and `SHARED_ASYNC`. The time to stop after a cancel, from 100 to 10,000 ms. `SHARED_ASYNC`: default 1,000 ms, then a second cancel. `ISOLATED`: no default; when set, the worker kills the child of a cancelled task that has not stopped by then. See [Cancellation](#cancellation) |

**Returns:** For a `def` function, a `TaskFunction`: a direct call returns the `Result`. For an `async def` function, an `AsyncTaskFunction`: a direct call returns a coroutine, and `await` gives the `Result`. Both send with `.send()` / `.send_async()`. A function whose first parameter is `runtime: TaskRuntime` gives a `RuntimeTaskFunction` or an `AsyncRuntimeTaskFunction`: the sends do not take the runtime, and a direct call takes it first. See [Task Runtime](#task-runtime).

`good_until` is not accepted here — task expiry deadlines are a per-send concern. See [Sending Tasks — Set a Task Deadline](/horsies/tasks/sending-tasks/#set-a-task-deadline).
