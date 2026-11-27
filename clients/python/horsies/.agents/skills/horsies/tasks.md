# horsies — Tasks

Detailed reference for defining, sending, and handling tasks.
Covers all parameters, error codes, serialization rules, retry patterns, and pitfalls.

## `@app.task` Decorator

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
@app.task(
    name: str,
    *,
    queue: str | None = None,
    retry_policy: RetryPolicy | None = None,
    exception_mapper: dict[type[BaseException], str] | None = None,
    default_unhandled_error_code: str | None = None,
    timeout_ms: int | None = None,
    execution: Execution = Execution.ISOLATED,
    cancel_grace_ms: int | None = None,
)
def my_task(*, ...) -> Result[T, TaskError]: ...
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Unique identifier. An empty name raises `TaskDefinitionError(HRS-102)`. Duplicate names raise `RegistryError(HRS-301)`. |
| `queue` | `str \| None` | `None` | Must be `None` in DEFAULT mode (raises `HRS-200`); must match a configured queue in CUSTOM mode (unrecognized raises `HRS-103`). |
| `retry_policy` | `RetryPolicy \| None` | `None` | See RetryPolicy section. |
| `exception_mapper` | `dict[type[BaseException], str] \| None` | `None` | Per-task exception-to-error-code mapping. See ExceptionMapper section. |
| `default_unhandled_error_code` | `str \| None` | `None` | Override global default for unmapped exceptions. Must be `UPPER_SNAKE_CASE`. |
| `error_codes` | `type[Enum] \| tuple[str, ...]` | `()` | The task's own error codes: an `Enum` class (its member values) or a tuple of strings. They are the values of the task's `error_code` metric label besides the built-in codes and `undeclared`; another custom code counts as `undeclared` there. The values of the task's `exception_mapper` and of `AppConfig.exception_mapper` are added with no second declaration. The task function keeps the codes in `error_codes`; `horsies check` lists them. |
| `timeout_ms` | `int \| None` | `None` | Execution time limit in ms, from 1000 to `MAX_TASK_TIMEOUT_MS` (4,294,967,295, about 49.7 days, the Rust port's `u32` limit), measured from the moment a child takes the task; time waiting for a free child does not count. On expiry the child and the subprocesses in its session are SIGKILLed, and after the child exits the task fails with `TASK_TIMEOUT`; retry is opt-in via `retry_for`, and a retry never runs next to the killed attempt. A deadline that fires before user code starts requeues the task instead, also after the exit. If the child does not exit within 30 s of the kill, nothing is written and stale-task recovery takes the task; with stale RUNNING recovery off, the worker writes the row once it sees the exit (a stop ends that wait). Other tasks on the worker continue. Use it as a hang backstop, not as routine control flow. |
| `execution` | `Execution` | `Execution.ISOLATED` | How a worker runs the task. `ISOLATED`: `def` or `async def`, `timeout_ms` optional. `SHARED_ASYNC`: `async def` only, `timeout_ms` required. `SHARED_THREAD`: `def` only, `timeout_ms` required. Every task row stores the class (`horsies_tasks.execution`), and a worker claims each class into the budget its claim plan routes it to (a worker with isolated children only runs every class isolated). |
| `cancel_grace_ms` | `int \| None` | `None` | The time to stop after a cancel, from `MIN_CANCEL_GRACE_MS` (100) to `MAX_CANCEL_GRACE_MS` (10,000). `SHARED_ASYNC`: the time to the second cancel (at the deadline and after an operator cancel); `None` stores `DEFAULT_CANCEL_GRACE_MS` (1,000). `ISOLATED`: the time from an operator cancel to the kill of the child when the task has not stopped; `None` stores nothing, and the cancel is cooperative only. `SHARED_THREAD`: refused (`HRS-110`). The stored options hold the value in effect at send. |

`good_until` is **not** accepted at definition time — it is a per-send concern. Passing it raises `HRS-102`. Use `.with_options(good_until=...)` at send time instead. For workflow nodes, use `.node(good_until=...)`.

### Validated at definition time

- Return type annotation present and is `Result[T, TaskError]` (or the same union written `Ok[T] | Err[TaskError]`) — missing raises `HRS-100`, wrong type raises `HRS-101`.
- Pre-decorated functions (with `__wrapped__` chain) are rejected — `HRS-104`.
- The function is `def` or `async def`. A function that yields is rejected — `HRS-101`, for a generator and for an async generator.
- `exception_mapper` keys must be `BaseException` subclasses, values must be `UPPER_SNAKE_CASE`.
- `error_codes`: a value that is neither an `Enum` class nor a tuple raises `HRS-102`; a code that is empty, not `UPPER_SNAKE_CASE` or an exception class name raises `HRS-112`; a built-in code or `WORKFLOW_STOPPED` raises `HRS-212`; a code given twice raises `HRS-113`. One error per refused code; several come in one `MultipleValidationErrors`. `TaskError[C]` is generic in its custom code type, default `str`. A task with `error_codes=ChargeCode` that returns `Result[T, TaskError[ChargeCode]]` passes members (`ChargeCode.CARD_DECLINED`), and `TaskHandle.get()` and `args_from` parameters annotated `TaskOutcome[T, TaskError[ChargeCode]]` give members back; so do `WorkflowHandle.result_for(node)` and `WorkflowContext.result_for(node)` for a `TaskNode[T, ChargeCode]` (from `task.node()(...)`), the entries of `results()`, `tasks()` and an outputless `get()` map, and `app.get_result()` (typed `TaskError[Any]`). The error of a workflow from `WorkflowHandle.get()` and a `SubWorkflowNode` keep text: the failed node can be any task. `TaskError[E]` without `error_codes=E` raises `HRS-105`; a mapper or default code that `E` does not name raises `HRS-209`. A task that returns `TaskError` keeps `str` codes (`ChargeCode.CARD_DECLINED.value`).
- `queue` cross-validated against `AppConfig.queue_mode`.
- `good_until` rejected — raises `HRS-102` with help text directing to `.with_options()` or `.node()`.
- Top-level `retry_for` kwarg is rejected — it belongs inside `RetryPolicy`.
- `retry_policy.retry_for` entries are code members: members of a built-in code enum, or members of the `Enum` class of `error_codes=`. A plain `str`, a member of another `Enum` class, or a custom member of a task that declares a tuple or no `error_codes` raises `HRS-102`; the notes name the task and the entry. One error per refused entry; several come in one `MultipleValidationErrors`.
- `execution` rules, one code each, checked in this order (the first broken rule is reported): `SHARED_ASYNC` on a `def` function — `HRS-107`; `SHARED_THREAD` on an `async def` function — `HRS-108`; `SHARED_ASYNC` or `SHARED_THREAD` without `timeout_ms` — `HRS-109`; `cancel_grace_ms` with `SHARED_THREAD` — `HRS-110`; `cancel_grace_ms` outside 100 to 10,000 — `HRS-111`. A value that is not an `Execution` member raises `HRS-102`.
- With `execution` written as a member, a type checker refuses the same declarations: the overloads require `timeout_ms` for a shared class and refuse `cancel_grace_ms` with `SHARED_THREAD`, and the decorator they return accepts only the function kind of the class. A class known only at run time requires `timeout_ms`.

Returns `TaskFunction[P, T]` for a `def` function, and `AsyncTaskFunction[P, T]` for an `async def` function. A function whose first parameter is `runtime: TaskRuntime` gives `RuntimeTaskFunction[P, T]` or `AsyncRuntimeTaskFunction[P, T]`; `P` is the parameters after the runtime. See `TaskRuntime` below.

### Shared async tasks

- A worker profile with `shared_async=SharedAsyncChildren(count, seats)` runs `SHARED_ASYNC` rows on shared async children: one process runs up to `seats` such tasks at once, as coroutines on one event loop, and a free seat takes the next task at once. A worker with no shared async children runs them in isolated children. An isolated child of a worker with shared children never runs a `SHARED_ASYNC` row: the claim takes such rows into the `SHARED_ASYNC` budget (its seats in service). A `SHARED_ASYNC` row whose function is now `def` is written `ISOLATED` and returned to the queue before dispatch.
- A new task goes to the open shared child with the fewest tasks in flight.
- The default executor of the loop of a shared async child has `SharedAsyncChildren.executor` threads (`ExecutorThreads(threads=N)`, or derived from the seats by default). `to_thread` and `run_in_executor(None, ...)` calls of the tasks of one child share those threads. An `on_shared_async_child_start` hook that sets its own default executor replaces it; the child still tracks the calls in it.
- An attempt ends when its coroutine has ended and no work that it started is alive: asyncio tasks it created (itself or through a library, also tasks built with `asyncio.Task(...)`) and default-executor calls (`to_thread`, `run_in_executor(None, ...)`, `getaddrinfo`). The child waits for that work before it reports the end. Work of the start hooks belongs to no task.
- A return before the deadline (a value, or an exception that the mapper turns into a result) with that work alive keeps what it returned; the deadline no longer applies. The child waits up to G for the work, then cancels it in the cancel rounds below and closes (`CloseReason.LEFTOVER_WORK`; replaced after its tasks end). Work still alive 0.3 s after that cancel is stuck: the task keeps its seat, the worker kills the child when every task left in it is stuck, then writes what the task returned (no `TASK_TIMEOUT`, no isolated retry). Any other end of the child whose exit the worker saw (an out-of-memory kill, a crash) also writes what the task returned. One child log line per such task names the work (up to 10 coroutine names) and how it ended: within the grace, cancelled, or stuck.
- Timeout (T = deadline, G = `cancel_grace_ms`): at T the child cancels the task. When the attempt has ended, the worker writes `TASK_TIMEOUT`, the retry and the workflow step; neighbours continue. A value returned after the cancel is still `TASK_TIMEOUT`. At T + G, when the attempt is alive: a second cancel to the task and its asyncio tasks, then 2 more rounds 0.1 s apart; the child closes (no new task, replaced after its tasks end). Still alive 0.3 s after the second cancel: stuck; it keeps its seat; the worker kills the child when every task left in it is stuck (no task has time left). Those tasks get `TASK_TIMEOUT`, and a retry runs isolated (`execution = 'ISOLATED'` written with the retry). The `TASK_TIMEOUT` `data` has `task_id`, `timeout_ms` and `cancel_end`: `RETURNED_AFTER_SIGNAL` (ended within G after the cancel at T), `DROPPED_AFTER_GRACE` (alive at T + G, second cancel sent) or `KILLED_WITH_CHILD` (ended when the worker killed the child, before the child reported the end).
- Blocked loop: a watchdog thread reports a loop that has not turned for 5 s, with the task and source line; the worker kills the child at once. The task that held it: `TASK_BLOCKED_LOOP` (retry isolated); other tasks before their deadline: `NEIGHBOUR_BLOCKED_LOOP` (retry only if `retry_for` names it); tasks past their deadline: `TASK_TIMEOUT`. C code that holds the GIL stops the watchdog too: the worker timer at `timeout_ms + cancel_grace_ms + 10 s` kills a child that reported neither the second cancel nor the end, and a second check at `+ 20.3 s` kills a child that reported that cancel (or the cancel of left work) but neither the task stuck nor its end; tasks past their deadline get `TASK_TIMEOUT`, others the crash path; only a task already reported stuck retries isolated (at every kill by the worker whose exit was seen). A refusal still in flight 145 s after its placement kills its child; its writes are bounded below that. A crash with no known cause: every task `WORKER_CRASHED` (or its retry).
- Author rules: let `CancelledError` propagate (catch only to clean up and re-raise; anyio shielded scopes do not stop a `Task.cancel()`); give each client call its own timeout below the task timeout (for `asyncio.to_thread()`, a cancel never stops the thread); open long-lived clients in `@app.on_shared_async_child_start` (a client opened in a task carries that task's mark: its background work is cancelled after the task returns, and each task that opens it costs one child restart); do not block the loop; send with `send_async()`; await what you start; `timeout_ms` is required; module state is shared by the tasks of the child.
- The RUNNING update and the finalizing marker run on an async connection pool of the child, so they do not block the loop. The worker process writes the runner heartbeats for every task of the child.
- Recycling: `SharedRecycle.after_per_seat` (`tasks × seats` tasks) or `above` (RSS after a task). The child closes: no new task, its tasks finish, then a new child starts.

### Shared thread tasks

- A worker profile with `shared_thread=SharedThreadChildren(count, seats)` runs `SHARED_THREAD` rows on shared thread children: one process runs up to `seats` such `def` tasks at once, each on a seat thread started at boot, and a free seat takes the next task at once. `seats` is 1 to 256. A worker with no shared thread children runs them in isolated children; its other children never run one. A `SHARED_THREAD` row whose function is now `async def` is written `ISOLATED` and returned to the queue before dispatch. A row without a usable stored `timeout_ms` fails with `WORKER_SERIALIZATION_ERROR` on every worker.
- When threads help (Linux, 4 CPUs, CPython 3.13 and 3.14): C code that releases the GIL (zlib, hashlib) scales with threads on every build, 4 threads 2.6 to 3.8 times one; pure-Python code gains nothing on a standard build (0.56 to 1.02 times) and scales on a free-threaded build (1.9 to 3.6 times); more threads than CPUs added nothing. Sync I/O waits release the GIL.
- Timeout: a thread cannot be stopped from outside. At `timeout_ms` the child reports the deadline, sets the cancel state of the run to `CancelRequested(cause=CancelCause.DEADLINE)` when no operator cancel came first, and closes (no new task); the task runs on. When every task still in the child is past its own deadline, the worker kills the child with its session; after the exit each of those tasks gets `TASK_TIMEOUT`, its retry and its workflow step; the task whose deadline closed the child retries isolated. A task alone in its child is killed at its deadline. A neighbour with time left runs to its end. A task that ends by itself after its deadline is `TASK_TIMEOUT`. A child that has not reported a deadline by `timeout_ms + 10 s` is frozen (C code holds the GIL) and killed: each task past its deadline gets `TASK_TIMEOUT`, the others take the crash path; no new culprit is named, and the task whose deadline already closed the child retries isolated (also at the drain deadline of a stop).
- Author rules: the function runs next to itself on other threads; no module state without a lock; `timeout_ms` is required, and each blocking call needs its own timeout below it; a timeout closes the child and the kill ends every task still in it; a crash of the child ends every task in it.
- The RUNNING update and the finalizing marker run on the seat thread, on the one sync pool of the child (`SharedThreadChildren.pool`, derived from the seats by default; a thread waits up to 30 s for a connection). The worker process writes the runner heartbeats.
- Recycling: as for shared async children (`SharedRecycle`).

### Async tasks

- This section is about `Execution.ISOLATED` (the default). An isolated child runs one task at a time, `def` or `async def`. The same RUNNING update, heartbeats, `timeout_ms` kill, recovery and retry policy apply. A `SHARED_ASYNC` task runs as described under Shared async tasks.
- Each child keeps one event loop until the child stops. An `async def` task runs on it, so an async client that a module keeps stays open across the tasks of one child.
- The loop runs only while an `async def` task runs. A `def` task runs with no running event loop and can call `asyncio.run()`. While the child runs a `def` task or waits for its next task, the timers of a module-level async client (a keepalive, a pool maintenance task) do not run.
- An `async def` task must await the work that it starts. When its body returns (a value, or an exception that the mapper turns into a result) and asyncio tasks that it started (`asyncio.create_task()`) are still on the loop, the child waits up to 1 s for them (`ISOLATED_LEFTOVER_GRACE_S`; no option), then cancels what is alive in three rounds 0.1 s apart. The worker writes what the task returned only after that work ended, then the retry and the workflow step; the child is replaced, and the next task runs in a new child. The return ends the deadline: `timeout_ms` bounds the body, never this wait, so the outcome is never `TASK_TIMEOUT` here. The child marks the row finalizing only after that work ended (never for stuck work), so the worker beats the row while it runs and the reaper does not fail it. Work alive 0.3 s after the first cancel is stuck: the worker kills the child and writes what the task returned after the exit (no exit seen in 30 s: the timeout-kill rule). A child that reports nothing within 26.3 s of the return (that work blocks the loop) is killed the same way; a crash of the child in the wait also keeps what the task returned. A client that keeps its own background tasks on the loop (a keepalive, a consumer) delays each outcome by about 1 s and costs a replacement after each async task. Only asyncio tasks are seen: a `loop.call_soon()` / `loop.call_later()` callback, or `loop.run_in_executor()` work that the task did not await, is not waited for, causes no replacement, and can still run during a later task of the same child.
- A child creates its event loop at boot, before it reports ready, when the app has an `async def` task; an app with none gets no loop. When the loop cannot be created, the child fails its boot and no task reaches it. EMFILE / ENFILE / ENOMEM / EAGAIN can clear, so only that slot is down: it takes no task and boots again after a growing wait, while other children serve; when every slot is down, the worker exits non-zero after three failed starts. The log names the slot and the cause: `Slot <n> is down: the OS ran short of a resource (EMFILE): <error>`. Any other failure, such as an event loop policy that raises, repeats (`TaskLoopCreateFailed`): each failed start counts one of three attempts, also while other children serve.
- A task that cancels a helper task in `finally` must also await it, for example `helper.cancel()` then `await asyncio.gather(helper, return_exceptions=True)`. Without the await, a helper that awaits while it stops (a close) is still on the loop when the task returns: the outcome waits for it, and the worker replaces the child. One worker log line per such task names the coroutine of each task left (up to 10) and how that work ended: `Task <name> (<id>) returned and left <n> task(s) on the event loop of child pid=<pid> (<names>); that work <end>`.
- A thread of `asyncio.to_thread()` that outlives its task is not waited for and does not cause a replacement. At the stop, the child does not wait for it past the close (at most 7 seconds): it exits, and `atexit` handlers do not run. An idle executor does not count: its shutdown gets at least 1 second, and so does the close of the async generators.
- In an `async def` task, send with `.send_async()`. The sync `.send()` returns `Err(TaskSendAsyncContext)` there and enqueues nothing.
- An exception, a wrong return, and the exception mappers give the same result as in a `def` task.

### Cancellation

- An operator cancel of a running task (`cancel_task(app, task_id, policy=CancelPolicy.INCLUDE_RUNNING)`, or the web UI) moves the task to history as `CANCELLED` at once, and writes its attempt row with outcome `CANCELLED` (`finished_at` = the cancel time, `recorded_by` `CANCEL`), in one statement. A waiting `handle.get()` returns `Ok(Failed(task_error))` with the code `TASK_CANCELLED`. What the task returns after the cancel is dropped.
- The worker gives the cancel to the task code: a `NOTIFY` on `horsies_task_cancel`; a lost notice is found by one read after a listener reconnect, or after a runner heartbeat of the run that does not land (at most `runner_heartbeat_interval_ms`, default 30 s).
- `async def` body (isolated or shared async): `asyncio.CancelledError` at the next `await`. Let it propagate; catch it only to clean up, then raise it again.
- `def` body (isolated or shared thread): read the cancel state. The first parameter `runtime: TaskRuntime` (before the `*`) gives the run; `runtime.cancel_state()` returns `CancelNotRequested()` or `CancelRequested(cause)`. `CancelCause`: `OPERATOR` (an operator cancel), `DEADLINE` (the deadline of a `SHARED_ASYNC` or `SHARED_THREAD` run). The first cause stays.
- A cancel after the start of the run and before the body starts: the body does not run.
- Code that does not stop: `ISOLATED` runs until it returns or its `timeout_ms` kill; with `cancel_grace_ms` on the task, the worker kills the child `cancel_grace_ms` after the cancel. `SHARED_ASYNC`: after `cancel_grace_ms` the second cancel and the cancel rounds; the child closes; a stuck run keeps its seat until no task in the child has time left. `SHARED_THREAD`: the thread runs until it returns.
- An isolated `async def` body that leaves asyncio tasks after a cancel: the child does not wait for them, and the worker replaces the child.
- Tests: `TaskRuntime.for_test(task_id=..., cancel_state=CancelRequested(cause=CancelCause.OPERATOR))` gives a runtime with a fixed cancel state.

```python
from horsies import CancelNotRequested, CancelRequested, Ok, Result, TaskError, TaskRuntime


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

## `TaskRuntime` and the fencing token

A task whose first parameter is `runtime: TaskRuntime` gets the runtime of its run. The worker builds one for each run and passes it as the first argument. The runtime gives `runtime.task_id` (the same on every attempt: an idempotency key), `runtime.attempt` (`AttemptNumber`, 1 = first run), `runtime.max_retries` (`MaxRetries`, 0 to 20), `runtime.retries_left` (`RetriesLeft`: `max_retries - (attempt - 1)`, 0 past the budget; a non-retryable code or `good_until` can end retries earlier), `runtime.fencing_token` and `runtime.cancel_state()` (see Cancellation above). Each number is in its `.value`. There is no global `current_attempt()`.

```python
from horsies import FencingToken, TaskRuntime


@app.task('read_fence')
def read_fence(runtime: TaskRuntime, *, order_id: str) -> Result[FencingToken, TaskError]:
    return Ok(runtime.fencing_token)
```

- Rules: the first parameter, before the `*`; the annotation is exactly `TaskRuntime` (not `TaskRuntime | None`); no default; any name. `TaskRuntime` in any other place (keyword-only, a second positional parameter, a model field, the ok type) raises `HRS-105`.
- Not part of the wire contract: `.send()`, `.node()()`, a schedule and a workflow node do not supply it. A kwarg with its name is refused (`TaskSendValidationFailed`, `HRS-205` for a schedule, `WORKFLOW_KWARGS_NOT_SERIALIZABLE` for a node).
- Direct call (tests): `read_fence(TaskRuntime.for_test(task_id=uuid.uuid4(), attempt=second_attempt), order_id='o-1')`. `attempt` is an `AttemptNumber` (default the first attempt). `max_retries` is a `MaxRetries` (default 0; `MaxRetries.parse(3)` returns `Ok(MaxRetries)`, or `Err(MaxRetriesRefused)` outside 0 to 20). `cancel_state` is a `RunCancelState` (default `CancelNotRequested()`). `AttemptNumber.parse(2)` returns `Ok(AttemptNumber)`, or `Err(AttemptNumberRefused)` for a number outside 1 to `2**31`. `for_test` cannot fail.
- `runtime.fencing_token` is a frozen `FencingToken(task_id: uuid.UUID, value: int)`. Two reads in one attempt give the same token. An attempt that runs after another attempt of the same task gives a strictly greater `value`. No clock. Today `value` is the attempt number (1 to 21); compare values of one task id, do not depend on the number.
- Pattern: use the task id in the fence key; at the start of the body take the fence (write the token when the stored value is lower or equal; no row changed means a later attempt holds it: stop); make each later write conditional on the stored value (one conditional statement, or a transaction that locks the fence row `FOR SHARE`). A read with no lock, then a write, is not enough.
- Limits: a rerun and a workflow node that a resume starts again get a new task id, value from 1; a stale attempt with no later attempt (failed, cancelled, expired) is not refused; a write before the later attempt took the fence is not refused; two writes in one attempt have the same token (use an idempotency key); not exactly-once; work that the body starts reads the token only when the body passes it on.
- Docs: `concepts/fencing-token`, `tasks/defining-tasks` (Task Runtime, Fencing Token), `operations/testing` (Pass a Task Runtime).

## `Result[T, TaskError]`

The only valid return type of a task function. `Result`, `Ok` and `Err` come from `horsies`. `Ok[T] | Err[TaskError]` is the same annotation.

### Construction

<!-- check: skip elided -- shortens the arguments on purpose -->
```python
Ok(value)              # success (value can be None)
Err(TaskError(...))    # failure
```

A direct call of the task returns this `Result`. A read of the stored result returns a `ReadResult` (see Reading a Result below).

### Strict values

Every boundary validates in strict mode: `"7"`, `7.0` and `True` are refused for an `int`, and a str is refused for a `datetime`, `Enum`, `UUID` or `Decimal`. `.send()`, schedules, workflow definitions and task results read a value back before it is stored. An unknown key is refused at any depth at send, and ignored on read. A subclass with fields that the declared type lacks is refused.

`@app.task` and each `SubWorkflowNode` refuse `Field(exclude=True)`, `exclude_if`, `extra='allow'`, `PlainValidator`, `WrapValidator`, `SkipValidation`, a schema hook, `Json[T]`, `Literal` with a plain `Enum`, an alias whose written key does not read back, and a default that does not hold its type. Use `AfterValidator` or `BeforeValidator`.

`TaskError.data` must be JSON-native. It takes `JsonInput`, so a `list[str]` fits, and it gives `JsonValue`, an exact `list` or `dict`. A bad value becomes `{'data_refused': <cause>}`, and the code and message stay. A `TaskError` subclass with extra fields does not cross the wire: use `data`, and match on `code`.

Changing a model that a task takes or returns: see `SKILL.md`, Deployment facts.

### Runtime return-type validation

The task wrapper validates the returned value against declared `T` using Pydantic's `TypeAdapter` in strict mode. A mismatch produces `Err(TaskError(code=ContractCode.RETURN_TYPE_MISMATCH))` — does not raise. The error stores the actual type and the failing fields (`data`: `variant`, `expected_type`, `actual_type`, `validation_errors`, `validation_error_count`), not the returned value. `variant` is `'ok'` for a wrong `Ok` value and `'err'` for a wrong `Err` value. A return that is not `Ok` or `Err`, `None` included, also produces `RETURN_TYPE_MISMATCH`, with `variant` `'not_a_task_result'`. `expected_type` is `'Result[<ok>, <err>]'`.

## Reading a Result: `ReadResult[T, TaskError]`

`TaskHandle.get()`, `WorkflowHandle.get()`, `WorkflowHandle.result_for()`, `app.get_result()` and their `*_async` forms return `ReadResult[T, TaskError]`. The result says whether the read worked. The outcome inside it says how the task ended.

<!-- check: skip elided -- shows the library definition, not user code -->
```python
type TaskOutcome[T, E] = Succeeded[T] | Failed[E]
type ReadResult[T, E] = Ok[Succeeded[T]] | Ok[Failed[E]] | Err[ReadError]
```

| Case | Meaning |
|---|---|
| `Ok(Succeeded(value))` | The task returned `Ok(value)` |
| `Ok(Failed(task_error))` | The task returned `Err(task_error)`, or the library recorded the error for the task (`TASK_CANCELLED`, `TASK_TIMEOUT`, `WORKER_CRASHED`, `TASK_EXPIRED`, `RETURN_TYPE_MISMATCH`, the workflow codes, ...) |
| `Err(read_error)` | The read gave no outcome. `read_error` is one `ReadError` class |

Read it with one `match`. Do not call `.unwrap()` on a `ReadResult`: it gives the `TaskOutcome`, not the value.

```python
from horsies import Err, Failed, Ok, Succeeded, TaskHandle


def show_total(handle: TaskHandle[int]) -> None:
    match handle.get(wait_ms=30_000):
        case Ok(Succeeded(value)):
            print(value)
        case Ok(Failed(task_error)):
            print(task_error.code, task_error.message)
        case Err(read_error):
            print(read_error.code, read_error.message)
```

`TaskOutcome[T, TaskError]` is also the type of a workflow node input from `args_from`, of `WorkflowContext.result_for()` and of `TaskInfo.decoded_result`. Read it with `case Succeeded(value):` and `case Failed(error):`.

### `ReadError`

`ReadError` is a closed union of frozen classes, one per catalog code. A `match` on it is exhaustive under a type checker. Every class has `code` (a class variable: its catalog member) and `message` (the subject and the cause). `ReadDatabaseError` adds `retryable` (whether the cause may clear) and `cause` (the exception of the cause, or `NoCause()`). Match one class with its arm, for example `case Err(ReadWaitTimeout()):`.

| Class | `code` | When |
|---|---|---|
| `ReadWaitTimeout` | `RetrievalCode.WAIT_TIMEOUT` | The task or the workflow had not ended when the wait limit passed. It may still run |
| `ReadTaskNotFound` | `RetrievalCode.TASK_NOT_FOUND` | Neither the live table nor the task history holds the task. A handle built by id can race the commit of its send |
| `ReadWorkflowNotFound` | `RetrievalCode.WORKFLOW_NOT_FOUND` | No workflow has the id of the handle |
| `ReadResultNotAvailable` | `RetrievalCode.RESULT_NOT_AVAILABLE` | The task reached a terminal status and stored no result, or a handle with no broker holds no result |
| `ReadResultNotReady` | `RetrievalCode.RESULT_NOT_READY` | `WorkflowHandle.result_for()`: the node stores no result yet |
| `ReadWaitInsideTask` | `RetrievalCode.WAIT_INSIDE_TASK` | A result wait in a process that runs task bodies. Nothing was read |
| `ReadAsyncContext` | `RetrievalCode.ASYNC_CONTEXT` | A sync read on a thread that runs an event loop. Nothing was read. Call the `*_async` form |
| `ReadInvalidInput` | `RetrievalCode.INVALID_INPUT` | `wait_ms` is not `None` or an int from 0 to `MAX_TASK_TIMEOUT_MS`. Nothing was read |
| `ReadResultDeserializationError` | `OperationalCode.RESULT_DESERIALIZATION_ERROR` | The stored result does not read as the result of the declared types |
| `ReadUnknownCustomCode` | `RetrievalCode.UNKNOWN_CUSTOM_CODE` | The stored error has a custom code that the declared `Enum` does not name (a row of another version of the task); `custom_code` is the stored text |
| `ReadDatabaseError` | `OperationalCode.DATABASE_ERROR` | The database, the connection or the core call failed. A later read may succeed |
| `ReadNoTypeAvailable` | `ContractCode.NO_TYPE_AVAILABLE` | The stored value succeeded, and this process has no declared type to read it with: the handle was built by id, or the task or workflow is not registered here |
| `ReadWorkflowContextMissingId` | `ContractCode.WORKFLOW_CONTEXT_MISSING_ID` | `WorkflowHandle.result_for()` got a node with no `node_id`. Nothing was read |

## `TaskError`

<!-- check: skip partial -- uses names that the page does not define -->
```python
class TaskError(BaseModel):
    code: BuiltInTaskCode | str
    message: str | None = None
    data: JsonValue = None  # takes JsonInput
    exception: dict[str, Any] | BaseException | None = None
```

`code` is required and must not be null: `retry_for` and outcome routing read it. `message` is optional, and domain errors typically set it. `data` carries structured context. `exception` populated by the library for unhandled exceptions.

**Construction rule**: reserved built-in code values must be passed as enum members, not raw strings. Custom codes must be plain `str`, not `str, Enum` subclasses.

<!-- check: skip partial -- uses a name before the page defines it -->
```python
TaskError(code=OperationalCode.DATABASE_ERROR, message="db down")  # OK
TaskError(code="MY_CUSTOM_CODE", message="card declined")             # OK
TaskError(code="DATABASE_ERROR", message="db down")                     # ValueError — reserved
TaskError(message="card declined")                                          # ValueError — no code
```

**Serialization**: built-in codes serialize as tagged dicts `{"__builtin_task_code__": "DATABASE_ERROR"}` so round-trip identity is preserved. Custom codes serialize as plain strings.

**Deserialization**: use standard `model_validate()` / `model_validate_json()` everywhere. No special rehydration methods needed — works for nested models too. The stored form (the `err` slot of a stored result) has the key `error_code` in place of `code`: `encode_task_error` writes it and `decode_task_error` reads it.

**Stored data**: `TaskError` refuses a key that it does not declare, at construction and at decode. A stored error with an unknown key, or without the key `error_code` (the stored key of `code`), is stored data that does not parse: `TaskHandle.get()`, `WorkflowHandle.get()` and `app.get_result()` return `Err(ReadResultDeserializationError(...))` with the cause.

### `SubWorkflowFailure`

Extends `TaskError` with `sub_workflow_id: str` and `sub_workflow_summary: SubWorkflowSummary[Any]`. Distinguish via pattern matching on the `Failed` error:

<!-- check: skip partial -- uses names that the page does not define -->
```python
match outcome:  # a TaskOutcome[T, TaskError]
    case Failed(SubWorkflowFailure() as error):
        print(error.sub_workflow_id)  # available
    case Failed(error):
        ...  # regular task error
    case Succeeded(value):
        ...
```

## Error Code Families

The old flat `LibraryErrorCode` enum has been split into 4 families. The umbrella type alias is `BuiltInTaskCode`.

### `OperationalCode`

Infra-level failures during task execution, brokering, or worker processing:

<!-- members: OperationalCode -->
- `UNHANDLED_EXCEPTION` — uncaught exception wrapped by library
- `TASK_EXCEPTION` — the worker caught an exception outside the task wrapper, or a validator raised on a default
- `WORKER_CRASHED` — worker process died during execution
- `DATABASE_ERROR` — DB/broker failure during result retrieval (`read_error.code` of `ReadDatabaseError`)
- `WORKER_RESOLUTION_ERROR` — task name not found in registry
- `WORKER_SERIALIZATION_ERROR` — failed to serialize/deserialize arguments
- `RESULT_DESERIALIZATION_ERROR` — a stored result or a stored workflow error is corrupt or does not decode (`read_error.code` of `ReadResultDeserializationError`)
- `WORKFLOW_ENQUEUE_FAILED` — workflow node failed during enqueue
- `SUBWORKFLOW_LOAD_FAILED` — sub-workflow definition could not be loaded
- `WORKFLOW_RECOVERY_LIMIT_REACHED` — workflow recovery raised a persistent error in `workflow_recovery_failure_limit` reaper passes, so the workflow failed closed
- `SUBWORKFLOW_DEPTH_LIMIT_REACHED` — the engine did not start a child workflow, because its depth would pass 32
- `TASK_BLOCKED_LOOP` — the task held the event loop of its shared async child past the block limit (5 s), before its `timeout_ms`; the worker killed the child, and a retry runs in an isolated child
- `NEIGHBOUR_BLOCKED_LOOP` — another task held the event loop of this task's shared async child past the block limit, before this task's `timeout_ms`; the worker killed the child
- `WORKFLOW_START_INCOMPLETE` — a RUNNING workflow has no node rows: its start did not finish, and recovery failed it

### `ContractCode`

Violations of the declared API contract between task author and library:

<!-- members: ContractCode -->
- `RETURN_TYPE_MISMATCH` — returned value doesn't fit the declared type, or is not `Ok` or `Err`, including `None`
- `WORKFLOW_CONTEXT_MISSING_ID` — a context read got a node with no `node_id` (`read_error.code` of `ReadWorkflowContextMissingId`; the code of `WorkflowContextMissingId`)
- `NODE_NOT_IN_CONTEXT` — a context read got a node with an id that is not in the context (the code of `NodeNotInContext`)
- `NO_TYPE_AVAILABLE` — a typed decode needs the success type, but the task or workflow is not registered locally (for example a handle built from an id) (`read_error.code` of `ReadNoTypeAvailable`)

### `RetrievalCode`

The `code` of a read error (`read_error.code`) when `handle.get()` / `handle.result_for()` gives no outcome. A read never puts these codes in a `TaskError`. Each member has its `ReadError` class:

<!-- members: RetrievalCode -->
- `WAIT_TIMEOUT` — `ReadWaitTimeout`: `get()` timed out; task may still be running, not cached, re-pollable
- `TASK_NOT_FOUND` — `ReadTaskNotFound`: task ID does not exist
- `WORKFLOW_NOT_FOUND` — `ReadWorkflowNotFound`: workflow ID does not exist, or retention removed the workflow after `get()` read its terminal status
- `RESULT_NOT_AVAILABLE` — `ReadResultNotAvailable`: the task reached a terminal status and stored no result
- `RESULT_NOT_READY` — `ReadResultNotReady`: the workflow node stores no result yet
- `ASYNC_CONTEXT` — `ReadAsyncContext`: sync `get()` / `result_for()` called on a thread that runs an event loop; nothing ran, not cached — call `get_async()` / `result_for_async()`
- `INVALID_INPUT` — `ReadInvalidInput`: an argument of `get()` / `get_async()` was refused before any read: `wait_ms` must be None or an int from 0 to `MAX_TASK_TIMEOUT_MS` (4,294,967,295 ms); not cached
- `WAIT_INSIDE_TASK` — `ReadWaitInsideTask`: a result wait (`TaskHandle.get()` / `get_async()`, `app.get_result()` / `get_result_async()`, `WorkflowHandle.get()` / `get_async()`) was called inside a worker child, also from a thread that the task started; it waited for nothing, not cached. It is the first check of the call, before `timeout_ms` and `ASYNC_CONTEXT`. The raw reads `app.get_raw_result()` and `broker.get_raw_result_record()` (and `*_async`) return `Err(DatabaseWaitInsideTask)` there. A task never waits for another task: compose them as a workflow. `send()`, starting a workflow, `info()` and `workflow_context.result_for()` still work in a task
- `UNKNOWN_CUSTOM_CODE` — `ReadUnknownCustomCode`: the stored error has a custom code that the declared code type of the task does not name; the same read gives the same error

### `OutcomeCode`

Terminal lifecycle outcomes for tasks and workflows:

<!-- members: OutcomeCode -->
- `TASK_CANCELLED` — task was cancelled
- `TASK_EXPIRED` — task's `good_until` deadline passed before execution started (PENDING or CLAIMED)
- `TASK_TIMEOUT` — task exceeded its `timeout_ms`; child process killed, retry opt-in via `retry_for`
- `WORKFLOW_PAUSED` — workflow is paused
- `WORKFLOW_EXPIRED` — a paused workflow passed `paused_workflow_cancel_after` and expired; `data` holds the policy name and the age
- `WORKFLOW_FAILED` — workflow has failed
- `WORKFLOW_CANCELLED` — workflow was cancelled
- `UPSTREAM_SKIPPED` — upstream task was skipped
- `SUB_WORKFLOW_FAILED` — sub-workflow has failed
- `WORKFLOW_SUCCESS_CASE_NOT_MET` — success condition not satisfied
- `WORKFLOW_OUTPUT_SKIPPED` — a `COMPLETED` workflow has no output: its output node, or a terminal node of an outputless workflow, is SKIPPED, and a success case does not require that node

### Reserved Built-In Codes

All built-in code string values are globally reserved. Custom error codes must be plain `str` and must not collide with any built-in value.

- `BUILTIN_CODE_REGISTRY` (from `horsies.core.models.tasks`) maps every reserved string to its canonical enum member
- `horsies check` reports `HRS-212` (`CHECK_RESERVED_CODE_COLLISION`) when `exception_mapper` values or `default_unhandled_error_code` collide with a reserved code
- The library default `UNHANDLED_EXCEPTION` for `default_unhandled_error_code` is intentionally built-in and is not flagged
- Custom `str, Enum` types degrade to plain `str` after JSON round-trip — only built-in families preserve enum identity

## `TaskFunction[P, T]`

All methods available after `@app.task(...)`:

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
task_name: str

# Direct call — bypasses queue, use only in tests
def __call__(*args: P.args, **kwargs: P.kwargs) -> Result[T, TaskError]
# AsyncTaskFunction[P, T] (an async def task) differs only here:
# def __call__(*args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, Result[T, TaskError]]
# RuntimeTaskFunction[P, T] / AsyncRuntimeTaskFunction[P, T] take the runtime first:
# def __call__(runtime: TaskRuntime, /, *args: P.args, **kwargs: P.kwargs) -> ...

# Enqueue for background execution
def send(*args: P.args, **kwargs: P.kwargs) -> TaskSendResult[TaskHandle[T]]
async def send_async(*args: P.args, **kwargs: P.kwargs) -> TaskSendResult[TaskHandle[T]]

# Per-send options builder (returns TaskFunctionWithOptions with .send/.send_async)
def with_options(*, good_until: datetime | None = None, idempotency_key: str | None = None, retention: SendRetention = <unset>, delay_seconds: int | None = None) -> TaskFunctionWithOptions[P, T]
# retention's default is absence, not 30 days: omitting it takes the
# queue's mapping, while choosing timedelta(days=30) overrides one.
# delay_seconds: whole seconds, 0 or more; None (the default) sends now.

# Replay failed send with stored payload (the delay of the first send
# included); another variant is a type error (and fails an assertion at run time)
def resend(error: TaskSendResendable) -> TaskSendResult[TaskHandle[T]]
async def resend_async(error: TaskSendResendable) -> TaskSendResult[TaskHandle[T]]

# Workflow node builder (returns NodeFactory, call with task kwargs to get TaskNode)
def node(
    *,
    waits_for: Sequence[TaskNode | SubWorkflowNode] | None = None,
    workflow_context_from: Sequence[TaskNode | SubWorkflowNode] | None = None,
    args_from: dict[str, TaskNode | SubWorkflowNode] | None = None,
    queue: str | None = None,
    priority: int | None = None,
    allow_failed_dependencies: bool = False,
    join: Literal['all', 'any', 'quorum'] = 'all',
    min_success: int | None = None,
    good_until: datetime | None = None,
    node_id: str | None = None,
) -> NodeFactory[P, T]
```

**Direct call vs `.send()`**: Direct call runs in current process, no queue, no retry, no persistence. For an `async def` task, the direct call returns a coroutine: `await` it. `.send()` enqueues for background execution by a worker.

## `TaskFunctionWithOptions[P, T]` — Per-Send Options

Returned by `task.with_options(...)`. Exposes `.send()` and `.send_async()` with overridden options applied to this specific send.

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Set a per-send start deadline: a task not started by then becomes EXPIRED and never runs
deadline = datetime.now(timezone.utc) + timedelta(minutes=5)
opts = my_task.with_options(good_until=deadline)

opts.send(**kwargs)              # sync
await opts.send_async(**kwargs)  # async

# Send after a delay of 60 seconds
my_task.with_options(delay_seconds=60).send(**kwargs)
```

**`delay_seconds`**: a whole number of seconds, `0` or more; the task becomes claimable after it. `None` (the default) and `0` send now. A negative value, a `bool`, a `float` or a `str` returns `Err(VALIDATION_FAILED)` before anything is written.

**`good_until`**: timezone-aware `datetime` or `None`. Naive datetimes return `Err(VALIDATION_FAILED)`. Passing `good_until=None` explicitly clears any inherited deadline.

**`idempotency_key`**: optional caller-supplied key, scoped per task name. The
enqueue reserves the key together with a canonical fingerprint of the request:
a resend under the same key with the same request is a replay and creates zero
rows; a resend under the same key with a *different* request returns
`Err(TaskSendIdempotencyKeyConflict)`. Its `owner_task_id` names the task
that holds the key. While the owner task is
live, the key stays reserved. After the owner task finishes, the key clears
when the reservation window (24 h by default) has passed; a send with the key
after that time starts a new task. A repeat of the same send does not complete
it, so the error is not retryable: send the same command as the owner task, or
use another key. The resend methods refuse the error. A resend that changes the key of a sent task id returns
`PAYLOAD_MISMATCH`. Keys are not a rerun contract — they deduplicate enqueues.

**`retention`**: how long the terminal record is kept. Precedence,
highest first:

1. What you pass here — a period in whole days, or `None` for forever.
   Choosing 30 days explicitly counts as a choice and beats a queue mapping.
2. The queue's entry in `AppConfig.retention.queue_retention`, if it has one.
3. The immutable 30-day default class.

So omitting the argument means "the queue's mapping if there is one, and
30 days otherwise" — not 30 days unconditionally.

This precedence holds on EVERY enqueue path — `.send()`, `.send_async()`
with or without a delay, a `TaskSchedule` firing on its cron, and a
workflow node's backing task. A cron fire resolves from the scheduler
process's own config; a workflow node from the queue its node runs on.

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

**When to use `with_options()` vs `.node(good_until=...)`**: `with_options()` is for ad-hoc sends (`.send()`, `.send_async()`). For workflow nodes, use `.node(good_until=...)` — the deadline is evaluated at workflow start time, not at definition time.

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
# TaskNode(function=consume, args_from={"data": producer}, waits_for=[producer])
```

Raises `WorkflowValidationError(HRS-008)` if argument is not a `TaskNode` or `SubWorkflowNode`.

## `TaskHandle[T]`

Returned by successful `.send()` / `.send_async()` / `.resend()` / `.resend_async()`.

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
task_id: UUID  # stable across retries of the same send

def get(wait_ms: int | None = None) -> ReadResult[T, TaskError]
async def get_async(wait_ms: int | None = None) -> ReadResult[T, TaskError]

def info(*, include_result: bool = False, include_failed_reason: bool = False, include_attempts: bool = False) -> DatabaseResult[TaskInfo | None]
async def info_async(*, include_result: bool = False, include_failed_reason: bool = False, include_attempts: bool = False) -> DatabaseResult[TaskInfo | None]
```

### Caching semantics

- An outcome (`Ok(Succeeded(...))`, `Ok(Failed(...))`) and the final read errors `ReadResultNotAvailable`, `ReadResultDeserializationError`, `ReadUnknownCustomCode` and `ReadNoTypeAvailable` **are** cached — repeated `get()` returns the cached value without DB query.
- The other read errors (`ReadWaitTimeout`, `ReadTaskNotFound`, `ReadDatabaseError`, `ReadAsyncContext`, `ReadWaitInsideTask`, `ReadInvalidInput`) are **not** cached — subsequent `get()` calls read from the broker again.

### `TaskInfo` fields

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
task_id: UUID, task_name: str, status: TaskStatus
queue: str, priority: int
retry_count: int, max_retries: int, next_retry_at: datetime | None
sent_at: datetime | None, due_at: datetime        # due_at: a worker may claim the task from this time
claimed_at: datetime | None, started_at: datetime | None
completed_at: datetime | None, failed_at: datetime | None
worker_hostname: str | None, worker_pid: int | None, worker_process_name: str | None
error_code: str | None                      # always returned (final TaskError.code, NULL if not failed)
raw_result: dict[str, Any] | None           # opt-in: include_result=True; the stored envelope
decoded_result: TaskOutcome[Any, TaskError] | None  # opt-in: include_result=True; set when this process decodes it
result_decoded: bool                         # True when decoded_result is set
failed_reason: str | None                    # opt-in: include_failed_reason=True
attempts: list[TaskAttemptInfo] | None       # opt-in: include_attempts=True (None when not requested, [] when empty); newest first
```

### `TaskAttemptInfo` fields

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
task_id: UUID, attempt: int
outcome: TaskAttemptOutcome  # COMPLETED, FAILED, WORKER_FAILURE, CANCELLED
will_retry: bool
started_at: datetime, finished_at: datetime
error_code: str | None, error_message: str | None, failed_reason: str | None
worker_id: str | None, worker_hostname: str | None, worker_pid: int | None, worker_process_name: str | None
```

`TaskAttemptOutcome`: `COMPLETED` (success), `FAILED` (domain/library error or recoverable worker/process failure stored as a `TaskError`), `WORKER_FAILURE` (legacy/rare worker failure path without a usable stored `TaskError`), `CANCELLED` (an operator cancelled the running task; the cancel wrote the row, and `finished_at` is the cancel time).

`TaskStatus`: `PENDING -> CLAIMED -> RUNNING -> COMPLETED | FAILED | CANCELLED | EXPIRED`. `is_terminal` is `True` for `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`. `EXPIRED` means the task's `good_until` deadline passed before execution started (either PENDING or CLAIMED).

## `TaskSendResult` / `TaskSendError`

<!-- check: skip elided -- shows the library definition, not user code -->
```python
type TaskSendResult[T] = Result[T, TaskSendError]
```

### `TaskSendError`

One frozen variant per `TaskSendErrorCode` member. Each variant has only the fields of its failure; `err.code` (a class variable) names its member. `payload`, `message` and `cause` are not in the repr.

<!-- check: skip elided -- shows the library definition, not user code -->
```python
type TaskSendError = (
    TaskSendSuppressed               # message
    | TaskSendAsyncContext           # task_id, payload, message
    | TaskSendValidationFailed       # message, cause
    | TaskSendEnqueueFailed          # task_id, payload, message, retryable, cause
    | TaskSendPayloadMismatch        # task_id, payload, message
    | TaskSendPayloadTooLarge        # message, size_bytes, limit_bytes
    | TaskSendIdempotencyKeyConflict # task_id, payload, message, owner_task_id; retryable is False
)
type TaskSendResendable = (
    TaskSendEnqueueFailed
    | TaskSendAsyncContext
)
type TaskSendResendable = TaskSendEnqueueFailed | TaskSendAsyncContext
```

`cause` is `BaseException | NoCause` on `TaskSendValidationFailed` and `TaskSendEnqueueFailed`. `NoCause()` means a check found the failure without a Python exception. `NoCause` is not an exception: match on `exception` before `raise ... from`, `exc_info=` or an error tracker call.

Validation failure text never contains the refused value: each failure is `<loc>: <msg> [<type>]`, at most 10, and a message part that comes from the input is `<input>`. Text that your own validators write is kept as written. A kwarg that fails its declared type gives `TaskSendValidationFailed.cause` as a `ValueValidationError` (`horsies.core.codec`), not a pydantic `ValidationError`. A key from the input can be part of `<loc>` for a dict or mapping type, and for a key that `extra='forbid'` refuses.

### `TaskSendErrorCode`

<!-- members: TaskSendErrorCode -->
| Code | Resend methods | When |
|---|---|---|
| `SEND_SUPPRESSED` | Refused | `.send()` called during worker import/discovery |
| `ASYNC_CONTEXT` | Accepted | Sync `.send()` (or a sync resend) called inside a running event loop; nothing was enqueued — the error keeps the payload, so `resend_async(err)` completes the same send |
| `VALIDATION_FAILED` | Refused | Serialization failed, invalid queue, the broker refused the input before the write (`INVALID_INPUT`, `INVALID_TASK_OPTIONS`), or a resend of another task's error |
| `ENQUEUE_FAILED` | Accepted | Broker/DB failure, or the core of this process could not run the send (`cause` is the core error: `TooManyPendingCalls` and `CoreCallTimedOut` are retryable; `RuntimeStartFailed` is not). The row may or may not exist. The `TaskSendPayload` keeps the task id, command, `sent_at` and delay for the resend, which returns the task id if the first send was written; `retryable` is the core's classification |
| `PAYLOAD_MISMATCH` | Refused | The task id exists, and the command or the idempotency key differs from the original send — the payload was altered between send and resend |
| `IDEMPOTENCY_KEY_CONFLICT` | Refused | The idempotency key is reserved by another task with a different command; nothing was enqueued. `owner_task_id` names the owner task. `retryable` is `False`: a repeat of the same send does not complete it; send the same command, or use another key |
| `PAYLOAD_TOO_LARGE` | Refused | The serialized input exceeded `payload.reject_bytes` bytes of UTF-8 text (off by default); nothing was enqueued — shrink the payload (pass a reference to external storage) or raise the limit |

### `TaskSendPayload`

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
@dataclass(slots=True, frozen=True)
class TaskSendPayload:
    task_name: str, queue_name: str, priority: int
    input_json: str | None  # the keyword arguments as one JSON object; None for none
    sent_at: datetime, good_until: datetime | None
    enqueue_delay_seconds: int | None, task_options: str | None
    idempotency_key: str | None, retention_class_key: RetentionClassKey | None
```

Pre-serialized at send time. Never construct manually — created by the library internally.

### Resend guard pattern

<!-- check: skip partial -- uses names that the page does not define -->
```python
match my_task.send(**kwargs):
    case Ok(handle):
        result = handle.get(wait_ms=5000)
    case Err(TaskSendEnqueueFailed(retryable=True) as err):
        match my_task.resend(err):
            case Ok(handle):
                result = handle.get(wait_ms=5000)
            case Err(resend_err):
                log_permanent_failure(resend_err)
    case Err(err):
        log_permanent_failure(err)
```

`resend` takes the error of a send with a delay too, and sends it again with the same delay. The error of another task returns `VALIDATION_FAILED`.

## `RetryPolicy`

Always use the class methods:

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
# Fixed backoff — the length of intervals_seconds defines max_retries
RetryPolicy.fixed(
    intervals_seconds: list[int],                      # e.g. [60, 300, 900]
    *,
    retry_for: list[BuiltInTaskCode | Enum],            # required: code members
    jitter: bool = True,
)

# Exponential backoff — delay = base * 2^(attempt-1)
RetryPolicy.exponential(
    base_seconds: int,
    *,
    max_retries: int,
    retry_for: list[BuiltInTaskCode | Enum],            # required: code members
    jitter: bool = True,
    max_delay_seconds: int | None = None,              # cap on each delay; None = no cap
)
```

### Validation rules

- `fixed`: `intervals_seconds` must have 1–20 entries. Each entry is 1–86400 seconds. The number of entries is the number of retries.
- `exponential`: `base_seconds` is 1–86400. `max_retries` is 1–20.
- `max_delay_seconds` (`exponential` only): `None` or an integer >= 1. The cap applies last, after backoff and jitter.
- `retry_for` must have at least 1 entry. Each entry is a code member (see below). Pyright refuses a plain `str`.
- A rule violation of the constructor raises `pydantic.ValidationError` (a `ValueError` subclass) at the constructor call. The constructor does not check the entries: `@app.task` checks them against the codes of the task.

### Jitter

`jitter=True` (default) adds a random 0–25% to each delay; it never shortens a delay. Prevents thundering herd. Disable only in tests.

### `retry_for` valid values

- `BuiltInTaskCode` members that a task failure produces, for example `OperationalCode.UNHANDLED_EXCEPTION`, `OperationalCode.WORKER_CRASHED`, `OutcomeCode.TASK_TIMEOUT`
- Members of the task's own code `Enum` class, declared with `error_codes=`: `ApiCode.RATE_LIMITED` with `error_codes=ApiCode`

`@app.task` refuses any other entry with `HRS-102`: a plain `str` (`"RATE_LIMITED"`), a member of another `Enum` class, or a custom member of a task that declares a tuple or no `error_codes`. A task that retries on a code of its own declares the code in an `Enum` class. The task body can return the member or its text: the stored code is the text in both cases.

The stored policy holds the text of each code (`"RATE_LIMITED"`). A retry starts when the stored error code of the failure is in the list and `retry_count < max_retries`.

### Examples

```python
class ApiCode(str, Enum):
    RATE_LIMITED = "RATE_LIMITED"
    TIMEOUT = "TIMEOUT"
    DEADLOCK = "DEADLOCK"

# 3 fixed retries: 1min, 5min, 15min
RetryPolicy.fixed([60, 300, 900], retry_for=[ApiCode.RATE_LIMITED])

# 5 exponential retries: 30s, 60s, 120s, 240s, 480s
RetryPolicy.exponential(base_seconds=30, max_retries=5, retry_for=[ApiCode.TIMEOUT])

# Quick deadlock retries, no jitter
RetryPolicy.fixed([1, 2, 5], retry_for=[ApiCode.DEADLOCK], jitter=False)

# A built-in code and a code of the task
@app.task(
    "sync_orders",
    error_codes=ApiCode,
    retry_policy=RetryPolicy.fixed(
        [60, 300],
        retry_for=[OperationalCode.WORKER_CRASHED, ApiCode.RATE_LIMITED],
    ),
)
def sync_orders() -> Result[int, TaskError[ApiCode]]:
    return Err(TaskError(code=ApiCode.RATE_LIMITED))
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

Only invoked for unhandled exceptions. If the task returns `Err(...)` explicitly, the mapper is never called.

A `KeyboardInterrupt` is never mapped. In a worker, every other exception of a `def` or an `async def` body is mapped, `GeneratorExit` and `CancelledError` included. A direct call of an `async def` task outside a worker lets `GeneratorExit` pass, and a `CancelledError` while its asyncio task has a cancel request.

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
  reaches the `T` of a task's `Result[T, TaskError]` — rejected by strict-serde at app startup
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

`app.get_result(task_id, ...)`, `TaskHandle.get(...)` and
`WorkflowHandle.get(...)` (and their `*_async` forms) return
`Err(ReadResultDeserializationError(...))` when the stored envelope is
malformed, misses the `__h_task_result__` marker, or does not decode as
the registered types. The read error is apart from any `TaskError` of
the task. The same read gives the same error until the stored value
changes, so a `TaskHandle` caches it.

A successful stored value with no declared type in this process (a
handle built by id, or a task that is not registered here) gives
`Err(ReadNoTypeAvailable(...))`. Read the raw result
(`app.get_raw_result()`), or import the task module.

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

### Forgetting the `Ok` wrapper

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — returns a raw value: the result is Err(TaskError(code=RETURN_TYPE_MISMATCH))
@app.task("bad")
def bad(*, x: int) -> Result[dict[str, int], TaskError]:
    return {"x": x}

# CORRECT
@app.task("good")
def good(*, x: int) -> Result[dict[str, int], TaskError]:
    return Ok({"x": x})
```

### `retry_for` at top level

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — rejected at definition time
@app.task("t", retry_for=[OutcomeCode.TASK_TIMEOUT])
def t() -> Result[str, TaskError]: ...

# CORRECT — goes inside RetryPolicy
@app.task("t", retry_policy=RetryPolicy.fixed([60], retry_for=[OutcomeCode.TASK_TIMEOUT]))
def t() -> Result[str, TaskError]: ...
```

### Decorator ordering — `HRS-104`

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — @app.task sees already-wrapped function
@app.task("t")
@some_other_decorator
def t() -> Result[str, TaskError]: ...

# CORRECT — @app.task must be applied to the raw function
@app.task("t")
def t() -> Result[str, TaskError]: ...
```

### Sync calls inside a coroutine or a Jupyter cell → `ASYNC_CONTEXT`

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — on a thread that runs an event loop, each sync call returns ASYNC_CONTEXT
async def report(handle: TaskHandle[str]) -> None:
    send_r = my_task.send(value="test")      # Err(ASYNC_CONTEXT), nothing enqueued
    result = handle.get(wait_ms=5000)     # Err(ReadAsyncContext(...)), not cached

# CORRECT — use the *_async forms
async def report() -> None:
    handle = (await my_task.send_async(value="test")).unwrap()
    result = await handle.get_async(wait_ms=5000)
```

### `ReadWaitTimeout` is not a terminal failure

<!-- check: skip partial -- uses names that the page does not define -->
```python
match handle.get(wait_ms=5000):
    case Err(ReadWaitTimeout()):
        # Task may still be running. NOT cached — calling get() again will re-poll.
        check_again_later(handle.task_id)
    case _:
        ...
```

### `queue` in DEFAULT mode

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# WRONG — raises HRS-200 at definition time if app uses QueueMode.DEFAULT
@app.task("t", queue="critical")
def t() -> Result[str, TaskError]: ...
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
    TaskError, SubWorkflowFailure, TaskInfo, TaskAttemptInfo,
    # Read result types: the outcome and the read errors
    ReadResult, TaskOutcome, Succeeded, Failed, ReadError,
    ReadWaitTimeout, ReadTaskNotFound, ReadWorkflowNotFound, ReadResultNotAvailable, ReadResultNotReady,
    ReadWaitInsideTask, ReadAsyncContext, ReadInvalidInput, ReadResultDeserializationError, ReadUnknownCustomCode,
    ReadDatabaseError, ReadNoTypeAvailable, ReadWorkflowContextMissingId,
    # The run of a task: cancel state
    TaskRuntime, RunCancelState, CancelNotRequested, CancelRequested, CancelCause,
    # Error code families
    BuiltInTaskCode, OperationalCode, ContractCode, RetrievalCode, OutcomeCode,
    # Retry
    RetryPolicy,
    # Send types
    TaskHandle, TaskFunctionWithOptions, TaskSendResult, TaskSendError, TaskSendErrorCode, TaskSendPayload,
    TaskSendResendable, NoCause, TaskSendSuppressed, TaskSendAsyncContext, TaskSendValidationFailed,
    TaskSendEnqueueFailed, TaskSendPayloadMismatch, TaskSendPayloadTooLarge,
    TaskSendIdempotencyKeyConflict,
    # Exception mapper
    ExceptionMapper,
    # Status
    TaskStatus, TaskAttemptOutcome, TASK_TERMINAL_STATES,
    # Broker result (for handle.info()); match on the variant
    DatabaseResult, DatabaseOperationError, DatabaseErrorCode,
    DatabaseOperationFailed, DatabaseEnqueueFailed, DatabaseInvalidInput,
    # Node data flow helper
    from_node,
    # Rerun (re-execute a terminal task as a new request)
    rerun_task_in_async, RerunCommand, RerunEnqueuePolicy, RerunOutcome,
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
  `rerun_task_in_async`, with a new task id and recorded lineage.
- Result and info reads still work after the move — they resolve across live
  and history.
- Rerun eligibility is decided **at enqueue**: `retain_rerun_input_default`
  (per-task override available) decides whether the enqueue input is preserved
  with the record. A `COMPLETED` source is not eligible for rerun.
- Every task carries a retention class assigned at enqueue (the send's own
  choice, else the queue's `queue_retention` mapping, else the immutable
  30-day class); records age by dropping whole partitions.
