# horsies (Rust) — Tasks

Reference for defining, sending and reading tasks with the `horsies`
crate: the task attributes, the stored input, `TaskError<E>`,
`TaskRuntime`, the cancel signals, the sends, the reads, the
blocking API and rerun.

## Task Attributes

```rust
use horsies::{blocking_task, task, TaskError};

#[task("import_feed", execution = shared_async, timeout_ms = 30_000, cancel_grace_ms = 2_000)]
async fn import_feed(feed_url: String) -> Result<usize, TaskError<std::convert::Infallible>> {
    Ok(feed_url.len())
}

#[blocking_task("resize_image", execution = shared_thread, timeout_ms = 60_000)]
fn resize_image(path: String, width: u32) -> Result<String, TaskError<std::convert::Infallible>> {
    Ok(format!("{path} at {width}px"))
}
```

`#[horsies::task]` takes an `async fn`. `#[horsies::blocking_task]` takes a
`fn`, which runs on a thread of its worker child. The task name comes
first. The attributes after it:

| Attribute | Value | Rule |
|---|---|---|
| `queue` | `"name"` | A queue of `Custom` queue mode. In `Default` mode a task that names a queue is refused (`HRS-200`) |
| `execution` | `isolated` (default), `shared_async`, `shared_thread` | An `async fn` takes `isolated` or `shared_async`; a `fn` takes `isolated` or `shared_thread`. Another class is a compile error |
| `timeout_ms` | `1000..=4294967295` | The deadline of a run. A shared class without `timeout_ms` is a compile error |
| `cancel_grace_ms` | `100..=10000` | `shared_async`: the time from the cancel signal (the deadline, or an operator cancel) to the drop of the body; default 1,000. `isolated`: the time from an operator cancel to the kill of the child when the body has not ended; no default, so no kill. On `shared_thread` it is a compile error |
| `retry_policy` | a `RetryPolicy` expression | Goes with `auto_retry_for` |
| `auto_retry_for` | `["CODE", ...]` | Goes with `retry_policy`, with at least one code. Each entry is a built-in code or a code of `E`; another entry is a compile error |
| `workflow_ctx` | no value | The task takes the workflow context in its parameter `workflow_ctx` (see `workflows.md`) |

- An unknown attribute is a compile error that lists the known ones.
- `good_until` is not a task attribute. Set a deadline per send with
  `TaskSendOptions::good_until`.
- A generic function, a method with `self`, a lifetime parameter and a
  missing return type are compile errors.
- The class and the options are stored with every send. The behaviour of
  each class is on the shared docs page `workers/execution-modes`.

## The Stored Input

The parameters form the input. The attribute generates a struct `Input`
with one public field per parameter, keyed by the parameter's name, as
Python stores keyword arguments.

| Signature | Input type `A` | Stored input |
|---|---|---|
| `fn ping()` | `()` | `{}` |
| `fn ping(_: ())` | `()` | `{}` |
| `fn double(x: i64)` | `double::Input` | `{"x": 2}` |
| `fn validate_order(order: Order)` | `validate_order::Input` | `{"order": {"order_id": "...", "total_cents": 900}}` |
| `fn add(a: i64, b: i64)` | `add::Input` | `{"a": 2, "b": 3}` |
| `fn sync_feed(rt: TaskRuntime, feed_url: String)` | `sync_feed::Input` | `{"feed_url": "..."}` |

- A single parameter is wrapped, whatever its type. `validate_order` does
  not store the fields of `Order` at the top level.
- A parameter whose value is stored needs a name. `_` is a compile error
  for it; it is allowed only for a single `()` parameter.
- A first parameter of type `horsies::TaskRuntime` is not input.
- The generated `Input` refuses an unknown key. A stored input that does
  not decode with `A` fails the run with `WORKER_SERIALIZATION_ERROR`.
- A raw identifier is stored without `r#`: `r#type` is the key `type`.
- A schedule's `input` and a workflow node's `input_json` text use this
  stored form.
- A date-time of type `horsies::StoredDateTime<T>` (`T` is a chrono
  `DateTime<Tz>` or `NaiveDateTime`) is stored in Python's text
  (`2026-08-12T09:10:12.500000Z`). A plain chrono value keeps chrono's serde
  text (`…12.500Z`). Both clients read either text to the same value.

## The Generated Module

`#[task("validate_order")] async fn validate_order(order: Order)`
generates the module `validate_order`. The module holds no state: each
send names its app, and a workflow builder resolves a node against its own
app. Two apps in one process share no registration.

| Item | Does |
|---|---|
| `validate_order::Input` | The input struct: `Input { order }` |
| `validate_order::register(&mut app)` | Registers the task. Returns `Result<TaskFunction<validate_order::Input, T, E>, HorsiesError>` |
| `validate_order::send(&app, order)` | Sends now through `app` |
| `validate_order::with_options(options).send(&app, order)` | Sends through `app` with `TaskSendOptions`; the delay is `TaskSendOptions::delay(WholeSeconds)` |
| `validate_order::node()` | `TaskNode<T, validate_order::Input>`: a workflow node. `builder.task(node)` resolves it against the builder's app |
| `validate_order::params::order()` | The field token `InputField<validate_order::Input, Order>` of a workflow node |
| `validate_order::handle(&runtime)` | `Result<TaskFunction<..>, TaskFunctionNotFound>` from a `TaskRuntime` |

- `send` and `with_options(..).send` through an app that does not register
  the task return `TaskSendError::ValidationFailed` with the cause
  `PreconditionFailed`. The message names the task. Nothing is written.
- `builder.task(node)` on a builder of an app that does not register the
  task returns a `HorsiesError` with `HRS-302`
  (`HorsiesErrorCode::WorkflowUnregisteredTask`). The node is not added.
- `register` checks the codes of the code type (`UserErrorCode::CODES`):
  a built-in or reserved name is `HRS-212`; a code that is empty, not
  `UPPER_SNAKE_CASE` or an exception class name is `HRS-112`; a code given
  twice is `HRS-113`. The derive refuses a built-in or reserved name and a
  repeated code at compile time. A variant name that gives a code outside
  `UPPER_SNAKE_CASE` (a leading `_`, a letter outside ASCII) and a
  hand-written impl reach the registration check.
- `CODES` are the task's declared error codes. `horsies check` lists them.
  The `error_code` metric label of the task has each built-in code, each
  declared code and `undeclared`; another user code counts as `undeclared`.
  No user work is needed: the task macros fill the codes from `E`.
- `TaskFunctionNotFound` is `NotRegistered { task_name }` or
  `OtherTypes { task_name, registered, asked }`.

## `TaskError<E>`

```rust
use horsies::{task, OperationalCode, TaskError, UserErrorCode};

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum ChargeCode {
    CardDeclined, // CARD_DECLINED
    RateLimited,  // RATE_LIMITED
}

#[task("charge_card")]
async fn charge_card(amount_cents: u64) -> Result<String, TaskError<ChargeCode>> {
    if amount_cents == 0 {
        return Err(TaskError::library(OperationalCode::TaskException, "nothing to charge"));
    }
    Err(TaskError::user(ChargeCode::CardDeclined, "the issuer declined the card")
        .with_data(serde_json::json!({ "amount_cents": amount_cents })))
}
```

`#[derive(UserErrorCode)]` goes on an enum of unit variants. Each variant's
code is its name in UPPER_SNAKE_CASE. These are compile errors: a variant
with fields, two variants with one code, a generic enum, a code with the
name of a built-in code or of the reserved code `WORKFLOW_STOPPED`.

| Item | Returns |
|---|---|
| `TaskError::user(code, message)` | An error with a code of `E` |
| `TaskError::library(code, message)` | An error with a library code: `BuiltInTaskCode` or a family (`OperationalCode`, `ContractCode`, `RetrievalCode`, `OutcomeCode`) |
| `.with_data(value)` | This error with `data`. Never fails |
| `.code()` | `&TaskErrorCode<E>`: `Library(BuiltInTaskCode)` or `User(E)` |
| `.code().text()` | The stored code text |
| `.message()` | `Option<&str>` |
| `.data()` | `Option<&JsonText>`: the stored text; `decode::<T>()` reads it, an integer of any width with all its digits |
| `.exception()` | `Option<&ExceptionRecord>`: set by the library for a panic of the body |
| `.sub_workflow()` | `Option<&SubWorkflowFailure>`: the failed child of a sub-workflow node |

- The fields are private. The exception record and the sub-workflow
  failure are set only by the library.
- `with_data(value)` takes any `Serialize` value. A value that cannot be
  stored is replaced with `{"data_refused": "<cause>"}`, and the replacement
  is logged at ERROR. The code and the message are kept. A value cannot be
  stored when it is not JSON-native (a non-finite float, a map key that is
  not a string, a `Serialize` impl that fails), when it holds a key that
  only the engine writes (`__h_...`, `__horsies_...`,
  `__builtin_task_code__`), or when it is nested deeper than the error
  bound.
- A `BuiltInTaskCode` is `Operational(..)`, `Contract(..)`, `Retrieval(..)`
  or `Outcome(..)`. Match a stored timeout as
  `TaskErrorCode::Library(BuiltInTaskCode::Outcome(OutcomeCode::TaskTimeout))`.

What the library records:

| Event | Stored error |
|---|---|
| The stored input does not decode with `A` | `WORKER_SERIALIZATION_ERROR`. The message names the type, the field path and the kind of failure; a part that comes from the input is written as `<input>` |
| The returned value does not read back with `T` | `WORKER_SERIALIZATION_ERROR`. The child encodes the value, decodes the text with `T`, encodes it again, and stores it only when the two texts are equal |
| The body panics | `.exception()` holds the record of the panic |
| Timeout, crash, cancel, expiry | The library code: `TASK_TIMEOUT`, `WORKER_CRASHED`, `TASK_CANCELLED`, `TASK_EXPIRED`, ... (shared docs page `tasks/errors`) |

## `TaskRuntime`

A first parameter of type `horsies::TaskRuntime` gives the body the app's
state, the cancel and the fencing token of its run, and work that belongs
to its run.

```rust
use horsies::{task, Horsies, HorsiesError, TaskError, TaskRuntime, UserErrorCode};

struct Mailer {
    sender: String,
}

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum MailCode {
    NotConfigured,
}

#[task("send_welcome")]
async fn send_welcome(rt: TaskRuntime, address: String) -> Result<String, TaskError<MailCode>> {
    let mailer = rt
        .state::<Mailer>()
        .map_err(|missing| TaskError::user(MailCode::NotConfigured, missing.to_string()))?;
    Ok(format!("{} -> {address}", mailer.sender))
}

fn register_mail(app: &mut Horsies) -> Result<(), HorsiesError> {
    app.provide(Mailer { sender: "noreply@example.com".to_owned() })?;
    let _welcome = send_welcome::register(app)?;
    Ok(())
}
```

| Method | Returns |
|---|---|
| `state::<T>()` | `Result<Arc<T>, StateNotProvided>`: the value that `Horsies::provide` gave. The first read freezes the app's state |
| `cancelled()` | A future that completes when the run is cancelled: an operator cancel, or the deadline of a `shared_async` run |
| `is_cancelled()` | Whether the run is cancelled; works in the async body, in `spawn` work and in a blocking body; `false` outside a run |
| `cancel_state()` | `RunCancelState`: `NotRequested`, `Requested { cause: CancelCause::Operator \| CancelCause::Deadline }`, or `NotInARun` |
| `spawn(name, future)` | `Result<tokio::task::JoinHandle<_>, RunSpawnRefused>`: starts `future` as work of the run |
| `fencing_token()` | `Result<FencingToken, FencingTokenRefused>`: the task id (`task_id()`) and the value (`value().get()`) of the run |
| `task_handle::<A, T, E>(task_name)` | `Result<TaskFunction<A, T, E>, TaskFunctionNotFound>` |
| `workflow_builder(name)` | A `WorkflowSpecBuilder` of the app of the runtime; its task nodes resolve against that app |
| `start::<T>(&spec)`, `start_with_id::<T>(&spec, workflow_id)` | Starts a workflow that the task built (see `workflows.md`) |

- Each process builds its own state: every worker child runs `main` again.
- `provide` refuses a second value of one type (wrap it in a newtype) and a
  value provided after a body read the state.

### Fencing Token

A stalled attempt can run on after the worker retries its task. Write the
fencing token with each write to another system, and make that write
conditional: the store refuses a token lower than the one it holds for the
task id. Two reads in one attempt give the same token; a later attempt of
the same task gives a greater value; the value depends on no clock. Today
the value is the attempt number: compare values of one task id, and do not
depend on the number.

```rust
use horsies::{task, TaskError, TaskRuntime, UserErrorCode};

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum ExportCode {
    NotInARun,
}

#[task("export_report")]
async fn export_report(
    rt: TaskRuntime,
    report_id: String,
) -> Result<String, TaskError<ExportCode>> {
    let token = rt
        .fencing_token()
        .map_err(|refused| TaskError::user(ExportCode::NotInARun, refused.to_string()))?;
    // UPDATE exports SET fence = $2, ... WHERE task_id = $1 AND fence <= $2
    // Zero rows changed: a later attempt holds the fence, so stop.
    Ok(format!("{report_id}:{}:{}", token.task_id(), token.value().get()))
}
```

- Outside a run (a direct call in a test, plain `tokio::spawn`,
  `spawn_blocking` from an async body, a thread that a blocking body
  starts) `fencing_token()` is `Err(FencingTokenRefused::NotInARun)`: read
  the token in the body and pass it on. `rt.spawn` work reads the token of
  its run.
- Limits: a rerun or a workflow node that a resume starts again gets a new
  task id and starts again at 1; a stale attempt with no later attempt, or
  one that writes before the later attempt took the fence, is not refused.
  Shared docs page `concepts/fencing-token`.

## Cancel Signals

The cancel signal of a run fires for two causes. The first cause stays.

- `CancelCause::Operator`: an operator cancel of the task
  (`horsies::monitoring::cancel_task` with `include_running = true`, or the
  web UI), in every execution class. One statement writes the attempt row
  with outcome `Cancelled` and moves the task to history as `CANCELLED`; a
  waiting `TaskHandle::get` returns `Failed` with `TASK_CANCELLED`. What the
  body returns after the cancel is dropped. A cancel before the body starts:
  the body does not run.
- `CancelCause::Deadline`: the deadline of a `shared_async` or a
  `shared_thread` run, at its `timeout_ms`. The run ends as `TASK_TIMEOUT`
  whatever the body returns. An `isolated` run is killed at its deadline
  with no signal.

The worker gets an operator cancel through a `NOTIFY` on
`horsies_task_cancel`; a lost notice is found by one read after a listener
reconnect, or after a runner heartbeat of the run that does not land (at
most `runner_heartbeat_interval_ms`, default 30 s).

```rust
use horsies::{task, TaskError, TaskRuntime, UserErrorCode};

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum FeedCode {
    Interrupted,
    WorkRefused,
    WorkFailed,
}

async fn fetch_items(feed_url: &str) -> usize {
    feed_url.len()
}

#[task("sync_feed", execution = shared_async, timeout_ms = 30_000)]
async fn sync_feed(rt: TaskRuntime, feed_url: String) -> Result<usize, TaskError<FeedCode>> {
    tokio::select! {
        count = fetch_items(&feed_url) => Ok(count),
        () = rt.cancelled() => Err(TaskError::user(FeedCode::Interrupted, "cancelled")),
    }
}

#[task("index_feed", execution = shared_async, timeout_ms = 30_000)]
async fn index_feed(rt: TaskRuntime, feed_urls: Vec<String>) -> Result<usize, TaskError<FeedCode>> {
    let work = rt
        .spawn("count-feeds", async move { feed_urls.len() })
        .map_err(|refused| TaskError::user(FeedCode::WorkRefused, refused.to_string()))?;
    work.await
        .map_err(|join_error| TaskError::user(FeedCode::WorkFailed, join_error.to_string()))
}
```

- `shared_async`: the body and the work it started with `spawn` have
  `cancel_grace_ms` to end after the signal. Then the child drops them.
- `isolated`: the body runs until it returns, or until its `timeout_ms`
  kill. With `cancel_grace_ms` on the task, the worker kills the child
  `cancel_grace_ms` after an operator cancel.
- `shared_thread`: the body runs until it returns.
- A blocking body reads `is_cancelled()` or `cancel_state()` between the
  steps of its work; `cancelled()` is a future and has no use there.
- Outside a run (a direct call in a test) `cancelled()` never completes,
  `is_cancelled()` is `false` and `cancel_state()` is `NotInARun`.
- Work started with `spawn` belongs to the run: the child waits for it when
  the body returns, cancels it after the grace, and reports work that does
  not stop. Work started with plain `tokio::spawn` is not tracked.
- `spawn` refuses with `RunSpawnRefused` outside the async body of a run in
  a worker child (`NotInARun`), after the run ended (`RunEnded`), and past
  10,000 pieces of live work (`TooMuchWork`). Nothing starts then.
  `RunSpawnRefused` is a type of `horsies-core`; read it through `Display`.

The `data` of a `TASK_TIMEOUT` error has `task_id` and `timeout_ms`. A
`shared_async` run on a shared async child also has `cancel_end`:

| `cancel_end` | The run |
|---|---|
| `RETURNED_AFTER_SIGNAL` | Ended within `cancel_grace_ms` after the cancel at the deadline |
| `DROPPED_AFTER_GRACE` | Was alive after the grace: the child sent the second cancel and dropped it |
| `KILLED_WITH_CHILD` | Ended when the worker killed its child, before the child reported the end |

An `isolated` or `shared_thread` timeout has no `cancel_end`. Read it from
the stored error:

```rust
use horsies::{BuiltInTaskCode, OutcomeCode, TaskError, TaskErrorCode, UserErrorCode};

fn cancel_end<E: UserErrorCode>(error: &TaskError<E>) -> Option<String> {
    match error.code() {
        TaskErrorCode::Library(BuiltInTaskCode::Outcome(OutcomeCode::TaskTimeout)) => {
            let data: serde_json::Value = error.data()?.decode().ok()?;
            data.get("cancel_end")?.as_str().map(str::to_owned)
        }
        _ => None,
    }
}
```

A blocking body that stops at a cancel:

```rust
use horsies::{blocking_task, RunCancelState, TaskError, TaskRuntime};

fn import_batch(batch_id: &str) {
    let _ = batch_id; // stands in for one step of the work
}

#[blocking_task("import_batches", timeout_ms = 600_000, cancel_grace_ms = 5_000)]
fn import_batches(
    rt: TaskRuntime,
    batch_ids: Vec<String>,
) -> Result<usize, TaskError<std::convert::Infallible>> {
    let mut done = 0;
    for batch_id in &batch_ids {
        match rt.cancel_state() {
            // What the task returns after an operator cancel is dropped.
            RunCancelState::Requested { .. } => break,
            RunCancelState::NotRequested | RunCancelState::NotInARun => {
                import_batch(batch_id);
                done += 1;
            }
        }
    }
    Ok(done)
}
```

The whole timeout sequence is on the shared docs page
`workers/shared-async-tasks` ("The Timeout"), and the rules of a cancel of
each task status on `monitoring/action-semantics`.

## Retry Policy

```rust
use horsies::{task, RetryPolicy, TaskError, UserErrorCode};

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum FetchCode {
    RateLimited,
}

#[task(
    "fetch_prices",
    retry_policy = RetryPolicy::fixed(vec![30, 60, 120], true).expect("a valid policy"),
    auto_retry_for = ["RATE_LIMITED", "TASK_TIMEOUT", "WORKER_CRASHED"],
    timeout_ms = 5_000
)]
async fn fetch_prices(symbol: String) -> Result<String, TaskError<FetchCode>> {
    Err(TaskError::user(FetchCode::RateLimited, format!("rate limited for {symbol}")))
}
```

| Constructor | Policy |
|---|---|
| `RetryPolicy::fixed(intervals, jitter)` | One delay per retry, in seconds. The number of intervals is the number of retries |
| `RetryPolicy::exponential(base_seconds, max_retries, jitter)` | Delay `base_seconds * 2^(attempt - 1)` |
| `.with_max_delay_seconds(seconds)` | A cap on the delay, after backoff and jitter |

Each interval is 1 to 86,400 s, and a policy has 1 to 20 retries. A value
outside is `RetryPolicyError`. A built-in name in `auto_retry_for` is stored
as the built-in code. When a retry starts is on the shared docs page
`tasks/retry-policy`.

## `TaskFunction<A, T, E>` — Sends

`register` returns the typed function: input `A`, output `T`, code type
`E`. Clones share one app.

| Method | Sends |
|---|---|
| `send(input)` | Now |
| `send_with_options(options, input)` | With `TaskSendOptions`: now, or after `TaskSendOptions::delay(WholeSeconds::from_secs(300))` |
| `with_options(options)` | A `TaskFunctionSendOptions` with `send(input)` |
| `retry_send(&error)` | The task of a failed send again, the delay included (below) |
| `node()` | A workflow node (see `workflows.md`) |
| `blocking()` | The blocking view (see Blocking API) |

`task_name()`, `queue_name()`, `priority()` and `task_options()` read the
registration.

A send runs these steps in order and stops at the first refusal:

1. A generated send finds the task in the app that it names. An app that
   does not register the task refuses with `ValidationFailed`.
2. Send suppression: a suppressed send returns `Suppressed`.
3. The retention of the send.
4. The read-back of the input: encode it, decode the text with `A`, encode
   that value again. Two different texts refuse the send with
   `ValidationFailed` before any statement.
5. The payload size: an input over `AppConfig.payload.reject_bytes` is
   `PayloadTooLarge`.
6. A new task id and the sent instant.
7. One enqueue through the core.

In a worker child a send goes through the child's own producer handle.

### Send options

```rust
use chrono::{Duration, Utc};
use horsies::{SendRetention, TaskSendOptions, WholeSeconds};

fn invoice_options(invoice_id: u64) -> TaskSendOptions {
    TaskSendOptions::new()
        .delay(WholeSeconds::from_secs(60))
        .good_until(Utc::now() + Duration::hours(1))
        .idempotency_key(format!("invoice-{invoice_id}"))
        .retention(SendRetention::Forever)
}
```

| Option | Does |
|---|---|
| `delay(WholeSeconds)` | The task starts no earlier than the delay after the send (`enqueue_delay_seconds`). Without it, the task starts when a worker claims it |
| `good_until(deadline)` | The task expires (`TASK_EXPIRED`) when no worker started it by `deadline` |
| `idempotency_key(key)` | Stores the task once per key within the reservation window (24 hours). A second send of the same command answers with the task that holds the key |
| `retention(SendRetention)` | The retention of the task's history record |
| `retain_forever()` | `retention(SendRetention::Forever)` |

| `SendRetention` | History record kept |
|---|---|
| `FromQueueOrDefault` (default) | For the period that `RetentionConfig.queue_retention` maps the queue to, else 30 days |
| `Forever` | Forever |
| `Period(RetentionDays)` | For a registered period of whole days. Build it with `RetentionDays::parse_period(chrono::Duration::days(n))`. Another period is refused with the allowed periods |

### `TaskSendError`

| Variant | Code | Fields | Retry methods |
|---|---|---|---|
| `Suppressed` | `SEND_SUPPRESSED` | `message` | Refused |
| `AsyncContext` | `ASYNC_CONTEXT` | `task_id`, `payload`, `message` | Accepted |
| `ValidationFailed` | `VALIDATION_FAILED` | `message`, `cause` | Refused |
| `EnqueueFailed` | `ENQUEUE_FAILED` | `task_id`, `payload`, `message`, `retryable`, `cause` | Accepted |
| `PayloadMismatch` | `PAYLOAD_MISMATCH` | `task_id`, `payload`, `message` | Refused |
| `PayloadTooLarge` | `PAYLOAD_TOO_LARGE` | `message`, `size_bytes`, `limit_bytes` | Refused |
| `IdempotencyKeyConflict` | `IDEMPOTENCY_KEY_CONFLICT` | `task_id`, `payload`, `message`, `owner_task_id` | Refused |

- `ValidationFailed` with the cause `PreconditionFailed` is also the answer
  of a generated send through an app that does not register the task.
- `EnqueueFailed`: the row may or may not exist. `retryable` is the
  classification of the cause.
- `cause` (`failure_cause()` on every variant): what the send met, a
  `FailureCause`: the server's error, no answer, a broken read contract, or
  a failed precondition.
- `AsyncContext`: a blocking send on a thread that runs a tokio runtime.
  Nothing was written. The async sends never return it.
- `PayloadMismatch`: the task id exists with another command or key.
  Nothing was written.
- `IdempotencyKeyConflict`: another task (`owner_task_id`) holds the key
  with another command. Nothing was written. A repeat of the same send does
  not complete it: send the same command as the owner task, or use another
  key. The reservation ends `reservation_window` after the owner task
  finishes; a send with the key after that time starts a new task.
- `error.code()` returns the `TaskSendErrorCode`.
- A send in a fork panics, as every call that runs on the core: see
  `SKILL.md`, Result Types.

```rust
use horsies::{task, TaskError, TaskFunction, TaskHandle, TaskSendError};

#[task("send_receipt")]
async fn send_receipt(order_id: String) -> Result<(), TaskError<std::convert::Infallible>> {
    let _sent = order_id;
    Ok(())
}

async fn send_with_one_retry(
    receipt: &TaskFunction<send_receipt::Input, (), std::convert::Infallible>,
    order_id: String,
) -> Result<TaskHandle<(), std::convert::Infallible>, TaskSendError> {
    match receipt.send(send_receipt::Input { order_id }).await {
        Err(error @ TaskSendError::EnqueueFailed { retryable: true, .. }) => {
            receipt.retry_send(&error).await
        }
        other => other,
    }
}
```

`retry_send` sends the same task id and payload again, as a repeat. The
payload of a send with a delay keeps its delay. A task that an earlier call
wrote answers with its id, also after it finished, and does not run again.
It refuses with `ValidationFailed` an error with no payload and the error of
another task.

With `AppConfig.resend_on_transient_error`, a retryable `EnqueueFailed` is
sent again up to 3 times, after 200, 400 and 800 ms, as a repeat.

## `TaskHandle<T, E>` — Reads

```rust
use std::time::Duration;

use horsies::{ReadError, TaskHandle, TaskOutcome, WaitLimit};

async fn print_outcome(
    handle: &TaskHandle<String, std::convert::Infallible>,
) -> Result<(), ReadError> {
    match handle.get(WaitLimit::Within(Duration::from_secs(30))).await? {
        TaskOutcome::Succeeded(order_id) => println!("validated {order_id}"),
        TaskOutcome::Failed(error) => println!("failed with {}", error.code().text()),
    }
    Ok(())
}
```

| `WaitLimit` | Wait |
|---|---|
| `WaitLimit::NoLimit` | Until the task ends |
| `WaitLimit::Within(duration)` | Up to `duration`. Zero reads once and does not wait |

- The read checks the limit first. A `Within` limit above
  `MAX_WAIT_LIMIT_MS` (4,294,967,295 ms) is `ReadError::InvalidInput`.
- This check comes before a kept read, `WaitInsideTask` and
  `AsyncContext`. No statement runs. The handle keeps nothing.

| `TaskOutcome<T, E>` | The task |
|---|---|
| `Succeeded(T)` | Returned this value |
| `Failed(TaskError<E>)` | Returned this error, or the library recorded a code (`TASK_CANCELLED`, `TASK_EXPIRED`, `TASK_TIMEOUT`, `WORKER_CRASHED`, ...) |

| `ReadError` | Code | When | Kept by the handle |
|---|---|---|---|
| `WaitTimedOut { task_id, status }` | `WAIT_TIMEOUT` | The task had not ended when the limit passed | No |
| `TaskNotFound { task_id, reason }` | `TASK_NOT_FOUND` | Neither the live table nor the history holds the task | No |
| `ResultNotAvailable { task_id, status }` | `RESULT_NOT_AVAILABLE` | Terminal status with no stored result | Yes |
| `StoredResultUnreadable { task_id, cause }` | `RESULT_DESERIALIZATION_ERROR` | The stored result does not read with `T` and `E` | Yes |
| `UnknownUserCode { task_id, code, code_type }` | `UNKNOWN_CUSTOM_CODE` | The stored user code is not a code of `E` | Yes |
| `InvalidInput { task_id, cause }` | `INVALID_INPUT` | The `Within` limit is above `MAX_WAIT_LIMIT_MS`; `cause` is `WaitLimitRefused`. Nothing was read | No |
| `WaitInsideTask { message }` | `WAIT_INSIDE_TASK` | A result wait in a worker child. Nothing was read | No |
| `AsyncContext { message }` | `ASYNC_CONTEXT` | A blocking read on a thread that runs a tokio runtime. Nothing was read | No |
| `Database { code, message, retryable }` | `DATABASE_ERROR` | The database or the connection failed | No |

- `ReadError::code()` returns the library code of the failure.
- `reason` is a `TaskAbsenceReason`: `PredatesRetainedHistory` (retention
  can have removed the task), `WithinRetainedHistory` (no removed history
  leaf can have held it), `NotClassified` (the id is not a UUIDv7).
- The handle keeps its first final read (an outcome, or a failure marked
  "Yes"). A later `get` returns it with no read. Clones share one kept read.
- `handle.task_id()` returns the `Uuid`.

`handle.info(TaskInfoParts { result, failed_reason, attempts })` returns the
`TaskInfo`: status and times, plus the parts asked for (attempts newest
first). Its `result` is a `TaskResult<JsonText>`: the value as its stored
text, read with `decode::<T>()`. The info is not kept. Its errors are `TaskNotFound`,
`StoredResultUnreadable` and `Database`.

Reads by task id, on the app:

| Method | Returns |
|---|---|
| `app.task_handle_of::<T, E>(task_id)` | A `TaskHandle<T, E>`. No I/O |
| `app.get_result::<T, E>(task_id, limit)` | `Result<TaskOutcome<T, E>, ReadError>`. Nothing is kept |
| `app.get_raw_result(task_id, limit)` | `Result<StoredResult, ReadError>`: `task_id`, `task_name`, `status`, `envelope_json` |
| `app.get_task_info(task_id, parts)` | `Result<TaskInfo, ReadError>` |

## Blocking API

`horsies::blocking` gives blocking views for a program with no tokio
runtime. A view method has the name, the arguments and the result type of
the async method. It runs on the core's process runtime and blocks the
calling thread.

| View | From | Methods |
|---|---|---|
| `TaskFunctionView` | `task_function.blocking()` | `send`, `send_with_options`, `with_options(..).send`, `retry_send` |
| `TaskHandleView` | `handle.blocking()` | `get`, `info` |
| `HorsiesView` | `app.blocking()` | `get_result`, `get_raw_result`, `get_task_info`, `ping_database`, `ping_workers`, `list_worker_states`, `get_worker_state`, `get_worker_state_history`, `get_execution_class_coverage`, `get_execution_class_report`, `check_live`, `start`, `start_with_id`, `start_workflow`, `start_workflow_with` |
| `WorkflowFunctionView` | `workflow_function.blocking()` | `start`, `start_with_id`, `retry_start` |
| `WorkflowTemplateView` | `workflow_template.blocking()` | `start`, `start_with_id` |
| `WorkflowHandleView` | `workflow_handle.blocking()` | `get`, `result_for`, `status`, `results`, `tasks`, `cancel`, `pause`, `resume` |

The starts of a definition by its type are methods of the app view:
`app.blocking().start_workflow::<D>()` and
`app.blocking().start_workflow_with::<D>(params)`.

- On a thread that runs a tokio runtime, a blocking call runs nothing and
  returns the `ASYNC_CONTEXT` member of its catalog:
  `TaskSendError::AsyncContext` (with the prepared task id and payload, so
  `retry_send(&error).await` completes the same send),
  `ReadError::AsyncContext`, `DatabaseOperationError::AsyncContext`, a
  `ValidationReport` with `HRS-217`,
  `WorkflowStartError::AsyncContext` (with the `workflow_id` of the
  start; `retry_start(&error).await` completes the same start from async
  code), `WorkflowHandleError::AsyncContext`, or
  `WorkflowReadError::AsyncContext`.
- The thread of a `#[blocking_task]` body in a worker child drives no async
  task, so a blocking send or workflow start works there. A result wait in
  a worker child is still `WaitInsideTask`.
- A refused input is refused before the thread check: a suppressed send is
  `Suppressed` on any thread, and a read with a `Within` limit above
  `MAX_WAIT_LIMIT_MS` is `InvalidInput` on any thread. A workflow start
  checks the thread first, as Python's `WorkflowSpec.start`: on a runtime
  thread a template's build does not run. `retry_start` checks its error
  first.

## Rerun a Terminal Task

A rerun reads a terminal source task and, for an eligible source with a
retained input, writes one new task with the lineage `RERUN`. Every outcome
but `Enqueued` writes nothing.

In a transaction of the app:

```rust
use horsies::rerun::{AppRerunError, RerunEnqueuePolicy, RerunOutcome, RerunTask};
use horsies::Horsies;
use uuid::Uuid;

async fn rerun_failed(app: &Horsies, source_task_id: Uuid) -> Result<Option<Uuid>, AppRerunError> {
    let command = RerunTask::new(source_task_id, None, None);
    let policy = RerunEnqueuePolicy::standard(true);
    match app.rerun_task(command, policy).await? {
        RerunOutcome::Enqueued { new_task_id, .. } => Ok(Some(new_task_id)),
        RerunOutcome::KeyReplay { existing_task_id } => Ok(Some(existing_task_id)),
        RerunOutcome::SourceLive { .. }
        | RerunOutcome::SourceAbsent { .. }
        | RerunOutcome::SourceCorrupt { .. }
        | RerunOutcome::NotEligible { .. }
        | RerunOutcome::InputUnavailable { .. }
        | RerunOutcome::InputCorrupt { .. }
        | RerunOutcome::KeyConflict { .. } => Ok(None),
    }
}
```

In a transaction that the caller owns (feature `sqlx`; feature
`tokio-postgres` gives `TokioPostgresCaller::new(&mut transaction)` for a
`tokio_postgres::Transaction`):

```rust
use chrono::Utc;
use horsies::rerun::{rerun_task_in, RerunEnqueuePolicy, RerunOutcome, RerunTask, SqlxCaller};
use horsies::Horsies;
use sqlx::PgPool;
use uuid::Uuid;

async fn rerun_with_audit(
    app: &Horsies,
    pool: &PgPool,
    source_task_id: Uuid,
) -> Result<RerunOutcome, Box<dyn std::error::Error>> {
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO audit_log (action, task_id) VALUES ('rerun', $1)")
        .bind(source_task_id)
        .execute(&mut *transaction)
        .await?;
    let outcome = rerun_task_in(
        &mut SqlxCaller::new(&mut transaction),
        &RerunTask::new(source_task_id, None, None),
        &RerunEnqueuePolicy::standard(true),
        &app.registered_task_codes(),
        Utc::now(),
    )
    .await?;
    transaction.commit().await?;
    Ok(outcome)
}
```

- `RerunTask::new(source_task_id, deadline, caller_key)`: `deadline` is an
  `Option<DateTime<Utc>>` for the new task; `caller_key` is an
  `Option<String>` idempotency key of the rerun.
- `RerunEnqueuePolicy::new(retention_class_key, retain_rerun_input,
  reservation_window)` returns `Result<_, RerunError>`.
  `RerunEnqueuePolicy::standard(retain_rerun_input)` uses the class
  `standard_30d` and a 24-hour window.
- `rerun_task_in` commits nothing. The caller's commit keeps the new task
  and the key claim; its rollback drops them. After an error the
  transaction may hold the key claim or the new row: do not commit it.
- `AppRerunError`: `Connect(BrokerError)` (nothing ran) or
  `Rerun(RerunError)`.
- `CallerRerunError<E>`: `Driver(E)`, `ClassNotRegistered { key }`,
  `ClientFamilyMismatch { stored }`, `UnknownStoredFamily` (a family row
  that holds no client family), `CallerKeyRefused`,
  `RetryCodeNotDeclared { task_name, retry_code }`, `TaskNotRegistered
  { task_name }`, `IdMintRefused`, `Row`.
- The stored retry list of the source is checked before the write: a
  built-in code passes; a custom code passes when the app registers the
  task and the task declares the code. `app.rerun_task` reads the app's
  registry; `rerun_task_in` takes `&app.registered_task_codes()`. A custom
  code that the task does not declare is `RetryCodeNotDeclared`
  (`RerunErrorCode::RetryCodeNotDeclared`); a custom code of a task that
  the app does not register is `TaskNotRegistered`
  (`RerunErrorCode::TaskNotRegistered`). Nothing is written; not
  retryable. `RerunError` has the same two variants.

| `RerunOutcome` | Meaning |
|---|---|
| `Enqueued { new_task_id, source_task_id, rerun_root_task_id }` | The new task is written |
| `SourceLive { task_id }` | The source has not ended |
| `SourceAbsent { task_id, reason }` | No record has the id; `reason` is a `TaskAbsenceReason` |
| `SourceCorrupt { task_id, detail }` | The source history row does not decode |
| `NotEligible { task_id, reason }` | `NotEligibleReason::CompletedSource` or `WorkflowTask` |
| `InputUnavailable { task_id, disposition }` | The source stores no input; `disposition` is the stored reason (`DECLINED_BY_POLICY`, `OVER_BOUND`) |
| `InputCorrupt { task_id, detail }` | The stored input fails its integrity or shape check |
| `KeyConflict { task_id, reserved_by_task_id }` | The caller key is held by another command |
| `KeyReplay { existing_task_id }` | The caller key already holds this rerun command; the existing task answers |

The rules are on the shared docs page `monitoring/action-semantics`
("Task rerun").

## Register by Hand

`async_task_fn!(function, InputType)` and
`blocking_task_fn!(function, InputType)` build a `RegisteredTask` from a
function that takes one input value. `app.register(name, task)` registers
it; `RegisteredTask::with_task_options(options)` sets its options. Prefer
the attributes: a task registered by hand has no generated module and no
typed function.

## Common Pitfalls

**Start nothing in `main` before `horsies::cli::run(app)`.** Every worker
child runs `main` again. A server, a thread or a loop started there starts
again in each child.

**Wait for no result in a task body.** A result wait in a worker child is
refused with `WaitInsideTask`. Pass the result with a workflow (`arg_from`).

<!-- rust-check: skip elided -- a Wrong / Correct pair of statements -->
```rust
// WRONG: in a task body
let total = other_task_handle.get(WaitLimit::NoLimit).await;

// CORRECT: a workflow node receives the dependency's outcome
// fn report(total: TaskOutcome<u64, Infallible>) ...
// builder.task(report::node().arg_from(report::params::total(), totals_node))?
```

**Rename a parameter only when nothing stored holds the old name.** The
parameter name is the stored key. A queued task, a schedule or a workflow
node with the old key fails with `WORKER_SERIALIZATION_ERROR`.

**Write the stored form in schedule input and `input_json`.** A task
`fn validate_order(order: Order)` reads `{"order": {...}}`, not the fields
of `Order`: a schedule's `.input` takes `validate_order::Input { order }`,
not the `Order`.

**Use the async forms on a runtime thread.** A blocking call on a thread
that runs a tokio runtime returns `ASYNC_CONTEXT` and runs nothing.

**Start run work with `rt.spawn`, not `tokio::spawn`, in a `shared_async`
body.** Plain `tokio::spawn` work is not tracked, not waited for and not
cancelled with the run.

**Send through the app that registered the task.** The generated `send`
and `with_options(..).send` find the task in the app that they name. An app
that does not register the task refuses the send with `ValidationFailed`.
A workflow builder of such an app refuses the node with `HRS-302`.

**Do not name a code type without its code.** `TaskError` without `<E>` is
a compile error. Use `TaskError<std::convert::Infallible>` for a task with
no codes of its own.

## All Key Imports

```rust
use horsies::{
    // Task definition
    blocking_task, task, UserErrorCode, TaskError, TaskErrorCode, TaskRuntime,
    RunCancelState, CancelCause, FencingToken, FencingTokenRefused,
    StoredDateTime, RetryPolicy, RetryPolicyError, Execution, TaskOptions,
    RegisteredTask, async_task_fn, blocking_task_fn,
    // Library codes
    BuiltInTaskCode, OperationalCode, ContractCode, RetrievalCode, OutcomeCode,
    ExceptionRecord, SubWorkflowFailure, UserCode, UserCodeRefused, UnknownUserCode,
    // Sends
    TaskFunction, TaskFunctionSendOptions, TaskFunctionNotFound, TaskSendOptions,
    SendRetention, RetentionDays, WholeSeconds, TaskSendError, TaskSendErrorCode,
    TaskSendPayload,
    // Reads
    TaskHandle, TaskOutcome, ReadError, WaitLimit, WaitLimitRefused, MAX_WAIT_LIMIT_MS,
    TaskInfo, TaskInfoParts, TaskAttemptInfo, TaskStatus, TaskAbsenceReason, StoredResult,
};
use horsies::blocking::{HorsiesView, TaskFunctionView, TaskHandleView};
use horsies::rerun::{
    rerun_task_in, AppRerunError, CallerRerunError, CallerTransaction, NotEligibleReason,
    RerunEnqueuePolicy, RerunError, RerunOutcome, RerunTask,
};
```
