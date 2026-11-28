---
title: Defining Tasks
summary: Define a task with #[horsies::task] or #[horsies::blocking_task], choose its execution class and retry codes, and read the app's state, the cancel and the fencing token of its run with TaskRuntime.
related: [errors, sending-tasks, ../configuration/app-config, ../../workers/execution-modes, ../../tasks/retry-policy, ../../concepts/fencing-token]
tags: [rust, task, macro, execution, retry, TaskRuntime, cancel, RunCancelState, FencingToken, fencing token]
---

# Defining Tasks

A task is an `async fn` with `#[horsies::task]` or a `fn` with `#[horsies::blocking_task]`. The function returns `Result<T, TaskError<E>>`: `T` is the output type and `E` is the code type of the task's own errors.

## Define a Task

```rust
use horsies::{task, CustomErrorCode, TaskError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    total_cents: u64,
}

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum OrderCode {
    LimitExceeded, // LIMIT_EXCEEDED
}

#[task("validate_order")]
async fn validate_order(order: Order) -> Result<String, TaskError<OrderCode>> {
    if order.total_cents > 1_000_000 {
        return Err(TaskError::custom(OrderCode::LimitExceeded, "order over the limit"));
    }
    Ok(order.order_id)
}
```

- The parameters form the input, stored as Python stores keyword arguments: a generated `Input` struct with one field per parameter, keyed by the parameter's name, a single parameter of any type included. `validate_order` stores `{"order": {"order_id": "...", "total_cents": ...}}`; `fn double(x: i64)` stores `{"x": 2}`. No parameter, or one parameter written `()`, is the input `()`: a send and a workflow node store `{}`, as Python stores no keyword arguments.
- A parameter whose value is stored needs a name: `_` is a compile error for it.
- A date-time of type `horsies::StoredDateTime<T>` (`T` is a chrono `DateTime<Tz>` or `NaiveDateTime`) is stored in Python's text: `2026-08-12T09:10:12.500000Z`. chrono's own serde form writes a whole millisecond with 3 digits (`…12.500Z`). Both clients read either text to the same value; use `StoredDateTime` in a parameter, the output type or a field of either when the stored bytes must equal Python's.
- A first parameter of type `horsies::TaskRuntime` is not input: the body gets the runtime of the app. See [Task Runtime](#task-runtime).
- A task with no codes of its own uses `TaskError<std::convert::Infallible>`. `TaskError` without its code type is a compile error.

See [Task Errors](/horsies/rust/tasks/errors/) for `TaskError<E>` and the `CustomErrorCode` derive.

## Attributes

The task name comes first. The attributes after it:

| Attribute | Value | Rule |
|-----------|-------|------|
| `queue` | `"name"` | A queue of `Custom` queue mode; see [App Config](/horsies/rust/configuration/app-config/#queues) |
| `execution` | `isolated` (default), `shared_async`, `shared_thread` | An `async fn` takes `isolated` or `shared_async`; a `fn` takes `isolated` or `shared_thread`. Another class is a compile error |
| `timeout_ms` | `1000..=4294967295` | The deadline of a run. A shared class without `timeout_ms` is a compile error |
| `cancel_grace_ms` | `100..=10000` | `shared_async`: the time from the cancel signal (the deadline, or an operator cancel) to the drop of the body; default 1,000. `isolated`: the time from an operator cancel to the kill of the child, when the body has not ended; no default, so no kill. On `shared_thread` it is a compile error |
| `retry_policy` | a `RetryPolicy` expression | Goes with `retry_for` |
| `retry_for` | `["CODE", ...]` | Goes with `retry_policy`, with at least one code |
| `workflow_context` | no value | The input gets the workflow context |

`good_until` is not a task attribute: set a deadline per send with `TaskSendOptions::good_until`. An unknown attribute is a compile error that lists the known ones. The class and the options are stored with every send. The behaviour of each class is on [Choosing an Execution Mode](/horsies/workers/execution-modes/).

```rust
use horsies::{blocking_task, task, TaskError};

#[task("import_feed", execution = shared_async, timeout_ms = 30_000, cancel_grace_ms = 2_000)]
async fn import_feed(feed_url: String) -> Result<u32, TaskError<std::convert::Infallible>> {
    Ok(feed_url.len() as u32)
}

#[blocking_task("resize", execution = shared_thread, timeout_ms = 60_000)]
fn resize(path: String) -> Result<String, TaskError<std::convert::Infallible>> {
    Ok(path)
}
```

### Retry Policy

`retry_policy` and `retry_for` go together: one without the other is a compile error. A `retry_for` entry is a built-in code (`TASK_TIMEOUT`, `WORKER_CRASHED`, ...), stored as that code, or a code of the task's code type `E`; any other entry is a compile error.

```rust
use horsies::{task, CustomErrorCode, RetryPolicy, TaskError};

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum FetchCode {
    RateLimited,
    UpstreamDown,
}

#[task(
    "fetch_prices",
    retry_policy = RetryPolicy::fixed(vec![30, 60, 120], true).expect("a valid policy"),
    retry_for = ["RATE_LIMITED", "TASK_TIMEOUT", "WORKER_CRASHED"],
    timeout_ms = 5_000
)]
async fn fetch_prices(symbol: String) -> Result<String, TaskError<FetchCode>> {
    Err(TaskError::custom(FetchCode::RateLimited, format!("rate limited for {symbol}")))
}
```

| Constructor | Policy |
|-------------|--------|
| `RetryPolicy::fixed(intervals_seconds, jitter)` | One delay per retry, in seconds; the number of intervals is the number of retries |
| `RetryPolicy::exponential(base_seconds, max_retries, jitter)` | Delay `base_seconds * 2^(attempt - 1)` |
| `.with_max_delay_seconds(seconds)` | A cap on the delay, applied after backoff and jitter |

Each interval is 1 to 86,400 s and a policy has 1 to 20 retries; a value outside is `RetryPolicyError`. When a retry starts and how the delay is computed is on [Retry Policy](/horsies/tasks/retry-policy/).

## The Generated Module

The attribute generates a module with the name of the function. The module holds no state: each send names its app, and a workflow builder resolves a node against its own app.

| Item | Does |
|------|------|
| `validate_order::register(&mut app)` | Registers the task and returns its `TaskFunction<validate_order::Input, String, OrderCode>`. The task declares the codes of `OrderCode` (`CustomErrorCode::CODES`); a code that breaks a rule is refused (`HRS-212`, `HRS-112`, `HRS-113`; see [Task Errors](/horsies/rust/tasks/errors/#the-code-type)) |
| `validate_order::send(&app, order)` | Sends the task now through `app` |
| `validate_order::with_options(options).send(&app, order)` | Sends the task through `app` with `TaskSendOptions`: a delay (`TaskSendOptions::delay`), a deadline, an idempotency key, a retention |
| `validate_order::node()` | A workflow node of the task (`TaskNode<String, validate_order::Input>`). The builder of an app resolves it when the node is added |
| `validate_order::function(&runtime)` | The `TaskFunction` from a `TaskRuntime`; `Err(TaskFunctionNotFound)` when the app did not register the task with these types |
| `validate_order::fields::order()` | The field token of the parameter `order` (`InputField<validate_order::Input, Order>`), for a workflow node's `set` and `arg_from` |

A send through an app that does not register the task returns `TaskSendError::ValidationFailed` with the cause `BrokerRefusalCause::Check`. The message names the task. Nothing is written. See [Sending Tasks](/horsies/rust/tasks/sending-tasks/).

## Task Runtime

A body whose first parameter is `horsies::TaskRuntime` reads the app's state, the cancel, the fencing token and the attempt facts of its run, and starts work that belongs to its run.

```rust
use horsies::{task, CustomErrorCode, TaskError, TaskRuntime};

struct HttpClient;

impl HttpClient {
    async fn sync(&self, feed_url: &str) -> u32 {
        feed_url.len() as u32
    }
}

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum FeedCode {
    NotConfigured,
    Interrupted,
}

#[task("sync_feed", execution = shared_async, timeout_ms = 30_000)]
async fn sync_feed(rt: TaskRuntime, feed_url: String) -> Result<u32, TaskError<FeedCode>> {
    let client = rt
        .state::<HttpClient>()
        .map_err(|missing| TaskError::custom(FeedCode::NotConfigured, missing.to_string()))?;
    tokio::select! {
        count = client.sync(&feed_url) => Ok(count),
        () = rt.cancelled() => Err(TaskError::custom(FeedCode::Interrupted, "cancelled")),
    }
}
```

| Method | Returns |
|--------|---------|
| `state::<T>()` | `Result<Arc<T>, StateNotProvided>`: the value that `Horsies::provide` gave. The first read freezes the app's state |
| `cancelled()` | A future that completes when the run is cancelled: an operator cancel, or the deadline of a `shared_async` run |
| `is_cancelled()` | Whether the run is cancelled. It works in the async body, in work started with `spawn`, and in a blocking body; `false` outside a run |
| `cancel_state()` | `RunCancelState`: `NotRequested`, `Requested { cause }` with a `CancelCause` (`Operator` or `Deadline`), or `NotInARun` |
| `spawn(name, future)` | `Result<JoinHandle<_>, RunSpawnRefused>`: starts `future` as work of the run |
| `fencing_token()` | `Result<FencingToken, FencingTokenRefused>`: the task id and the value of the run. See [Fencing Token](#fencing-token) |
| `task_id()` | `Result<Uuid, TaskAttemptRefused>`: the id of the task, the same on every attempt: a stable idempotency key for a side effect |
| `attempt()` | `Result<AttemptNumber, TaskAttemptRefused>`: 1 is the first run, 2 the first retry (`.get()`) |
| `max_retries()` | `Result<MaxRetries, TaskAttemptRefused>`: the retry budget of the task, 0 to 20 (`.get()`) |
| `retries_left()` | `Result<RetriesLeft, TaskAttemptRefused>`: `max_retries - (attempt - 1)`, and 0 past the budget. A non-retryable error code or `good_until` can end the retries before it reaches 0, so the last attempt is not known in advance |
| `task_function::<A, T, E>(task_name)` | `Result<TaskFunction<A, T, E>, TaskFunctionNotFound>` (`NotRegistered`, `OtherTypes`) |
| `workflow_builder(name)` | A `WorkflowSpecBuilder` of the app of the runtime: its task nodes resolve against that app. See [Workflow API](/horsies/rust/concepts/workflows/workflow-api/) |

The cancel signal fires in two cases. The first cause stays: a later cause does not change it.

- **An operator cancel** of the task (`cancel_task` with `CancelPolicy::IncludeRunning`, or the web UI), in every execution class: the cause is `CancelCause::Operator`. The task is `CANCELLED` at once, and what the body returns after the cancel is dropped. A cancel that comes before the body starts: the body does not run.
- **The deadline** of a `shared_async` or a `shared_thread` run, at its `timeout_ms`: the cause is `CancelCause::Deadline`. The run then ends as `TASK_TIMEOUT` whatever the body returns. At its deadline an `isolated` run is killed with no signal.

When the body does not stop:

- `shared_async`: the body and the work it started with `spawn` have `cancel_grace_ms` to end before the child drops them.
- `isolated`: the body runs until it returns, or until its `timeout_ms` kill. With `cancel_grace_ms` on the task, the worker kills the child `cancel_grace_ms` after an operator cancel.
- `shared_thread`: the body runs until it returns.

A blocking body reads `is_cancelled()` or `cancel_state()` between the steps of its work; `cancelled()` is a future, so it has no use there. Outside a run (a direct call in a test) `cancelled()` never completes, `is_cancelled()` is `false` and `cancel_state()` is `NotInARun`. The rules of each task status are on [Action Semantics](/horsies/monitoring/action-semantics/#cancelling-a-running-task), and the timeout behaviour is on [Shared Async Tasks](/horsies/workers/shared-async-tasks/#the-timeout).

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

Work started with `spawn` belongs to the run: the child waits for it when the body returns, cancels it after the grace, and reports work that does not stop. Work started with plain `tokio::spawn` is not tracked. `spawn` refuses with `RunSpawnRefused` outside the async body of a run in a worker child, after the run ended, and past `MAX_RUN_WORK` pieces of live work; nothing starts then.

### Fencing Token

A stalled attempt can run on after the worker retries its task. horsies refuses the late writes of that attempt to its own tables. It cannot refuse its writes to your systems. Write the fencing token of the run with each such write, and make the store refuse a write whose token is lower than the token that it holds for the task id. The pattern and its limits are on [Fencing Token](/horsies/concepts/fencing-token/).

`rt.fencing_token()` returns a `FencingToken`:

| Method | Returns |
|--------|---------|
| `task_id()` | `Uuid`: the id of the task. The token orders the attempts of this task only |
| `value()` | `AttemptNumber` (`horsies::events::AttemptNumber`); `value().get()` gives a `u32` |

- Two reads in one attempt give the same token.
- An attempt that runs after another attempt of the same task gives a greater value.
- The value depends on no clock.
- Today the value is the attempt number, 1 to 21. Compare the values of one task id; do not depend on the number itself.

<!-- rust-check: alone -->
```rust
use horsies::{task, CustomErrorCode, TaskError, TaskRuntime};
use sqlx::PgPool;

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum PaymentCode {
    NotConfigured,
    NotInARun,
    LaterAttempt,
    StoreFailed,
}

// CREATE TABLE payments (task_id uuid PRIMARY KEY, fence bigint NOT NULL,
//                        order_id text NOT NULL, cents bigint NOT NULL);
#[task("record_payment")]
async fn record_payment(
    rt: TaskRuntime,
    order_id: String,
    cents: i64,
) -> Result<String, TaskError<PaymentCode>> {
    let pool = rt
        .state::<PgPool>()
        .map_err(|missing| TaskError::custom(PaymentCode::NotConfigured, missing.to_string()))?;
    let token = rt
        .fencing_token()
        .map_err(|refused| TaskError::custom(PaymentCode::NotInARun, refused.to_string()))?;
    // One conditional write: the row changes only when no later attempt of
    // this task wrote it.
    let written = sqlx::query(
        "INSERT INTO payments (task_id, fence, order_id, cents) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (task_id) DO UPDATE \
         SET fence = EXCLUDED.fence, order_id = EXCLUDED.order_id, cents = EXCLUDED.cents \
         WHERE payments.fence <= EXCLUDED.fence",
    )
    .bind(token.task_id())
    .bind(i64::from(token.value().get()))
    .bind(&order_id)
    .bind(cents)
    .execute(&*pool)
    .await
    .map_err(|error| TaskError::custom(PaymentCode::StoreFailed, error.to_string()))?;
    match written.rows_affected() {
        0 => Err(TaskError::custom(
            PaymentCode::LaterAttempt,
            "a later attempt of this task holds the fence",
        )),
        _ => Ok(order_id),
    }
}
```

`fencing_token()` returns `Err(FencingTokenRefused::NotInARun)` outside a run:

- a direct call of the function in a test;
- work started with plain `tokio::spawn`, or with `spawn_blocking` from an async body;
- a thread that a blocking body starts.

The body of a run, work that an async body started with `rt.spawn`, and the thread of a blocking body read the token of the run. In other places, read the token in the body and pass it on.

Limits:

- A rerun, and a workflow node that a resume starts again, get a new task id. Their value starts again at 1.
- A stale attempt with no later attempt (the task failed, was cancelled or expired) is not refused.
- A stale attempt that writes before the later attempt took the fence is not refused. Take the fence at the start of the body.

## Register by Hand

`async_task_fn!` and `blocking_task_fn!` build a `RegisteredTask` from a function that takes one input value and returns `Result<T, TaskError<E>>`. Attach the options with `RegisteredTask::with_task_options`, then register it with `Horsies::register`.

```rust
use horsies::{async_task_fn, Horsies, HorsiesError, TaskError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct AddInput {
    a: i64,
    b: i64,
}

async fn add(input: AddInput) -> Result<i64, TaskError<std::convert::Infallible>> {
    Ok(input.a + input.b)
}

fn register_add(app: &mut Horsies) -> Result<(), HorsiesError> {
    app.register("add", async_task_fn!(add, AddInput))
}
```

The registered task decodes the input with the input type, runs the function, and stores what it returned, as the attribute does. It records the module of the call site, which a panic's exception record names.
