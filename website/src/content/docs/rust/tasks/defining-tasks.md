---
title: Defining Tasks
summary: Define a task with #[horsies::task] or #[horsies::blocking_task], choose its execution class and retry codes, and read the app's state with TaskRuntime.
related: [errors, sending-tasks, ../configuration/app-config, ../../workers/execution-modes, ../../tasks/retry-policy]
tags: [rust, task, macro, execution, retry, TaskRuntime]
---

# Defining Tasks

A task is an `async fn` with `#[horsies::task]` or a `fn` with `#[horsies::blocking_task]`. The function returns `Result<T, TaskError<E>>`: `T` is the output type and `E` is the code type of the task's own errors.

## Define a Task

```rust
use horsies::{task, TaskError, UserErrorCode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    total_cents: u64,
}

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum OrderCode {
    LimitExceeded, // LIMIT_EXCEEDED
}

#[task("validate_order")]
async fn validate_order(order: Order) -> Result<String, TaskError<OrderCode>> {
    if order.total_cents > 1_000_000 {
        return Err(TaskError::user(OrderCode::LimitExceeded, "order over the limit"));
    }
    Ok(order.order_id)
}
```

- The parameters form the input, stored as Python stores keyword arguments: a generated `Input` struct with one field per parameter, keyed by the parameter's name, a single parameter of any type included. `validate_order` stores `{"order": {"order_id": "...", "total_cents": ...}}`; `fn double(x: i64)` stores `{"x": 2}`. No parameter, or one parameter written `()`, is the input `()`: a send and a workflow node store `{}`, as Python stores no keyword arguments.
- A parameter whose value is stored needs a name: `_` is a compile error for it.
- A date-time of type `horsies::StoredDateTime<T>` (`T` is a chrono `DateTime<Tz>` or `NaiveDateTime`) is stored in Python's text: `2026-08-12T09:10:12.500000Z`. chrono's own serde form writes a whole millisecond with 3 digits (`…12.500Z`). Both clients read either text to the same value; use `StoredDateTime` in a parameter, the output type or a field of either when the stored bytes must equal Python's.
- A first parameter of type `horsies::TaskRuntime` is not input: the body gets the runtime of the app. See [Task Runtime](#task-runtime).
- A task with no codes of its own uses `TaskError<std::convert::Infallible>`. `TaskError` without its code type is a compile error.

See [Task Errors](/horsies/rust/tasks/errors/) for `TaskError<E>` and the `UserErrorCode` derive.

## Attributes

The task name comes first. The attributes after it:

| Attribute | Value | Rule |
|-----------|-------|------|
| `queue` | `"name"` | A queue of `Custom` queue mode; see [App Config](/horsies/rust/configuration/app-config/#queues) |
| `execution` | `isolated` (default), `shared_async`, `shared_thread` | An `async fn` takes `isolated` or `shared_async`; a `fn` takes `isolated` or `shared_thread`. Another class is a compile error |
| `timeout_ms` | `1000..=4294967295` | The deadline of a run. A shared class without `timeout_ms` is a compile error |
| `cancel_grace_ms` | `100..=10000`, default 1,000 | `shared_async` only: the time from the cancel signal at the deadline to the drop of the body. On another class it is a compile error |
| `retry_policy` | a `RetryPolicy` expression | Goes with `auto_retry_for` |
| `auto_retry_for` | `["CODE", ...]` | Goes with `retry_policy`, with at least one code |
| `workflow_ctx` | no value | The input gets the workflow context |

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

`retry_policy` and `auto_retry_for` go together: one without the other is a compile error. An `auto_retry_for` entry is a built-in code (`TASK_TIMEOUT`, `WORKER_CRASHED`, ...), stored as that code, or a code of the task's code type `E`; any other entry is a compile error.

```rust
use horsies::{task, RetryPolicy, TaskError, UserErrorCode};

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum FetchCode {
    RateLimited,
    UpstreamDown,
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
|-------------|--------|
| `RetryPolicy::fixed(intervals, jitter)` | One delay per retry, in seconds; the number of intervals is the number of retries |
| `RetryPolicy::exponential(base_seconds, max_retries, jitter)` | Delay `base_seconds * 2^(attempt - 1)` |
| `.with_max_delay_seconds(seconds)` | A cap on the delay, applied after backoff and jitter |

Each interval is 1 to 86,400 s and a policy has 1 to 20 retries; a value outside is `RetryPolicyError`. When a retry starts and how the delay is computed is on [Retry Policy](/horsies/tasks/retry-policy/).

## The Generated Module

The attribute generates a module with the name of the function:

| Item | Does |
|------|------|
| `validate_order::register(&mut app)` | Registers the task and returns its `TaskFunction<validate_order::Input, String, OrderCode>`. The task declares the codes of `OrderCode` (`UserErrorCode::CODES`); a code that breaks a rule is refused (`HRS-212`, `HRS-112`, `HRS-113`; see [Task Errors](/horsies/rust/tasks/errors/#the-code-type)) |
| `validate_order::send(input)` | Sends the task through the function of the first `register` in this process |
| `validate_order::schedule(delay, input)` | Sends the task to start after `delay: WholeSeconds` |
| `validate_order::with_options(options)` | The two sends with `TaskSendOptions` |
| `validate_order::handle(&runtime)` | The `TaskFunction` from a `TaskRuntime`; `Err(TaskFunctionNotFound)` when the app did not register the task with these types |
| `validate_order::params::order()` | The field token of the parameter `order` (`InputField<validate_order::Input, Order>`), for a workflow node's `set` and `arg_from` |

A send before `register` in the process returns `TaskSendError::ValidationFailed`, which names the task. See [Sending Tasks](/horsies/rust/tasks/sending-tasks/).

## Task Runtime

A body whose first parameter is `horsies::TaskRuntime` reads the app's state, the cancel signal of its run, and starts work that belongs to its run.

```rust
use horsies::{task, TaskError, TaskRuntime, UserErrorCode};

struct HttpClient;

impl HttpClient {
    async fn sync(&self, feed_url: &str) -> u32 {
        feed_url.len() as u32
    }
}

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum FeedCode {
    NotConfigured,
    Interrupted,
}

#[task("sync_feed", execution = shared_async, timeout_ms = 30_000)]
async fn sync_feed(rt: TaskRuntime, feed_url: String) -> Result<u32, TaskError<FeedCode>> {
    let client = rt
        .state::<HttpClient>()
        .map_err(|missing| TaskError::user(FeedCode::NotConfigured, missing.to_string()))?;
    tokio::select! {
        count = client.sync(&feed_url) => Ok(count),
        () = rt.cancelled() => Err(TaskError::user(FeedCode::Interrupted, "cancelled at the deadline")),
    }
}
```

| Method | Returns |
|--------|---------|
| `state::<T>()` | `Result<Arc<T>, StateNotProvided>`: the value that `Horsies::provide` gave. The first read freezes the app's state |
| `cancelled()` | A future that completes when the run is cancelled at its deadline |
| `is_cancelled()` | Whether the run is cancelled; `false` outside the async body of a run |
| `spawn(name, future)` | `Result<JoinHandle<_>, RunSpawnRefused>`: starts `future` as work of the run |
| `task_handle::<A, T, E>(task_name)` | `Result<TaskFunction<A, T, E>, TaskFunctionNotFound>` (`NotRegistered`, `OtherTypes`) |

The cancel signal fires only for a `SHARED_ASYNC` run, at its `timeout_ms`. The run then ends as `TASK_TIMEOUT` whatever the body returns. The body and the work it started with `spawn` have `cancel_grace_ms` to end before the child drops them. In an `ISOLATED` run the worker kills the child at the deadline, so the signal never fires. Outside the async body of a run (a blocking body, a direct call in a test) there is no deadline, and `cancelled()` never completes. The timeout behaviour is on [Shared Async Tasks](/horsies/workers/shared-async-tasks/#the-timeout).

Work started with `spawn` belongs to the run: the child waits for it when the body returns, cancels it after the grace, and reports work that does not stop. Work started with plain `tokio::spawn` is not tracked. `spawn` refuses with `RunSpawnRefused` outside the async body of a run in a worker child, after the run ended, and past `MAX_RUN_WORK` pieces of live work; nothing starts then.

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
