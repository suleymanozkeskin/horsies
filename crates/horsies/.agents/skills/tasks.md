---
name: horsies-rust-tasks
description: Task authoring, registration, and producing guidance for horsies-rust, including #[horsies::task] proc macro, unified `horsies::Horsies`, `TaskFunction`, send/schedule/retry APIs, retry policy, and serialization rules. Use when implementing, debugging, or reviewing task-related code.
---

# horsies-rust — Tasks

Detailed reference for defining, registering, sending, and handling tasks.

## Define a Task

Annotate an async function with `#[horsies::task]`:

```rust
use horsies::{task, TaskError, TaskResult};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct AddNumbersInput {
    a: i32,
    b: i32,
}

#[task("add_numbers", queue = "urgent")]
async fn add_numbers(input: AddNumbersInput) -> Result<i32, TaskError> {
    Ok(input.a + input.b)
}
```

Requirements:
- Single argument of type `A: Serialize + DeserializeOwned` (use a struct for multiple fields, `()` for no args).
- Returns `Result<T, TaskError>` where `T: Serialize + DeserializeOwned`.
- Must be `async fn` for `#[task]`, or `fn` for `#[blocking_task]`.

### `#[horsies::task]` Attribute

```rust
#[horsies::task(
    "task_name",                                          // required
    queue = "queue_name",                                 // optional
    retry_policy = RetryPolicy::fixed(vec![60, 300], true).unwrap(),  // optional
    good_until = Utc::now() + Duration::hours(1),         // optional
    auto_retry_for = ["RATE_LIMITED", "TIMEOUT"],          // optional
)]
async fn my_task(args: MyArgs) -> Result<MyOutput, TaskError> { ... }
```

| Attribute | Type | Description |
|---|---|---|
| First positional | string literal | Required: unique task name |
| `queue` | string literal | Target queue (validated at registration) |
| `retry_policy` | Rust expression | Retry timing/backoff configuration |
| `good_until` | Rust expression | Task expiry deadline |
| `auto_retry_for` | string array | Error codes that trigger automatic retries |

Expression-valued attributes are parsed as real Rust expressions and
evaluated at registration time, not compile time.

### `#[horsies::blocking_task]`

For CPU-bound work that runs on `spawn_blocking`:

```rust
#[horsies::blocking_task("cpu_heavy", queue = "background")]
fn cpu_heavy(input: HeavyInput) -> Result<HeavyOutput, TaskError> { ... }
```

### Compile-time validation

The macro rejects invalid signatures at compile time:
- Multiple arguments → "use a struct for multiple fields"
- Methods with `self` → "must be free functions"
- Generic or lifetime parameters → "not supported"
- Wrong return type → "must return Result<T, TaskError>"
- Missing return type → error
- Sync fn with `#[task]` → "use #[blocking_task]"
- Async fn with `#[blocking_task]` → "use #[task]"

## Register Tasks

### Primary path: `my_task::register(&mut app)`

```rust
use horsies::Horsies;

let mut app = Horsies::new(config)?;

let add_numbers_task = add_numbers::register(&mut app)?;
let fetch_data_task = fetch_data::register(&mut app)?;
let process_data_task = process_data::register(&mut app)?;
```

Each `#[task]` generates a companion module with a `register()` function.
That function calls the builder API internally and returns a `TaskFunction<A, T>`.

### Registrar module pattern

```rust
// tasks.rs
pub fn register(app: &mut Horsies) -> Result<Tasks, HorsiesError> {
    Ok(Tasks {
        fetch_data: fetch_data::register(app)?,
        process_data: process_data::register(app)?,
        save_result: save_result::register(app)?,
    })
}
```

### Manual builder path (advanced)

For cases where the proc macro is not used:

```rust
let task = app
    .task::<MyArgs, MyOutput>("name", async_task_fn!(my_fn, MyArgs))?
    .queue("processing")
    .task_options(opts)
    .register()?;
```

### Low-level core path (internal)

```rust
let mut core = horsies::core::Horsies::new(config)?;
core.register("my_task", async_task_fn!(my_task, MyArgs))?;
let (config, registry, wf_registry) = core.into_parts();
```

## `TaskFunction<A, T>`

The Rust equivalent of Python's `TaskFunction[P, T]`. Returned by
`my_task::register(&mut app)`. Holds task identity, lazy broker,
queue/priority defaults, task options.

Prefer explicit branching around `.send()` / `.schedule()` in docs and app
code when the failure path matters operationally:

```rust
match add_numbers_task.send(AddNumbersInput { a: 5, b: 3 }).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(30))).await;
        match result {
            TaskResult::Ok(value) => tracing::info!(value, "add_numbers completed"),
            TaskResult::Err(err) => tracing::warn!(error = ?err, "add_numbers failed or timed out"),
        }
    }
    Err(err) if err.retryable => {
        let handle = add_numbers_task.retry_send(&err).await?;
        let result = handle.get(Some(Duration::from_secs(30))).await;
        match result {
            TaskResult::Ok(value) => tracing::info!(value, "add_numbers completed after retry"),
            TaskResult::Err(err) => tracing::warn!(error = ?err, "add_numbers failed or timed out"),
        }
    }
    Err(err) => {
        tracing::warn!(error = %err.message, "failed to send add_numbers");
    }
}
```

### Methods

```rust
// Sending
async fn send(&self, args: A) -> TaskSendResult<TaskHandle<T>>
async fn schedule(&self, delay: Duration, args: A) -> TaskSendResult<TaskHandle<T>>

// Retry (with cross-method guards)
async fn retry_send(&self, err: &TaskSendError) -> TaskSendResult<TaskHandle<T>>
async fn retry_schedule(&self, err: &TaskSendError) -> TaskSendResult<TaskHandle<T>>

// Workflow integration
fn node(&self) -> TaskNode<T, A>
fn node_with(&self, args: A) -> Result<TaskNode<T, A>, HorsiesError>

// Accessors
fn task_name(&self) -> &str
fn queue_name(&self) -> &str
fn priority(&self) -> u32
fn task_options(&self) -> Option<&TaskOptions>
```

### `send()` behavior

1. Checks send suppression → `SendSuppressed` if active.
2. Serializes `A` → args/kwargs JSON (struct→kwargs, null→nothing, scalar→args).
3. Builds payload once (task_id, sent_at, SHA).
4. If `resend_on_transient_err` enabled: retries up to 3 times (4 total) with exponential backoff (200ms, 400ms, 800ms, cap 2000ms). Same payload identity across all attempts.
5. Returns `TaskHandle<T>` with `.get()` ready to call.

### `schedule()` behavior

Same as `send()` but with `enqueued_at = now() + delay`. The scheduled time is fixed at the first attempt, not recomputed on retry.

### `retry_send()` / `retry_schedule()` guards

- `retry_send` **rejects** errors with `enqueue_delay_seconds` → "use retry_schedule instead"
- `retry_schedule` **requires** `enqueue_delay_seconds` → "use retry_send instead"
- Both reject non-`ENQUEUE_FAILED` errors and cross-task replays.

### `node()` behavior

Returns `TaskNode<T, A>` pre-configured with task name, queue, priority, good_until, and serialized task_options. Bridges registered tasks to workflow construction.

`node_with(args)` additionally serializes `args` onto the node. Use it for root tasks or parameterized dynamic workflow nodes that need explicit input.

The generated task module helpers are fallible wrappers around the registered handle:

```rust
fn add_numbers::node() -> Result<TaskNode<i32, AddNumbersInput>, HorsiesError>
fn add_numbers::node_with(args: AddNumbersInput) -> Result<TaskNode<i32, AddNumbersInput>, HorsiesError>
```

They fail if `add_numbers::register(&mut app)?` has not populated the generated module handle.

## `TaskHandle<T>`

```rust
async fn get(&self, timeout: Option<Duration>) -> TaskResult<T>
```

Task handles do **not** return `HandleResult`. Retrieval outcomes are represented as `TaskResult<T>`:

- `TaskResult::Ok(value)` — task completed successfully.
- `TaskResult::Err(TaskError)` — task failed, timed out while waiting, was not found, or hit a broker/result retrieval error.

This differs from `WorkflowHandle<T>` only partly:

- `WorkflowHandle::get()` and `WorkflowHandle::result_for()` also fold retrieval and infrastructure failures into `TaskResult::Err(TaskError)`.
- `WorkflowHandle::status()`, `results()`, `tasks()`, `cancel()`, `pause()`, and `resume()` use `HandleResult<...>`.

## Serialization Rules

### Args encoding (at `send()` time)

| `serde_json::to_value(&args)` | Sent as |
|---|---|
| Object (`{...}`) | `kwargs_json` |
| Null (unit `()`) | No args, no kwargs |
| Anything else (scalar, array) | `args_json` |

### Output validation (at execution time)

The `async_task_fn!` macro validates the return value round-trips through the declared `T`. If `serde_json::from_slice::<T>(&serialized_bytes)` fails → `RETURN_TYPE_MISMATCH`.

## `TaskResult<T>`

```rust
pub enum TaskResult<T> {
    Ok(T),
    Err(TaskError),
}
```

| Method | Description |
|---|---|
| `is_ok()` / `is_err()` | Check variant |
| `unwrap()` | Panics on Err |
| `is_transient()` | True if error is retrieval-class or broker error (safe to retry) |
| `is_terminal()` | True if `!is_transient()` |

### Caching in `TaskHandle`

- Terminal results are cached after first retrieval.
- Transient errors (`WAIT_TIMEOUT`, broker errors) are never cached — caller can retry.

## `TaskError`

```rust
pub struct TaskError {
    pub error_code: Option<TaskErrorCode>,
    pub message: Option<String>,
    pub cause: Option<serde_json::Value>,  // #[serde(rename = "exception")]
    pub data: Option<serde_json::Value>,
}
```

### Construction helpers

```rust
TaskError::builtin(OperationalErrorCode::BrokerError, "connection refused")
TaskError::user("MY_CUSTOM_CODE", "something went wrong")
```

## `TaskSendResult` / `TaskSendError`

```rust
pub type TaskSendResult<T> = Result<T, TaskSendError>;

pub struct TaskSendError {
    pub code: TaskSendErrorCode,
    pub message: String,
    pub retryable: bool,
    pub task_id: Option<String>,
    pub payload: Option<TaskSendPayload>,
}
```

### `TaskSendErrorCode`

| Code | Retryable | When |
|---|---|---|
| `SendSuppressed` | No | `send()` during check-time builder execution |
| `ValidationFailed` | No | Serialization failure, bad queue, cross-method retry |
| `EnqueueFailed` | Yes | Broker/DB failure |
| `PayloadMismatch` | No | Retry payload SHA mismatch |

## `RetryPolicy`

```rust
RetryPolicy::fixed(vec![60, 300, 900], true)?   // 3 retries, jitter on
RetryPolicy::exponential(30, 5, false)?          // 5 retries, base 30s, no jitter
```

Validation: max_retries 1–20, intervals 1–86400s, fixed requires `len == max_retries`, exponential requires exactly 1 interval.

## Error Code Families

### `OperationalErrorCode`

`UnhandledError`, `TaskError`, `WorkerCrashed`, `BrokerError`, `WorkerResolutionError`, `WorkerSerializationError`, `ResultDeserializationError`, `WorkflowEnqueueFailed`, `SubworkflowLoadFailed`

### `ContractCode`

`ReturnTypeMismatch`, `ArgumentTypeMismatch`, `WorkflowCtxMissingId`

### `RetrievalCode`

`WaitTimeout`, `TaskNotFound`, `WorkflowNotFound`, `ResultNotAvailable`, `ResultNotReady`

### `OutcomeCode`

`TaskCancelled`, `TaskExpired`, `WorkflowPaused`, `WorkflowFailed`, `WorkflowCancelled`, `UpstreamSkipped`, `SubworkflowFailed`, `WorkflowSuccessCaseNotMet`

## `TaskStatus`

```
PENDING → CLAIMED → RUNNING → COMPLETED | FAILED | CANCELLED | EXPIRED
```

Terminal: `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`.

## All Key Imports

```rust
use horsies::{
    // App
    Horsies, AppConfig, PostgresConfig,
    // Proc macro
    task, blocking_task,
    // Declarative macros (advanced/internal)
    async_task_fn, blocking_task_fn,
    // Task types
    TaskFunction, TaskResult, TaskError, TaskErrorCode, TaskHandle, TaskInfo, TaskOptions,
    TaskSendError, TaskSendErrorCode, TaskSendPayload, TaskSendResult, RegisteredTask,
    // Retry
    RetryPolicy, ResolvedEnqueue,
    // Error code families
    OperationalErrorCode, ContractCode, RetrievalCode, OutcomeCode,
    // Status
    TaskStatus, TaskAttemptOutcome, TaskAttemptInfo,
    // Broker result (for handle.info())
    BrokerResult, BrokerOperationError, BrokerErrorCode,
};
```
