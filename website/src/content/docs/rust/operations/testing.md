---
title: Testing
summary: Test task bodies by calling them, give them a TaskRuntime, the facts of a run (TestRun), a dependency's outcome or a workflow context, check the app without a database, and suppress sends.
related: [../tasks/defining-tasks, ../concepts/workflows/typed-node-builder, ../configuration/app-config, ../cli]
tags: [rust, testing, unit test, TaskRuntime, TestRun, WorkflowContext, suppress_sends, check]
---

# Testing

`#[horsies::task]` keeps the function as written, next to its generated module. A test calls the function with its parameters and reads the `Result<T, TaskError<E>>` it returns. No worker and no database take part.

## Why and When

- Test the logic of a task body with a direct call.
- Test the input that a send or a workflow node stores with the generated `Input` type.
- Test the registrations, the workflows and the schedules of the app with `app.check()`, which needs no database.
- Test the stored outcome, the retries and the worker with a database: start a worker of the app against a test database.

## Call a Task Directly

```rust
use horsies::{task, CustomErrorCode, TaskError, TaskErrorCode};

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum OrderCode {
    LimitExceeded,
}

#[task("validate_order")]
async fn validate_order(total_cents: u64) -> Result<u64, TaskError<OrderCode>> {
    match total_cents > 1_000_000 {
        true => Err(TaskError::custom(OrderCode::LimitExceeded, "order over the limit")),
        false => Ok(total_cents),
    }
}

#[tokio::test]
async fn an_order_over_the_limit_is_refused() {
    let refused = validate_order(2_000_000).await.unwrap_err();
    assert_eq!(refused.code(), &TaskErrorCode::Custom(OrderCode::LimitExceeded));
}
```

A `#[horsies::blocking_task]` function is a plain `fn`: call it with no `.await`.

## Give a Body Its Runtime

A body whose first parameter is `TaskRuntime` takes the runtime of an app built in the test: `app.task_runtime()`.

```rust
use horsies::{task, AppConfig, CustomErrorCode, Horsies, TaskError, TaskRuntime};

struct Greeter {
    greeting: String,
}

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum WelcomeCode {
    NotConfigured,
}

#[task("welcome")]
async fn welcome(rt: TaskRuntime, user: String) -> Result<String, TaskError<WelcomeCode>> {
    let greeter = rt
        .state::<Greeter>()
        .map_err(|missing| TaskError::custom(WelcomeCode::NotConfigured, missing.to_string()))?;
    Ok(format!("{}, {user}", greeter.greeting))
}

#[tokio::test]
async fn welcome_reads_the_greeter() {
    let mut app = Horsies::new(AppConfig::from_database_url("postgresql://localhost/horsies_test"))
        .unwrap();
    app.provide(Greeter { greeting: "welcome".to_owned() }).unwrap();
    let message = welcome(app.task_runtime(), "ada".to_owned()).await.unwrap();
    assert_eq!(message, "welcome, ada");
}
```

- `Horsies::new` does no I/O, so the test needs no database.
- In a direct call there is no run: `cancelled()` never completes, `is_cancelled()` is `false`, `cancel_state()` is `RunCancelState::NotInARun`, and `spawn` refuses with `RunSpawnRefused`.

## Give a Body the Facts of a Run

In a direct call each attempt fact is refused with `TaskAttemptRefused::NotInARun`. `TestRun` installs the facts of a run for the duration of a future or a closure. The reads of `TaskRuntime` give them there, as in a worker child.

```rust
use horsies::events::AttemptNumber;
use horsies::{
    task, AppConfig, CustomErrorCode, Horsies, MaxRetries, TaskError, TaskRuntime, TestRun,
};

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum ChargeCode {
    NoRun,
}

#[task("charge_once")]
async fn charge_once(rt: TaskRuntime, order_id: String) -> Result<String, TaskError<ChargeCode>> {
    let task_id = rt
        .task_id()
        .map_err(|refused| TaskError::custom(ChargeCode::NoRun, refused.to_string()))?;
    Ok(format!("{order_id}:{task_id}"))
}

#[tokio::test]
async fn the_charge_is_keyed_by_the_task_id() {
    let app = Horsies::new(AppConfig::from_database_url("postgresql://localhost/horsies_test"))
        .unwrap();
    let task_id = uuid::Uuid::from_u128(7);
    let run = TestRun::new(
        task_id,
        AttemptNumber::try_from(2).unwrap(),
        MaxRetries::try_from(3).unwrap(),
    );
    let key = run
        .run(charge_once(app.task_runtime(), "order-1".to_owned()))
        .await
        .unwrap();
    assert_eq!(key, format!("order-1:{task_id}"));
}
```

| Item | Does |
|------|------|
| `TestRun::new(task_id, attempt, max_retries)` | The facts of attempt `attempt` of the task `task_id`. The cancel state is `NotRequested` |
| `.with_cancel_state(TestRunCancel::Requested { cause })` | A cancel is requested with `cause` (`CancelCause::Operator` or `CancelCause::Deadline`). `cancelled()` completes at once, `is_cancelled()` is `true` and `cancel_state()` is `Requested { cause }` |
| `.run(future).await` | Drives the async body `future` with the facts, and gives its output |
| `.run_blocking(\|\| ...)` | Calls the blocking body on this thread with the facts, and gives its output |

- `task_id()`, `attempt()`, `max_retries()`, `retries_left()` and `fencing_token()` give the facts of the `TestRun`. `retries_left()` is `max_retries - (attempt - 1)`, and 0 past the budget.
- The facts are gone when the future or the closure ends: the reads give `NotInARun` again.
- `rt.spawn` works inside `run`. The work it started is aborted when the future ends.
- Build the future inside the call, as above: the facts are installed only while `run` drives it.
- The attempt number is an `AttemptNumber`, from 1; `horsies::events::AttemptNumber::try_from(2)` gives it.

## Pass a Dependency's Outcome

A parameter that takes a dependency's result with `arg_from` is a `TaskOutcome<S, E>`. Pass the outcome to test each case:

```rust
use std::convert::Infallible;

use horsies::{task, OutcomeCode, TaskError, TaskOutcome};

#[task("send_receipt")]
async fn send_receipt(
    order_id: String,
    charged: TaskOutcome<u64, Infallible>,
) -> Result<String, TaskError<Infallible>> {
    match charged {
        TaskOutcome::Succeeded(cents) => Ok(format!("{order_id}: charged {cents}")),
        TaskOutcome::Failed(error) => Ok(format!("{order_id}: {}", error.code().as_str())),
    }
}

#[tokio::test]
async fn a_skipped_charge_is_reported() {
    let skipped = TaskOutcome::Failed(TaskError::built_in(OutcomeCode::UpstreamSkipped, "skipped"));
    let receipt = send_receipt("order-1".to_owned(), skipped).await.unwrap();
    assert_eq!(receipt, "order-1: UPSTREAM_SKIPPED");
}
```

## Build a Workflow Context

A task with the `workflow_context` attribute takes a `WorkflowContext`. A test builds one from its stored form: `workflow_id`, `task_index`, `task_name`, and `results_by_id`, the stored result of each node by node id. `serde_json::to_value` of a `TaskOutcome` writes a stored result.

```rust
use std::convert::Infallible;

use horsies::{task, NodeKey, TaskError, TaskOutcome, WorkflowContext};
use serde_json::json;

#[task("audit_checkout", workflow_context)]
async fn audit_checkout(workflow_context: WorkflowContext) -> Result<bool, TaskError<Infallible>> {
    let charged = workflow_context.result_for::<u64, Infallible>(&NodeKey::new("charge"));
    Ok(matches!(charged, Ok(TaskOutcome::Succeeded(_))))
}

#[tokio::test]
async fn a_completed_charge_passes_the_audit() {
    let charged = serde_json::to_value(TaskOutcome::<u64, Infallible>::Succeeded(900)).unwrap();
    let context: WorkflowContext = serde_json::from_value(json!({
        "workflow_id": "00000000-0000-0000-0000-000000000007",
        "task_index": 2,
        "task_name": "audit_checkout",
        "results_by_id": { "charge": charged },
    }))
    .unwrap();
    assert!(audit_checkout(context).await.unwrap());
}
```

`summaries_by_id` holds the `SubWorkflowSummary` of each sub-workflow node by node id; both maps default to empty.

## Test the Stored Input

The generated `Input` struct is the stored input of the task: one public field per parameter, keyed by the parameter's name.

```rust
#[test]
fn the_stored_input_names_each_parameter() {
    let input = send_receipt::Input {
        order_id: "order-1".to_owned(),
        charged: TaskOutcome::Succeeded(900),
    };
    let stored = serde_json::to_value(&input).unwrap();
    assert!(stored.get("order_id").is_some());
}
```

A schedule's `input` and a node's `input_json` use this form. A rename of a parameter changes the stored key: see [Defining Tasks](/horsies/rust/tasks/defining-tasks/#define-a-task).

## Check the App

`app.check()` builds and checks each workflow definition, each check case of a workflow builder, the task options of each task registered by hand, and each enabled schedule. It needs no database.

<!-- rust-check: skip partial -- build_app is the app builder of the user's binary -->
```rust
#[test]
fn the_app_passes_its_check() {
    let app = build_app().unwrap();
    if let Err(report) = app.check() {
        panic!("{report}");
    }
}
```

`build_app` is the function that `main` calls to build the app. `ValidationReport` holds the refusals of the first phase that refused; `refusals()` returns them with their `HRS-xxx` codes. See [CLI Reference](/horsies/rust/cli/#check).

## Suppress Sends

A test that runs code which sends can suppress the sends of the process:

| Item | Does |
|------|------|
| `app.suppress_sends(true)` | Suppresses the sends of this process; `suppress_sends(false)` stops it |
| `HORSIES_SUPPRESS_SENDS=1` (`horsies::SUPPRESS_SENDS_ENV`) | Suppresses the sends of every app in the process. The variable is read at each send |
| `app.are_sends_suppressed()` | `true` when either is set |

A suppressed send returns `TaskSendError::Suppressed` and writes nothing. The check runs before any I/O of the send, so the test needs no database. A generated send (`task_name::send(&app, ...)`) is suppressed by the app that it names.

## Things to Avoid

**Do not test the worker's behaviour with direct calls.** A direct call has no deadline, no retry, no cancel and no stored outcome. Start a worker against a test database for those.

**Do not run a test that sends against the database of a deployment.** Build the app of the test with the URL of a test database, or suppress the sends.
