---
title: Testing
summary: Test task bodies by calling them, give them a TaskRuntime, a dependency's outcome or a workflow context, check the app without a database, and suppress sends.
related: [../tasks/defining-tasks, ../concepts/workflows/typed-node-builder, ../configuration/app-config, ../cli]
tags: [rust, testing, unit test, TaskRuntime, WorkflowContext, suppress_sends, check]
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
use horsies::{task, TaskError, TaskErrorCode, UserErrorCode};

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum OrderCode {
    LimitExceeded,
}

#[task("validate_order")]
async fn validate_order(total_cents: u64) -> Result<u64, TaskError<OrderCode>> {
    match total_cents > 1_000_000 {
        true => Err(TaskError::user(OrderCode::LimitExceeded, "order over the limit")),
        false => Ok(total_cents),
    }
}

#[tokio::test]
async fn an_order_over_the_limit_is_refused() {
    let refused = validate_order(2_000_000).await.unwrap_err();
    assert_eq!(refused.code(), &TaskErrorCode::User(OrderCode::LimitExceeded));
}
```

A `#[horsies::blocking_task]` function is a plain `fn`: call it with no `.await`.

## Give a Body Its Runtime

A body whose first parameter is `TaskRuntime` takes the runtime of an app built in the test: `app.task_runtime()`.

```rust
use horsies::{task, AppConfig, Horsies, TaskError, TaskRuntime, UserErrorCode};

struct Greeter {
    greeting: String,
}

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum WelcomeCode {
    NotConfigured,
}

#[task("welcome")]
async fn welcome(rt: TaskRuntime, user: String) -> Result<String, TaskError<WelcomeCode>> {
    let greeter = rt
        .state::<Greeter>()
        .map_err(|missing| TaskError::user(WelcomeCode::NotConfigured, missing.to_string()))?;
    Ok(format!("{}, {user}", greeter.greeting))
}

#[tokio::test]
async fn welcome_reads_the_greeter() {
    let mut app = Horsies::new(AppConfig::for_database_url("postgresql://localhost/horsies_test"))
        .unwrap();
    app.provide(Greeter { greeting: "welcome".to_owned() }).unwrap();
    let message = welcome(app.task_runtime(), "ada".to_owned()).await.unwrap();
    assert_eq!(message, "welcome, ada");
}
```

- `Horsies::new` does no I/O, so the test needs no database.
- In a direct call there is no run: `cancelled()` never completes, `is_cancelled()` is `false`, `cancel_state()` is `RunCancelState::NotInARun`, and `spawn` refuses with `RunSpawnRefused`.

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
        TaskOutcome::Failed(error) => Ok(format!("{order_id}: {}", error.code().text())),
    }
}

#[tokio::test]
async fn a_skipped_charge_is_reported() {
    let skipped = TaskOutcome::Failed(TaskError::library(OutcomeCode::UpstreamSkipped, "skipped"));
    let receipt = send_receipt("order-1".to_owned(), skipped).await.unwrap();
    assert_eq!(receipt, "order-1: UPSTREAM_SKIPPED");
}
```

## Build a Workflow Context

A task with the `workflow_ctx` attribute takes a `WorkflowContext`. A test builds one from its stored form: `workflow_id`, `task_index`, `task_name`, and `results_by_id`, the stored result of each node by node id. `serde_json::to_value` of a `TaskOutcome` writes a stored result.

```rust
use std::convert::Infallible;

use horsies::{task, NodeKey, TaskError, TaskOutcome, WorkflowContext};
use serde_json::json;

#[task("audit_checkout", workflow_ctx)]
async fn audit_checkout(workflow_ctx: WorkflowContext) -> Result<bool, TaskError<Infallible>> {
    let charged = workflow_ctx.result_for::<u64, Infallible>(&NodeKey::new("charge"));
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

`build_app` is the function that `main` calls to build the app. `CheckReport` holds the refusals of the first phase that refused; `errors()` returns them with their `HRS-xxx` codes. See [CLI Reference](/horsies/rust/cli/#check).

## Suppress Sends

A test that runs code which sends can suppress the sends of the process:

| Item | Does |
|------|------|
| `app.suppress_sends(true)` | Suppresses the sends of this process; `suppress_sends(false)` stops it |
| `HORSIES_SUPPRESS_SENDS=1` (`horsies::SUPPRESS_SENDS_ENV`) | Suppresses the sends of every app in the process. The variable is read at each send |
| `app.are_sends_suppressed()` | `true` when either is set |

A suppressed send returns `TaskSendError::SendSuppressed` and writes nothing. The check runs before any I/O of the send, so the test needs no database. A generated send (`task_name::send(&app, ...)`) is suppressed by the app that it names.

## Things to Avoid

**Do not test the worker's behaviour with direct calls.** A direct call has no deadline, no retry, no cancel and no stored outcome. Start a worker against a test database for those.

**Do not run a test that sends against the database of a deployment.** Build the app of the test with the URL of a test database, or suppress the sends.
