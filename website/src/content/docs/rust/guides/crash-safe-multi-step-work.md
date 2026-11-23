---
title: Crash-Safe Multi-Step Work
summary: Put each step that has an effect in its own workflow node, so that a retry or a worker crash runs only the step that failed.
related: [../concepts/workflows/workflow-api, ../concepts/workflows/typed-node-builder, ../tasks/defining-tasks, ../../concepts/workflows/workflow-semantics]
tags: [rust, guides, workflow, retry, WORKER_CRASHED, idempotency, arg_from, TaskOutcome]
---

# Crash-Safe Multi-Step Work

Horsies has no durable steps inside one task. Each attempt of a task runs its body from the first line. For work with several steps that have effects, use a workflow with one node per step. The engine stores the result of each node, so a retry or a worker crash runs only the node that failed.

## Why and When

Use this pattern when a task does two or more steps with effects outside the task, for example a payment, a database write or a message.

A checkout in one task body:

<!-- rust-check: skip elided -- the one-task form, with the step bodies left out -->
```rust
#[task(
    "checkout",
    retry_policy = RetryPolicy::exponential(30, 5, true).expect("a valid policy"),
    auto_retry_for = ["PAYMENTS_UNAVAILABLE", "MAIL_UNAVAILABLE", "WORKER_CRASHED"]
)]
async fn checkout(order: Order) -> Result<String, TaskError<CheckoutCode>> {
    let charge = charge_the_card(&order).await?;             // 1. money moves
    let invoice = write_the_invoice(&order, &charge).await?; // 2. a record is written
    send_the_receipt(&order, &invoice).await                 // 3. a message is sent
}
```

What a failure does to this task:

- An error code in `auto_retry_for` starts a retry. The retry runs the body from the first line. A transient failure in step 3 charges the card again in step 1.
- A worker crash while the body runs ends the attempt with `WORKER_CRASHED`. When `auto_retry_for` does not name `WORKER_CRASHED`, the task is `FAILED`. This is the default. The steps before the crash have run, and the stored error does not tell which steps ran.
- When `auto_retry_for` names `WORKER_CRASHED`, the retry runs the body from the first line. Steps 1 and 2 run again.

The rules are on [Stale RUNNING Recovery](/horsies/workers/heartbeats-recovery/#stale-running-recovery) and [Auto-Retry Triggers](/horsies/tasks/retry-policy/#auto-retry-triggers).

## Define One Task per Step

Each step is a task with its own retry policy. A step takes the result of the step before it as a parameter of type `TaskOutcome<T, E>`.

```rust
use horsies::{task, RetryPolicy, TaskError, TaskOutcome, UserErrorCode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Charge {
    charge_id: String,
    amount_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invoice {
    invoice_number: String,
    charge_id: String,
}

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum CheckoutCode {
    PaymentsUnavailable,
    ChargeRefused,
    BillingUnavailable,
    InvoiceRefused,
    MailUnavailable,
    MailRefused,
}

/// A failed call to a provider.
enum CallFailure {
    /// No answer, or a 5xx answer: a later call can succeed.
    Unavailable(String),
    /// Another answer that is not 2xx: the same call gets the same answer.
    Refused(String),
}

impl CallFailure {
    /// The task error of this failure, with the two codes of the step.
    fn into_task_error(
        self,
        unavailable: CheckoutCode,
        refused: CheckoutCode,
    ) -> TaskError<CheckoutCode> {
        match self {
            Self::Unavailable(detail) => TaskError::user(unavailable, detail),
            Self::Refused(detail) => TaskError::user(refused, detail),
        }
    }
}

// The three calls below stand in for the provider clients of the app. Each call
// sends the idempotency key. The provider answers a repeated key with the answer
// of the first call, and does not do the effect again.

async fn create_charge(
    customer_id: &str,
    amount_cents: u64,
    idempotency_key: &str,
) -> Result<String, CallFailure> {
    let _request = (customer_id, amount_cents);
    Ok(format!("ch_{idempotency_key}"))
}

async fn create_invoice(
    order_id: &str,
    charge: &Charge,
    idempotency_key: &str,
) -> Result<String, CallFailure> {
    let _request = (order_id, charge);
    Ok(format!("inv_{idempotency_key}"))
}

async fn send_message(
    to_address: &str,
    invoice: &Invoice,
    idempotency_key: &str,
) -> Result<String, CallFailure> {
    let _request = (to_address, invoice);
    Ok(format!("msg_{idempotency_key}"))
}

#[task(
    "charge_card",
    retry_policy = RetryPolicy::exponential(30, 5, true).expect("a valid policy"),
    auto_retry_for = ["PAYMENTS_UNAVAILABLE", "WORKER_CRASHED"]
)]
async fn charge_card(
    order_id: String,
    customer_id: String,
    amount_cents: u64,
) -> Result<Charge, TaskError<CheckoutCode>> {
    let key = format!("checkout-{order_id}-charge");
    let charge_id = create_charge(&customer_id, amount_cents, &key)
        .await
        .map_err(|failure| {
            failure.into_task_error(CheckoutCode::PaymentsUnavailable, CheckoutCode::ChargeRefused)
        })?;
    Ok(Charge {
        charge_id,
        amount_cents,
    })
}

#[task(
    "write_invoice",
    retry_policy = RetryPolicy::exponential(10, 5, true).expect("a valid policy"),
    auto_retry_for = ["BILLING_UNAVAILABLE", "WORKER_CRASHED"]
)]
async fn write_invoice(
    order_id: String,
    charge: TaskOutcome<Charge, CheckoutCode>,
) -> Result<Invoice, TaskError<CheckoutCode>> {
    let charge = match charge {
        TaskOutcome::Succeeded(charge) => charge,
        TaskOutcome::Failed(error) => return Err(error),
    };
    let key = format!("checkout-{order_id}-invoice");
    let invoice_number = create_invoice(&order_id, &charge, &key)
        .await
        .map_err(|failure| {
            failure.into_task_error(CheckoutCode::BillingUnavailable, CheckoutCode::InvoiceRefused)
        })?;
    Ok(Invoice {
        invoice_number,
        charge_id: charge.charge_id,
    })
}

#[task(
    "send_receipt",
    retry_policy = RetryPolicy::fixed(vec![60, 300, 900], true).expect("a valid policy"),
    auto_retry_for = ["MAIL_UNAVAILABLE", "WORKER_CRASHED"]
)]
async fn send_receipt(
    order_id: String,
    to_address: String,
    invoice: TaskOutcome<Invoice, CheckoutCode>,
) -> Result<String, TaskError<CheckoutCode>> {
    let invoice = match invoice {
        TaskOutcome::Succeeded(invoice) => invoice,
        TaskOutcome::Failed(error) => return Err(error),
    };
    let key = format!("checkout-{order_id}-receipt");
    send_message(&to_address, &invoice, &key)
        .await
        .map_err(|failure| {
            failure.into_task_error(CheckoutCode::MailUnavailable, CheckoutCode::MailRefused)
        })
}
```

- The `auto_retry_for` of each step names `WORKER_CRASHED` and the code that the step returns when its provider is not available. Another code, for example `CHARGE_REFUSED`, ends the step as `FAILED` at once.
- Each call sends an idempotency key made from the order id and the step name. The key makes a repeated run of the step safe. See [External Effects Need Idempotency](#external-effects-need-idempotency).
- With the default dependency rules, a step runs only when the step before it is `COMPLETED`. The `Failed` arm returns the error that the parameter holds. See [Take a Dependency's Result](/horsies/rust/concepts/workflows/typed-node-builder/#take-a-dependencys-result).

## Connect the Steps in a Workflow

```rust
use horsies::{
    Horsies, HorsiesError, WorkflowDefinition, WorkflowHandle, WorkflowSpec, WorkflowSpecBuilder,
    WorkflowStartError, WorkflowTemplate,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckoutParams {
    order_id: String,
    customer_id: String,
    customer_email: String,
    amount_cents: u64,
}

struct Checkout;

impl WorkflowDefinition for Checkout {
    type Output = String;
    type Params = CheckoutParams;

    fn name() -> &'static str {
        "checkout"
    }

    fn definition_key() -> &'static str {
        "shop.checkout.v1"
    }

    fn build_with(params: CheckoutParams) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(Self::name());
        builder.definition_key(Self::definition_key());
        let charge = builder.task(
            charge_card::node()?
                .node_id("charge_card")
                .set(charge_card::params::order_id(), params.order_id.clone())?
                .set(charge_card::params::customer_id(), params.customer_id)?
                .set(charge_card::params::amount_cents(), params.amount_cents)?,
        );
        let invoice = builder.task(
            write_invoice::node()?
                .node_id("write_invoice")
                .set(write_invoice::params::order_id(), params.order_id.clone())?
                .arg_from(write_invoice::params::charge(), charge),
        );
        let receipt = builder.task(
            send_receipt::node()?
                .node_id("send_receipt")
                .set(send_receipt::params::order_id(), params.order_id)?
                .set(send_receipt::params::to_address(), params.customer_email)?
                .arg_from(send_receipt::params::invoice(), invoice),
        );
        builder.output(receipt);
        builder.build()
    }
}

fn register_checkout(
    app: &mut Horsies,
) -> Result<WorkflowTemplate<CheckoutParams, String>, HorsiesError> {
    let _charge_card = charge_card::register(app)?;
    let _write_invoice = write_invoice::register(app)?;
    let _send_receipt = send_receipt::register(app)?;
    app.workflow_template::<Checkout>()
}

async fn start_checkout(
    checkout: &WorkflowTemplate<CheckoutParams, String>,
    params: CheckoutParams,
) -> Result<WorkflowHandle<String>, WorkflowStartError> {
    checkout.start(params).await
}
```

- `.arg_from(write_invoice::params::charge(), charge)` gives the result of the `charge_card` node to the `charge` parameter, and the node waits for `charge_card`.
- The `node_id` of a node is the name of the step in the web UI, and the key of its result in `handle.results()`.
- Call `register_checkout` when `main` builds the app. Register the tasks before the definition. See [Workflow API](/horsies/rust/concepts/workflows/workflow-api/#register-and-start).
- `checkout.start(params)` returns the `WorkflowHandle<String>` or a `WorkflowStartError`. To start one checkout per order, call `checkout.start_with_id(params, workflow_id)` with an id derived from the order id. A start with the id of a workflow of the same name starts nothing and returns the handle of that workflow.

## What Runs Again

| Event | One task with three steps | One node per step |
|---|---|---|
| Step 3 returns a code in `auto_retry_for` | The retry runs steps 1, 2 and 3 | The retry runs step 3 |
| The worker crashes during step 3, and `auto_retry_for` names `WORKER_CRASHED` | The retry runs steps 1, 2 and 3 | The retry runs step 3 |
| The worker crashes during step 3, and `auto_retry_for` does not name `WORKER_CRASHED` | The task is `FAILED` with `WORKER_CRASHED`. The error does not tell which steps ran | Step 3 is `FAILED` with `WORKER_CRASHED`. Steps 1 and 2 are `COMPLETED` and keep their results |
| Step 3 fails after its last retry | The task is `FAILED` | Step 3 is `FAILED`, and the workflow is `FAILED`. Steps 1 and 2 keep their results |

The rules of the table:

- When a node ends, the engine stores its result. A dependent node gets the stored results as its input when the engine enqueues it.
- A retry runs on the same task row, with the same input. The other nodes do not change. See [Retries and Crash Recovery](/horsies/concepts/workflows/workflow-semantics/#retries-and-crash-recovery).
- A `COMPLETED` node does not run again in the same workflow run.
- A workflow run has no restart. A new start of the workflow runs every node again. See [No workflow restart](/horsies/monitoring/action-semantics/#no-workflow-restart).

## See Each Step in the Web UI

Each step is a task with its own name and its own attempts. The Workflows surface of the [web UI](/horsies/monitoring/web-ui-overview/#workflows) shows the run as a graph with one node per step:

- the status of each node;
- for a selected node: its error, its task and its attempt history;
- failure navigation, which moves through the `FAILED` nodes.

The Tasks surface lists each step as a task. The task name filter shows one step across all orders.

## When One Task Is the Better Choice

A node per step adds queue work. Use one task in these cases:

- **Short steps.** Each node is one task: a task row, a claim by a worker, a stored result, and the update that makes the next node ready. The next node starts only after a worker claims it. When the work of a step is smaller than this queue work, put the steps in one task.
- **Shared live state.** A node gets its input from stored results only. Each result is encoded as JSON, stored, and decoded for the next node, which can run in another process. An open database transaction, a connection, a session or a lock cannot pass from one node to the next. Steps that must commit in one database transaction belong in one task.
- **Decisions on intermediate results.** The DAG of a workflow is fixed when the workflow starts. A node cannot select the next node from a value. A sub-workflow node whose params take a dependency's result builds its child when it becomes ready. See [Subworkflows](/horsies/rust/concepts/workflows/subworkflows/). When the next step depends on intermediate values at many points, keep that logic in one task.

Group the steps by effect: one node for each external effect, with the short steps and the decisions of that effect in the same node.

## External Effects Need Idempotency

A node runs at least once. Its body can run more than once:

- The worker crashes after the effect and before the result is stored. With `WORKER_CRASHED` in `auto_retry_for`, the retry does the effect again.
- The answer is lost. The provider did the effect, but the call timed out. The step returns a code in `auto_retry_for`, and the retry calls the provider again.
- The workflow starts again. A new start runs every node again.

Rules:

- A step that calls an external API sends the idempotency key of that API. The API answers a repeated key with the result of the first call, and does not do the effect again.
- Make the key from a stable business id and the step name, for example `checkout-{order_id}-charge`. The same order and the same step give the same key on each run.
- A step that writes to your own database uses a unique key for the write, for example `INSERT ... ON CONFLICT (order_id) DO NOTHING`.
- A call to an API with no idempotency key can do its effect again on each run of its step. Put that call in a node with no other step, so that only its own failures repeat it.

## Things to Avoid

**Do not put two effects in one node.** A retry of the node repeats both effects.

**Do not make an idempotency key from a random value or from the task id.** A random value changes on each run. A task id changes on each new start of the workflow. In both cases the provider gets a new key and does the effect again.

**Do not add `WORKER_CRASHED` to `auto_retry_for` of a step whose effect has no idempotency key.** The retry does the effect again.

**Do not use `TaskSendOptions::idempotency_key` for this.** A second send of the same command answers with the task that holds the key. The key does not stop a retry of the task body. See [Send Options](/horsies/rust/tasks/sending-tasks/#send-options).
