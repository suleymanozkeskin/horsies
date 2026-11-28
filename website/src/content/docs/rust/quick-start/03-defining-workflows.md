---
title: Defining Workflows
summary: Compose the Acme order tasks into a DAG with WorkflowDefinition, pass results between nodes with arg_from, start the workflow and read its nodes.
related: [02-producing-tasks, 04-scheduling, ../concepts/workflows/workflow-api, ../concepts/workflows/typed-node-builder]
tags: [rust, quickstart, workflows, WorkflowDefinition, arg_from]
---

# Defining Workflows

A workflow is a DAG of tasks. This page builds the workflow `order_fulfillment` of Acme Clothing: validate the order, reserve each line, authorize the payment, capture it, and email the customer.

## Overview

The DAG has three shapes:

- **Parallel branches.** The reservations of the order lines can run at the same time. The queue limits (`max_concurrency`) decide how many do.
- **Convergence.** The payment is authorized after every reservation ended.
- **Sequential chain.** Each node of the chain waits for the node before it.

```text
                 validate_order
                       |
        +--------------+--------------+
        v              v              v
 reserve_stock_1  reserve_stock_2  reserve_stock_N      one node for each order line
        +--------------+--------------+
                       v
               authorize_payment
                       v
                capture_payment
                       v
                send_order_email
```

A Rust workflow has one way to be built: a `WorkflowSpecBuilder` of the app. A `WorkflowDefinition` wraps that build with a name, a stable definition key and a registration. Both ways of starting are on this page.

## The Tasks of the Workflow

Add the new codes and types to `src/domain.rs`:

```rust
use horsies::CustomErrorCode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, CustomErrorCode)]
pub enum AcmeCode {
    OrderNotFound,  // ORDER_NOT_FOUND
    UnknownSku,     // UNKNOWN_SKU
    CardDeclined,   // CARD_DECLINED
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderLine {
    pub line_no: i32,
    pub sku: String,
    pub quantity: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Order {
    pub order_id: String,
    pub total_cents: i32,
    pub lines: Vec<OrderLine>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StockReservation {
    pub order_id: String,
    pub sku: String,
    pub quantity: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaymentAuthorization {
    pub order_id: String,
    pub authorization_id: String,
    pub amount_cents: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaymentCapture {
    pub order_id: String,
    pub authorization_id: String,
    pub amount_cents: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmailReceipt {
    pub order_id: String,
    pub template: String,
}
```

The enum replaces the one of page 2. Keep `OrderValidation` and `ShippingNotice` in the file.

In `src/tasks/inventory.rs`:

<!-- rust-check: skip partial -- uses the domain module of the example package -->
```rust
use horsies::{task, TaskError};

use crate::domain::{AcmeCode, StockReservation};

#[task("reserve_stock", queue = "fulfillment")]
pub async fn reserve_stock(
    order_id: String,
    line_no: i32,
    sku: String,
    quantity: i32,
) -> Result<StockReservation, TaskError<AcmeCode>> {
    if sku.is_empty() {
        return Err(TaskError::custom(AcmeCode::UnknownSku, "the SKU is empty"));
    }
    Ok(StockReservation { order_id, sku, quantity })
}
```

In `src/tasks/payments.rs`:

<!-- rust-check: skip partial -- uses the domain module of the example package -->
```rust
use horsies::{task, TaskError, TaskOutcome};

use crate::domain::{AcmeCode, PaymentAuthorization, PaymentCapture};

#[task("authorize_payment", queue = "payments")]
pub async fn authorize_payment(
    order_id: String,
    amount_cents: i32,
) -> Result<PaymentAuthorization, TaskError<AcmeCode>> {
    if amount_cents <= 0 {
        return Err(TaskError::custom(AcmeCode::CardDeclined, "the amount is not positive"));
    }
    Ok(PaymentAuthorization {
        authorization_id: format!("auth_{order_id}"),
        order_id,
        amount_cents,
    })
}

#[task("capture_payment", queue = "payments")]
pub async fn capture_payment(
    order_id: String,
    authorization: TaskOutcome<PaymentAuthorization, AcmeCode>,
) -> Result<PaymentCapture, TaskError<AcmeCode>> {
    let hold = match authorization {
        TaskOutcome::Succeeded(hold) => hold,
        TaskOutcome::Failed(error) => return Err(error),
    };
    Ok(PaymentCapture {
        order_id,
        authorization_id: hold.authorization_id,
        amount_cents: hold.amount_cents,
    })
}
```

In `src/tasks/notify.rs`, next to the two tasks of page 2:

<!-- rust-check: skip partial -- uses the domain module of the example package -->
```rust
use horsies::{task, TaskError, TaskOutcome};

use crate::domain::{AcmeCode, EmailReceipt, PaymentCapture};

#[task("send_order_email", queue = "notifications")]
pub async fn send_order_email(
    order_id: String,
    capture: TaskOutcome<PaymentCapture, AcmeCode>,
) -> Result<EmailReceipt, TaskError<AcmeCode>> {
    let template = match capture {
        TaskOutcome::Succeeded(_) => "order-confirmation",
        TaskOutcome::Failed(_) => "order-not-completed",
    };
    Ok(EmailReceipt { order_id, template: template.to_owned() })
}
```

Add the three new tasks to the registrar functions of their modules. `src/tasks/inventory.rs` and `src/tasks/payments.rs` each get a `register` function, as `orders.rs` has. Add `pub mod inventory;` and `pub mod payments;` to `src/tasks/mod.rs`.

## Node IDs

Each node has a node id. The id names the node in the workflow's records, in the logs and in the monitoring UI.

| Source | Node id |
|--------|---------|
| `.node_id("validate_order")` | `validate_order` |
| No call | `<workflow name slug>:<index>`, for example `order_fulfillment:0` |

Set the node id on every node:

- A read of one node's result by key needs the node id: `NodeKey::new("validate_order")`.
- An index-based id changes when you reorder the nodes. The records of the old runs keep the old id.
- `validate_order` is clearer than `order_fulfillment:0` in a log.

A node id is unique in the workflow. It uses the characters `A-Z a-z 0-9 _ - : .` and has at most 128 characters.

## The Definition

In `src/workflows/order_fulfillment.rs`:

<!-- rust-check: skip partial -- uses the tasks and the domain module of the example package -->
```rust
use horsies::{HorsiesError, OnError, WorkflowDefinition, WorkflowSpec, WorkflowSpecBuilder};

use crate::domain::{Order, OrderLine, PaymentCapture};
use crate::tasks::inventory::reserve_stock;
use crate::tasks::notify::send_order_email;
use crate::tasks::orders::validate_order;
use crate::tasks::payments::{authorize_payment, capture_payment};

pub struct OrderFulfillment;

impl WorkflowDefinition for OrderFulfillment {
    type Output = PaymentCapture;
    type Parameters = Order;

    fn name() -> &'static str {
        "order_fulfillment"
    }

    fn definition_key() -> &'static str {
        "acme.order_fulfillment.v1"
    }

    fn build_with(
        mut builder: WorkflowSpecBuilder,
        order: Order,
    ) -> Result<WorkflowSpec, HorsiesError> {
        builder
            .definition_key(Self::definition_key())
            .on_error(OnError::Fail);

        let validate = builder.task(
            validate_order::node()
                .node_id("validate_order")
                .set(validate_order::fields::order_id(), order.order_id.clone())?,
        )?;
        let mut reservations = Vec::new();
        for line in &order.lines {
            reservations.push(
                builder.task(
                    reserve_stock::node()
                        .node_id(format!("reserve_stock_{}", line.line_no))
                        .set(reserve_stock::fields::order_id(), order.order_id.clone())?
                        .set(reserve_stock::fields::line_no(), line.line_no)?
                        .set(reserve_stock::fields::sku(), line.sku.clone())?
                        .set(reserve_stock::fields::quantity(), line.quantity)?
                        .waits_for(validate),
                )?,
            );
        }
        let authorize = builder.task(
            authorize_payment::node()
                .node_id("authorize_payment")
                .set(authorize_payment::fields::order_id(), order.order_id.clone())?
                .set(authorize_payment::fields::amount_cents(), order.total_cents)?
                .waits_for_all(reservations.iter().copied()),
        )?;
        let capture = builder.task(
            capture_payment::node()
                .node_id("capture_payment")
                .set(capture_payment::fields::order_id(), order.order_id.clone())?
                .arg_from(capture_payment::fields::authorization(), authorize),
        )?;
        builder.task(
            send_order_email::node()
                .node_id("send_order_email")
                .set(send_order_email::fields::order_id(), order.order_id)?
                .arg_from(send_order_email::fields::capture(), capture)
                .allow_failed_dependencies(true),
        )?;
        builder.output(capture);
        builder.build()
    }
}

pub fn check_order() -> Order {
    Order {
        order_id: "ACME-CHECK-0001".to_owned(),
        total_cents: 4_900,
        lines: vec![OrderLine { line_no: 1, sku: "ACME-SKU-0001".to_owned(), quantity: 1 }],
    }
}
```

- `type Output` is the type of the output node. `type Parameters` is the input of the workflow.
- `build_with` gets a builder of the app, named after the definition. It adds the nodes and calls `builder.build()`.
- `validate_order::node()` is the node of the task. `validate_order::fields::order_id()` is the field token of one parameter. A value of the wrong type is a compile error.
- `builder.task(node)` returns a `TypedNodeRef`. Later nodes use it in `waits_for`, `waits_for_all` and `arg_from`.
- The loop adds one node for each order line. The number of nodes follows the order.
- `builder.output(capture)` makes the node `capture_payment` the result of the workflow.
- `OnError::Fail` is the default. A failed node fails the workflow after the DAG resolves. `OnError::Pause` pauses the workflow at a failed node, and `handle.resume()` continues it.
- The definition key is the stable identity of the workflow. Keep it when you rename the type.

## Register the Workflow

In `src/workflows/mod.rs`:

<!-- rust-check: skip partial -- uses the workflows module of the example package -->
```rust
use horsies::{Horsies, HorsiesError, WorkflowDefinition};

use crate::domain::Order;

pub mod order_fulfillment;

use order_fulfillment::{check_order, OrderFulfillment};

pub fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _template = app.register_workflow_definition_with::<OrderFulfillment>()?;
    app.check_workflow_builder(
        OrderFulfillment::name(),
        |builder, order: &Order| OrderFulfillment::build_with(builder, order.clone()),
    )
    .case(check_order())
    .register()?;
    Ok(())
}
```

- `register_workflow_definition_with` registers a definition that has parameters. It returns a `WorkflowTemplate`, which starts the workflow with `start(parameters)`.
- `check_workflow_builder` gives `app.check()` a case to build. The check cannot invent an `Order`.
- Register the tasks first. A workflow node of a task that the app does not register is refused (`HRS-302`).

In `src/lib.rs`, add `pub mod workflows;`. In `src/app.rs`, list the registrars in this order:

<!-- rust-check: skip partial -- a line of build_app in the example package -->
```rust
app.discover([
    tasks::notify::register,
    tasks::orders::register,
    tasks::inventory::register,
    tasks::payments::register,
    workflows::register,
])?;
```

## Start the Workflow

In `src/bin/start_order.rs`:

<!-- rust-check: skip partial -- names the library crate of the example package -->
```rust
use std::time::Duration;

use getting_started::app::build_app;
use getting_started::domain::{AcmeCode, Order, OrderLine, OrderValidation};
use getting_started::workflows::order_fulfillment::OrderFulfillment;
use horsies::{NodeKey, TaskOutcome, WaitLimit};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = build_app()?;
    let order = Order {
        order_id: "ACME-1001".to_owned(),
        total_cents: 4_900,
        lines: vec![
            OrderLine { line_no: 1, sku: "ACME-SKU-0001".to_owned(), quantity: 2 },
            OrderLine { line_no: 2, sku: "ACME-SKU-0002".to_owned(), quantity: 1 },
        ],
    };

    let handle = app.start_workflow_with::<OrderFulfillment>(order).await?;
    match handle.get(WaitLimit::Within(Duration::from_secs(30))).await {
        Ok(TaskOutcome::Succeeded(capture)) => println!("captured {} cents", capture.amount_cents),
        Ok(TaskOutcome::Failed(error)) => println!("workflow failed: {}", error.code()),
        Err(read_error) => println!("not read: {read_error}"),
    }

    let key = NodeKey::<OrderValidation>::new("validate_order");
    match handle.result_for::<OrderValidation, AcmeCode>(&key).await {
        Ok(TaskOutcome::Succeeded(validation)) => println!("validated {}", validation.order_id),
        Ok(TaskOutcome::Failed(error)) => println!("validation failed: {}", error.code()),
        Err(read_error) => println!("not read: {read_error}"),
    }
    Ok(())
}
```

```bash
cargo run --bin start_order
```

```text
captured 4900 cents
validated ACME-1001
```

- `app.start_workflow_with::<OrderFulfillment>(order)` builds the spec from the order and starts it. It refuses with `ValidationFailed` when this app did not register the definition.
- `handle.get(wait)` waits for the workflow, up to the limit. It returns `Ok(TaskOutcome::Succeeded(value))` with the value of the output node.
- `handle.result_for::<V, E>(&key)` reads the outcome of one node, with its types. It does not wait. It returns `Err(WorkflowReadError::ResultNotReady { .. })` for a node that has not ended.
- `Ok(TaskOutcome::Failed(error))` of `get` holds the stored error of a failed workflow. The code is read as checked text, because a workflow holds the codes of many tasks.

## Start a Spec Without Registration

A spec that you build yourself needs no registration. Build it on a builder of the app, and start it with the type of its output:

<!-- rust-check: skip partial -- a fragment that uses the definition and the order of this page -->
```rust
let spec = OrderFulfillment::build_with(app.workflow_builder("order_fulfillment"), order)?;
let handle = app.start::<PaymentCapture>(&spec).await?;
```

A spec started this way has a definition key but no registration. A sub-workflow node in it needs a registered child. See [Workflow API](/horsies/rust/concepts/workflows/workflow-api/).

## Handling Upstream Results

`arg_from(field, source)` gives the outcome of `source` to a parameter of type `TaskOutcome<S, E>`. The node waits for `source`.

| State of the dependency | Default | With `allow_failed_dependencies(true)` |
|-------------------------|---------|----------------------------------------|
| Completed | The node runs with `Succeeded(value)` | The same |
| Failed | The node is skipped | The node runs with `Failed(error)` |
| Skipped | The node is skipped | The node runs with `Failed` and the code `UPSTREAM_SKIPPED` |

In this workflow:

- `capture_payment` has the default. When `authorize_payment` fails with `CARD_DECLINED`, the capture is skipped.
- `send_order_email` allows failed dependencies. It runs when the capture was skipped, and it sends the template `order-not-completed`.
- The parameter name is the field token. `capture_payment::fields::authorization()` fills the parameter `authorization`.

## Key Concepts

| Concept | Description |
|---------|-------------|
| `.node_id(id)` | The id of a node, for the records and for `NodeKey` |
| `.waits_for(node)`, `.waits_for_all(nodes)` | The nodes that must end before this node runs. No data moves |
| `.arg_from(field, source)` | The outcome of `source` becomes a parameter. The node waits for `source` |
| `.allow_failed_dependencies(true)` | The node runs when a dependency failed or was skipped |
| `builder.output(node)` | The node whose value is the result of the workflow |
| `OnError` | `Fail` fails the workflow after the DAG resolves. `Pause` pauses it at a failed node |
| `handle.result_for::<V, E>(&key)` | The typed outcome of one node. It does not wait |
