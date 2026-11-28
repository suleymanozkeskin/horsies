---
title: Workflow Patterns
summary: Five patterns of the Acme shop in Rust - build a spec at run time, fan out, chain, start and forget, and start a workflow from a task.
related: [03-defining-workflows, 04-scheduling, ../concepts/workflows/workflow-api, ../concepts/workflows/subworkflows]
tags: [rust, quickstart, workflows, patterns]
---

# Workflow Patterns

Each pattern shows when to use it and how to build the DAG. The tasks come from the pages before. A block that uses a task which the page does not define names its module.

## Build a Spec at Run Time

Use this when the shape of the workflow depends on the input or on a flag. The builder is plain Rust, so a loop or an `if` adds the nodes.

<!-- rust-check: skip partial -- uses the tasks and the domain module of the example package -->
```rust
use horsies::{Horsies, HorsiesError, OnError, WorkflowSpec};

use crate::domain::Order;
use crate::tasks::inventory::reserve_stock;
use crate::tasks::orders::validate_order;
use crate::tasks::payments::authorize_payment;

pub fn payment_spec(
    app: &Horsies,
    order: &Order,
    reserve: bool,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = app.workflow_builder("order_fulfillment");
    builder
        .definition_key("acme.order_fulfillment.dynamic.v1")
        .on_error(OnError::Fail);

    let validate = builder.task(
        validate_order::node()
            .node_id("validate_order")
            .set(validate_order::fields::order_id(), order.order_id.clone())?,
    )?;
    let mut reservations = Vec::new();
    if reserve {
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
    }
    let authorize = builder.task(
        authorize_payment::node()
            .node_id("authorize_payment")
            .set(authorize_payment::fields::order_id(), order.order_id.clone())?
            .set(authorize_payment::fields::amount_cents(), order.total_cents)?
            .waits_for(validate)
            .waits_for_all(reservations.iter().copied()),
    )?;
    builder.output(authorize);
    builder.build()
}
```

Start it with the type of the output node:

<!-- rust-check: skip partial -- a fragment that uses payment_spec above -->
```rust
let spec = payment_spec(&app, &order, true)?;
let handle = app.start::<PaymentAuthorization>(&spec).await?;
```

- The output node has one type, so the output type of the spec is fixed. Nodes that depend on a flag are nodes before it, not the output.
- `waits_for(validate)` keeps the order when `reserve` is `false` and the list of reservations is empty.
- `build` checks the spec before anything is written. A cycle cannot be built: a node refers only to nodes that the builder returned before.
- A spec that is built at run time has no registration. `app.check()` cannot build it. Register check cases with `app.check_workflow_builder`. See [Workflow API](/horsies/rust/concepts/workflows/workflow-api/#check-cases-of-a-dynamic-builder).

## Fan-Out (Parallel Execution)

Use this to run many independent tasks at one time. The queue `max_concurrency` limits how many run together.

<!-- rust-check: skip partial -- uses the regional_rollup task of the analytics module, which the page does not define -->
```rust
use horsies::{HorsiesError, WorkflowSpec, WorkflowSpecBuilder};

use crate::tasks::analytics::regional_rollup;

const REGIONS: [&str; 4] = ["eu-central", "uk", "turkiye", "nordics"];

pub fn regional_rollups_spec(mut builder: WorkflowSpecBuilder) -> Result<WorkflowSpec, HorsiesError> {
    builder.definition_key("acme.regional_rollups.v1");
    for region in REGIONS {
        builder.task(
            regional_rollup::node()
                .node_id(format!("rollup_{region}"))
                .set(regional_rollup::fields::region(), region.to_owned())?,
        )?;
    }
    builder.build()
}
```

`regional_rollup` is a task of the queue `analytics`. It takes `region: String` and returns `RegionalRollup`.

- No node has `waits_for`, so all nodes can run at one time. The queue `analytics` has `max_concurrency` 4.
- The workflow has no output node. Its result is `NoWorkflowOutput`; `result_for` and `results()` read the value of each node.

Read one node after the workflow ended:

<!-- rust-check: skip partial -- a fragment that uses the spec and the types above -->
```rust
let handle = app.start::<NoWorkflowOutput>(&spec).await?;
let _ended = handle.get(WaitLimit::Within(Duration::from_secs(60))).await?;

for region in REGIONS {
    let key = NodeKey::<RegionalRollup>::new(format!("rollup_{region}"));
    match handle.result_for::<RegionalRollup, AcmeCode>(&key).await {
        Ok(TaskOutcome::Succeeded(rollup)) => println!("{region}: {} orders", rollup.orders_counted),
        Ok(TaskOutcome::Failed(error)) => println!("{region}: {}", error.code()),
        Err(read_error) => println!("{region}: not read, {read_error}"),
    }
}
```

`result_for` does not wait. It returns `Err(WorkflowReadError::ResultNotReady { .. })` for a node that has not ended.

## Linear Chain

Use this when each task needs the result of the task before it. The workflow `shipping` is a chain: book the courier, print the label, seed the tracking code.

<!-- rust-check: skip partial -- uses the shipping tasks, which the page does not define -->
```rust
use horsies::{HorsiesError, OnError, WorkflowDefinition, WorkflowSpec, WorkflowSpecBuilder};
use serde::{Deserialize, Serialize};

use crate::domain::TrackingSeed;
use crate::tasks::shipping::{book_courier, print_label, tracking_seed};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShippingParams {
    pub order_id: String,
    pub courier: String,
    pub express: bool,
}

pub struct Shipping;

impl WorkflowDefinition for Shipping {
    type Output = TrackingSeed;
    type Parameters = ShippingParams;

    fn name() -> &'static str {
        "shipping"
    }

    fn definition_key() -> &'static str {
        "acme.shipping.v1"
    }

    fn build_with(
        mut builder: WorkflowSpecBuilder,
        params: ShippingParams,
    ) -> Result<WorkflowSpec, HorsiesError> {
        builder
            .definition_key(Self::definition_key())
            .on_error(OnError::Fail);
        let book = builder.task(
            book_courier::node()
                .node_id("book_courier")
                .set(book_courier::fields::order_id(), params.order_id.clone())?
                .set(book_courier::fields::courier(), params.courier)?
                .set(book_courier::fields::express(), params.express)?,
        )?;
        let label = builder.task(
            print_label::node()
                .node_id("print_label")
                .set(print_label::fields::order_id(), params.order_id.clone())?
                .arg_from(print_label::fields::booking(), book),
        )?;
        let tracking = builder.task(
            tracking_seed::node()
                .node_id("tracking_seed")
                .set(tracking_seed::fields::order_id(), params.order_id)?
                .arg_from(tracking_seed::fields::label(), label),
        )?;
        builder.output(tracking);
        builder.build()
    }
}
```

The three tasks are in `src/tasks/shipping.rs`, on the queue `fulfillment`:

| Task | Parameters | Result |
|------|------------|--------|
| `book_courier` | `order_id: String`, `courier: String`, `express: bool` | `CourierBooking` |
| `print_label` | `order_id: String`, `booking: TaskOutcome<CourierBooking, AcmeCode>` | `ShippingLabel` |
| `tracking_seed` | `order_id: String`, `label: TaskOutcome<ShippingLabel, AcmeCode>` | `TrackingSeed` |

`arg_from` makes the dependency. A node needs no `waits_for` for a source that it takes with `arg_from`.

## Start and Forget

Use this to start a workflow and return at once. The workflow runs in the workers.

<!-- rust-check: skip partial -- uses the Shipping definition above and its registration -->
```rust
use horsies::{Horsies, WorkflowStartError};

pub async fn ship_order(app: &Horsies, params: ShippingParams) -> Result<String, WorkflowStartError> {
    let handle = app.start_workflow_with::<Shipping>(params).await?;
    Ok(handle.workflow_id().to_string())
}
```

- `start_workflow_with` returns `Err(WorkflowStartError)` when the start failed. `Ok` means the workflow is stored.
- `workflow_id()` is the id of the workflow. Another process reads the workflow later with `app.workflow_handle::<TrackingSeed>(workflow_id)`. That call does no I/O.
- A start with the same workflow id and the same parameters starts nothing new. See [Start a Workflow Once per Entity](/horsies/guides/start-workflow-once/).

## Start a Workflow from a Task

Use this to make a whole workflow one schedulable unit. The task builds a spec through its `TaskRuntime`, starts it, and returns the id.

The showcase task `replenish_catalog` (queue `analytics`) runs the nightly stocktake in its body, and a schedule sends it. The block shows the same task with a body that starts the `restock` workflow of the showcase:

<!-- rust-check: skip partial -- uses the domain module and the restock workflow of the example package -->
```rust
use horsies::{task, TaskError, TaskRuntime, WorkflowDefinition};

use crate::domain::{AcmeCode, StockUpdate};
use crate::workflows::restock::Restock;

#[task("replenish_catalog", queue = "analytics")]
pub async fn replenish_catalog(rt: TaskRuntime) -> Result<String, TaskError<AcmeCode>> {
    let spec = Restock::build(rt.workflow_builder(Restock::name())).map_err(|refused| {
        TaskError::custom(AcmeCode::StoreUnavailable, format!("restock spec refused: {refused}"))
    })?;
    let handle = rt.start::<StockUpdate>(&spec).await.map_err(|error| {
        TaskError::custom(AcmeCode::StoreUnavailable, format!("restock not started: {error}"))
    })?;
    Ok(handle.workflow_id().to_string())
}
```

- `rt.workflow_builder(name)` gives a builder of the app that runs the task. `rt.start` starts the spec.
- A task body cannot wait for a workflow. `handle.get(..)` in a worker child is refused with `WaitInsideTask`. Return the id, and read the workflow from another process.
- A retry of the task builds the spec again. Use `rt.start_with_id` with an id from the task input to start the workflow once. The spec must be the same on each attempt.
- A schedule can send this task. See [Scheduling Tasks](/horsies/rust/quick-start/04-scheduling/).

## Pattern Summary

| Pattern | `waits_for` and `arg_from` | Use |
|---------|----------------------------|-----|
| Run-time spec | A loop or an `if` adds the nodes | The shape follows the input |
| Fan-out | None | Independent tasks in parallel |
| Linear chain | `arg_from` of the node before | Sequential dependencies |
| Diamond | `waits_for_all` of several nodes | Fan-out, then fan-in, as in page 3 |
| Start and forget | Not used | Background work |
| Start from a task | A spec built inside a task | One schedulable unit |
