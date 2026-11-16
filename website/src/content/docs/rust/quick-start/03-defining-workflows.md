---
title: Defining Workflows
summary: Compose tasks into DAG-based workflows.
related: [02-producing-tasks, 04-scheduling]
tags: [quickstart, workflows]
---

## Overview

This page demonstrates workflow composition using an **order processing** example, exhibiting DAG patterns.

- **Parallel branches:** Nodes which **can** run concurrently ( here a concurrent run is not guaranteed, concurrency depends on the queue capacities )
- **Convergence:** Multiple results feed into a single node
- **Sequential chain:** Nodes which `waits_for` a terminal state from previous

We cover two approaches: **declarative** (trait-based via `WorkflowDefinition`) and **imperative** (builder-scoped). Both produce equivalent `WorkflowSpec` objects.

For both approaches, workflows need a stable `definition_key`. In trait-based workflows you declare it on `WorkflowDefinition::definition_key()`; in imperative workflows you set it on the builder.

## Order Processing Workflow

```text
                    ┌─────────────────┐
                    │  validate_order │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
      ┌───────────────┐ ┌──────────────┐ ┌─────────────────┐
      │check_inventory│ │calculate_cost│ │ check_address   │
      └───────┬───────┘ └──────┬───────┘ └────────┬────────┘
              │                │                  │
              └────────────────┼──────────────────┘
                               ▼
                    ┌─────────────────────┐
                    │  reserve_inventory  │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │   create_shipment   │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │  send_notification  │
                    └─────────────────────┘
```

## Node ID

Each `TaskNode` can be given a `node_id` for identification within the workflow.

**Auto-assignment behavior:**

| Context | Default `node_id` |
|---|---|
| Declarative (`WorkflowDefinition`) | `{workflow_name}:{index}` |
| Imperative (`WorkflowSpecBuilder`) | `{workflow_name}:{index}` |

**We strongly recommend explicit `node_id` values:**

- **Stable observability:** Index-based IDs shift when you reorder tasks
- **Meaningful tracing:** `validate_order` is clearer than `order_processing:0` in logs
- **Result retrieval:** Node IDs are used when fetching individual task results

```rust
// Recommended: explicit node_id
validate_order::node()?.set_input(order)?.node_id("validate")

// Avoid: relying on auto-assignment
validate_order::node()?.set_input(order)?  // becomes "order_processing:0"
```

## Declarative Workflow (WorkflowDefinition trait)

For workflows with runtime input, implement `build_with(...)` directly. Use `define(...)` for static no-input workflows.

```rust
use horsies::{
    HorsiesError, OnError,
    WorkflowDefinition, WorkflowSpecBuilder,
};

struct OrderProcessingWorkflow;

impl WorkflowDefinition for OrderProcessingWorkflow {
    type Output = NotificationResult;
    type Params = Order;

    fn name() -> &'static str { "order_processing" }
    fn definition_key() -> &'static str { "myapp.order_processing.v1" }
    fn on_error() -> OnError { OnError::Fail }

    fn build_with(order: Order) -> Result<horsies::WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(Self::name());
        builder.definition_key(Self::definition_key());
        builder.on_error(Self::on_error());

        // Root task
        let validate = builder.task(
            validate_order::node()?
                .set_input(order)?
                .node_id("validate"),
        );

        // Fan-out: three parallel checks
        let inventory = builder.task(
            check_inventory::node()?
                .node_id("inventory")
                .waits_for(validate)
                .arg_from(check_inventory::params::order(), validate),
        );
        let cost = builder.task(
            calculate_shipping_cost::node()?
                .node_id("shipping_cost")
                .waits_for(validate)
                .arg_from(calculate_shipping_cost::params::order(), validate),
        );
        let address = builder.task(
            check_address::node()?
                .node_id("address")
                .waits_for(validate)
                .arg_from(check_address::params::order(), validate),
        );

        // Fan-in: reserve waits for all three
        let reserve = builder.task(
            reserve_inventory::node()?
                .node_id("reserve")
                .waits_for(inventory)
                .waits_for(cost)
                .waits_for(address)
                .arg_from(reserve_inventory::params::inventory(), inventory)
                .arg_from(reserve_inventory::params::cost(), cost)
                .arg_from(reserve_inventory::params::address(), address),
        );

        // Sequential tail
        let shipment = builder.task(
            create_shipment::node()?
                .node_id("shipment")
                .waits_for(reserve)
                .arg_from(create_shipment::params::reservation(), reserve),
        );
        let notify = builder.task(
            send_notification::node()?
                .node_id("notify")
                .waits_for(shipment)
                .arg_from(send_notification::params::shipment(), shipment),
        );

        builder.output(notify);
        builder.build()
    }
}
```

## Registering and Starting a Workflow

```rust
// Register the workflow template for a parameterized workflow
let order_wf = app.workflow_template::<OrderProcessingWorkflow>();

// Start the workflow
match order_wf.start(order).await {
    Ok(handle) => {
        println!("Workflow {} started", handle.workflow_id());

        match handle.get(Some(Duration::from_secs(60))).await {
            TaskResult::Ok(notification) => {
                println!("Order {} processed", notification.order_id);
            }
            TaskResult::Err(err) => {
                println!("Failed: {:?} - {:?}", err.error_code, err.message);
            }
        }
    }
    Err(start_err) => {
        println!("Start failed: {} - {}", start_err.code, start_err.message);
    }
}
```

## Imperative Workflow (WorkflowSpecBuilder)

Build a workflow using `WorkflowSpecBuilder` directly:

```rust
use horsies::{WorkflowSpecBuilder, OnError};

fn build_order_processing(
    order: Order,
) -> Result<horsies::WorkflowSpec, horsies::HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("order_processing");
    builder.definition_key("myapp.order_processing.v1");
    builder.on_error(OnError::Fail);

    let validate = builder.task(
        validate_order::node()?
            .set_input(order)?
            .node_id("validate"),
    );

    let inventory = builder.task(
        check_inventory::node()?
            .node_id("inventory")
            .waits_for(validate)
            .arg_from(check_inventory::params::order(), validate),
    );

    let cost = builder.task(
        calculate_shipping_cost::node()?
            .node_id("shipping_cost")
            .waits_for(validate)
            .arg_from(calculate_shipping_cost::params::order(), validate),
    );

    let address = builder.task(
        check_address::node()?
            .node_id("address")
            .waits_for(validate)
            .arg_from(check_address::params::order(), validate),
    );

    let reserve = builder.task(
        reserve_inventory::node()?
            .node_id("reserve")
            .waits_for(inventory)
            .waits_for(cost)
            .waits_for(address)
            .arg_from(reserve_inventory::params::inventory(), inventory)
            .arg_from(reserve_inventory::params::cost(), cost)
            .arg_from(reserve_inventory::params::address(), address),
    );

    let shipment = builder.task(
        create_shipment::node()?
            .node_id("shipment")
            .waits_for(reserve)
            .arg_from(create_shipment::params::reservation(), reserve),
    );

    let notify = builder.task(
        send_notification::node()?
            .node_id("notify")
            .waits_for(shipment)
            .arg_from(send_notification::params::shipment(), shipment),
    );

    builder.output(notify);
    builder.build()
}
```

Start the built spec via the app:

```rust
match app.start::<NotificationResult>(spec).await {
    Ok(handle) => {
        println!("Workflow {} started", handle.workflow_id());
    }
    Err(start_err) => {
        println!("Start failed: {} - {}", start_err.code, start_err.message);
    }
}
```

## Handling Upstream Results

`arg_from` injects upstream task results as `TaskResult<T>` into the receiving task's parameters. The downstream task receives the full `TaskResult` wrapper — not the raw success value — so it must handle both the `Ok` and `Err` variants.

Use multi-parameter task signatures where each upstream result is a separate `TaskResult<T>` parameter. The `#[task]` macro generates `task_name::params::param_name()` tokens for workflow wiring:

```rust
use horsies::{task, TaskError, TaskResult};
use serde::{Deserialize, Serialize};

#[task("check_inventory", queue = "standard")]
async fn check_inventory(order: TaskResult<ValidatedOrder>) -> Result<InventoryStatus, TaskError> {
    let order = match order {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => return Err(TaskError::new(
            "UPSTREAM_FAILED",
            format!("Cannot check inventory: upstream failed - {:?}", e.error_code),
        )),
    };

    let availability: HashMap<String, bool> = order
        .items
        .iter()
        .map(|item| (item.sku.clone(), true))
        .collect();

    Ok(InventoryStatus {
        order_id: order.order_id,
        all_available: availability.values().all(|&v| v),
        item_availability: availability,
    })
}
```

When multiple nodes converge into a single task (fan-in), each upstream result is a separate `TaskResult<T>` parameter:

```rust
#[task("reserve_inventory", queue = "urgent")]
async fn reserve_inventory(
    inventory: TaskResult<InventoryStatus>,
    cost: TaskResult<ShippingCost>,
    address: TaskResult<AddressValidation>,
) -> Result<Reservation, TaskError> {
    let inventory = match inventory {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => return Err(TaskError::new(
            "UPSTREAM_FAILED",
            format!("Cannot reserve: inventory check failed - {:?}", e.error_code),
        )),
    };
    let cost = match cost {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => return Err(TaskError::new(
            "UPSTREAM_FAILED",
            format!("Cannot reserve: cost calculation failed - {:?}", e.error_code),
        )),
    };
    let address = match address {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => return Err(TaskError::new(
            "UPSTREAM_FAILED",
            format!("Cannot reserve: address check failed - {:?}", e.error_code),
        )),
    };

    if !inventory.all_available {
        return Err(TaskError::new(
            "ITEMS_UNAVAILABLE",
            "Some items are not available",
        ));
    }

    Ok(Reservation {
        order_id: inventory.order_id.clone(),
        reservation_id: uuid::Uuid::new_v4().to_string(),
        reserved_items: Vec::new(),
        reserved_at: chrono::Utc::now(),
        shipping_cost_cents: cost.total_cost_cents,
    })
}
```

The `arg_from` parameter names must match the function parameter names. For example, `.arg_from(reserve_inventory::params::inventory(), inventory_node)` maps to the `inventory` parameter in `reserve_inventory`.

## Key Concepts

| Concept | Description |
|---------|-------------|
| `node_id` | Unique identifier for observability and result retrieval |
| `waits_for` | Node that must complete before this task runs |
| `arg_from` | Inject upstream result as a typed parameter via `task_name::params::*` tokens |
| `output` | Node whose result becomes the workflow result |
| `on_error` | `Fail` continues DAG resolution then marks workflow Failed; `Pause` suspends immediately |
| `WorkflowDefConfig` | Returned from `define()` to set output and success policy |
