---
title: Workflow Patterns
summary: Real-world workflow patterns for production use.
related: [03-defining-workflows, 04-scheduling]
tags: [quickstart, workflows, patterns]
---

## Overview

This page demonstrates common workflow patterns for production scenarios. Each pattern shows:

- When to use it
- How to construct the DAG
- Type-safe node construction

All tasks used in these patterns must be defined with `#[task]` and registered via `task_name::register(&mut app)?` before building workflows. See [Producing Tasks](../02-producing-tasks/) and [Defining Workflows](../03-defining-workflows/).

## Linear Chain Pattern

Sequential execution where each task waits for the previous.

Key is to use `waits_for` with the previous node.

```rust
use horsies::{WorkflowSpecBuilder, OnError};

fn fulfillment_workflow(
    order: &Order,
) -> Result<horsies::WorkflowSpec, horsies::HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new(
        format!("fulfillment_{}", order.order_id)
    );
    builder.definition_key("myapp.fulfillment.v1");
    builder.on_error(OnError::Fail);

    let validate = builder.task(
        validate_order::node()?
            .node_id("validate_order")
            .set_input(order.clone())?,
    );

    let inventory = builder.task(
        check_inventory::node()?
            .node_id("check_inventory")
            .waits_for(validate)
            .arg_from(check_inventory::params::order(), validate),
    );

    let reserve = builder.task(
        reserve_inventory::node()?
            .node_id("reserve_inventory")
            .waits_for(inventory)
            .arg_from(reserve_inventory::params::inventory(), inventory),
    );

    let shipment = builder.task(
        create_shipment::node()?
            .node_id("create_shipment")
            .waits_for(reserve)
            .arg_from(create_shipment::params::reservation(), reserve),
    );

    let notify = builder.task(
        send_notification::node()?
            .node_id("send_notification")
            .waits_for(shipment)
            .arg_from(send_notification::params::shipment(), shipment),
    );

    let tracking = builder.task(
        update_tracking::node()?
            .node_id("update_tracking")
            .waits_for(notify)
            .arg_from(update_tracking::params::notification(), notify),
    );

    builder.output(tracking);
    builder.build()
}
```

## Fan-Out Pattern (Parallel Execution)

Run many independent tasks in parallel. Queue `max_concurrency` controls parallelism.

```rust
use horsies::WorkflowSpecBuilder;

fn sync_all_warehouses(
    warehouses: &[String],
) -> Result<horsies::WorkflowSpec, horsies::HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("sync_all_warehouses");
    builder.definition_key("myapp.sync_all_warehouses.v1");

    let mut last_ref = None;
    for warehouse in warehouses {
        let node_ref = builder.task(
            sync_warehouse::node()?
                .node_id(format!("sync_{}", warehouse))
                .set_input(SyncWarehouseInput {
                    warehouse: warehouse.clone(),
                })?,
        );
        last_ref = Some(node_ref);
    }

    // No waits_for = all run in parallel
    // Queue max_concurrency limits simultaneous execution
    if let Some(output) = last_ref {
        builder.output(output);
    }

    builder.build()
}
```

## Fire-and-Forget Pattern

Start a workflow without waiting for completion. Useful for background processing.

```rust
async fn trigger_shipment_tracking(
    app: &mut horsies::Horsies,
    shipment_id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut builder = WorkflowSpecBuilder::new(
        format!("tracking_{}", shipment_id)
    );
    builder.definition_key("myapp.tracking.v1");

    let fetch = builder.task(
        fetch_carrier_status::node()?
            .node_id("fetch_carrier_status")
            .set_input(CarrierStatusInput {
                shipment_id: shipment_id.to_owned(),
            })?,
    );

    let update = builder.task(
        update_shipment_status::node()?
            .node_id("update_shipment_status")
            .waits_for(fetch)
            .arg_from(update_shipment_status::params::carrier(), fetch),
    );

    let notify = builder.task(
        notify_customer::node()?
            .node_id("notify_customer")
            .waits_for(update)
            .arg_from(notify_customer::params::status(), update),
    );

    builder.output(notify);
    let spec = builder.build()?;

    // Start and return immediately — workflow runs in background
    match app.start::<NotificationResult>(spec).await {
        Ok(_handle) => Ok(true),
        Err(err) => {
            tracing::warn!(error = %err.message, "failed to start tracking workflow");
            Ok(false)
        }
    }
}
```

## Dynamic Workflow Building

Build workflows conditionally based on runtime flags. Useful when workflow structure depends on input data or feature flags.

```rust
use horsies::{NodeRef, OnError, WorkflowSpecBuilder};

fn build_order_workflow(
    order: &Order,
    include_inventory_check: bool,
    include_address_validation: bool,
) -> Result<horsies::WorkflowSpec, horsies::HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new(
        format!("order_{}", order.order_id)
    );
    builder.definition_key("myapp.order_processing.dynamic.v1");
    builder.on_error(OnError::Fail);

    let validate = builder.task(
        validate_order::node()?
            .node_id("validate_order")
            .set_input(order.clone())?,
    );

    let mut last: NodeRef = validate.into();

    if include_inventory_check {
        let inventory = builder.task(
            check_inventory::node()?
                .node_id("check_inventory")
                .waits_for(last)
                .arg_from(check_inventory::params::order(), validate),
        );
        last = inventory.into();
    }

    if include_address_validation {
        let address = builder.task(
            check_address::node()?
                .node_id("check_address")
                .waits_for(last)
                .arg_from(check_address::params::order(), validate),
        );
        last = address.into();
    }

    builder.output(last);
    builder.build()
}
```

## Task-Wrapping Workflows

A task that orchestrates a workflow internally using `TaskRuntime`. Useful for composing complex operations as a single schedulable unit.

```rust
use horsies::{task, TaskError, TaskResult, TaskRuntime, WorkflowSpecBuilder, OnError};

#[task("process_returns", queue = "standard")]
async fn process_returns(rt: TaskRuntime) -> Result<String, TaskError> {
    let mut builder = WorkflowSpecBuilder::new("returns_processing");
    builder.definition_key("myapp.returns_processing.v1");
    builder.on_error(OnError::Fail);

    let web = builder.task(
        process_web_returns::node()?
            .node_id("web"),
    );
    let store = builder.task(
        process_store_returns::node()?
            .node_id("store"),
    );
    let phone = builder.task(
        process_phone_returns::node()?
            .node_id("phone"),
    );

    let aggregate = builder.task(
        aggregate_return_stats::node()?
            .node_id("aggregate")
            .waits_for(web)
            .waits_for(store)
            .waits_for(phone)
            .arg_from(aggregate_return_stats::params::web(), web)
            .arg_from(aggregate_return_stats::params::store(), store)
            .arg_from(aggregate_return_stats::params::phone(), phone),
    );

    builder.output(aggregate);
    let spec = builder.build().map_err(|e| {
        TaskError::new("WORKFLOW_BUILD_FAILED", e.to_string())
    })?;

    let handle = rt.start::<AggregatedStats>(spec).await.map_err(|e| {
        TaskError::new("WORKFLOW_START_FAILED", e.message)
    })?;

    match handle.get(Some(std::time::Duration::from_secs(60))).await {
        TaskResult::Ok(_) => Ok("Returns processed".into()),
        TaskResult::Err(err) => Err(err),
    }
}
```

## Pattern Summary

| Pattern         | `waits_for`    | Use Case                      |
|-----------------|----------------|-------------------------------|
| Linear chain    | `prev_node`    | Sequential dependencies       |
| Fan-out         | `[]` (empty)   | Parallel independent tasks    |
| Fire-and-forget | N/A            | Background processing         |
| Dynamic         | Conditional    | Runtime workflow construction |
| Task-wrapping   | Mixed          | Workflow inside a task        |
