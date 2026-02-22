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
- Type inference with `reveal_type()`

## Dynamic Workflow Building

Build workflows conditionally based on runtime flags. Useful when workflow structure depends on input data or feature flags.

```python
from typing import Any
from horsies import TaskNode, SubWorkflowNode, OnError
from .config import app
from .tasks import (
    validate_order,
    check_inventory,
    check_address,
)

def start_order_workflow(
    order: Order,
    include_inventory_check: bool = True,
    include_address_validation: bool = True,
) -> None:
    nodes: list[TaskNode[Any] | SubWorkflowNode[Any]] = []
    prev_node: TaskNode[Any] | None = None
    output_node: TaskNode[Any] | None = None

    node_validate: TaskNode[Any] = TaskNode(
        fn=validate_order,
        kwargs={'order': order},
        node_id="validate_order",
    )
    reveal_type(node_validate)  # TaskNode[Any]
    nodes.append(node_validate)
    prev_node = node_validate
    output_node = node_validate

    if include_inventory_check:
        node_inventory: TaskNode[Any] = TaskNode(
            fn=check_inventory,
            waits_for=[prev_node] if prev_node else [],
            args_from={"order_result": prev_node} if prev_node else {},
            node_id="check_inventory",
        )
        nodes.append(node_inventory)
        prev_node = node_inventory
        output_node = node_inventory

    if include_address_validation:
        node_address: TaskNode[Any] = TaskNode(
            fn=check_address,
            waits_for=[prev_node] if prev_node else [],
            args_from={"order_result": node_validate},
            node_id="check_address",
        )
        nodes.append(node_address)
        output_node = node_address

    workflow = app.workflow(
        name=f"order_{order.order_id}",
        tasks=nodes,
        output=output_node,
        on_error=OnError.FAIL,
    )
    reveal_type(workflow)  # WorkflowSpec

    start_result = workflow.start()
    handle = start_result.ok_value
    reveal_type(handle)  # WorkflowHandle

    # Wait for workflow completion
    handle.get(timeout_ms=30000)

    # result_for() is non-blocking - returns RESULT_NOT_READY if task hasn't completed
    result = handle.result_for(output_node)
    reveal_type(result)  # TaskResult[Any, TaskError]
```

## Fan-Out Pattern (Parallel Execution)

Run many independent tasks in parallel. Queue `max_concurrency` controls parallelism.

```python
from horsies import TaskResult, TaskError
from horsies import TaskNode, AnyNode

# List of warehouse sync tasks
ALL_WAREHOUSE_TASKS = [
    sync_warehouse_berlin,
    sync_warehouse_munich,
    sync_warehouse_hamburg,
    sync_warehouse_frankfurt,
    sync_warehouse_cologne,
]

@app.task('sync_all_warehouses', queue_name='low')
def sync_all_warehouses_task() -> TaskResult[dict[str, int], TaskError]:
    # Create nodes with node_id from task name
    nodes: list[AnyNode] = [
        TaskNode(fn=task, node_id=task.task_name)
        for task in ALL_WAREHOUSE_TASKS[:-1]
    ]

    # Keep typed reference to last node for result_for()
    last_node: TaskNode[WarehouseSyncResult] = TaskNode(
        fn=ALL_WAREHOUSE_TASKS[-1],
        node_id=ALL_WAREHOUSE_TASKS[-1].task_name,
    )
    reveal_type(last_node)  # TaskNode[WarehouseSyncResult]
    nodes.append(last_node)

    # No waits_for = all run in parallel
    # Queue max_concurrency limits simultaneous execution
    workflow = app.workflow(
        name='sync_all_warehouses',
        tasks=nodes,
        output=last_node,
    )
    reveal_type(workflow)  # WorkflowSpec

    start_result = workflow.start()
    handle = start_result.ok_value
    reveal_type(handle)  # WorkflowHandle

    # Wait for workflow completion before accessing result_for()
    handle.get(timeout_ms=60000)

    result = handle.result_for(last_node)
    reveal_type(result)  # TaskResult[WarehouseSyncResult, TaskError]

    return TaskResult(ok={'warehouses_synced': len(ALL_WAREHOUSE_TASKS)})
```

## Linear Chain Pattern

Sequential execution where each task waits for the previous.

Key is to use `waits_for` with one of the target previous nodes.

```python
from .tasks import (
    validate_order,
    check_inventory,
    reserve_inventory,
    create_shipment,
    send_notification,
    update_tracking,
)

def fulfillment_workflow(order: Order) -> None:
    node_validate: TaskNode[ValidatedOrder] = TaskNode(
        fn=validate_order,
        kwargs={'order': order},
        node_id="validate_order",
    )
    reveal_type(node_validate)  # TaskNode[ValidatedOrder]

    node_inventory = TaskNode(
        fn=check_inventory,
        waits_for=[node_validate],
        args_from={"order_result": node_validate},
        node_id="check_inventory",
    )
    node_reserve = TaskNode(
        fn=reserve_inventory,
        waits_for=[node_inventory],
        args_from={"inventory_result": node_inventory},
        node_id="reserve_inventory",
    )
    node_shipment = TaskNode(
        fn=create_shipment,
        waits_for=[node_reserve],
        args_from={"reservation_result": node_reserve},
        node_id="create_shipment",
    )
    node_notify = TaskNode(
        fn=send_notification,
        waits_for=[node_shipment],
        args_from={"shipment_result": node_shipment},
        node_id="send_notification",
    )
    node_tracking: TaskNode[TrackingResult] = TaskNode(
        fn=update_tracking,
        waits_for=[node_notify],
        args_from={"notification_result": node_notify},
        node_id="update_tracking",
    )
    reveal_type(node_tracking)  # TaskNode[TrackingResult]

    workflow = app.workflow(
        name=f"fulfillment_{order.order_id}",
        tasks=[
            node_validate,
            node_inventory,
            node_reserve,
            node_shipment,
            node_notify,
            node_tracking,
        ],
        output=node_tracking,
        on_error=OnError.FAIL,
    )
    reveal_type(workflow)  # WorkflowSpec

    workflow.start()  # Fire-and-forget (Result discarded)
```

## Fire-and-Forget Pattern

Start a workflow without waiting for completion. Useful for background processing.

```python
def trigger_shipment_tracking(shipment_id: str) -> bool:
    node_fetch: TaskNode[CarrierStatus] = TaskNode(
        fn=fetch_carrier_status,
        kwargs={'shipment_id': shipment_id},
        node_id="fetch_carrier_status",
    )
    reveal_type(node_fetch)  # TaskNode[CarrierStatus]

    node_update = TaskNode(
        fn=update_shipment_status,
        waits_for=[node_fetch],
        args_from={"carrier_result": node_fetch},
        node_id="update_shipment_status",
    )
    node_notify: TaskNode[NotificationResult] = TaskNode(
        fn=notify_customer,
        waits_for=[node_update],
        args_from={"status_result": node_update},
        node_id="notify_customer",
    )
    reveal_type(node_notify)  # TaskNode[NotificationResult]

    workflow = app.workflow(
        name=f"tracking_{shipment_id}",
        tasks=[node_fetch, node_update, node_notify],
        output=node_notify,
    )
    reveal_type(workflow)  # WorkflowSpec

    # Start and return immediately
    start_result = workflow.start()
    handle = start_result.ok_value
    reveal_type(handle)  # WorkflowHandle

    return True  # Workflow runs in background
```

## Task-Wrapping Workflows

A task that orchestrates a workflow internally. Useful for composing complex operations as a single schedulable unit.

```python
@app.task('process_returns', queue_name='standard')
def process_returns_task() -> TaskResult[dict[str, str], TaskError]:
    # Define parallel nodes - process returns from multiple channels
    node_web: TaskNode[ReturnStats] = TaskNode(
        fn=process_web_returns,
        node_id="process_web_returns",
    )
    node_store: TaskNode[ReturnStats] = TaskNode(
        fn=process_store_returns,
        node_id="process_store_returns",
    )
    node_phone: TaskNode[ReturnStats] = TaskNode(
        fn=process_phone_returns,
        node_id="process_phone_returns",
    )

    # Aggregate results
    node_aggregate: TaskNode[AggregatedStats] = TaskNode(
        fn=aggregate_return_stats,
        waits_for=[node_web, node_store, node_phone],
        args_from={
            "web_result": node_web,
            "store_result": node_store,
            "phone_result": node_phone,
        },
        node_id="aggregate_return_stats",
    )
    reveal_type(node_aggregate)  # TaskNode[AggregatedStats]

    workflow = app.workflow(
        name='returns_processing',
        tasks=[node_web, node_store, node_phone, node_aggregate],
        output=node_aggregate,
        on_error=OnError.FAIL,
    )
    reveal_type(workflow)  # WorkflowSpec

    start_result = workflow.start()
    handle = start_result.ok_value
    reveal_type(handle)  # WorkflowHandle

    # Wait for workflow completion before accessing result_for()
    handle.get(timeout_ms=60000)

    result = handle.result_for(node_aggregate)
    reveal_type(result)  # TaskResult[AggregatedStats, TaskError]

    return TaskResult(ok={'message': 'Returns processed'})
```

## Pattern Summary

| Pattern         | `waits_for`    | Use Case                      |
|-----------------|----------------|-------------------------------|
| Fan-out         | `[]` (empty)   | Parallel independent tasks    |
| Linear chain    | `[prev_node]`  | Sequential dependencies       |
| Diamond         | Multiple deps  | Fan-out then fan-in           |
| Dynamic         | Conditional    | Runtime workflow construction |
| Fire-and-forget | N/A            | Background processing         |
