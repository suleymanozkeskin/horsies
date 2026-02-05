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

We cover two approaches: **declarative** (class-based) and **imperative** (function-scoped). Both produce equivalent `WorkflowSpec` objects.

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

Each `TaskNode` requires a `node_id` for identification within the workflow.

**Auto-assignment behavior:**

| Context                            | Default `node_id`                                      |
|------------------------------------|--------------------------------------------------------|
| Declarative (`WorkflowDefinition`) | Attribute name (e.g., `validate`)                      |
| Imperative (`app.workflow()`)      | `{workflow_name}:{index}` (e.g., `order_processing:0`) |

**We strongly recommend explicit `node_id` values:**

- **Typed results:** `handle.result_for(node)` returns `TaskResult[T, TaskError]` only when `node_id` is set
- **Stable observability:** Index-based IDs shift when you reorder tasks
- **Meaningful tracing:** `validate_order` is clearer than `order_processing:0` in logs

```python
# Recommended: explicit node_id
TaskNode(fn=validate_order, node_id="validate_order")

# Avoid: relying on auto-assignment
TaskNode(fn=validate_order)  # becomes "order_processing:0"
```

For type-safe node construction with static arguments, see [Typed Node Builder](../concepts/workflows/typed-node-builder).

## Declarative Workflow

Define nodes at module level with explicit `node_id`, then assign to class attributes:

```python
from horsies import TaskNode, WorkflowDefinition, OnError

# Module-level nodes with explicit node_id
_validate_node: TaskNode[ValidatedOrder] = TaskNode(
    fn=validate_order,
    node_id="validate",
)
reveal_type(_validate_node)  # TaskNode[ValidatedOrder]

_inventory_node = TaskNode(
    fn=check_inventory,
    waits_for=[_validate_node],
    args_from={"order_result": _validate_node},
    node_id="inventory",
)
reveal_type(_inventory_node)  # TaskNode[InventoryStatus]

_cost_node = TaskNode(
    fn=calculate_shipping_cost,
    waits_for=[_validate_node],
    args_from={"order_result": _validate_node},
    node_id="shipping_cost",
)
reveal_type(_cost_node)  # TaskNode[ShippingCost]

_address_node = TaskNode(
    fn=check_address,
    waits_for=[_validate_node],
    args_from={"order_result": _validate_node},
    node_id="address",
)
reveal_type(_address_node)  # TaskNode[AddressValidation]

_reserve_node: TaskNode[Reservation] = TaskNode(
    fn=reserve_inventory,
    waits_for=[_inventory_node, _cost_node, _address_node],
    args_from={
        "inventory_result": _inventory_node,
        "cost_result": _cost_node,
        "address_result": _address_node,
    },
    node_id="reserve",
)
reveal_type(_reserve_node)  # TaskNode[Reservation]

_shipment_node: TaskNode[Shipment] = TaskNode(
    fn=create_shipment,
    waits_for=[_reserve_node],
    args_from={"reservation_result": _reserve_node},
    node_id="shipment",
)
reveal_type(_shipment_node)  # TaskNode[Shipment]

_notify_node: TaskNode[NotificationResult] = TaskNode(
    fn=send_notification,
    waits_for=[_shipment_node],
    args_from={"shipment_result": _shipment_node},
    node_id="notify",
)
reveal_type(_notify_node)  # TaskNode[NotificationResult]


class OrderProcessingWorkflow(WorkflowDefinition[NotificationResult]):
    """Process an order from validation to shipment notification."""

    name = "order_processing"

    validate = _validate_node
    inventory = _inventory_node
    shipping_cost = _cost_node
    address = _address_node
    reserve = _reserve_node
    shipment = _shipment_node
    notify = _notify_node

    class Meta:
        output = _notify_node
        on_error = OnError.FAIL
```

## Starting a Workflow

```python
spec = OrderProcessingWorkflow.build(app)
reveal_type(spec)  # WorkflowSpec

handle = spec.start()
reveal_type(handle)  # WorkflowHandle

handle.get(timeout_ms=30000)  # Wait for completion

# Type-safe result: TaskResult[NotificationResult, TaskError]
result = handle.result_for(OrderProcessingWorkflow.notify)
reveal_type(result)  # TaskResult[NotificationResult, TaskError]

if result.is_err():
    error = result.err_value
    reveal_type(error)  # TaskError
    print(f"Failed: {error.error_code} - {error.message}")
else:
    notification = result.ok_value
    reveal_type(notification)  # NotificationResult
    print(f"Order {notification.order_id} processed")
```

## Imperative Workflow

Define nodes within a function scope, then pass to `app.workflow()`:

```python
def start_order_processing(order: Order) -> None:
    """Start the order processing workflow."""
    validate_node: TaskNode[ValidatedOrder] = TaskNode(
        fn=validate_order,
        args=(order,),
        node_id="validate_order",
    )
    reveal_type(validate_node)  # TaskNode[ValidatedOrder]

    inventory_node = TaskNode(
        fn=check_inventory,
        waits_for=[validate_node],
        args_from={"order_result": validate_node},
        node_id="check_inventory",
    )

    cost_node = TaskNode(
        fn=calculate_shipping_cost,
        waits_for=[validate_node],
        args_from={"order_result": validate_node},
        node_id="calculate_cost",
    )

    address_node = TaskNode(
        fn=check_address,
        waits_for=[validate_node],
        args_from={"order_result": validate_node},
        node_id="check_address",
    )

    reserve_node = TaskNode(
        fn=reserve_inventory,
        waits_for=[inventory_node, cost_node, address_node],
        args_from={
            "inventory_result": inventory_node,
            "cost_result": cost_node,
            "address_result": address_node,
        },
        node_id="reserve_inventory",
    )

    shipment_node = TaskNode(
        fn=create_shipment,
        waits_for=[reserve_node],
        args_from={"reservation_result": reserve_node},
        node_id="create_shipment",
    )

    notify_node: TaskNode[NotificationResult] = TaskNode(
        fn=send_notification,
        waits_for=[shipment_node],
        args_from={"shipment_result": shipment_node},
        node_id="send_notification",
    )
    reveal_type(notify_node)  # TaskNode[NotificationResult]

    spec = app.workflow(
        name="order_processing",
        tasks=[
            validate_node,
            inventory_node,
            cost_node,
            address_node,
            reserve_node,
            shipment_node,
            notify_node,
        ],
        output=notify_node,
        on_error=OnError.FAIL,
    )
    reveal_type(spec)  # WorkflowSpec

    handle = spec.start()
    reveal_type(handle)  # WorkflowHandle

    handle.get(timeout_ms=30000)

    # Type-safe result access
    result = handle.result_for(notify_node)
    reveal_type(result)  # TaskResult[NotificationResult, TaskError]

    if result.is_err():
        error = result.err_value
        reveal_type(error)  # TaskError
        print(f"Order processing failed: {error.error_code} - {error.message}")
        return

    notification = result.ok_value
    reveal_type(notification)  # NotificationResult
    print(f"Order {notification.order_id} processed successfully")
```

## Handling Upstream Results

`args_from` injects upstream `TaskResult` values as keyword arguments into the receiving task function. Use `is_err()` to check for errors before accessing `.ok_value`:

**Note:** When using `args_from` or `workflow_ctx_from`, positional `args` are not allowed. Put static inputs in `kwargs` instead.

```python
@app.task("check_inventory", queue_name="standard")
def check_inventory(
    order_result: TaskResult[ValidatedOrder, TaskError],
) -> TaskResult[InventoryStatus, TaskError]:
    if order_result.is_err():
        return TaskResult(err=TaskError(
            error_code="UPSTREAM_FAILED",
            message="Cannot check inventory: order validation failed",
        ))

    order = order_result.ok_value  # ValidatedOrder (safe after is_err check)

    status = InventoryStatus(
        order_id=order.order_id,
        all_available=True,
        item_availability={item.sku: True for item in order.items},
    )
    return TaskResult(ok=status)
```

When multiple nodes converge into a single task (fan-in), each upstream result arrives as a separate keyword argument:

```python
@app.task("reserve_inventory", queue_name="urgent")
def reserve_inventory(
    inventory_result: TaskResult[InventoryStatus, TaskError],
    cost_result: TaskResult[ShippingCost, TaskError],
    address_result: TaskResult[AddressValidation, TaskError],
) -> TaskResult[Reservation, TaskError]:
    if inventory_result.is_err():
        return TaskResult(err=TaskError(
            error_code="INVENTORY_CHECK_FAILED",
            message="Cannot reserve: inventory check failed",
        ))

    if cost_result.is_err():
        return TaskResult(err=TaskError(
            error_code="COST_CALCULATION_FAILED",
            message="Cannot reserve: shipping cost calculation failed",
        ))

    if address_result.is_err():
        return TaskResult(err=TaskError(
            error_code="ADDRESS_VALIDATION_FAILED",
            message="Cannot reserve: address validation failed",
        ))

    inventory = inventory_result.ok_value
    cost = cost_result.ok_value
    address = address_result.ok_value

    reservation = Reservation(
        order_id=inventory.order_id,
        reservation_id=str(uuid.uuid4()),
        reserved_items=[],
        reserved_at=datetime.now(),
        shipping_cost_cents=cost.total_cost_cents,
    )
    return TaskResult(ok=reservation)
```

The kwarg names must match the keys defined in `args_from`. For example, the `reserve_inventory` wiring above uses `"inventory_result"`, `"cost_result"`, and `"address_result"` as keys — these correspond directly to the function parameter names.

## Key Concepts

| Concept | Description |
|---------|-------------|
| `node_id` | Unique identifier for observability and `result_for()` |
| `waits_for` | List of nodes that must complete before this task runs |
| `args_from` | Inject upstream `TaskResult` as kwargs |
| `output` | Node whose result becomes the workflow result |
| `on_error` | `FAIL` continues DAG, `PAUSE` stops immediately |
| `result_for(node)` | Non-blocking type-safe result access. Returns `RESULT_NOT_READY` error if task hasn't completed |
