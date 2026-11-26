---
title: Defining Workflows
section: python
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

For both approaches, workflows need a stable `definition_key`. In class-based workflows you declare it on the `WorkflowDefinition`; in imperative workflows you pass it to `app.workflow(...)`.

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

- **Typed results:** `handle.result_for(node)` returns `ReadResult[T, TaskError]` only when `node_id` is set
- **Stable observability:** Index-based IDs shift when you reorder tasks
- **Meaningful tracing:** `validate_order` is clearer than `order_processing:0` in logs

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Recommended: explicit node_id
TaskNode(fn=validate_order, node_id="validate_order")

# Avoid: relying on auto-assignment
TaskNode(fn=validate_order)  # becomes "order_processing:0"
```

For type-safe node construction with static arguments, see [Typed Node Builder](/horsies/concepts/workflows/typed-node-builder/).

## Declarative Workflow

Define nodes at module level with explicit `node_id`, then assign to class attributes:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import TaskNode, WorkflowDefinition, WorkflowDefinitionConfig, OnError

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
    definition_key = "myapp.order_processing.v1"

    validate = _validate_node
    inventory = _inventory_node
    shipping_cost = _cost_node
    address = _address_node
    reserve = _reserve_node
    shipment = _shipment_node
    notify = _notify_node

    config = WorkflowDefinitionConfig(
        output=_notify_node,
        on_error=OnError.FAIL,
    )
```

## Starting a Workflow

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Err, Failed, Ok, Succeeded

spec = OrderProcessingWorkflow.build(app)
reveal_type(spec)  # WorkflowSpec[NotificationResult]

# start() returns Ok(handle) or Err(WorkflowStartError)
match spec.start():
    case Err(start_error):
        print(f"Start failed: {start_error.code} - {start_error.message}")
    case Ok(handle):
        reveal_type(handle)  # WorkflowHandle[NotificationResult]

        handle.get(timeout_ms=30000)  # Wait for completion

        # Type-safe result: ReadResult[NotificationResult, TaskError]
        result = handle.result_for(OrderProcessingWorkflow.notify)
        reveal_type(result)  # ReadResult[NotificationResult, TaskError]

        match result:
            case Ok(Succeeded(notification)):
                reveal_type(notification)  # NotificationResult
                print(f"Order {notification.order_id} processed")
            case Ok(Failed(error)):
                reveal_type(error)  # TaskError
                print(f"Failed: {error.error_code} - {error.message}")
            case Err(read_error):
                print(f"Read failed: {read_error.code} - {read_error.message}")
```

## Imperative Workflow

Define nodes within a function scope, then pass to `app.workflow()`:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Err, Failed, Ok, Succeeded

def start_order_processing(order: Order) -> None:
    """Start the order processing workflow."""
    validate_node: TaskNode[ValidatedOrder] = TaskNode(
        fn=validate_order,
        kwargs={'order': order},
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
        definition_key="myapp.order_processing.v1",
        output=notify_node,
        on_error=OnError.FAIL,
    )
    reveal_type(spec)  # WorkflowSpec

    match spec.start():
        case Err(start_error):
            raise RuntimeError(start_error.message)
        case Ok(handle):
            pass
    reveal_type(handle)  # WorkflowHandle[NotificationResult]

    handle.get(timeout_ms=30000)

    # Type-safe result access
    result = handle.result_for(notify_node)
    reveal_type(result)  # ReadResult[NotificationResult, TaskError]

    match result:
        case Ok(Succeeded(notification)):
            reveal_type(notification)  # NotificationResult
            print(f"Order {notification.order_id} processed successfully")
        case Ok(Failed(error)):
            reveal_type(error)  # TaskError
            print(f"Order processing failed: {error.error_code} - {error.message}")
        case Err(read_error):
            print(f"Read failed: {read_error.code} - {read_error.message}")
```

## Handling Upstream Results

`args_from` injects upstream `TaskOutcome` values as keyword arguments into the receiving task function: `Succeeded(value)` or `Failed(task_error)`. Use `match` to get the value or the error:

**Note:** When using `args_from` or `workflow_context_from`, positional `args` are not allowed. Put static inputs in `kwargs` instead.

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task("check_inventory", queue_name="standard")
def check_inventory(
    *,
    order_result: TaskOutcome[ValidatedOrder, TaskError],
) -> Result[InventoryStatus, TaskError]:
    match order_result:
        case Failed():
            return Err(TaskError(
                error_code="UPSTREAM_FAILED",
                message="Cannot check inventory: order validation failed",
            ))
        case Succeeded(order):  # ValidatedOrder
            status = InventoryStatus(
                order_id=order.order_id,
                all_available=True,
                item_availability={item.sku: True for item in order.items},
            )
            return Ok(status)
```

When multiple nodes converge into a single task (fan-in), each upstream result arrives as a separate keyword argument:

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task("reserve_inventory", queue_name="urgent")
def reserve_inventory(
    *,
    inventory_result: TaskOutcome[InventoryStatus, TaskError],
    cost_result: TaskOutcome[ShippingCost, TaskError],
    address_result: TaskOutcome[AddressValidation, TaskError],
) -> Result[Reservation, TaskError]:
    match (inventory_result, cost_result, address_result):
        case (Failed(), _, _):
            return Err(TaskError(
                error_code="INVENTORY_CHECK_FAILED",
                message="Cannot reserve: inventory check failed",
            ))
        case (_, Failed(), _):
            return Err(TaskError(
                error_code="COST_CALCULATION_FAILED",
                message="Cannot reserve: shipping cost calculation failed",
            ))
        case (_, _, Failed()):
            return Err(TaskError(
                error_code="ADDRESS_VALIDATION_FAILED",
                message="Cannot reserve: address validation failed",
            ))
        case (Succeeded(inventory), Succeeded(cost), Succeeded(_address)):
            reservation = Reservation(
                order_id=inventory.order_id,
                reservation_id=str(uuid.uuid4()),
                reserved_items=[],
                reserved_at=datetime.now(),
                shipping_cost_cents=cost.total_cost_cents,
            )
            return Ok(reservation)
```

The kwarg names must match the keys defined in `args_from`. For example, the `reserve_inventory` wiring above uses `"inventory_result"`, `"cost_result"`, and `"address_result"` as keys — these correspond directly to the function parameter names.

## Key Concepts

| Concept | Description |
|---------|-------------|
| `node_id` | Unique identifier for observability and `result_for()` |
| `waits_for` | List of nodes that must complete before this task runs |
| `args_from` | Inject the upstream `TaskOutcome` as kwargs |
| `output` | Node whose result becomes the workflow result |
| `on_error` | `FAIL` continues DAG, `PAUSE` stops immediately |
| `result_for(node)` | Non-blocking type-safe result access. Returns `Err(ResultNotReady(...))` if task hasn't completed |
