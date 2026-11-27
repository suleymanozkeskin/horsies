---
title: Typed Node Builder
summary: Build the task nodes of a workflow with task_name::node(), the field tokens of task_name::fields, arg_from and the workflow context.
related: [workflow-api, subworkflows, ../../tasks/defining-tasks, ../../../concepts/workflows/workflow-semantics]
tags: [rust, workflow, TaskNode, InputField, arg_from, WorkflowContext]
---

# Typed Node Builder

A task node has the types of its task: `TaskNode<T, I>`, with `T` the task's output type and `I` its input type. The input is set through field tokens of the task's own module, so a field of another task, a value of another type, or a source of another output type is a compile error. A node names its task and reads no registration: the workflow builder of an app resolves it against that app when the node is added.

## Build a Node

`#[horsies::task]` generates `task_name::node()` and one field token per parameter, `task_name::fields::<name>()`.

```rust
use std::convert::Infallible;

use horsies::{task, Horsies, HorsiesError, TaskError, TaskOutcome, WorkflowSpec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    total_cents: u64,
}

#[task("charge_order")]
async fn charge_order(order: Order) -> Result<u64, TaskError<Infallible>> {
    Ok(order.total_cents)
}

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

fn checkout_spec(app: &Horsies, order: Order) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = app.workflow_builder("checkout");
    builder.definition_key("shop.checkout.v1");
    let order_id = order.order_id.clone();
    let charged = builder.task(
        charge_order::node()
            .node_id("charge")
            .set(charge_order::fields::order(), order)?,
    )?;
    let receipt = builder.task(
        send_receipt::node()
            .node_id("receipt")
            .set(send_receipt::fields::order_id(), order_id)?
            .arg_from(send_receipt::fields::charged(), charged),
    )?;
    builder.output(receipt);
    builder.build()
}
```

| Source | Returns |
|--------|---------|
| `task_name::node()` | `TaskNode<T, task_name::Input>`: a node of the task. It reads no registration |
| `function.node()` | `TaskNode<T, A>` of a `TaskFunction<A, T, E>`, the same node |
| `task_name::fields::<name>()` | `InputField<task_name::Input, F>`: the token of the parameter `<name>` of type `F` |

A new node has the input `{}`. Add it with `builder.task(node)`, which returns `Result<TypedNodeRef<T>, HorsiesError>`. `builder.task` gives the node the queue (unless the node names one with `.queue(name)`), the priority of that queue, the options and the workflow context flag of the task's registration on the builder's app. It refuses with `HRS-302` (`HorsiesErrorCode::WorkflowUnregisteredTask`) when the builder's app does not register the task, or registers it with another input or output type. The node is then not added.

## Set the Input

| Method | Does | Refusal |
|--------|------|---------|
| `.set(field, value)` | Sets one parameter. The value is encoded and read back as a send does | `HRS-031`: a value that does not encode or read back as `F` |
| `.set_input(input)` | Sets the whole input from a value of `task_name::Input`. Replaces every member set before | `HRS-031`; `HRS-026` for an input type that does not encode as a JSON object |
| `.input_json(text)` | Sets the whole input from the text of one JSON object, as written | `HRS-026` for a text that is not an object; `HRS-031` for a text that breaks the strict JSON rules; `HRS-019` for a member name of the engine (`__h_...`, `__horsies_...`) |

Each method returns `Result<TaskNode<T, I>, HorsiesError>`. The text of `input_json` is in the stored form: one member per parameter, by name. See [Defining Tasks](/horsies/rust/tasks/defining-tasks/#define-a-task).

`WorkflowSpecBuilder::build` decodes the input of each node with the task's input type:

| Input | Code |
|-------|------|
| A key of the input that binds to no parameter | `HRS-019` |
| A parameter with no value and no `arg_from` | `HRS-020` |
| A key that binds to no field, or a field with no value, inside the value of a parameter | `HRS-031` |
| A value of another type | `HRS-031` |
| A parameter that both `set` and `arg_from` give | `HRS-021` |

## Take a Dependency's Result

`.arg_from(field, source)` gives the outcome of `source` to a parameter of type `TaskOutcome<S, E>`, and the node waits for `source`.

- `S` is the output type of `source`. A source of another output type is a compile error.
- `E` reads the custom codes of the source's task. `horsies::CustomCode` reads any custom code as checked text. A stored custom code that `E` does not name fails the input decode of the run with `WORKER_SERIALIZATION_ERROR`.
- `arg_from` refuses nothing when it is called. `build` checks the node.

What the parameter gets:

| Dependency | Default | `allow_failed_dependencies(true)` |
|------------|---------|---------------------------|
| COMPLETED | The node runs with `TaskOutcome::Succeeded(value)` | The same |
| FAILED | The node is SKIPPED | The node runs with `TaskOutcome::Failed(error)` |
| SKIPPED | The node is SKIPPED | The node runs with `Failed` and the code `UPSTREAM_SKIPPED` |

A `TaskOutcome<S, E>` parameter has no default. A node that joins on any or on a quorum can start before a source ended, so `build` refuses `arg_from` on such a node with `HRS-013`. Give that node its dependency results through the workflow context. The rules are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/#dependency-semantics).

## Read Results through the Workflow Context

A task that takes the workflow context has the attribute `workflow_context` and a parameter named `workflow_context` of type `WorkflowContext`. The node names the dependencies whose results the context holds with `.workflow_context_from(nodes)`.

```rust
use std::convert::Infallible;

use horsies::{task, NodeKey, TaskError, TaskOutcome, WorkflowContext};

#[task("audit_checkout", workflow_context)]
async fn audit_checkout(workflow_context: WorkflowContext) -> Result<bool, TaskError<Infallible>> {
    let charged = workflow_context.result_for::<u64, Infallible>(&NodeKey::new("charge"));
    Ok(matches!(charged, Ok(TaskOutcome::Succeeded(_))))
}
```

In the spec of [Build a Node](#build-a-node):

<!-- rust-check: skip partial -- a fragment of checkout_spec above: builder and charged are its locals -->
```rust
let _audit = builder.task(
    audit_checkout::node()
        .waits_for(charged)
        .workflow_context_from([charged]),
)?;
```

- Each node of `workflow_context_from` must be a dependency of the node: `HRS-009` otherwise. `waits_for` and `arg_from` make a dependency.
- `workflow_context_from` on a task without the `workflow_context` attribute is `HRS-010`.
- The worker writes the context into the parameter `workflow_context` only for a node with `workflow_context_from`. A node of such a task without `workflow_context_from` is `HRS-020`: its parameter has no value.
- Give each context source a node id, and read it with `NodeKey::new(id)`. `WorkflowSpec::key(node_ref)` and `TaskNode::key()` also give the key.

| `WorkflowContext` method | Returns |
|--------------------------|---------|
| `workflow_id()` | The workflow's `Uuid` |
| `node_index()` | The index of the task's node |
| `task_name()` | The task name of the node |
| `result_for::<T, E>(&key)` | `Result<TaskOutcome<T, E>, WorkflowContextError>` |
| `has_result(node_id)` | Whether the context holds the node's result |
| `summary_for(&key)` | `Result<&SubWorkflowSummary, WorkflowContextError>` of a sub-workflow node |
| `output_for::<T>(&key)` | `Result<Option<T>, WorkflowContextError>`: the output of a sub-workflow node's child; `None` when the child stored none |
| `has_summary(node_id)` | Whether the context holds the node's summary |

| `WorkflowContextError` | When |
|------------------------|------|
| `NotInContext { node_id }` | The context holds no entry for the node: it is not in `workflow_context_from`, or it did not end. The core code for this state is `NODE_NOT_IN_CONTEXT` |
| `ResultDeserializationError { node_id, cause }` | The stored value does not read as the asked type; `cause` names no value |
| `UnknownCustomCode { node_id, cause }` | The stored error has a custom code that `E` does not name |

## Node Options

| Method | Does |
|--------|------|
| `.waits_for(node)`, `.waits_for_all(nodes)` | Waits for the nodes, with no data |
| `.node_id(id)` | Sets the node id: unique in the spec (`HRS-004`), `[A-Za-z0-9_\-:.]`, at most 128 characters (`HRS-003`). Without one the node gets `<workflow name slug>:<index>` |
| `.queue(name)` | Runs the task on this queue in place of the task's queue |
| `.priority(p)` | Claims the task with priority 1 (first) to 100 (last), in place of the queue's priority. Another value is `HRS-034` |
| `.allow_failed_dependencies(true)` | Runs the node when a dependency failed or was skipped |
| `.join_all()` (default), `.join_any()`, `.join_quorum(min_success)` | When the node becomes ready. `min_success` is 1 to the node's dependencies (`HRS-013`) |
| `.good_until(deadline)` | The task expires when it has not started by `deadline: DateTime<Utc>` |
| `.key()` | `Option<NodeKey<T>>`: the key of the node's result when the node has a given node id |
| `.task_name()` | The task name |

`JoinType` is `All`, `Any` or `Quorum { min_success }`. The join rules are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/#join-modes).

## References and Keys

| Type | Is |
|------|----|
| `TypedNodeRef<T>` | What `builder.task` and `builder.sub_workflow` return: a node of this builder with the output type `T`. `Copy`. `untyped()` returns its `NodeRef`; it converts with `Into<NodeRef>` |
| `NodeRef` | A reference with no output type. `waits_for`, `workflow_context_from`, `output` and `SuccessCase::new` take `Into<NodeRef>` |
| `NodeKey<T>` | The key of a node's result: its node id, with the output type `T`. `NodeKey::new(node_id)`, `node_id()` |
| `InputField<I, F>` | The token of the field `F` of the input type `I`. `name()` returns the member name |

## Things to Avoid

**Do not take a dependency's result in a node that joins on any or on a quorum.** `arg_from` on such a node is `HRS-013`. Read the results through the workflow context.

**Do not name a context source that the node does not wait for.** `workflow_context_from` without a dependency on the node is `HRS-009`.

<!-- rust-check: skip elided -- a Wrong / Correct pair of expressions -->
```rust
// Wrong
audit_checkout::node().workflow_context_from([charged])

// Correct
audit_checkout::node().waits_for(charged).workflow_context_from([charged])
```

**Do not add a node to a builder of an app that does not register its task.** `builder.task(node)` is then `HRS-302`. Register the task on the app that makes the builder.

**Do not write the fields of a struct parameter at the top level of `input_json`.** A task `fn charge_order(order: Order)` reads `{"order": {...}}`.
