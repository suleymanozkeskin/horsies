---
title: Workflow API
summary: Signatures and return types for the current Rust workflow surface.
tags: [concepts, workflows, api, reference]
---

# Workflow API

This page is a reference map of the current Rust workflow surface.

For teaching-oriented examples, see:

- [Typed Node Builder](./typed-node-builder)
- [Workflows and Subworkflows](./subworkflows)
- [Workflow Semantics](./workflow-semantics)

## `WorkflowSpec`

| Field / Method | Type | Description |
|---|---|---|
| `name` | `String` | Workflow name |
| `definition_key` | `Option<String>` | Stable definition identity |
| `tasks` | `Vec<AnyNode>` | All nodes in the DAG |
| `on_error` | `OnError` | Workflow error policy |
| `output_index` | `Option<usize>` | Output node index |
| `success_policy` | `Option<SuccessPolicy>` | Optional custom success criteria |

## `WorkflowSpecBuilder`

| Method | Signature | Description |
|---|---|---|
| `new(name)` | `fn(impl Into<String>) -> Self` | Create a builder |
| `task(node)` | `fn(&mut self, TaskNode<T, I>) -> TypedNodeRef<T>` | Add a task node |
| `sub_workflow(node)` | `fn(&mut self, SubWorkflowNode<P, T>) -> TypedNodeRef<T>` | Add a child workflow node |
| `definition_key(key)` | `fn(&mut self, impl Into<String>) -> &mut Self` | Set definition key |
| `on_error(policy)` | `fn(&mut self, OnError) -> &mut Self` | Set error policy |
| `output(node_ref)` | `fn<R: Into<NodeRef>>(&mut self, R) -> &mut Self` | Set output node |
| `success_policy(policy)` | `fn(&mut self, SuccessPolicy) -> &mut Self` | Set success policy |
| `build()` | `fn(self) -> Result<WorkflowSpec, HorsiesError>` | Build and validate |

## `WorkflowDefinition`

```rust
pub trait WorkflowDefinition {
    type Output;
    type Params;

    fn name() -> &'static str;
    fn definition_key() -> &'static str;
    fn on_error() -> OnError { OnError::Fail }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError>;
    fn build() -> Result<WorkflowSpec, HorsiesError>;
    fn build_registered() -> Result<RegisteredWorkflowSpec, HorsiesError>;
    fn build_with(params: Self::Params) -> Result<WorkflowSpec, HorsiesError>;
}
```

Use this trait in two modes:

- static workflow: implement `define(...)` and use `Params = ()`
- parameterized workflow: implement `build_with(params)`
- `define(...)` defaults to an error, so parameterized workflows do not need a placeholder `define(...)`

## `WorkflowDefConfig`

```rust
pub struct WorkflowDefConfig {
    pub output: Option<NodeRef>,
    pub success_policy: Option<SuccessPolicy>,
}
```

Builder methods:

```rust
WorkflowDefConfig::new()
    .output(node_ref)
    .success_policy(policy)
```

## `WorkflowFunction<T>`

Returned by `app.register_workflow_definition::<D>()` for zero-param workflows.

| Method | Description |
|---|---|
| `name()` | Workflow name |
| `definition_key()` | Optional definition key |
| `spec()` | Resolved workflow spec |
| `tasks()` | Node list |
| `start().await` | Start the workflow |
| `start_with_id(id).await` | Start with explicit workflow ID |
| `retry_start(&err).await` | Retry a retryable start failure |
| `handle(workflow_id).await` | Get a handle for an existing workflow |

## `WorkflowTemplate<P, T>`

Returned by `app.workflow_template::<D>()` or `app.register_parameterized_workflow(...)` for parameterized workflows.

| Method | Description |
|---|---|
| `name()` | Workflow name |
| `definition_key()` | Definition key |
| `node()` | Create a typed child-workflow node bound to this template |
| `build(params)` | Build a spec from params |
| `start(params).await` | Build and start |
| `start_with_id(params, id).await` | Build and start with explicit workflow ID |

## Global Start Helpers

```rust
pub async fn start_workflow<D>() -> WorkflowStartResult<WorkflowHandle<D::Output>>
where
    D: WorkflowDefinition<Params = ()> + 'static;

pub async fn start_workflow_with<D>(
    params: D::Params,
) -> WorkflowStartResult<WorkflowHandle<D::Output>>
where
    D: WorkflowDefinition + 'static;
```

Registration requirements:

- `start_workflow::<D>()` requires `app.register_workflow_definition::<D>()`
- `start_workflow_with::<D>(...)` requires `app.workflow_template::<D>()`

## `WorkflowStartResult<T>`

```rust
type WorkflowStartResult<T> = Result<T, WorkflowStartError>;
```

Important fields on `WorkflowStartError`:

| Field | Type |
|---|---|
| `code` | `WorkflowStartErrorCode` |
| `message` | `String` |
| `retryable` | `bool` |
| `workflow_name` | `String` |
| `workflow_id` | `String` |

Main codes:

| Code | When |
|---|---|
| `ValidationFailed` | Spec build/validation failed |
| `EnqueueFailed` | Database/broker enqueue failed |
| `InternalFailed` | Internal/runtime failure before start |

## `WorkflowHandle<T>`

Rust uses a split error model here:

- `get()` and `result_for()` fold retrieval/infra errors into `TaskResult::Err(TaskError)`
- `status()`, `results()`, `tasks()`, `cancel()`, `pause()`, `resume()` return `HandleResult<_>`

| Method | Signature | Description |
|---|---|---|
| `workflow_id()` | `&str` | Workflow instance ID |
| `get(timeout)` | `async -> TaskResult<T>` | Wait for completion |
| `result_for(node_id)` | `async -> TaskResult<V>` | Get one node result by node ID |
| `result_for_key(key)` | `async -> TaskResult<V>` | Get one node result by typed `NodeKey<V>` |
| `status()` | `async -> HandleResult<WorkflowStatus>` | Current status |
| `results()` | `async -> HandleResult<HashMap<String, TaskResult<Value>>>` | All results |
| `tasks()` | `async -> HandleResult<Vec<WorkflowTaskInfo>>` | Workflow task info |
| `cancel()` | `async -> HandleResult<()>` | Cancel workflow |
| `pause()` | `async -> HandleResult<bool>` | Pause workflow |
| `resume()` | `async -> HandleResult<bool>` | Resume workflow |

Example:

```rust
use std::time::Duration;
use horsies::TaskResult;

match workflow.start().await {
    Ok(handle) => match handle.get(Some(Duration::from_secs(30))).await {
        TaskResult::Ok(value) => println!("Success: {:?}", value),
        TaskResult::Err(err) => println!("Workflow failed: {:?}", err.error_code),
    },
    Err(err) if err.retryable => {
        let _retry = workflow.retry_start(&err).await?;
    }
    Err(err) => {
        println!("[{}] {}", err.code, err.message);
    }
}
```

## `TaskNode<T, I>`

The stable public authoring path is through generated task modules:

```rust
my_task::node()?
```

Main builder methods:

| Method | Description |
|---|---|
| `.set_input(value)?` | Bind the whole explicit input |
| `.set(field, value)?` | Bind one explicit input field |
| `.arg_from(field, dep)` | Inject `TaskResult<S>` from an upstream typed node |
| `.waits_for(dep)` | Add one dependency |
| `.waits_for_all(deps)` | Add multiple dependencies |
| `.workflow_ctx_from(deps)` | Select context source nodes |
| `.node_id(id)` | Set stable identifier |
| `.queue(queue)` | Override queue |
| `.priority(p)` | Override priority |
| `.allow_failed_deps(bool)` | Run even if upstream failed |
| `.join_all()` | All-deps join |
| `.join_any()` | Any-success join |
| `.join_quorum(min)` | Quorum join |
| `.good_until(dt)` | Expiry deadline |
| `.task_options(json)` | Raw task options JSON |

Low-level JSON setters (`args_json`, `kwargs_json`) still exist, but they are not the recommended authoring path.

## `SubWorkflowNode<P, T>`

`SubWorkflowNode` represents a child workflow inside a parent DAG.

Preferred public constructors:

- `template.node()` when you already have a registered `WorkflowTemplate<P, T>`
- `SubWorkflowNode::from_definition::<D>()` when composing from a `WorkflowDefinition`
- `SubWorkflowNode::typed(name)` / `SubWorkflowNode::new(name)` for lower-level or untyped paths

Main methods:

| Method | Description |
|---|---|
| `SubWorkflowNode::typed(spec_name)` | Create a typed child node |
| `SubWorkflowNode::new(spec_name)` | Convenience for untyped/JSON child output |
| `.set_input(value)?` | Bind the whole child params value explicitly |
| `.set(field, value)?` | Bind one child param field explicitly |
| `.arg_from(field, dep)` | Inject a `TaskResult<S>` into child params |
| `.waits_for(dep)` | Add dependency |
| `.waits_for_all(deps)` | Add multiple dependencies |
| `.workflow_ctx_from(deps)` | Select context source nodes |
| `.node_id(id)` | Set stable identifier |
| `.queue(q)` | Override queue |
| `.priority(p)` | Override priority |
| `.allow_failed_deps(bool)` | Run even if upstream failed |
| `.join_all()` / `.join_any()` / `.join_quorum(min)` | Join semantics |
| `.good_until(dt)` | Expiry deadline |

## `TypedNodeRef<T>` and `NodeRef`

```rust
pub struct TypedNodeRef<T> { pub index: usize, ... }
pub struct NodeRef { pub index: usize }
```

- `TypedNodeRef<T>` is returned by `builder.task(...)` and `builder.sub_workflow(...)`
- use it for typed `.arg_from(...)`
- erase to `NodeRef` with `.into()` only when you need heterogeneous collections

## Registration on `Horsies`

| Method | Description |
|---|---|
| `register_workflow_definition::<D>()` | Register a zero-param `WorkflowDefinition` |
| `workflow_template::<D>()` | Get a `WorkflowTemplate` for parameterized workflows |
| `register_parameterized_workflow_with_children::<P, T, _>(...)` | Register a parameterized child workflow and declare child workflow definition keys for static cycle checks |
| `register_workflow_spec::<T>(spec)` | Register a pre-built spec |
| `start::<T>(spec)` | Start an ad hoc spec |
| `check_workflow_builder(name, builder)` | Register representative builder cases for `app.check()` |
| `check_workflow_builder0(name, builder)` | Zero-param variant |

Practical selection:

- use `register_workflow_definition::<D>()` for reusable zero-param workflows
- use `workflow_template::<D>()` when `D: WorkflowDefinition` is parameterized
- use `register_parameterized_workflow(...)` when you want a lightweight builder closure instead of a `WorkflowDefinition`
- use `register_parameterized_workflow_with_children(...)` when that builder may reference child workflows and you want static cycle checks at registration time
