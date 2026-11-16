---
title: Typed Node Builder
summary: Build workflow nodes with Rust type checking using `task::node()?`.
tags: [concepts, workflows, types, api]
---

# Typed Node Builder

Rust workflow composition in Horsies is built around generated task modules, parameter tokens, and typed node references.

The normal authoring flow is:

1. define a task with `#[horsies::task]`
2. register it at startup with `task_name::register(&mut app)?`
3. create a node with `task_name::node()?`
4. bind inputs with one of:
   - `.set_input(...)`
   - `.set(task_name::params::field(), ...)`
   - `.arg_from(task_name::params::field(), dep)`

`WorkflowSpecBuilder::task(...)` returns `TypedNodeRef<T>`. That ref keeps the upstream output type available for later `.arg_from(...)` calls.

For child workflows, the equivalent path is `template.node()` on a registered `WorkflowTemplate<P, T>`, or `SubWorkflowNode::from_definition::<D>()` when composing from a `WorkflowDefinition`.

## Choose the Right Binding API

Use the binding method that matches where the value comes from.

| Situation | Use |
| --- | --- |
| You already have the whole input value now | `.set_input(value)?` |
| One field is known now, but other fields come from upstream nodes | `.set(task::params::field(), value)?` |
| A field should receive a `TaskResult<T>` from an upstream node | `.arg_from(task::params::field(), dep)` |
| You are wiring a child workflow instead of a task | same methods on `template.node()` / `SubWorkflowNode` |

The simplest rule is:

- `set_input(...)` for whole explicit input
- `set(...)` for one explicit field
- `arg_from(...)` for dependency-driven input

## Single-Input Task

When the task takes one direct input value, bind the whole value with `set_input(...)`.

```rust
use horsies::{task, TaskError, WorkflowSpecBuilder};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FetchUserInput {
    user_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: i64,
    email: String,
}

#[task("fetch_user")]
async fn fetch_user(input: FetchUserInput) -> Result<User, TaskError> {
    Ok(User {
        id: input.user_id,
        email: "alice@example.com".into(),
    })
}

fn build_spec(user_id: i64) -> Result<horsies::WorkflowSpec, horsies::HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("fetch_user_workflow");
    builder.definition_key("docs.fetch_user_workflow.v1");

    let fetch = builder.task(
        fetch_user::node()?
            .set_input(FetchUserInput { user_id })?
            .node_id("fetch"),
    );

    builder.output(fetch);
    builder.build()
}
```

## Multi-Parameter Task

The task macro also supports multiple parameters directly. This is the preferred shape when a task mixes upstream `TaskResult<_>` values with local explicit values.

```rust
use horsies::{task, TaskError, TaskResult};

#[task("notify_user")]
async fn notify_user(
    user: TaskResult<User>,
    urgent: bool,
) -> Result<(), TaskError> {
    let user = match user {
        TaskResult::Ok(v) => v,
        TaskResult::Err(err) => {
            return Err(TaskError::new(
                "UPSTREAM_FAILED",
                format!("cannot notify user: {:?}", err.error_code),
            ))
        }
    };

    let _ = (user, urgent);
    Ok(())
}
```

The task macro generates parameter tokens for this task:

```rust
notify_user::params::user()
notify_user::params::urgent()
```

Those tokens are then used in workflow composition.

## Field-Level Binding

Use `.set(...)` when part of the input is explicit and part comes from other nodes.

```rust
fn build_notify_spec(user_id: i64) -> Result<horsies::WorkflowSpec, horsies::HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("notify_user_workflow");
    builder.definition_key("docs.notify_user_workflow.v1");

    let fetch = builder.task(
        fetch_user::node()?
            .set_input(FetchUserInput { user_id })?
            .node_id("fetch"),
    );

    let notify = builder.task(
        notify_user::node()?
            .node_id("notify")
            .waits_for(fetch)
            .arg_from(notify_user::params::user(), fetch)
            .set(notify_user::params::urgent(), true)?,
    );

    builder.output(notify);
    builder.build()
}
```

## Child Workflow Example

Subworkflow composition follows the same model. The difference is the node constructor: use `template.node()` for a registered parameterized child workflow.

```rust
use horsies::{
    task, Horsies, HorsiesError, TaskError, TaskResult, WorkflowInput, WorkflowSpec,
    WorkflowSpecBuilder, WorkflowTemplate,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RenderedEmail {
    body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, WorkflowInput)]
struct ChildParams {
    user: TaskResult<User>,
    urgent: bool,
}

#[task("render_email")]
async fn render_email(input: ChildParams) -> Result<RenderedEmail, TaskError> {
    let user = match input.user {
        TaskResult::Ok(v) => v,
        TaskResult::Err(err) => {
            return Err(TaskError::new(
                "UPSTREAM_FAILED",
                format!("cannot render email: {:?}", err.error_code),
            ))
        }
    };

    Ok(RenderedEmail {
        body: format!("Email for {} (urgent={})", user.email, input.urgent),
    })
}

fn register_child(
    app: &mut Horsies,
) -> Result<WorkflowTemplate<ChildParams, RenderedEmail>, HorsiesError> {
    app.register_parameterized_workflow::<ChildParams, RenderedEmail, _>(
        "render_email_child",
        "docs.render_email_child.v1",
        |params| {
            let mut builder = WorkflowSpecBuilder::new("render_email_child");
            builder.definition_key("docs.render_email_child.v1");

            let render = builder.task(
                render_email::node()?
                    .set_input(params)?
                    .node_id("render"),
            );

            builder.output(render);
            builder.build()
        },
    )
}

fn build_parent(
    child_template: &WorkflowTemplate<ChildParams, RenderedEmail>,
    user_id: i64,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("parent_workflow");
    builder.definition_key("docs.parent_workflow.v1");

    let fetch = builder.task(
        fetch_user::node()?
            .set_input(FetchUserInput { user_id })?
            .node_id("fetch"),
    );

    let child = builder.sub_workflow(
        child_template
            .node()
            .node_id("child")
            .set(ChildParams::field_urgent(), true)?
            .arg_from(ChildParams::field_user(), fetch),
    );

    builder.output(child);
    builder.build()
}
```

The mental model stays the same:

- `child_template.node()` creates the child-workflow node
- `.set(...)` binds explicit child params
- `.arg_from(...)` injects upstream parent results into child params

## `TypedNodeRef<T>` vs `NodeRef`

`builder.task(...)` returns `TypedNodeRef<T>`. Keep refs typed when possible.

That is what makes this compile:

```rust
.arg_from(notify_user::params::user(), fetch)
```

because:

- `fetch` is `TypedNodeRef<User>`
- `notify_user::params::user()` is `InputField<_, TaskResult<User>>`

If you need a heterogeneous collection of refs, erase the output type:

```rust
use horsies::NodeRef;

let mut deps: Vec<NodeRef> = Vec::new();
deps.push(fetch.into());
deps.push(notify.into());
```

Most builder methods accept `Into<NodeRef>`, so manual erasure is often unnecessary:

- `.waits_for(dep)`
- `.waits_for_all([...])`
- `builder.output(dep)`
- `.workflow_ctx_from([...])`

Reach for `NodeRef` only when you genuinely need a heterogeneous collection.

## `workflow_ctx_from`

Workflow context sources are ref-based, not string-based.

```rust
let enrich = builder.task(
    process_user::node()?
        .node_id("process")
        .waits_for(fetch)
        .workflow_ctx_from([fetch]),
);
```

`workflow_ctx_from(...)` does not add dependencies automatically. Every context source must still appear in `waits_for(...)` or `waits_for_all(...)`.

## Fallback: Manual Input Structs

For some manual or reusable input shapes, deriving `WorkflowInput` on a named-field struct is still useful:

```rust
use horsies::{TaskResult, WorkflowInput};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, WorkflowInput)]
struct AggregateInput {
    left: TaskResult<i32>,
    right: TaskResult<i32>,
}
```

This generates:

```rust
AggregateInput::field_left()
AggregateInput::field_right()
```

Use this pattern when the input shape is shared, reused, or also serves as a domain value. For ordinary workflow tasks, prefer direct multi-parameter task signatures over wrapper structs that exist only for wiring.
