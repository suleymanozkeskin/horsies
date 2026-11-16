---
title: Workflows and Subworkflows
summary: Compose parent and child workflows with `SubWorkflowNode`.
related: [workflow-semantics, result-handling]
tags: [concepts, workflows, subworkflows, composition]
---

## What is a Subworkflow?

A subworkflow is a child workflow executed as one node inside a parent DAG.

In the parent workflow:

- a task node runs one task function
- a subworkflow node runs one registered child workflow

Subworkflows are useful when:

- you want to reuse child workflow logic
- the child has its own success policy
- the child should appear as one unit in the parent DAG

## Defining a Child Workflow

Static child workflow:

```rust
use horsies::{
    HorsiesError, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder,
};

struct ChildPipeline;

impl WorkflowDefinition for ChildPipeline {
    type Output = String;
    type Params = ();

    fn name() -> &'static str { "child_pipeline" }
    fn definition_key() -> &'static str { "docs.child_pipeline.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let fetch = builder.task(
            fetch_data::node()?
                .set_input(FetchDataInput {
                    source: "https://api.example.com".into(),
                })?
                .node_id("fetch"),
        );

        let process = builder.task(
            process_data::node()?
                .node_id("process")
                .waits_for(fetch)
                .arg_from(process_data::params::data(), fetch),
        );

        Ok(WorkflowDefConfig::new().output(process))
    }
}
```

Parameterized child workflow:

```rust
use horsies::{HorsiesError, WorkflowDefinition, WorkflowSpecBuilder};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildParams {
    source_url: String,
}

struct ChildPipelineWithParams;

impl WorkflowDefinition for ChildPipelineWithParams {
    type Output = String;
    type Params = ChildParams;

    fn name() -> &'static str { "child_pipeline_with_params" }
    fn definition_key() -> &'static str { "docs.child_pipeline_with_params.v1" }

    fn build_with(params: ChildParams) -> Result<horsies::WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(Self::name());
        builder.definition_key(Self::definition_key());

        let fetch = builder.task(
            fetch_data::node()?
                .set_input(FetchDataInput {
                    source: params.source_url,
                })?
                .node_id("fetch"),
        );

        builder.output(fetch);
        builder.build()
    }
}
```

## Starting a Workflow Directly

Zero-param workflow:

```rust
let wf = app.register_workflow_definition::<ChildPipeline>()?;
let handle = wf.start().await?;
```

Parameterized workflow:

```rust
let template = app.workflow_template::<ChildPipelineWithParams>();
let handle = template
    .start(ChildParams {
        source_url: "https://api.example.com".into(),
    })
    .await?;
```

`handle.get(...)` returns `TaskResult<T>`.

## Composing a Child Workflow Into a Parent

Use `SubWorkflowNode::from_definition::<D>()` when the child is a `WorkflowDefinition`.
That carries both the workflow name and definition key.

```rust
use horsies::{SubWorkflowNode, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder};

struct ParentPipeline;

impl WorkflowDefinition for ParentPipeline {
    type Output = bool;
    type Params = ();

    fn name() -> &'static str { "parent_pipeline" }
    fn definition_key() -> &'static str { "docs.parent_pipeline.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, horsies::HorsiesError> {
        let child = builder.sub_workflow(
            SubWorkflowNode::from_definition::<ChildPipeline>()
                .node_id("child"),
        );

        let store = builder.task(
            store_result::node()?
                .node_id("store")
                .waits_for(child)
                .arg_from(store_result::params::result(), child),
        );

        Ok(WorkflowDefConfig::new().output(store))
    }
}
```

The parent receives the child workflow output just like any other upstream node.

If you obtained a registered template from `app.workflow_template::<ChildPipelineWithParams>()`,
the equivalent authoring path is `template.node()`.

## Parameterized Child Workflow as a Node

Child workflow params can be supplied in the same two ways as task-node input:

- whole child params with `.set_input(...)`
- mixed static plus injected child params with `.set(...)` and `.arg_from(...)`

```rust
use horsies::{TaskResult, WorkflowInput};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, WorkflowInput)]
struct ChildSeedParams {
    seed: TaskResult<String>,
    limit: usize,
}

fn build_parent(
    app: &horsies::Horsies,
) -> Result<horsies::WorkflowSpec, horsies::HorsiesError> {
    let child_template = app.workflow_template::<ChildPipelineWithParams>();
    let mut builder = WorkflowSpecBuilder::new("aggregate_parent");
    builder.definition_key("docs.aggregate_parent.v1");

    let fetch = builder.task(
        fetch_data::node()?
            .set_input(FetchDataInput {
                source: "https://api.example.com".into(),
            })?
            .node_id("fetch"),
    );

    let child = builder.sub_workflow(
        child_template
            .node()
            .node_id("child")
            .set(ChildSeedParams::field_limit(), 100)?
            .arg_from(ChildSeedParams::field_seed(), fetch),
    );

    let store = builder.task(
        store_result::node()?
            .node_id("store")
            .waits_for(child)
            .arg_from(store_result::params::result(), child),
    );

    builder.output(store);
    builder.build()
}
```

Use `#[derive(WorkflowInput)]` on the child params type when you want field-level
bindings like `.set(...)` and `.arg_from(...)`. For whole explicit params, prefer
`.set_input(params)?`.

## Dependencies and Context

Subworkflow nodes support the same dependency primitives as task nodes:

- `.waits_for(...)`
- `.waits_for_all(...)`
- `.arg_from(...)`
- `.workflow_ctx_from([...])`
- `.allow_failed_deps(true)`
- `.join_all()`, `.join_any()`, `.join_quorum(min)`

Example:

```rust
    let child_template = app.workflow_template::<ChildPipelineWithParams>();

    let child = builder.sub_workflow(
        child_template
            .node()
            .node_id("child")
            .waits_for(fetch)
            .workflow_ctx_from([fetch])
            .arg_from(ChildSeedParams::field_seed(), fetch),
    );
```

As with task nodes, `workflow_ctx_from(...)` does not add dependencies automatically.

## Lifecycle Mapping

Parent subworkflow-node state follows child workflow state:

- child `Running` -> parent node `Running`
- child `Completed` -> parent node `Completed`
- child `Failed` -> parent node `Failed`
- child `Paused` -> parent node remains non-terminal until resumed
- child `Cancelled` -> parent node `Failed` with a child cancellation error
- child `Expired` -> parent node `Failed` with a child expiry error

The child output becomes the upstream value for downstream parent nodes.

## Error Handling

Downstream parent tasks can opt into recovery logic the same way they do for task-task edges:

```rust
use horsies::{task, TaskError, TaskResult};

#[task("recovery_handler")]
async fn recovery_handler(primary_result: TaskResult<String>) -> Result<String, TaskError> {
    match primary_result {
        TaskResult::Ok(data) => Ok(data),
        TaskResult::Err(_err) => Ok("fallback".into()),
    }
}
```

Use `.allow_failed_deps(true)` on the downstream node if it should still run after child failure.
