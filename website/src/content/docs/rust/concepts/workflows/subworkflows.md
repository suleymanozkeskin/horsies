---
title: Subworkflows
summary: Run a child workflow as a node, with params from build_with, field tokens from #[derive(WorkflowInput)], and a child built at run time from a dependency's result.
related: [workflow-api, typed-node-builder, ../../../concepts/workflows/workflow-semantics]
tags: [rust, workflow, subworkflow, SubWorkflowNode, build_with, WorkflowInput]
---

# Subworkflows

A sub-workflow node runs a child workflow. The node names the child by its `WorkflowDefinition`. When the child's definition takes params, the engine builds the child spec from the params of the node, so a child can have one node per element of a dependency's result. The status rules of a child and its node are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/#success-cases-partial-success-policy).

## A Definition with Params

A definition with params implements `build_with` in place of `define`.

```rust
use std::convert::Infallible;

use horsies::{
    task, HorsiesError, OutputlessResults, TaskError, WorkflowDefinition, WorkflowInput,
    WorkflowSpec, WorkflowSpecBuilder,
};
use serde::{Deserialize, Serialize};

#[task("fetch_report")]
async fn fetch_report(region: String) -> Result<String, TaskError<Infallible>> {
    Ok(format!("report of {region}"))
}

#[derive(Debug, Clone, Serialize, Deserialize, WorkflowInput)]
struct RegionParams {
    region: String,
}

struct RegionReport;

impl WorkflowDefinition for RegionReport {
    type Output = OutputlessResults;
    type Params = RegionParams;

    fn name() -> &'static str {
        "region_report"
    }

    fn definition_key() -> &'static str {
        "reports.region_report.v1"
    }

    fn build_with(
        mut builder: WorkflowSpecBuilder,
        params: RegionParams,
    ) -> Result<WorkflowSpec, HorsiesError> {
        builder.definition_key(Self::definition_key());
        let _fetched = builder.task(
            fetch_report::node().set(fetch_report::params::region(), params.region)?,
        )?;
        builder.build()
    }
}
```

- `build_with` gets a builder of the app that registered the definition, named after the definition. Its task nodes resolve against that app. Set the definition key, and `on_error` when it is not FAIL, on it.
- The engine calls `build_with` for each sub-workflow node that gives params, and can call it more than once for one node. Give it no side effects.
- When the engine or the check builds a child, a panic of `build_with` is a refusal of the build, with the panic text.
- Register the definition with `app.workflow_template::<D>()`, which returns a `WorkflowTemplate<D::Params, D::Output>`. A template also starts the definition as a workflow of its own: see [Workflow API](/horsies/rust/concepts/workflows/workflow-api/#register-and-start).

## Add a Sub-Workflow Node

```rust
use horsies::{
    HorsiesError, OutputlessResults, SubWorkflowParams, WorkflowDefConfig, WorkflowDefinition,
    WorkflowSpecBuilder,
};

struct AllRegions;

impl WorkflowDefinition for AllRegions {
    type Output = OutputlessResults;
    type Params = ();

    fn name() -> &'static str {
        "all_regions"
    }

    fn definition_key() -> &'static str {
        "reports.all_regions.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let _eu = builder.sub_workflow(
            RegionReport::node(SubWorkflowParams::Given(RegionParams { region: "eu".to_owned() }))?
                .node_id("eu"),
        );
        let _us = builder.sub_workflow(
            RegionReport::node(SubWorkflowParams::NotGiven)?
                .set(RegionParams::field_region(), "us".to_owned())?
                .node_id("us"),
        );
        Ok(WorkflowDefConfig::new())
    }
}
```

`RegionReport` and `RegionParams` are the definition and the params of [A Definition with Params](#a-definition-with-params).

A node of the child definition `D` comes from:

| Call | Returns |
|------|---------|
| `D::node(params)` | `Result<SubWorkflowNode<D::Params, D::Output>, HorsiesError>` |
| `SubWorkflowNode::of::<D>(params)` | The same |
| `template.node(params)` | The same, from a registered `WorkflowTemplate` |

`builder.sub_workflow(node)` adds it and returns its `TypedNodeRef<T>`, with `T` the child's output type.

`SubWorkflowParams<P>`:

| Variant | The node |
|---------|----------|
| `NotGiven` | Gives no params: the child builds as defined, and the node stores the input `{}` |
| `Given(params)` | Gives `params` to the child's `build_with` |

Params given to a definition without params are refused with `HRS-022`. Params that do not encode or read back as `P` are `HRS-031`; params that do not encode as a JSON object are `HRS-026`.

## Field Tokens of the Params

`#[derive(WorkflowInput)]` on a struct with named fields generates one token per field that serde reads: `RegionParams::field_region()` returns `InputField<RegionParams, String>`.

- The token has the key that serde reads: `rename`, `rename_all` and raw identifiers (`r#type` is `type`) apply.
- A field with `#[serde(skip)]` or `#[serde(skip_deserializing)]` gets no token.
- A `#[serde(flatten)]` field is a compile error.

The parameters of a task have their tokens in the task's own module: `task_name::params::<name>()`. See [Typed Node Builder](/horsies/rust/concepts/workflows/typed-node-builder/).

## SubWorkflowNode

| Method | Does |
|--------|------|
| `.set(field, value)` | Gives the child one param, encoded and read back as a send does. The node then gives params. `Result<Self, HorsiesError>` |
| `.set_params(SubWorkflowParams)` | `Given(params)` replaces every param set before; `NotGiven` also drops the `arg_from` members. `Result<Self, HorsiesError>` |
| `.arg_from(field, source)` | Gives the child the outcome of `source` as the param `field` of type `TaskOutcome<S, E>`. The node then gives params and waits for `source` |
| `.waits_for(node)`, `.waits_for_all(nodes)` | Waits for the nodes, with no data |
| `.node_id(id)` | Sets the node id, as `TaskNode::node_id` |
| `.allow_failed_deps(true)` | Starts the child when a dependency failed or was skipped |
| `.join_all()` (default), `.join_any()`, `.join_quorum(min_success)` | When the node becomes ready |
| `.key()` | `Option<NodeKey<T>>`: the key of the node's result when the node has a given node id |
| `.workflow_name()`, `.definition_key()` | The child's workflow name and definition key |

A sub-workflow node has no queue, priority, deadline or workflow context: the nodes of the child have their own.

## Build the Child from a Dependency's Result

A child whose params take a dependency's result with `arg_from` is built when the node becomes ready. Its `build_with` can add one node per element of the result.

```rust
use std::convert::Infallible;

use horsies::{
    task, HorsiesError, OutputlessResults, SubWorkflowParams, TaskError, TaskOutcome,
    WorkflowDefConfig, WorkflowDefinition, WorkflowInput, WorkflowSpec, WorkflowSpecBuilder,
};
use serde::{Deserialize, Serialize};

#[task("list_regions")]
async fn list_regions() -> Result<Vec<String>, TaskError<Infallible>> {
    Ok(vec!["eu".to_owned(), "us".to_owned()])
}

#[derive(Debug, Clone, Serialize, Deserialize, WorkflowInput)]
struct RegionListParams {
    regions: TaskOutcome<Vec<String>, Infallible>,
}

struct RegionFanOut;

impl WorkflowDefinition for RegionFanOut {
    type Output = OutputlessResults;
    type Params = RegionListParams;

    fn name() -> &'static str {
        "region_fan_out"
    }

    fn definition_key() -> &'static str {
        "reports.region_fan_out.v1"
    }

    fn build_with(
        mut builder: WorkflowSpecBuilder,
        params: RegionListParams,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let regions = match params.regions {
            TaskOutcome::Succeeded(regions) => regions,
            TaskOutcome::Failed(error) => {
                return Err(HorsiesError::new(format!("no region list: {error}")));
            }
        };
        builder.definition_key(Self::definition_key());
        for region in regions {
            let _fetched = builder.task(
                fetch_report::node()
                    .node_id(format!("fetch_{region}"))
                    .set(fetch_report::params::region(), region)?,
            )?;
        }
        builder.build()
    }
}

struct DailyRegions;

impl WorkflowDefinition for DailyRegions {
    type Output = OutputlessResults;
    type Params = ();

    fn name() -> &'static str {
        "daily_regions"
    }

    fn definition_key() -> &'static str {
        "reports.daily_regions.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        let listed = builder.task(list_regions::node())?;
        let _reports = builder.sub_workflow(
            RegionFanOut::node(SubWorkflowParams::NotGiven)?
                .arg_from(RegionListParams::field_regions(), listed)
                .node_id("reports"),
        );
        Ok(WorkflowDefConfig::new())
    }
}
```

`fetch_report` is the task of [A Definition with Params](#a-definition-with-params).

- A FAILED or SKIPPED source skips the node, unless the node has `allow_failed_deps(true)`; then `build_with` gets `TaskOutcome::Failed`.
- A `build_with` that refuses or panics at run time fails its node with `WORKFLOW_ENQUEUE_FAILED`. A list with no element builds a spec with no node, which `build` refuses (`HRS-002`).
- The check does not build a child whose node has an `arg_from`: its params exist only at run time.

## Register the Children

Register each child definition when `main` builds the app, so that every process has it, the worker children included. The engine builds a child from the definitions registered in the process that advances the workflow.

```rust
use horsies::{Horsies, HorsiesError};

fn register_reports(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _fetch = fetch_report::register(app)?;
    let _list = list_regions::register(app)?;
    let _region_report = app.workflow_template::<RegionReport>()?;
    let _fan_out = app.workflow_template::<RegionFanOut>()?;
    let _all_regions = app.register_workflow_definition::<AllRegions>()?;
    let _daily_regions = app.register_workflow_definition::<DailyRegions>()?;
    Ok(())
}
```

- A start refuses a spec with a sub-workflow node of a definition that is not registered: `ValidationFailed` with `HRS-020`.
- At run time, a child whose definition is not registered fails its node with `SUBWORKFLOW_LOAD_FAILED`.
- A resume of a PAUSED workflow builds the children it owes from the definitions registered on the app of the handle.

## Read a Child's Result

The result of a sub-workflow node is the result of its child: the child's output, or its error. A downstream node reads it as it reads a task node:

| Read | Gives |
|------|-------|
| `arg_from(field, child_node)` | `TaskOutcome<T, E>` with `T` the child's output type |
| `workflow_ctx.summary_for(&key)` | The `SubWorkflowSummary` of the ended child |
| `workflow_ctx.output_for::<T>(&key)` | `Option<T>`: the child's output, `None` when the child stored none |
| `handle.tasks()` | `WorkflowNodeInfo` with `sub_workflow_id` and `sub_workflow_summary` |

`SubWorkflowSummary`:

| Field | Type |
|-------|------|
| `status` | `WorkflowStatus`: `Completed`, `Failed`, `Cancelled` or `Expired` |
| `output` | `Option<JsonText>`: the child's output as its stored text, set only when the child is COMPLETED and stored an ok value. `get()` gives the text, `decode::<T>()` the value |
| `total_tasks`, `completed_tasks`, `failed_tasks`, `skipped_tasks` | `i32` |
| `error_summary` | `Option<String>`: the message of the child's stored error |

A failed child gives its node a `TaskError` whose `sub_workflow()` returns the `SubWorkflowFailure`: `sub_workflow_id` and `sub_workflow_summary`.

## Nesting Limit

A child is at most 32 levels deep. At run time the engine does not start a deeper child, and the node fails with `SUBWORKFLOW_DEPTH_LIMIT_REACHED`. `app.check()` reports a nesting with no wait on its path:

| Code | Refusal |
|------|---------|
| `HRS-036` | The nesting starts a child deeper than 32 for certain |
| `HRS-037` | A definition includes itself with the same params, with no node between the two that waits for other nodes |

The codes are on the [Errors Reference](/horsies/tasks/errors/#workflow-validation-hrs-001-hrs-099).

## Things to Avoid

**Do not give `build_with` side effects.** The engine can call it more than once for one node.

**Do not omit the definition key in `build_with`.** The builder is new, and a start refuses a spec without a key.

**Do not register a child only in the producer.** The process that advances the workflow can be a worker child, and it builds the child from its own registrations.
