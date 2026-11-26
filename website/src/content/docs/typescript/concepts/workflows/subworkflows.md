---
title: Subworkflows
summary: Run a child workflow as a node with w.subWorkflow, give the child its params as values or upstream nodes, build a child from a dependency's result, and read the child's result.
related: [workflow-api, ../../tasks/wire-types, ../../../concepts/workflows/workflow-semantics]
tags: [typescript, workflow, subworkflow, w.subWorkflow, SubWorkflowSummary]
---

# Subworkflows

A sub-workflow node runs a child workflow. The node names the child by its workflow contract, and the engine finds the child by its definition key. The engine builds the child from the params of the node when the node becomes ready, so a child can have one node per element of a dependency's result. The status rules of a child and its node are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/#success-cases-partial-success-policy).

## Add a Sub-Workflow Node

```ts
import { defineTask, defineWorkflow, h, Horsies, WorkflowId } from "horsies";

const fetchReport = defineTask({
  name: "fetch_report",
  input: { region: h.string() },
  output: h.string(),
});

const countReports = defineTask({
  name: "count_reports",
  input: {},
  output: h.int(),
});

export const regionReport = defineWorkflow(
  {
    name: "region_report",
    definitionKey: "reports.region_report.v1",
    params: { region: h.string() },
    output: h.string(),
    checkCases: [{ region: "eu" }],
  },
  (w, { region }) => ({ output: w.task(fetchReport, { region }, { id: "fetch" }) }),
);

export const allRegions = defineWorkflow(
  { name: "all_regions", definitionKey: "reports.all_regions.v1", params: {}, output: h.int() },
  (w) => {
    const eu = w.subWorkflow(regionReport, { region: "eu" }, { id: "eu" });
    const us = w.subWorkflow(regionReport, { region: "us" }, { id: "us" });

    return { output: w.task(countReports, {}, { id: "count", waitsFor: [eu, us], allowFailedDeps: true }) };
  },
);
```

`w.subWorkflow(workflow, params, options)` returns a `NodeRef<O, string, Id>`. `O` is the child's declared `output`; the codes are open (`string`), because a child holds the codes of many tasks.

`params` has one member per parameter of the child, with the rules of a task node's input:

- A plain parameter takes a value of its type. An optional or a defaulted one may be left out.
- A parameter declared `h.resultOf(T)` takes a node of the same build whose output is a `T`. See [Build the Child from a Dependency's Result](#build-the-child-from-a-dependencys-result).
- A child with `params: {}` takes `{}`. A member for it is a type error, and `HRS-022` at run time.

The value of each parameter is encoded and read back as a send does: a value that does not encode is `HRS-031`. The other rules are on [Workflow API](/horsies/typescript/concepts/workflows/workflow-api/#what-the-checks-refuse).

## SubWorkflowNodeOptions

| Option | Type | Rule |
|--------|------|------|
| `id` | `string` | Required. The node id, with the rules of a task node |
| `waitsFor` | `readonly AnyNode[]` | Nodes of this build that the node waits for, in addition to the sources of its parameters |
| `join` | `Join` | When the node becomes ready. Left out: `{ kind: "all" }` |
| `allowFailedDeps` | `boolean` | `true`: the child starts when a dependency failed or was skipped. Left out: the node is `SKIPPED` |

A sub-workflow node has no `queue`, `priority` or `goodUntil`: the nodes of the child have their own.

## Build the Child from a Dependency's Result

A child parameter declared `h.resultOf(T)` takes the result of an upstream node. The child is built when its node becomes ready, with that result in its params, so its build can add one node per element of the result.

```ts
const listRegions = defineTask({
  name: "list_regions",
  input: {},
  output: h.array(h.string()),
});

export const regionFanOut = defineWorkflow(
  {
    name: "region_fan_out",
    definitionKey: "reports.region_fan_out.v1",
    params: { regions: h.resultOf(h.array(h.string())) },
    output: h.int(),
    checkCases: [{}],
  },
  (w, { regions }) => {
    const regionList = regions.kind === "succeeded" ? regions.value : [];
    const fetched = regionList.map((region) => w.task(fetchReport, { region }, { id: `fetch-${region}` }));

    return { output: w.task(countReports, {}, { id: "count", waitsFor: fetched, allowFailedDeps: true }) };
  },
);

export const dailyRegions = defineWorkflow(
  { name: "daily_regions", definitionKey: "reports.daily_regions.v1", params: {}, output: h.int() },
  (w) => {
    const listed = w.task(listRegions, {}, { id: "list" });

    return { output: w.subWorkflow(regionFanOut, { regions: listed }, { id: "reports" }) };
  },
);
```

- The build gets `regions` as a `TaskOutcome<string[], string>`. A FAILED or SKIPPED source skips the node, unless the node has `allowFailedDeps: true`; then the build gets the `failed` outcome.
- A check case holds the plain params only. `regionFanOut` has no plain param, so its one case is `{}`. `regionFanOut` runs only as a child: `app.start` of it is a type error, because a sender cannot give an `h.resultOf` parameter.
- A build that throws, returns a thenable or gives nodes that fail the checks fails its node with `WORKFLOW_ENQUEUE_FAILED`. The workflow step goes on.

### The Wait for the Build

The core asks the app for the child's spec inside a workflow step. The build runs on the JavaScript thread of the process that runs the step. The wait for the JavaScript thread to take the call of one build is at most 5,000 ms (the core's loop-block limit). Past it, the build does not run, and the workflow step rolls back and runs again through its retry or the workflow recovery. After the thread takes the call, the wait for the build's answer has no bound of its own. In a worker child, the loop watch bounds a blocked event loop. The Python client has no bound on the wait for its callback thread.

Keep a build sync and short: it runs on the same thread as the task bodies and the other calls of the process.

## List the Children

List each child workflow in `Horsies.create({ workflows })`, next to its parents. Every process of the app runs the same entry, so every process that advances a workflow has the children.

```ts
const created = Horsies.create({
  broker: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [fetchReport, countReports, listRegions],
  workflows: [regionReport, allRegions, regionFanOut, dailyRegions],
});

if (!created.ok) {
  for (const refusal of created.error.refusals) {
    console.error(`${refusal.code} at ${refusal.path}: ${refusal.message}`);
  }

  process.exit(1);
}

const app = created.value;
```

- A sub-workflow node of a child that the app does not list is `HRS-020`: a refusal of `Horsies.create` for a parent with no params, and `VALIDATION_FAILED` at a start.
- At run time, a child whose definition key the process does not list fails its node with `SUBWORKFLOW_LOAD_FAILED`.
- A resume of a PAUSED workflow builds the children that it owes from the workflows of the app of the handle.

## Read a Child's Result

The result of a sub-workflow node is the result of its child: the child's output, or its error.

| Read | Gives |
|------|-------|
| A downstream parameter `h.resultOf(T)` fed by the node | `TaskOutcome<T, string>`, `T` the child's output type |
| `handle.resultFor(id)`, for a node of the returned object | `TaskOutcome<O, string>`, `O` the child's declared output |
| `app.workflowHandle(parent, id).resultFor(nodeId, child)` | `TaskOutcome<O, string>`, read with the declared output of `child` |
| `handle.tasks()` | `WorkflowTaskInfo` with `subWorkflowId` and `subWorkflowSummary` |

```ts
export async function readRegion(text: string): Promise<void> {
  const workflowId = WorkflowId.parse(text);

  if (!workflowId.ok) {
    return;
  }

  const handle = app.workflowHandle(allRegions, workflowId.value);
  const eu = await handle.resultFor("eu", regionReport);

  if (!eu.ok) {
    console.error(`the read failed: ${eu.error.kind}`);

    return;
  }

  const outcome = eu.value;

  switch (outcome.kind) {
    case "succeeded":
      console.log(outcome.value);
      break;
    case "failed": {
      const child = outcome.error.subWorkflow;

      if (child !== null) {
        const { status, failedTasks, totalTasks } = child.summary;

        console.log(`the child ended ${status}: ${failedTasks} of ${totalTasks} tasks failed`);
      }

      break;
    }
  }
}
```

A failed child gives its node a `TaskError` whose `subWorkflow` is a `SubWorkflowFailure`: the child's `workflowId` and its `summary`. A task error that no child gave has `subWorkflow: null`.

`SubWorkflowSummary`:

| Member | Type |
|--------|------|
| `status` | `WorkflowStatus`: the status of the ended child |
| `output` | `JsonValue`: the child's output as stored, with no type; `null` when the child stored none |
| `totalTasks`, `completedTasks`, `failedTasks`, `skippedTasks` | `number` |
| `errorSummary` | `string \| null`: the message of the child's stored error |

## Nesting Limit

A child is at most 32 levels deep. At run time the engine does not start a deeper child, and its node fails with `SUBWORKFLOW_DEPTH_LIMIT_REACHED`.

## Things to Avoid

**Do not give a build side effects.** The engine can run the build of a child more than once for one node.

**Do not list a child only in the producer's app.** The process that advances a workflow can be a worker child. It builds the child from the workflows of its own `Horsies.create`.

**Do not block the JavaScript thread in a build.** The build of a child waits for the thread of its process, and a thread that does not take the call within 5,000 ms rolls the workflow step back.
