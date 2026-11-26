---
title: Workflow API
summary: Define a workflow with defineWorkflow, build its nodes with the builder w, list it in Horsies.create, start it with app.start, and read and control it through WorkflowHandle.
related: [subworkflows, ../../tasks/defining-tasks, ../../tasks/retrieving-results, ../../../concepts/workflows/workflow-semantics]
tags: [typescript, workflow, defineWorkflow, WorkflowBuilder, WorkflowHandle, WorkflowStartError]
---

# Workflow API

A workflow is a DAG of task nodes and sub-workflow nodes. `defineWorkflow` gives the contract of a workflow: its name, its definition key, its params, its declared output and its build function. The build function makes the nodes as data. The client checks each build with the rules and the `HRS-xxx` codes of the Python and Rust clients before anything is written. The statuses, the joins, the failure propagation, pause and resume are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/).

## Define and Start a Workflow

```ts
import { defineTask, defineWorkflow, h, Horsies, WorkflowId } from "horsies";

const Order = h.object({
  orderId: h.uuid(),
  lines: h.array(h.object({ sku: h.string(), quantity: h.int() })),
});
const ValidatedOrder = h.object({ orderId: h.uuid(), total: h.decimal() });
const Receipt = h.object({ receiptId: h.string() });

export const validateOrder = defineTask({
  name: "validate_order",
  input: { order: Order },
  output: ValidatedOrder,
  errors: ["EMPTY_ORDER"],
});

export const reserveStock = defineTask({
  name: "reserve_stock",
  input: { order: h.resultOf(ValidatedOrder), warehouse: h.string() },
  output: h.null(),
});

export const chargePayment = defineTask({
  name: "charge_payment",
  input: { order: h.resultOf(ValidatedOrder) },
  output: Receipt,
  errors: ["PAYMENT_DECLINED"],
});

export const sendReceipt = defineTask({
  name: "send_receipt",
  input: { receipt: h.resultOf(Receipt) },
  output: h.null(),
});

export const fulfilOrder = defineWorkflow(
  {
    name: "fulfil_order",
    definitionKey: "shop.fulfil_order.v1",
    params: { order: Order },
    output: Receipt,
    checkCases: [
      { order: { orderId: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b", lines: [{ sku: "tea", quantity: 1 }] } },
    ],
  },
  (w, { order }) => {
    const validated = w.task(validateOrder, { order }, { id: "validate" });
    const reserved = w.task(reserveStock, { order: validated, warehouse: "eu-1" }, { id: "reserve" });
    const charged = w.task(chargePayment, { order: validated }, { id: "charge", waitsFor: [reserved] });

    w.task(sendReceipt, { receipt: charged }, { id: "notify" });

    return { output: charged };
  },
);
```

- A parameter declared `h.resultOf(T)` takes an upstream node whose output is a `T`. The task's body gets the upstream outcome, a `TaskOutcome<T, string>`. Every other parameter takes a value.
- The node `charge` waits for `validate`, the source of its parameter, and for `reserve`, which `waitsFor` names.
- The build returns `{ output }`: the node whose result is the workflow's result. Its output type is the declared `output`.
- Each task of a node needs an implementation in the worker's entry. See [Defining Tasks](/horsies/typescript/tasks/defining-tasks/).

List the tasks and the workflows in `Horsies.create`, then start the workflow:

```ts
const created = Horsies.create({
  broker: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [validateOrder, reserveStock, chargePayment, sendReceipt],
  workflows: [fulfilOrder],
});

if (!created.ok) {
  for (const refusal of created.error.refusals) {
    console.error(`${refusal.code} at ${refusal.path}: ${refusal.message}`);
  }

  process.exit(1);
}

const app = created.value;

function assertNever(value: never): never {
  throw new Error(`no case for ${JSON.stringify(value)}`);
}

export async function fulfil(): Promise<void> {
  const started = await app.start(fulfilOrder, {
    order: { orderId: crypto.randomUUID(), lines: [{ sku: "tea", quantity: 2 }] },
  });

  if (!started.ok) {
    console.error(`${started.error.code}: ${started.error.message}`);

    return;
  }

  const read = await started.value.get({ timeoutMs: 60_000 });

  if (!read.ok) {
    console.error(`the read failed: ${read.error.kind}`);

    return;
  }

  const outcome = read.value;

  switch (outcome.kind) {
    case "succeeded":
      console.log(`receipt ${outcome.value.receiptId}`);
      break;
    case "failed":
      console.log(`the workflow ended with ${outcome.error.code.code} (${outcome.error.code.kind})`);
      break;
    default:
      assertNever(outcome);
  }
}
```

A node whose run is a child workflow is on [Subworkflows](/horsies/typescript/concepts/workflows/subworkflows/).

## defineWorkflow

`defineWorkflow(definition, build)` returns a `WorkflowContract<P, R>`. It does no I/O and throws for no rule: `Horsies.create` and `app.start` report what a definition breaks. A sender can import the contract without the worker's code.

| Field | Type | Required | Rule |
|-------|------|----------|------|
| `name` | `string` | Yes | The workflow name. Each start stores it. Not empty (`HRS-001`) |
| `definitionKey` | `string` | Yes | The stable identity. A sub-workflow node finds its child by it. Not empty (`HRS-016`), unique in the app (`HRS-017`) |
| `params` | `ParameterSchemas` | Yes | One schema per parameter of the build, with the rules of a task input: `h.*` wire types or zod schemas. `{}` for none. See [Wire Types](/horsies/typescript/tasks/wire-types/) |
| `output` | `OutputSchema` | Yes | The type of the workflow's result. The output node of every build has this type (`HRS-025`), and every read of the workflow's result decodes with it |
| `onError` | `"fail" \| "pause"` | No | Left out: `"fail"`, a failed node fails the workflow after the DAG resolves. `"pause"` pauses the workflow at a failed node; `handle.resume()` continues it |
| `checkCases` | `readonly SendInput<P>[]` | When `params` has a key | One params value per case, in the form that a sender gives. A workflow with params and no case is a type error and `HRS-027`. A workflow with `params: {}` takes none |

The build function is `(w: WorkflowBuilder, params: BodyInput<P>) => R`:

- `params` holds every parameter, a default filled. It is the value that the params give after an encode and a read back, as a child build gets it from the stored text.
- The build is sync. An `async` build is a type error. A build that returns a thenable at run time is refused.
- The build does no I/O and reads no clock and no random source. The client can run it more than once: `Horsies.create` runs it once for a workflow with no params, each start runs it once, and the engine runs it for each sub-workflow node whose child it is.
- `R` is the object that the build returns: `{ output, successPolicy? }`. Its type gives the typed node ids of `handle.resultFor`.

## The Builder

| Method | Returns |
|--------|---------|
| `w.task(contract, input, options)` | `NodeRef<O, E, Id>`: a node that runs the task of `contract`. `O` is the task's output type, `E` its codes, `Id` the node id |
| `w.subWorkflow(workflow, params, options)` | `NodeRef<O, string, Id>`: a node that runs the workflow of `workflow` as a child. `O` is the child's declared output. See [Subworkflows](/horsies/typescript/concepts/workflows/subworkflows/) |

`input` has one member per parameter of the task. A plain parameter takes a value of its type; an optional or a defaulted one may be left out. A parameter declared `h.resultOf(T)` takes a node of the same build whose output is a `T`; a value for it is a type error. A node can name only the nodes that the build made before it, so a build has no cycle.

### Node Options

| Option | Type | Node | Rule |
|--------|------|------|------|
| `id` | `string` | Both | Required. Matches `[A-Za-z0-9_\-:.]+`, at most 128 characters (`HRS-003`), unique in the build (`HRS-004`). The key of the node in the stored workflow and in `resultFor` |
| `waitsFor` | `readonly AnyNode[]` | Both | Nodes of this build that the node waits for, in addition to the sources of its parameters |
| `join` | `Join` | Both | When the node runs. Left out: `{ kind: "all" }` |
| `allowFailedDeps` | `boolean` | Both | `true`: the node runs when a dependency failed, and its parameter gets the failure. Left out: the node is `SKIPPED` |
| `queue` | `string` | Task | A queue that the app admits (`HRS-014`). Left out: the task's queue |
| `priority` | `number` | Task | An integer from 1 to 100 (`HRS-034`). Left out: the priority of the queue |
| `goodUntil` | `Temporal.Instant` | Task | The node's task expires if no worker starts it before this instant. Years 1 to 9999 (`HRS-033`) |
| `ctxFrom` | `readonly AnyNode[]` | Task | Only for a task defined with `workflowContext: true`; on another task it is a type error and `HRS-010`. Each node is a dependency of this node: a source of a parameter or a node of `waitsFor` (`HRS-009`). The workflow context of the task holds the results of these nodes |

### What the Checks Refuse

The types refuse each rule below in typed code. The run-time checks refuse the same rules in code that the types do not cover, with the code of the Python and Rust clients:

| Rule | Code |
|------|------|
| The build made no node | `HRS-002` |
| The build made more than 10,000 nodes | `HRS-038` |
| A node names a node of another build | `HRS-006` |
| The build returns no output node of its own build | `HRS-011` |
| The output node's type is not the declared `output` | `HRS-025` |
| A member that is not a parameter, or an input that is not a plain object | `HRS-019` |
| A required parameter has no value and no node | `HRS-020` |
| A node for a plain parameter, a value for an `h.resultOf` parameter, or a source of another output type | `HRS-024` |
| A value that does not encode and read back as its parameter's type | `HRS-031` |
| `ctxFrom` on a task that takes no workflow context | `HRS-010` |
| A `ctxFrom` node that is not a dependency of the node | `HRS-009` |
| A task that the app does not list | `HRS-302` |

The codes are on the [Errors Reference](/horsies/tasks/errors/#workflow-validation-hrs-001-hrs-099).

## Joins

```ts
const quoteShipping = defineTask({
  name: "quote_shipping",
  input: { carrier: h.string() },
  output: h.int(),
});

const pickCarrier = defineTask({
  name: "pick_carrier",
  input: { dhlQuote: h.resultOf(h.int()).optional() },
  output: h.string(),
});

export const shipping = defineWorkflow(
  { name: "shipping", definitionKey: "shop.shipping.v1", params: {}, output: h.string() },
  (w) => {
    const dhl = w.task(quoteShipping, { carrier: "dhl" }, { id: "quote-dhl" });
    const ups = w.task(quoteShipping, { carrier: "ups" }, { id: "quote-ups" });
    const fedex = w.task(quoteShipping, { carrier: "fedex" }, { id: "quote-fedex" });

    const picked = w.task(
      pickCarrier,
      { dhlQuote: dhl },
      { id: "pick", waitsFor: [ups, fedex], join: { kind: "quorum", minSuccess: 2 } },
    );

    return { output: picked, successPolicy: { cases: [{ required: [picked] }], optional: [dhl, ups, fedex] } };
  },
);
```

The node `pick` has three dependencies: `quote-dhl`, the source of its parameter, and the two nodes of `waitsFor`. It runs when two of them succeeded.

| `Join` | The node runs |
|--------|---------------|
| `{ kind: "all" }` | When every dependency ended |
| `{ kind: "any" }` | When one dependency succeeded |
| `{ kind: "quorum", minSuccess }` | When `minSuccess` dependencies succeeded. `minSuccess` is an integer from 1 to the number of dependencies (`HRS-013`) |

Under an `any` or a `quorum` join, a parameter fed by a node must be declared `.optional()`: its source may not have ended. A required one is a type error, and `HRS-013` at run time. The join rules are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/#join-modes).

## Output and Success Policy

The build returns a `BuildReturn`:

| Member | Type | Rule |
|--------|------|------|
| `output` | `AnyNode` | Required. A node of this build whose output type is the declared `output` |
| `successPolicy` | `SuccessPolicy` | Optional. When the workflow succeeds |

| `SuccessPolicy` member | Type | Does |
|------------------------|------|------|
| `cases` | `readonly SuccessCase[]` | The workflow succeeds when every node of `required` of one case completed. A policy with no case, and a case with no required node, are `HRS-012` |
| `optional` | `readonly AnyNode[]` | Records the nodes that may fail. It has no effect at run time |

`SuccessCase` is `{ required: readonly AnyNode[] }`. The nodes of `output` and of the success policy are the typed set of `handle.resultFor`. The rules are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/#success-cases-partial-success-policy).

## List the Workflows

`Horsies.create({ ..., tasks, workflows })` checks each workflow of `workflows`, in list order, and returns every refusal in `CreateRefused.refusals`:

| Check | Code |
|-------|------|
| An empty name | `HRS-001` |
| An empty definition key | `HRS-016` |
| A definition key that an earlier workflow of the list has, or a workflow listed twice | `HRS-017` |
| A name or a definition key that the core cannot store, or an `onError` that is not `"fail"` or `"pause"` | `HRS-033` |
| A parameter or the declared `output` that breaks a rule of a task input or a task output | `HRS-105` |
| A workflow with params and no check case | `HRS-027` |
| A workflow with no params: its build throws or returns a thenable | `HRS-029` |
| A workflow with no params: its build breaks a rule of [What the Checks Refuse](#what-the-checks-refuse) | The rule's code |

`Horsies.create` does not build a workflow with params: a start builds it with the params of the start.

List every workflow that a process starts or reads, and every child workflow, in `Horsies.create` of the app's entry. Every process of the app runs the same entry, the worker's children included, so each process has the list. See [App Config](/horsies/typescript/configuration/app-config/).

## Start a Workflow

| Method | Returns |
|--------|---------|
| `app.start(workflow, params, options?)` | `Promise<Result<WorkflowHandle<R>, WorkflowStartError<R>>>` |
| `app.retryStart(error)` | `Promise<Result<WorkflowHandle<R>, WorkflowStartError<R>>>`: the start again of an `ENQUEUE_FAILED` error |
| `app.workflowHandle(workflow, workflowId)` | `WorkflowHandleById<R>`: the handle of a stored workflow id. No I/O |

`params` takes the plain parameters of the workflow; an optional or a defaulted one may be left out. `StartOptions` has one member: `workflowId`, a `WorkflowId` (`WorkflowId.parse(text)`). Left out, the start gives the workflow a new UUID.

A start runs these steps in order:

1. The workflow is in the app's `workflows` list. Else `VALIDATION_FAILED`.
2. The workflow id is a UUID in the lower-case 8-4-4-4-12 form. Else `VALIDATION_FAILED` with `HRS-032`.
3. The params encode and read back with their types. Else `VALIDATION_FAILED`.
4. The build runs once with the params, and its nodes pass the checks of [What the Checks Refuse](#what-the-checks-refuse). Else `VALIDATION_FAILED`; the message names each finding.
5. The core writes the workflow.

A refusal of steps 1 to 4 writes nothing.

- A start with an id that already exists starts nothing. The core compares the start command of the start with the stored one ([Start a Workflow Once per Entity](/horsies/guides/start-workflow-once/)):
  - The same workflow name and the same start command: the handle of that workflow.
  - The same workflow name and another start command: `WORKFLOW_COMMAND_CONFLICT`.
  - The id holds a sub-workflow: `WORKFLOW_COMMAND_CONFLICT`.
  - Another workflow name: `WORKFLOW_ID_CONFLICT`.
- With `resendOnTransientErr: true` in the app config, the core runs a retryable start failure again with the same workflow id and sent instant.

### Start from a Task

A task body can start a workflow through the app. In a worker child, the start goes through the child's own database connections. The body does not wait for the workflow: `handle.get` in a process that runs task bodies is `waitInsideTask`.

<!-- ts-check: alone -->
```ts
import { defineTask, defineWorkflow, h, Horsies, WorkflowId } from "horsies";

const buildReport = defineTask({ name: "build_report", input: { region: h.string() }, output: h.string() });

const regionReport = defineWorkflow(
  {
    name: "region_report",
    definitionKey: "reports.region_report.v1",
    params: { region: h.string() },
    output: h.string(),
    checkCases: [{ region: "eu" }],
  },
  (w, { region }) => ({ output: w.task(buildReport, { region }, { id: "build" }) }),
);

const startReport = defineTask({
  name: "start_report",
  input: { region: h.string(), reportId: h.uuid() },
  output: h.uuid(),
  errors: ["START_REFUSED", "START_FAILED"],
});

const created = Horsies.create({
  broker: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [buildReport, startReport],
  workflows: [regionReport],
});

if (!created.ok) {
  process.exit(1);
}

const app = created.value;

app.implement(buildReport, ({ region }) => buildReport.ok(`report of ${region}`));

app.implement(startReport, async ({ region, reportId }) => {
  const workflowId = WorkflowId.parse(reportId);

  if (!workflowId.ok) {
    return startReport.err({ code: "START_REFUSED", message: "the report id is not a workflow id" });
  }

  const started = await app.start(regionReport, { region }, { workflowId: workflowId.value });

  if (!started.ok) {
    const code = started.error.retryable ? "START_FAILED" : "START_REFUSED";

    return startReport.err({ code, message: started.error.message });
  }

  return startReport.ok(started.value.workflowId);
});
```

The workflow id comes from the task's input, so the workflow starts once: after a retry of the task, the start finds the workflow and starts nothing. The retry builds the workflow again, so the build must give the same nodes on each attempt. A build that gives other nodes is another start command, and the start is `WORKFLOW_COMMAND_CONFLICT`.

### WorkflowStartError

`WorkflowStartError<R>` is a union told apart by `code`. Every member has `code`, `retryable`, `message`, `workflowName` and `workflowId`: the id of the start, the caller's or the new one.

| `code` | `retryable` | Other members | When |
|--------|-------------|---------------|------|
| `VALIDATION_FAILED` | `false` | `exception` | Steps 1 to 4 of a start; spec data that the core refuses; `retryStart` of an error that no start of this process gave. Nothing was written |
| `WORKFLOW_ID_CONFLICT` | `false` | `existingWorkflowName` | The id belongs to another workflow name. Nothing was written |
| `WORKFLOW_COMMAND_CONFLICT` | `false` | - | The id holds a workflow of the same name that another start command started, or a sub-workflow. Nothing was written |
| `ENQUEUE_FAILED` | The classification of the cause | `exception` | The connect, the schema install, the database, or the build of a sub-workflow child failed. When the commit reply was lost, the workflow may exist |

`app.retryStart(error)` takes only an `ENQUEUE_FAILED` error; another code is a type error. It sends the same spec again, with the same workflow id and the same sent instant. A workflow that the failed attempt wrote comes back as the handle of that workflow. The meaning of each code is on the [Errors Reference](/horsies/tasks/errors/#workflow-start-error-codes).

```ts
export async function startWithRetry(): Promise<void> {
  const started = await app.start(fulfilOrder, {
    order: { orderId: crypto.randomUUID(), lines: [{ sku: "tea", quantity: 1 }] },
  });

  if (started.ok) {
    console.log(`started ${started.value.workflowId}`);

    return;
  }

  switch (started.error.code) {
    case "ENQUEUE_FAILED":
      if (started.error.retryable) {
        const again = await app.retryStart(started.error);

        console.log(again.ok ? `started ${again.value.workflowId}` : `not started: ${again.error.code}`);
      }

      break;
    case "VALIDATION_FAILED":
    case "WORKFLOW_ID_CONFLICT":
    case "WORKFLOW_COMMAND_CONFLICT":
      console.error(started.error.message);
      break;
    default:
      assertNever(started.error);
  }
}
```

## WorkflowHandle

`app.start` and `app.retryStart` give a `WorkflowHandle<R>`. A handle does no I/O until a call.

| Member | Returns |
|--------|---------|
| `workflowId` | `WorkflowId` |
| `get(options?)` | `Promise<Result<TaskOutcome<O, string>, WorkflowReadError>>`: waits while the workflow runs, up to `timeoutMs` |
| `resultFor(id)` | `Promise<Result<TaskOutcome<NodeOutput, NodeCode>, WorkflowReadError>>`: the outcome of one node, read with the types of its node. Not a wait |
| `status()` | `Promise<Result<WorkflowStatus, HandleCoreError>>` |
| `results()` | `Promise<Result<ReadonlyMap<string, NodeResult>, HandleCoreError>>`: the stored result of each node that has one, by node id. A node stored without a node id is left out |
| `tasks()` | `Promise<Result<readonly WorkflowTaskInfo[], HandleCoreError>>`: every node in index order |
| `pause()` | `Promise<Result<boolean, HandleCoreError>>`: `true` when a RUNNING workflow paused |
| `resume()` | `Promise<Result<boolean, HandleCoreError>>`: `true` when a PAUSED workflow resumed. The children that the resume owes build from the app's listed workflows |
| `cancel()` | `Promise<Result<void, HandleCoreError>>`: cancels the workflow and its descendants |

`get`:

- `timeoutMs` is a whole number of milliseconds from 0 to 4,294,967,295. Left out, the wait has no bound. Another value is `invalidWait`, before any statement.
- reads the result with the workflow's declared `output` and open user codes (`string`), because a workflow holds the codes of many tasks;
- returns `failed` with the stored error of a FAILED, CANCELLED or EXPIRED workflow, or with the library code of its status (`WORKFLOW_FAILED`, `WORKFLOW_CANCELLED`, `WORKFLOW_EXPIRED`) when it stored none;
- returns `failed` with `WORKFLOW_PAUSED` for a PAUSED workflow, which a resume can continue;
- in a process that runs task bodies, is `waitInsideTask` before any statement.

`resultFor(id)` takes the id of a node of the returned object: the output node and the nodes of the success policy. Another id is a type error. The result has the output type and the codes of that node's contract.

```ts
export async function readNodes(): Promise<void> {
  const started = await app.start(fulfilOrder, {
    order: { orderId: crypto.randomUUID(), lines: [{ sku: "tea", quantity: 1 }] },
  });

  if (!started.ok) {
    return;
  }

  const charged = await started.value.resultFor("charge");

  if (charged.ok && charged.value.kind === "failed" && charged.value.error.code.kind === "user") {
    console.log(`charge failed with ${charged.value.error.code.code}`);
  }

  const nodes = await started.value.tasks();

  if (nodes.ok) {
    for (const node of nodes.value) {
      console.log(node.index, node.name, node.status);
    }
  }
}
```

`charged.value.error.code.code` has the type `"PAYMENT_DECLINED"`: the codes of `chargePayment`.

### A Handle from a Stored Id

`app.workflowHandle(workflow, workflowId)` gives a `WorkflowHandleById<R>`. It has every member of `WorkflowHandle` except the typed `resultFor`: a handle by id has no build, so `resultFor` takes what the node runs.

| Call | Reads the node |
|------|----------------|
| `resultFor(id, contract)` | A task node, with the output type and the codes of the task contract |
| `resultFor(id, workflow)` | A sub-workflow node, with the declared output of the child workflow and open user codes. The app lists the child, else `workflowNotListed` |

```ts
export async function readStored(text: string): Promise<void> {
  const workflowId = WorkflowId.parse(text);

  if (!workflowId.ok) {
    return;
  }

  const handle = app.workflowHandle(fulfilOrder, workflowId.value);
  const charged = await handle.resultFor("charge", chargePayment);

  if (charged.ok && charged.value.kind === "succeeded") {
    console.log(charged.value.value.receiptId);
  }
}
```

### NodeResult and WorkflowTaskInfo

`NodeResult` is the stored result of one node read with no type:

| `kind` | Members |
|--------|---------|
| `outcome` | `outcome`: `TaskOutcome<JsonValue, string>` |
| `unreadable` | `cause`: why the stored result does not read |

`WorkflowTaskInfo`:

| Member | Type |
|--------|------|
| `nodeId` | `Recorded<string>` |
| `index` | `number` |
| `name` | `string`: the task name, or the child's workflow name |
| `status` | `WorkflowTaskStatus` |
| `result` | `Recorded<NodeResult>` |
| `startedAt`, `completedAt` | `Recorded<Temporal.Instant>` |
| `subWorkflowId` | `Recorded<string>`: the child of a sub-workflow node |
| `subWorkflowSummary` | `Recorded<SubWorkflowSummary>`: the summary of the ended child |

`Recorded<T>` is `{ kind: "recorded", value: T }` or `{ kind: "notRecorded" }`.

### WorkflowReadError

`get` and `resultFor` return a `WorkflowReadError`, told apart by `kind`. A read error is not the workflow's outcome.

| `kind` | Members | When |
|--------|---------|------|
| `waitTimedOut` | - | The workflow had not ended when the wait ended. A later read may find it ended |
| `workflowNotFound` | - | No workflow has the id |
| `resultNotReady` | - | `resultFor`: the node stores no result yet, or the workflow has no node of this id |
| `storedResultUnreadable` | `cause` | The stored value does not read with the handle's types. `cause` holds no value |
| `unknownUserCode` | `code` | The stored error has a user code that the node's contract does not list |
| `waitInsideTask` | - | A wait in a process that runs task bodies. Nothing was read |
| `workflowNotListed` | `workflowName` | The app does not list the workflow, so its output has no read form. Nothing was read |
| `database` | `code`, `retryable`, `message` | The database or the connection failed; `code` is a `HandleErrorCode` |
| `invalidWait` | `maxMs` | A `timeoutMs` that is not a whole number of milliseconds from 0 to `maxMs`. Nothing was read |

### HandleCoreError

`status`, `results`, `tasks`, `pause`, `resume` and `cancel` return a `HandleCoreError`, a union told apart by `code`. Every member has `code`, `retryable`, `message` and `workflowId`.

| `code` | `retryable` | When |
|--------|-------------|------|
| `WORKFLOW_NOT_FOUND` | `false` | No workflow has the id |
| `DB_OPERATION_FAILED` | The classification of the cause | The connect, a statement or the commit failed. `exception` holds the cause |
| `STORED_DATA_INVALID` | `false` | A stored value does not parse. Nothing changed |
| `RESUME_RECOVERY_FAILED` | `false` | `resume`: the resume committed and the recovery pass after it failed. The workflow is RUNNING |

The meaning of each code is on the [Errors Reference](/horsies/tasks/errors/#workflow-handle-error-codes).

## Statuses

| `WorkflowStatus` | `WorkflowTaskStatus` |
|------------------|----------------------|
| `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `PAUSED`, `CANCELLED`, `EXPIRED` (`WORKFLOW_STATUSES`) | `PENDING`, `READY`, `ENQUEUED`, `RUNNING`, `COMPLETED`, `FAILED`, `SKIPPED` (`WorkflowTaskStatus`) |

The transitions are on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/#workflow-status-lifecycle).

## Things to Avoid

**Do not wait for a workflow in a task body.** `handle.get()` in a worker child is `waitInsideTask`. Start the workflow and return its id, or make the waiting work a node of the workflow.

**Do not give a build I/O, a clock or a random source.** The client and the engine run a build more than once, and a start again after a retry must give the same nodes.

<!-- ts-check: skip elided -- a Wrong / Correct pair of builds -->
```ts
// Wrong: each run of the build gives another node id.
(w) => ({ output: w.task(pickCarrier, {}, { id: `pick-${Date.now()}` }) });

// Correct: the node ids come from the params.
(w, { region }) => ({ output: w.task(pickCarrier, {}, { id: `pick-${region}` }) });
```

**Do not use a node of another build.** A node reference belongs to the build that made it: a node of another build is `HRS-006`.

**Do not leave a workflow out of `Horsies.create`.** A start of a workflow that the app does not list is `VALIDATION_FAILED`, and a read of it is `workflowNotListed`.
