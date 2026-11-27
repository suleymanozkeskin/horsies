# horsies (TypeScript) — Workflows

Reference for building, starting and reading workflow DAGs with the
`horsies` package: workflow definitions, the builder `w`, task nodes, data
flow, sub-workflows, the starts and the workflow handle. The behaviour
(statuses, joins, failure propagation, pause and resume) is on the shared
docs page `concepts/workflows/workflow-semantics`.

The build function makes the spec as data in the client. The package checks
the spec with the rules and the `HRS-nnn` codes of the Python and Rust
clients before anything is written.

## A Workflow Definition

```ts
import { defineTask, defineWorkflow, h } from "horsies";

const Order = h.object({ orderId: h.uuid(), totalCents: h.int() });
const Receipt = h.object({ receiptId: h.string(), chargedCents: h.int() });

const chargeOrder = defineTask({
  name: "charge_order",
  input: { order: Order },
  output: h.int(),
  errorCodes: ["CARD_DECLINED"],
});

const sendReceipt = defineTask({
  name: "send_receipt",
  input: { orderId: h.uuid(), charged: h.taskOutcome(chargeOrder) },
  output: Receipt,
});

export const checkout = defineWorkflow(
  {
    name: "checkout",
    definitionKey: "shop.checkout.v1",
    parameters: { order: Order },
    outputType: Receipt,
    onError: "fail",
    checkCases: [{ order: { orderId: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b", totalCents: 900 } }],
  },
  (w, { order }) => {
    const charged = w.task(chargeOrder, { order }, { nodeId: "charge" });
    const receipt = w.task(sendReceipt, { orderId: order.orderId, charged }, { nodeId: "receipt" });

    return { output: receipt, successPolicy: { cases: [{ required: [charged, receipt] }] } };
  },
);
```

`defineWorkflow(definition, build)` returns a frozen `WorkflowContract`. It
does no I/O and throws for no rule: `Horsies.create` and `check` report what
the definition breaks.

| Field | Rule |
|---|---|
| `name` | The workflow name. Each start stores it. Not empty (`HRS-001`) |
| `definitionKey` | The stable identity. A sub-workflow node and the engine find the workflow by it. Not empty (`HRS-016`), unique in the app (`HRS-017`) |
| `parameters` | One schema per parameter of the build, with the rules of a task input (`h.*` or zod). `{}` for none |
| `outputType` | The schema of the workflow's result, with the rules of a task output. The output node of every build has this type (typecheck; `HRS-025`), and every read of the result decodes with it. Optional: left out, the workflow has no output node (see "A Workflow With No Output") |
| `onError` | `"fail"` (default) or `"pause"`. Another value is `HRS-033` |
| `checkCases` | One parameters value per case. Required when `parameters` has a key (typecheck; `HRS-027`); a workflow with `parameters: {}` takes none. A case that is not parameters of the workflow is `HRS-028` |

The build function `(w, parameters) => ({ output, successPolicy })`, or
`(w, parameters) => ({ successPolicy })` for a workflow with no `outputType`:

- It is sync. It does no I/O and reads no clock and no random source. It may
  run more than once: at each start, for each check case, and for each
  sub-workflow node of the workflow.
- `parameters` is the value that the stored parameters text reads back to: defaults
  are filled.
- A build that throws or returns a thenable is refused: at `check` with
  `HRS-029`, at a start with `VALIDATION_FAILED`.
- List the workflow in `Horsies.create({ tasks, workflows })`, with every
  task that its nodes run. `Horsies.create` builds a workflow with no parameters
  once and runs every spec check on it.

## The Builder `w`

| Method | Gives |
|---|---|
| `w.task(contract, input, options)` | A task node: `NodeRef<O, E, Id>`, with the contract's output type and codes |
| `w.subWorkflow(workflow, parameters, options)` | A sub-workflow node: `NodeRef<O, string, Id>`, with the child's output type (`NoWorkflowOutput` for a child with no `outputType`) |

The input of a node has one member per parameter:

- A plain parameter takes a value of its type.
- A parameter declared `h.taskOutcome(contract)` takes a node whose output
  and codes fit those of the contract: a node of the contract, or of another
  task with the same output type and the same or fewer codes. For a workflow
  contract it takes a sub-workflow node of that workflow. The node waits for
  that source, and the body gets the source's outcome, typed by the
  contract (`outcome.value`, `outcome.error.code`).
- An `.optional()` or defaulted parameter may be left out.
- A node for a plain parameter, a value for an `h.taskOutcome` parameter, a
  missing parameter, an unknown parameter, a source of another output type
  and a source with codes that the contract does not list fail typecheck. Untyped code gets `HRS-024`, `HRS-020`, `HRS-019` or
  `HRS-031`.

| Option | Rule |
|---|---|
| `nodeId` | Required. `[A-Za-z0-9_\-:.]+`, at most 128 characters (`HRS-003`), unique in the build (`HRS-004`). The key of the node in the stored workflow and in `resultFor` |
| `waitsFor` | Nodes of this build to wait for, with no data |
| `join` | `{ kind: "all" }` (default), `{ kind: "any" }`, `{ kind: "quorum", minSuccess }` (1 to the node's dependencies; `HRS-013`) |
| `allowFailedDependencies` | `true`: the node runs when a dependency failed |
| `queue` | Task node: a queue that the app admits, in place of the task's queue (`HRS-014`) |
| `priority` | Task node: an integer 1 (first) to 100 (last) (`HRS-034`) |
| `goodUntil` | Task node: a `Temporal.Instant`; the node's task expires if no worker starts it by then |
| `workflowContextFrom` | Task node of a task with `workflowContext: true`: dependencies whose results the core puts in the task's workflow context |

- A node can name only nodes that the build made before it, so a spec has
  no cycle. A node of another build is refused (`HRS-006`, `HRS-011`,
  `HRS-012`).
- A build has at least one node (`HRS-002`) and at most 10,000 (`HRS-038`).
- The return object holds `output`, a node of this build whose type is the
  declared `outputType` (`HRS-011`, `HRS-025`), and an optional
  `successPolicy`. A workflow with no `outputType` returns no `output`; an
  output node there fails typecheck (untyped code: `HRS-025`).
- `workflowContextFrom` on a task without `workflowContext: true` fails
  typecheck (untyped code: `HRS-010`). Each `workflowContextFrom` node is a
  dependency of the node (`HRS-009`): name it in `waitsFor` or as a
  parameter source.

### The Workflow Context

A task with `workflowContext: true` has `runtime.workflowContext`. A task
without it has no such member (typecheck).

| Value | When |
|---|---|
| `{ kind: "context", context }` | The run is a workflow node that names `workflowContextFrom` |
| `{ kind: "noContext" }` | A direct send, or a node with no `workflowContextFrom` |

- `context` holds `workflowId`, `nodeIndex` and `taskName`, and reads the
  nodes of `workflowContextFrom`: `resultFor(nodeId, contract)` (the node's
  `TaskOutcome`, read with the contract's output type and codes),
  `hasResult(nodeId)`, and for a sub-workflow node `summaryFor(nodeId)`,
  `outputFor(nodeId, workflow)` and `hasSummary(nodeId)`.
- A read gives `notInContext` for a node that is not in
  `workflowContextFrom` or that did not end, `unreadable` for a value that
  does not read with the contract, and `unknownCustomCode` (with the stored
  code in `storedCode`) for a code that the contract does not list.

<!-- ts-check: alone -->
```ts
import { defineTask, defineWorkflow, h, type Horsies } from "horsies";

const charge = defineTask({ name: "charge", input: { amount: h.int() }, output: h.int() });
const audit = defineTask({ name: "audit", input: {}, output: h.boolean(), workflowContext: true });

export const checkout = defineWorkflow(
  { name: "checkout", definitionKey: "shop.checkout.v1", parameters: {}, outputType: h.boolean() },
  (w) => {
    const charged = w.task(charge, { amount: 10 }, { nodeId: "charge" });

    return { output: w.task(audit, {}, { nodeId: "audit", waitsFor: [charged], workflowContextFrom: [charged] }) };
  },
);

export function implementAudit(app: Horsies) {
  return app.implement(audit, (_input, { ok, workflowContext }) => {
    switch (workflowContext.kind) {
      case "context": {
        const charged = workflowContext.context.resultFor("charge", charge);

        return ok(charged.ok && charged.value.kind === "succeeded");
      }

      case "noContext":
        return ok(false);
    }
  });
}
```

Data flow of an `h.taskOutcome` parameter:

| Source | Default | `allowFailedDependencies: true` |
|---|---|---|
| COMPLETED | Runs with `{ kind: "succeeded", value }` | Same |
| FAILED | The node is SKIPPED | Runs with `{ kind: "failed", error }` |
| SKIPPED | The node is SKIPPED | Runs with `failed` and the code `UPSTREAM_SKIPPED` |

Under an `any` or `quorum` join a source may not have ended when the node
runs, so an `h.taskOutcome` parameter fed by a node under such a join must be
declared `.optional()` (typecheck; `HRS-013`). It is absent when its source
did not end.

```ts
const quoteShipping = defineTask({
  name: "quote_shipping",
  input: { carrier: h.string() },
  output: h.string(),
});

const pickCarrier = defineTask({
  name: "pick_carrier",
  input: { first: h.taskOutcome(quoteShipping).optional() },
  output: h.string(),
});

export const shipping = defineWorkflow(
  { name: "shipping", definitionKey: "shop.shipping.v1", parameters: {}, outputType: h.string() },
  (w) => {
    const quotes = ["dhl", "ups", "fedex"].map((carrier) =>
      w.task(quoteShipping, { carrier }, { nodeId: `quote-${carrier}` }),
    );
    const [first] = quotes;
    const picked = w.task(
      pickCarrier,
      first === undefined ? {} : { first },
      { nodeId: "pick", waitsFor: quotes, join: { kind: "quorum", minSuccess: 2 } },
    );

    return { output: picked, successPolicy: { cases: [{ required: [picked] }], optional: quotes } };
  },
);
```

## Success Policy and `onError`

- The workflow succeeds when one case has every required node COMPLETED.
- `optional` is declarative: it records which nodes may fail, and it has no
  effect at run time. A node that no case requires does not change the
  result, with or without `optional`.
- A policy that names a node of another build is `HRS-012`.
- `onError: "fail"` (default): a failed node fails the workflow after the
  DAG resolves. `onError: "pause"`: a failed node pauses the workflow;
  `handle.resume()` continues it.

## Sub-Workflows

```ts
const fetchRegion = defineTask({
  name: "fetch_region",
  input: { region: h.string() },
  output: h.int(),
});

export const regionReport = defineWorkflow(
  {
    name: "region_report",
    definitionKey: "reports.region_report.v1",
    parameters: { region: h.string() },
    outputType: h.int(),
    checkCases: [{ region: "eu" }],
  },
  (w, { region }) => ({ output: w.task(fetchRegion, { region }, { nodeId: "fetch" }) }),
);

const addTotals = defineTask({
  name: "add_totals",
  input: { eu: h.taskOutcome(regionReport), us: h.taskOutcome(regionReport) },
  output: h.int(),
});

export const allRegions = defineWorkflow(
  { name: "all_regions", definitionKey: "reports.all_regions.v1", parameters: {}, outputType: h.int() },
  (w) => {
    const eu = w.subWorkflow(regionReport, { region: "eu" }, { nodeId: "eu" });
    const us = w.subWorkflow(regionReport, { region: "us" }, { nodeId: "us" });

    return { output: w.task(addTotals, { eu, us }, { nodeId: "total", allowFailedDependencies: true }) };
  },
);
```

- `w.subWorkflow(workflow, parameters, options)` runs the workflow as a child.
  Its parameters follow the input rules of a node; a child with `parameters: {}`
  takes `{}` (`HRS-022`). Its options are `nodeId`, `waitsFor`, `join` and
  `allowFailedDependencies`.
- The node's output type is the child's declared `outputType` and its code type
  is `string` (a workflow holds the codes of many tasks), so the node feeds
  an `h.taskOutcome(workflow)` parameter of that child. For a child with no
  `outputType` the success value is `NoWorkflowOutput`
  (`{ kind: "noWorkflowOutput" }`): the core stores `null` for the node, as
  Python reads `None`.
- List the child in `Horsies.create({ workflows })`. A start of a parent
  whose child is not listed is `VALIDATION_FAILED` (`HRS-020`). A child
  whose definition key the app of the worker does not list fails its node
  with `SUBWORKFLOW_LOAD_FAILED`.
- The engine asks the app for the child's spec inside a workflow step and
  runs the child's build there. The wait for the JavaScript thread to take
  that call is bounded at 5 s; past the bound the step rolls back and runs
  again through its retry or the workflow recovery. The Python client has no
  such bound. A child build that throws, or whose spec fails a check, fails
  its node with `WORKFLOW_ENQUEUE_FAILED`.
- The depth limit of nested children is 32: a deeper node fails with
  `SUBWORKFLOW_DEPTH_LIMIT_REACHED`. `check` reports a nesting that reaches
  it for certain (`HRS-036`) and a child that includes itself with the same
  parameters and no wait between (`HRS-037`).
- A failed child fails its node with `SUB_WORKFLOW_FAILED`. The node's
  `error.subWorkflow` holds the child's `workflowId` and its `summary`
  (status, output, task counts, error summary).

## A Workflow With No Output

```ts
import { defineTask, defineWorkflow, h, type Horsies } from "horsies";

const refreshRegion = defineTask({ name: "refresh_region", input: { region: h.string() }, output: h.int() });

export const refreshAll = defineWorkflow(
  { name: "refresh_all", definitionKey: "reports.refresh_all.v1", parameters: {} },
  (w) => {
    for (const region of ["eu", "us"]) {
      w.task(refreshRegion, { region }, { nodeId: `refresh-${region}` });
    }

    return {};
  },
);

export async function refreshed(app: Horsies): Promise<void> {
  const started = await app.start(refreshAll, {});

  if (!started.ok) {
    return;
  }

  const result = await started.value.get({ waitMs: 60_000 });

  if (result.ok && result.value.kind === "succeeded") {
    for (const [nodeId, outcome] of result.value.value.byNodeId) {
      console.log(nodeId, outcome.kind);
    }
  }
}
```

- `outputType` left out: the build returns no `output`, and the core keeps no
  output node (`NO_OUTPUT`), as Python's `output=None` and Rust's
  `OutputlessResults`.
- `get` of a COMPLETED workflow gives `WorkflowTerminalResults`:
  `{ kind: "terminalResults", byNodeId }`, the outcome of each terminal node by
  node id. Each value is a `JsonValue` and each custom code is open. A node
  entry that does not read is a failed outcome with
  `RESULT_DESERIALIZATION_ERROR` and the node id in `data.node_id`; the other
  entries stay readable.
- A stored result whose form does not match the definition (the definition
  gained or lost `outputType` between the start and the read) is
  `storedResultUnreadable`.
- A sub-workflow node of such a child, `resultFor` of that node and an
  `h.taskOutcome(workflow)` parameter give `NoWorkflowOutput`.

## Starts

```ts
import { type Horsies, WorkflowId } from "horsies";

export async function runCheckout(app: Horsies, orderId: string): Promise<void> {
  const started = await app.start(checkout, { order: { orderId, totalCents: 900 } });

  if (!started.ok) {
    switch (started.error.kind) {
      case "enqueueFailed":
        if (started.error.retryable) {
          const again = await app.resendStart(started.error);

          console.log(again.ok ? `started ${again.value.workflowId}` : again.error.message);
        }

        break;
      case "idConflict":
      case "commandConflict":
      case "validationFailed":
        console.error(started.error.message);
        break;
    }

    return;
  }

  const handle = started.value;
  const result = await handle.get({ waitMs: 60_000 });

  if (!result.ok) {
    console.error(`the read failed: ${result.error.kind}`);

    return;
  }

  const outcome = result.value;

  switch (outcome.kind) {
    case "succeeded":
      console.log(`receipt ${outcome.value.receiptId}`);
      break;
    case "failed":
      console.log(`checkout ended with ${outcome.error.code}`);
      break;
  }

  const charged = await handle.resultFor("charge");

  if (charged.ok && charged.value.kind === "failed") {
    const error = charged.value.error;

    console.log(error.kind === "custom" ? `declined: ${error.code}` : `built in: ${error.code}`);
  }
}
```

`app.start(workflow, parameters, { workflowId })` runs these steps and stops at
the first refusal. Every refusal before the core's write writes nothing.

1. The app lists the workflow. Else `VALIDATION_FAILED`.
2. `workflowId` is a `WorkflowId`; left out, a new UUID. A text that is not
   the lower-case 8-4-4-4-12 form is `VALIDATION_FAILED`.
3. The parameters encode and read back; the build runs once with the read-back
   value and passes every spec check. Else `VALIDATION_FAILED`.
4. One start through the core.

- A start with an id that exists starts nothing. The core compares the start
  command with the stored one:
  - same name, same start command: the handle of that workflow;
  - same name, another start command: `WORKFLOW_COMMAND_CONFLICT`;
  - another name: `WORKFLOW_ID_CONFLICT`.
- The start command is the spec: the name, the definition key, `onError`,
  the output, the success policy, and each top-level node (id, task name,
  input byte for byte, queue, priority, dependencies, join, options). A
  spec that changes between two starts with one id (a value from the
  clock, a deploy) is a conflict.
- A task body may start a workflow. It does not wait for it: a wait in a
  worker child is `waitInsideTask`.
- A workflow with a required `h.taskOutcome` param cannot be started with
  `app.start` (typecheck); only a sub-workflow node runs it.

`WorkflowStartError<R>`, told apart by `kind`, named after its `code`. Every
variant has `message`, `workflowName`, `workflowId` (the id of the start:
the caller's, or the one minted for it) and `retryable`.

| `kind` | `code` | Fields | When |
|---|---|---|---|
| `validationFailed` | `VALIDATION_FAILED` | `cause` | Before any write: not listed, a bad id, parameters that do not read back, a build that threw, a spec check, a child that is not listed. Not retryable |
| `idConflict` | `WORKFLOW_ID_CONFLICT` | `existingWorkflowName` | The id belongs to another workflow name. Nothing was written. Not retryable |
| `commandConflict` | `WORKFLOW_COMMAND_CONFLICT` | - | The id holds a workflow of the same name with another start command, or a sub-workflow. Nothing was written. Not retryable |
| `enqueueFailed` | `ENQUEUE_FAILED` | `cause` | The connect, the schema install or the start failed. `retryable` is the classification of the cause. A start again with the same id finds the workflow when the commit reply was lost |

`app.resendStart(error)` sends the same spec text, workflow id and sent
instant again. It takes an `ENQUEUE_FAILED` error that a start of this
process gave; another error is `VALIDATION_FAILED`.

## `WorkflowHandle`

| Member | Gives |
|---|---|
| `workflowId` | The `WorkflowId` |
| `get({ waitMs })` | `Result<TaskOutcome<O, string>, WorkflowReadError>`: waits while the workflow runs, at most `waitMs` (0 to 4,294,967,295; left out: no bound). `O` is `WorkflowTerminalResults` for a workflow with no `outputType` |
| `resultFor(nodeId)` | `Result<TaskOutcome<O, E>, WorkflowReadError>`: one node of the returned object (the output and the success policy nodes), typed by its contract. Not a wait |
| `status()` | `Result<WorkflowStatus, HandleCoreError>` |
| `results()` | `Result<ReadonlyMap<string, NodeResult>, HandleCoreError>`: the stored result of each node that has one, by node id |
| `nodes()` | `Result<readonly WorkflowNodeInfo[], HandleCoreError>`: every node in index order |
| `pause()` | `Result<boolean, HandleCoreError>`: `true` when a RUNNING workflow paused |
| `resume()` | `Result<boolean, HandleCoreError>`: `true` when a PAUSED workflow resumed |
| `cancel()` | `Result<void, HandleCoreError>`: cancels the workflow and its descendants |

- `get` reads custom codes as open text (`string`), because a workflow holds
  the codes of many tasks. A FAILED, CANCELLED or EXPIRED workflow is
  `failed` with its stored error, or with the built-in code of its status
  (`WORKFLOW_FAILED`, `WORKFLOW_CANCELLED`, `WORKFLOW_EXPIRED`). A PAUSED
  workflow is `failed` with `WORKFLOW_PAUSED`; a resume can continue it.
- `NodeResult` is `{ kind: "outcome", outcome }` (a
  `TaskOutcome<JsonValue, string>`) or `{ kind: "unreadable", cause }`.
- `WorkflowNodeInfo` has `nodeId`, `index`, `name`, `status`
  (`WorkflowNodeStatus`), `result`, `startedAt`, `completedAt`,
  `subWorkflowId` and `subWorkflowSummary`; a fact that the row may not hold
  is a `Recorded<T>`.

A handle of a stored id:

```ts
export async function checkoutState(app: Horsies, text: string): Promise<void> {
  const workflowId = WorkflowId.parse(text);

  if (!workflowId.ok) {
    console.error(workflowId.error.detail);

    return;
  }

  const handle = app.workflowHandle(checkout, workflowId.value);
  const status = await handle.status();

  if (!status.ok) {
    console.error(`${status.error.code}: ${status.error.message}`);

    return;
  }

  if (status.value === "PAUSED") {
    const resumed = await handle.resume();

    console.log(resumed.ok ? `resumed: ${String(resumed.value)}` : resumed.error.message);
  }

  const charged = await handle.resultFor("charge", chargeOrder);

  if (charged.ok) {
    console.log(charged.value.kind);
  }
}
```

- `app.workflowHandle(workflow, workflowId)` does no I/O. It has no build,
  so `resultFor(nodeId, contract)` takes what the node runs: the task contract
  of a task node, or the workflow contract of a sub-workflow node (read
  with the child's declared output, or as `NoWorkflowOutput` for a child
  with no `outputType`, and open custom codes).
- A read of a workflow that the app does not list is `noTypeAvailable`,
  before any statement.

| `WorkflowReadError` `kind` | `code` | Fields | When |
|---|---|---|---|
| `waitTimeout` | `WAIT_TIMEOUT` | - | The workflow had not ended when the wait ended |
| `workflowNotFound` | `WORKFLOW_NOT_FOUND` | - | No workflow has the id |
| `resultNotReady` | `RESULT_NOT_READY` | - | `resultFor`: the node stores no result yet, or the workflow has no node of the id |
| `storedResultUnreadable` | `RESULT_DESERIALIZATION_ERROR` | `cause` | The stored value does not read with the handle's types |
| `unknownCustomCode` | `UNKNOWN_CUSTOM_CODE` | `storedCode` | The stored custom code is not in the node contract's `errorCodes` |
| `waitInsideTask` | `WAIT_INSIDE_TASK` | - | A wait in a worker child. Nothing was read |
| `noTypeAvailable` | `NO_TYPE_AVAILABLE` | `workflowName` | The app does not list the workflow, so its output has no type to read with. Nothing was read |
| `databaseError` | `DATABASE_ERROR` | `databaseCode` (the handle code), `retryable`, `message` | The database or the connection failed |
| `invalidInput` | `INVALID_INPUT` | `maxMs` | `waitMs` outside its range. Nothing was read |
| `seatRelayFull` | `SEAT_RELAY_FULL` | `retryable: true`, `message` | A `SHARED_THREAD` body only: the seat relay holds its bound of waiting calls. Nothing was read |

`HandleCoreError` (from `status`, `results`, `nodes`, `pause`, `resume`,
`cancel`), told apart by `kind`, named after its `code`; every variant has
`message`, `workflowId` and `retryable`:

| `kind` | `code` | When |
|---|---|---|
| `notFound` | `WORKFLOW_NOT_FOUND` | No workflow has the id |
| `databaseOperationFailed` | `DATABASE_OPERATION_FAILED` | The database or the connection failed; `retryable` is the classification of the cause |
| `storedDataInvalid` | `STORED_DATA_INVALID` | A stored value does not read |
| `resumeRecoveryFailed` | `RESUME_RECOVERY_FAILED` | `resume` committed and the work after it failed: the workflow is RUNNING |

## Statuses

| `WorkflowStatus` (`WORKFLOW_STATUSES`) | `WorkflowNodeStatus` |
|---|---|
| `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `PAUSED`, `CANCELLED`, `EXPIRED` | `PENDING`, `READY`, `ENQUEUED`, `RUNNING`, `COMPLETED`, `FAILED`, `SKIPPED` |

## Check Cases

`check` builds each workflow with no parameters once and each workflow with
parameters once per check case, runs every spec check on each spec, and walks
the sub-workflow nodes down to the depth limit. A finding is a refusal with
its `HRS-nnn` code; the check fails.

- A check case gives every param of the build. A plain param takes the
  value that a start gives; it is read back as a start reads it. An
  `h.taskOutcome` param takes the outcome that its upstream node would give:
  `{ kind: "succeeded", value }` (the value is read back with the contract's
  output type) or `{ kind: "failed", error }` (given to the build as it is).
  So a workflow that only a sub-workflow node starts has check cases too.
- Give the cases that reach each branch of the build: `check` sees only the
  specs that the cases build.

## Validation Codes

| Code | HorsiesError |
|---|---|
| `HRS-001` | An empty workflow name |
| `HRS-002` | A build with no node |
| `HRS-003`, `HRS-004` | A node id outside the pattern or the length; two nodes with one id |
| `HRS-006`, `HRS-011`, `HRS-012` | A node of another build (dependency, output, success policy) |
| `HRS-009`, `HRS-010` | A `workflowContextFrom` node that is not a dependency; `workflowContextFrom` on a task without `workflowContext: true` |
| `HRS-013` | A quorum outside 1 to the node's dependencies, or a required `h.taskOutcome` parameter under an `any` or quorum join |
| `HRS-014`, `HRS-034` | A node queue that the app does not admit; a priority outside 1 to 100 |
| `HRS-016`, `HRS-017` | An empty definition key; a key that another listed workflow has |
| `HRS-019` | An input key that binds to no parameter, or an input that is not a plain object |
| `HRS-020` | A missing parameter; a child workflow that the app does not list |
| `HRS-022` | Parameters given to a child with `parameters: {}` |
| `HRS-024` | A node for a plain parameter |
| `HRS-025` | An output node whose type is not the declared `outputType`; an output node with no `outputType`, or none with an `outputType` |
| `HRS-027`, `HRS-028` | A workflow with parameters and no check cases; a case that is not parameters of the workflow |
| `HRS-029` | A build that throws or returns a thenable |
| `HRS-031` | A value that does not encode or read back, or has another type |
| `HRS-033` | An `onError` that is not `"fail"` or `"pause"` |
| `HRS-036`, `HRS-037` | A sub-workflow nesting that reaches the depth limit, or includes itself with the same parameters |
| `HRS-038` | More than 10,000 nodes |
| `HRS-302` | A node of a task that the app does not list |

## Common Pitfalls

**Do not wait for a workflow in a task body.** `handle.get()` in a worker
child is `waitInsideTask`. Start the workflow and return, or make the
waiting work a node of the workflow.

**Give the build no side effects and no clock.** It runs at each start, for
each check case and for each sub-workflow node. A value from the clock
changes the start command, so a second start with the same id is a
`WORKFLOW_COMMAND_CONFLICT`.

**Declare `h.taskOutcome(contract)` for a parameter that a node feeds.** A plain
parameter takes a value only.

**Declare a node-fed parameter `.optional()` under an `any` or quorum
join.** Its source may not have ended.

**List every task and child workflow.** A node of an unlisted task is
`HRS-302`; an unlisted child is `HRS-020`.

**Give every workflow with parameters its check cases.** `check` builds only
what the cases reach.

## All Key Imports

<!-- ts-check: alone -->
```ts
import {
  // Definitions and builds
  defineWorkflow, type WorkflowContract, type WorkflowDefinition, type WorkflowBuilder,
  type WorkflowBuild, type OnError, type CheckCase, type CheckCases, type BuildReturn,
  // Nodes
  type NodeRef, type AnyNode, type JoinType, type TaskNodeOptions, type SubWorkflowNodeOptions,
  type TaskOutcomeWire, type UpstreamValue, type UpstreamCode,
  type SuccessPolicy, type SuccessCase, type WorkflowOutput, type SubWorkflowOutput,
  type BuildReturnWithoutOutput, type WorkflowTerminalResults, type NoWorkflowOutput,
  // Starts
  WorkflowId, type StartOptions, type WorkflowStartError, type StartEnqueueFailed,
  // Handles
  type WorkflowHandle, type WorkflowHandleById, type WorkflowReadError, type HandleCoreError,
  type NodeResult, type WorkflowNodeInfo, type WorkflowStatus, WorkflowNodeStatus,
  WORKFLOW_STATUSES, type SubWorkflowSummary, type SubWorkflowFailure, type TaskOutcome,
  // The workflow context of a task
  type RuntimeWorkflowContext, type WorkflowContext, type ContextReadError,
} from "horsies";
```
