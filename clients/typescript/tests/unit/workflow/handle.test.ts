// The workflow handle: each method's outcomes and errors, waitInsideTask before any call, unknownUserCode in
// resultFor, open codes in get, stored texts that the strict check refused; the seat operations through a stub relay;
// the child spec source answers; and the workflow context.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { checkContractList, contractLookup } from "../../../horsies/src/app/contract-list.ts";
import type { SeatOperationMap, SeatRelay } from "../../../horsies/src/app/core-access.ts";
import { writtenJsonText } from "../../../horsies/src/codec/json-text.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { WorkflowId } from "../../../horsies/src/contract/ids.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { childSpecAnswer } from "../../../horsies/src/workflow/child-spec.ts";
import { workflowContextOf } from "../../../horsies/src/workflow/context.ts";
import { relayedWorkflowCore, serveWorkflowsSeatOperation } from "../../../horsies/src/workflow/core.ts";
import { workflowHandleByIdOf, workflowHandleOf } from "../../../horsies/src/workflow/handle.ts";
import type { BuildReturn } from "../../../horsies/src/workflow/node.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";
import { listedWorkflowOf, type WorkflowRegistry, workflowRegistry } from "../../../horsies/src/workflow/registry.ts";
import {
  type CoreScript,
  fakeCore,
  type FakeCore,
  okEnvelope,
  rawDbFailed,
  userErrorEnvelope,
  userErrorText,
  WORKFLOW_ID,
} from "./core-support.ts";

const count = defineTask({ name: "count", input: {}, output: h.int(), errors: ["TOO_MANY"] });
const name = defineTask({ name: "name", input: { n: h.resultOf(h.int()) }, output: h.string() });

const flow = defineWorkflow({ name: "flow", definitionKey: "tests.flow.v1", output: h.string(), params: {} }, (w) => {
  const counted = w.task(count, {}, { id: "count" });

  return { output: w.task(name, { n: counted }, { id: "name" }), successPolicy: { cases: [{ required: [counted] }] } };
});

const child = defineWorkflow(
  { name: "child", definitionKey: "tests.child.v1", output: h.int(), params: { n: h.int() }, checkCases: [{ n: 1 }] },
  (w, { n }) => {
    if (n < 0) {
      throw new RangeError("negative");
    }

    return { output: w.task(count, {}, { id: n === 0 ? "bad id" : "count" }) };
  },
);

function registry(): WorkflowRegistry {
  const listed = [flow, child].map((workflow) => {
    const entry = listedWorkflowOf(workflow);

    assert(entry.ok);

    return entry.value;
  });

  return workflowRegistry(listed, contractLookup(checkContractList([count, name], { kind: "default" }).entries), {
    kind: "default",
  });
}

function workflowId(): WorkflowId {
  const parsed = WorkflowId.parse(WORKFLOW_ID);

  assert(parsed.ok);

  return parsed.value;
}

function handle(script: CoreScript = {}, core: FakeCore = fakeCore(script)) {
  const targets = new Map([
    ["count", { kind: "task", contract: count } as const],
    ["name", { kind: "task", contract: name } as const],
  ]);

  return {
    core,
    handle: workflowHandleOf<BuildReturn>({
      core,
      registry: registry(),
      workflow: flow,
      workflowId: workflowId(),
      targets,
    }),
  };
}

describe("WorkflowHandle.get", () => {
  test("a completed workflow: the output node's value, read with the output kind", async () => {
    const { handle: wf } = handle({
      wait: { kind: "completed", result: { kind: "checked", text: okEnvelope('"seven"') } },
    });

    assert.deepEqual(await wf.get({ timeoutMs: 10 }), { ok: true, value: { kind: "succeeded", value: "seven" } });
  });

  test("an error end: the stored error with an open user code; no stored error: the code of the status", async () => {
    const stored = await handle({
      wait: { kind: "endedWithError", status: "FAILED", error: { kind: "checked", text: userErrorText("ANY_CODE") } },
    }).handle.get();

    const none = await handle({
      wait: { kind: "endedWithError", status: "CANCELLED", error: { kind: "none" } },
    }).handle.get();

    assert(stored.ok && stored.value.kind === "failed");
    assert.deepEqual(stored.value.error.code, { kind: "user", code: "ANY_CODE" });
    assert(none.ok && none.value.kind === "failed");
    assert.deepEqual(none.value.error.code, { kind: "library", code: "WORKFLOW_CANCELLED" });
  });

  test("a paused workflow: WORKFLOW_PAUSED", async () => {
    const read = await handle({ wait: { kind: "paused" } }).handle.get();

    assert(read.ok && read.value.kind === "failed");
    assert.deepEqual(read.value.error.code, { kind: "library", code: "WORKFLOW_PAUSED" });
  });

  const errors: readonly (readonly [string, CoreScript, string])[] = [
    ["a wait that ends first", { wait: { kind: "timedOut" } }, "waitTimedOut"],
    ["no workflow of the id", { wait: { kind: "notFound" } }, "workflowNotFound"],
    [
      "a text that the strict check refused",
      { wait: { kind: "completed", result: { kind: "refused", cause: "x" } } },
      "storedResultUnreadable",
    ],
    ["a NULL result", { wait: { kind: "completed", result: { kind: "none" } } }, "storedResultUnreadable"],
    ["a failed read", { wait: { kind: "failed", error: rawDbFailed() } }, "database"],
  ];

  for (const [title, script, kind] of errors) {
    test(`${title}: ${kind}`, async () => {
      const read = await handle(script).handle.get();

      assert(!read.ok);
      assert.equal(read.error.kind, kind);
    });
  }

  test("in a process that runs task bodies: waitInsideTask before any wait", async () => {
    const { core, handle: wf } = handle({ role: "RUNS_TASK_BODIES" });

    assert.deepEqual(await wf.get(), { ok: false, error: { kind: "waitInsideTask" } });
    assert.deepEqual(core.calls, ["processRole"]);
  });

  test("a timeoutMs out of range: invalidWait, and nothing is read", async () => {
    const { core, handle: wf } = handle();

    assert.deepEqual(await wf.get({ timeoutMs: -1 }), {
      ok: false,
      error: { kind: "invalidWait", maxMs: 4_294_967_295 },
    });
    assert.deepEqual(core.calls, []);
  });
});

describe("the read form of a sub-workflow output", () => {
  test("it is the child's declared output: no build of the child runs, whatever its params", async () => {
    let builds = 0;

    const counted = defineWorkflow(
      {
        name: "counted",
        definitionKey: "tests.counted.v1",
        output: h.int(),
        params: { n: h.int() },
        checkCases: [{ n: 1 }],
      },
      (w) => {
        builds += 1;

        return { output: w.task(count, {}, { id: "count" }) };
      },
    );

    const parent = defineWorkflow(
      { name: "parent", definitionKey: "tests.parent.v1", output: h.int(), params: {} },
      (w) => ({
        output: w.subWorkflow(counted, { n: 2 }, { id: "child" }),
      }),
    );

    const listed = [flow, child, counted, parent].map((workflow) => {
      const entry = listedWorkflowOf(workflow);

      assert(entry.ok);

      return entry.value;
    });

    const withChild = workflowRegistry(
      listed,
      contractLookup(checkContractList([count, name], { kind: "default" }).entries),
      {
        kind: "default",
      },
    );

    const core = fakeCore({
      wait: { kind: "completed", result: { kind: "checked", text: okEnvelope("7") } },
      nodeResult: { kind: "stored", result: { kind: "checked", text: okEnvelope("8") } },
    });

    const targets = new Map([["child", { kind: "subWorkflow", workflow: counted } as const]]);

    const wf = workflowHandleOf<BuildReturn>({
      core,
      registry: withChild,
      workflow: parent,
      workflowId: workflowId(),
      targets,
    });

    assert.deepEqual(await wf.get(), { ok: true, value: { kind: "succeeded", value: 7 } });
    assert.deepEqual(await wf.resultFor("child"), { ok: true, value: { kind: "succeeded", value: 8 } });
    assert.equal(builds, 0);
  });
});

describe("WorkflowHandle.resultFor", () => {
  test("a stored result is read with the node's contract", async () => {
    const read = await handle({
      nodeResult: { kind: "stored", result: { kind: "checked", text: okEnvelope("7") } },
    }).handle.resultFor("count");

    assert.deepEqual(read, { ok: true, value: { kind: "succeeded", value: 7 } });
  });

  test("a user code that the contract does not list: unknownUserCode", async () => {
    const read = await handle({
      nodeResult: { kind: "stored", result: { kind: "checked", text: userErrorEnvelope("OTHER") } },
    }).handle.resultFor("count");

    assert.deepEqual(read, { ok: false, error: { kind: "unknownUserCode", code: "OTHER" } });
  });

  test("no stored result: resultNotReady; a failed read: database", async () => {
    assert.deepEqual(await handle().handle.resultFor("count"), { ok: false, error: { kind: "resultNotReady" } });

    const failed = await handle({ nodeResult: { kind: "failed", error: rawDbFailed() } }).handle.resultFor("count");

    assert(!failed.ok && failed.error.kind === "database");
  });

  test("a stored text that the core's strict check refuses ends as storedResultUnreadable and never reaches the reader", async () => {
    // The addon ran the strict check (`nodeResult`); a refused text crosses as its cause only, so no text is decoded.
    const refused = await handle({
      nodeResult: { kind: "stored", result: { kind: "refused", cause: "a repeated key" } },
    }).handle.resultFor("count");

    assert.deepEqual(refused, { ok: false, error: { kind: "storedResultUnreadable", cause: "a repeated key" } });
  });

  test("a handle by id reads with the contract that the caller gives", async () => {
    const core = fakeCore({ nodeResult: { kind: "stored", result: { kind: "checked", text: okEnvelope('"x"') } } });

    const byId = workflowHandleByIdOf<BuildReturn>({
      core,
      registry: registry(),
      workflow: flow,
      workflowId: workflowId(),
      targets: new Map(),
    });

    assert.deepEqual(await byId.resultFor("name", name), { ok: true, value: { kind: "succeeded", value: "x" } });
  });
});

describe("the other handle methods", () => {
  test("status, pause, resume and cancel give the core's answer; a failed one gives the handle class", async () => {
    const { handle: wf } = handle();

    assert.deepEqual(await wf.status(), { ok: true, value: "RUNNING" });
    assert.deepEqual(await wf.pause(), { ok: true, value: true });
    assert.deepEqual(await wf.resume(), { ok: true, value: true });
    assert.deepEqual(await wf.cancel(), { ok: true, value: undefined });

    const failed = handle({
      status: { kind: "failed", error: rawDbFailed() },
      cancel: { kind: "failed", error: rawDbFailed() },
    }).handle;

    const status = await failed.status();

    assert(!status.ok && status.error.code === "DB_OPERATION_FAILED");
    assert(!(await failed.cancel()).ok);
  });

  test("results: one entry per node with an id, an unreadable text as unreadable; no workflow: WORKFLOW_NOT_FOUND", async () => {
    const results = await handle({
      nodeResults: {
        kind: "nodes",
        nodes: [
          { nodeId: { kind: "recorded", text: "count" }, result: { kind: "checked", text: okEnvelope("7") } },
          { nodeId: { kind: "recorded", text: "name" }, result: { kind: "refused", cause: "dup" } },
          { nodeId: { kind: "notRecorded" }, result: { kind: "checked", text: okEnvelope("1") } },
        ],
      },
    }).handle.results();

    assert(results.ok);
    assert.deepEqual([...results.value.keys()], ["count", "name"]);
    assert.deepEqual(results.value.get("count"), { kind: "outcome", outcome: { kind: "succeeded", value: 7 } });
    assert.deepEqual(results.value.get("name"), { kind: "unreadable", cause: "dup" });

    const absent = await handle({ nodeResults: { kind: "workflowAbsent" } }).handle.results();

    assert(!absent.ok && absent.error.code === "WORKFLOW_NOT_FOUND");
  });

  test("tasks: the nodes; a stored status that is not a node status: STORED_DATA_INVALID", async () => {
    const node = {
      nodeId: { kind: "recorded", text: "count" },
      index: 0,
      name: "count",
      status: "COMPLETED",
      result: { kind: "none" },
      startedAt: { kind: "recorded", epochMicros: 1_000_000n },
      completedAt: { kind: "notRecorded" },
      subWorkflowId: { kind: "notRecorded" },
      subWorkflowSummary: { kind: "none" },
    } as const;

    const tasks = await handle({ nodes: { kind: "nodes", nodes: [node] } }).handle.tasks();
    const invalid = await handle({ nodes: { kind: "nodes", nodes: [{ ...node, status: "LOST" }] } }).handle.tasks();

    assert(tasks.ok);
    assert.equal(tasks.value[0]?.status, "COMPLETED");
    assert.deepEqual(tasks.value[0]?.startedAt, {
      kind: "recorded",
      value: Temporal.Instant.fromEpochMilliseconds(1_000),
    });
    assert(!invalid.ok && invalid.error.code === "STORED_DATA_INVALID");
  });
});

describe("the seat operations", () => {
  test("each operation goes through the relay to the main-thread side, and gets its reply", async () => {
    const mainThread = fakeCore({ nodeResult: { kind: "stored", result: { kind: "checked", text: okEnvelope("1") } } });
    const asked: string[] = [];

    const relay: SeatRelay = {
      request: async <A extends keyof SeatOperationMap>(area: A, operation: SeatOperationMap[A]["operation"]) => {
        asked.push(`${area} ${operation.kind}`);

        // SAFETY: this relay serves the workflows area only; the test asks only its operations.
        return (await serveWorkflowsSeatOperation(
          mainThread,
          operation as SeatOperationMap["workflows"]["operation"],
        )) as SeatOperationMap[A]["reply"];
      },
    };

    const core = relayedWorkflowCore(relay);

    assert.deepEqual(await core.status(WORKFLOW_ID), { kind: "status", status: "RUNNING" });
    assert.deepEqual(await core.nodeResults(WORKFLOW_ID), { kind: "nodes", nodes: [] });
    assert.deepEqual(await core.nodes(WORKFLOW_ID), { kind: "nodes", nodes: [] });
    assert.deepEqual(await core.cancel(WORKFLOW_ID), { kind: "done" });
    assert.deepEqual(await core.pause(WORKFLOW_ID), { kind: "changed", changed: true });
    assert.deepEqual(await core.resume(WORKFLOW_ID), { kind: "changed", changed: true });
    assert.equal((await core.nodeResult(WORKFLOW_ID, "count")).kind, "stored");
    assert.equal(
      (await core.restart({ specData: "{}", workflowId: WORKFLOW_ID, sentAtMicros: 1n, trace: { kind: "noCarrier" } }))
        .kind,
      "outcome",
    );
    assert.deepEqual(asked, [
      "workflows status",
      "workflows nodeResults",
      "workflows nodes",
      "workflows cancel",
      "workflows pause",
      "workflows resume",
      "workflows nodeResult",
      "workflows restart",
    ]);
  });

  test("get in a seat isolate: waitInsideTask, with no message to the main thread", async () => {
    const asked: string[] = [];

    const relay: SeatRelay = {
      request: async (area) => {
        asked.push(area);

        throw new Error("no message is sent");
      },
    };

    const core = relayedWorkflowCore(relay);

    const wf = workflowHandleOf<BuildReturn>({
      core,
      registry: registry(),
      workflow: flow,
      workflowId: workflowId(),
      targets: new Map(),
    });

    assert.deepEqual(await wf.get(), { ok: false, error: { kind: "waitInsideTask" } });
    assert.deepEqual(asked, []);
  });
});

describe("the child spec source", () => {
  const facts = { taskOptionsText: () => "{}", queueClass: () => "standard_30d", queuePriority: () => 100 };

  test("a listed child with params that decode: its spec data", () => {
    const answer = childSpecAnswer(registry(), facts, { definitionKey: "tests.child.v1", inputText: '{"n":1}' });

    assert.equal(answer.kind, "built");
    assert(answer.kind === "built" && answer.specData.startsWith('{"data_version":1,"name":"child"'));
  });

  test("each other answer: notListed, memberRefused, paramsUndecodable, buildFailed", () => {
    const answer = (definitionKey: string, inputText: string) =>
      childSpecAnswer(registry(), facts, { definitionKey, inputText });

    assert.deepEqual(answer("tests.none.v1", "{}"), { kind: "notListed" });
    assert.deepEqual(answer("tests.child.v1", '{"n":1,"x":2}'), {
      kind: "memberRefused",
      key: "x",
      cause: "no param takes the member",
    });
    assert.equal(answer("tests.child.v1", '{"n":"one"}').kind, "memberRefused");
    assert.equal(answer("tests.child.v1", "{}").kind, "paramsUndecodable");
    assert.deepEqual(answer("tests.child.v1", '{"n":-1}'), {
      kind: "buildFailed",
      code: { kind: "noCode" },
      message: "the build threw RangeError: negative",
    });

    const checks = answer("tests.child.v1", '{"n":0}');

    assert(checks.kind === "buildFailed");
    assert.deepEqual(checks.code, { kind: "catalog", code: "HRS-003" });
  });
});

describe("the workflow context", () => {
  const text = writtenJsonText(
    `{"workflow_id":"${WORKFLOW_ID}","task_index":2,"task_name":"audit","results_by_id":` +
      `{"count":${okEnvelope("7")},"other":${userErrorEnvelope("OTHER")},"bad":${okEnvelope('"x"')}}}`,
  );

  test("resultFor reads a context node with the contract; each error has its kind", () => {
    const context = workflowContextOf(text);

    assert(context.ok);
    assert.equal(context.value.taskIndex, 2);
    assert.deepEqual(context.value.resultFor("count", count), { ok: true, value: { kind: "succeeded", value: 7 } });
    assert.deepEqual(context.value.resultFor("other", count), {
      ok: false,
      error: { kind: "unknownUserCode", nodeId: "other", code: "OTHER" },
    });
    assert.deepEqual(context.value.resultFor("none", count), {
      ok: false,
      error: { kind: "notInContext", nodeId: "none" },
    });
    assert.equal(context.value.resultFor("bad", count).ok, false);
  });

  test("a context text with a member of another type is refused with the member", () => {
    const refused = workflowContextOf(writtenJsonText(`{"workflow_id":"7","task_index":0,"task_name":"a"}`));

    assert.deepEqual(refused, {
      ok: false,
      error: { kind: "ContextRefused", member: "workflow_id", detail: "not a UUID in the lower-case 8-4-4-4-12 form" },
    });
  });
});
