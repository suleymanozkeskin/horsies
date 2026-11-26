// The start and retryStart: each outcome of a start, the refusals before the core, the kept retry, and the params
// text that no error shows.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { checkContractList, contractLookup } from "../../../horsies/src/app/contract-list.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { WorkflowId } from "../../../horsies/src/contract/ids.ts";
import type { TraceArgument } from "../../../horsies/src/native/values/records.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import { createWorkflowArea } from "../../../horsies/src/workflow/app-area.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";
import { listedWorkflowOf, type WorkflowRegistry, workflowRegistry } from "../../../horsies/src/workflow/registry.ts";
import { NO_OP_START_OBSERVER, type StartContext, type StartObserver } from "../../../horsies/src/workflow/start.ts";
import {
  fakeCore,
  type FakeCore,
  rawCommandConflict,
  rawIdConflict,
  rawStartEnqueueFailed,
  rawValidationFailed,
  SPEC_DATA,
  WORKFLOW_ID,
} from "./core-support.ts";

const greet = defineTask({ name: "greet", input: { who: h.string() }, output: h.string() });

const hello = defineWorkflow(
  {
    name: "hello",
    definitionKey: "tests.hello.v1",
    output: h.string(),
    params: { who: h.string() },
    checkCases: [{ who: "a" }],
  },
  (w, { who }) => ({ output: w.task(greet, { who }, { id: "greet" }) }),
);

const thrower = defineWorkflow(
  { name: "thrower", definitionKey: "tests.thrower.v1", output: h.null(), params: {} },
  () => {
    throw new RangeError("no node today");
  },
);

// SAFETY: the test returns a thenable from a build, which the types refuse; the start refuses it at run time too.
const later = defineWorkflow(
  { name: "later", definitionKey: "tests.later.v1", output: h.null(), params: {} },
  (async () => ({})) as never,
);

const duplicate = defineWorkflow(
  { name: "duplicate", definitionKey: "tests.duplicate.v1", output: h.string(), params: {} },
  (w) => {
    w.task(greet, { who: "a" }, { id: "same" });

    return { output: w.task(greet, { who: "b" }, { id: "same" }) };
  },
);

const unlisted = defineWorkflow(
  { name: "unlisted", definitionKey: "tests.unlisted.v1", output: h.string(), params: {} },
  (w) => ({
    output: w.task(greet, { who: "a" }, { id: "greet" }),
  }),
);

const SENT_AT = Temporal.Instant.from("2026-11-26T10:00:00.123456Z");

function registry(): WorkflowRegistry {
  const listed = [hello, thrower, later, duplicate].map((workflow) => {
    const entry = listedWorkflowOf(workflow);

    assert(entry.ok);

    return entry.value;
  });

  return workflowRegistry(listed, contractLookup(checkContractList([greet], { kind: "default" }).entries), {
    kind: "default",
  });
}

/** The code of a start error, or `started`. */
function codeOf(started: Result<unknown, { readonly code: string }>): string {
  return started.ok ? "started" : started.error.code;
}

function contextOf(core: FakeCore, observer: StartObserver = NO_OP_START_OBSERVER): StartContext {
  return { core, registry: registry(), observer, now: () => SENT_AT, newWorkflowId: () => WORKFLOW_ID };
}

describe("app.start", () => {
  test("a started tree gives a handle; the core gets the checked spec, the id, the sent instant and no carrier", async () => {
    const core = fakeCore();
    const started = await createWorkflowArea(contextOf(core)).start(hello, { who: "eu" });

    assert(started.ok);
    assert.equal(started.value.workflowId, WORKFLOW_ID);
    assert.equal(core.starts.length, 1);
    assert.equal(core.starts[0]?.spec.nodes[0]?.staticInput, '{"who":"eu"}');
    assert.equal(core.starts[0]?.sentAtMicros, 1_795_687_200_123_456n);
    assert.deepEqual(core.starts[0]?.trace, { kind: "noCarrier" });
  });

  test("the same id with the same command gives the existing run as a handle", async () => {
    const core = fakeCore({ started: [{ kind: "exists", existing: "SAME_COMMAND" }] });
    const id = WorkflowId.parse(WORKFLOW_ID);

    assert(id.ok);

    const started = await createWorkflowArea(contextOf(core)).start(hello, { who: "eu" }, { workflowId: id.value });

    assert(started.ok);
    assert.equal(started.value.workflowId, WORKFLOW_ID);
  });

  test("another command under the id: WORKFLOW_COMMAND_CONFLICT; another workflow: WORKFLOW_ID_CONFLICT", async () => {
    const command = await createWorkflowArea(
      contextOf(fakeCore({ started: [{ kind: "failed", error: rawCommandConflict("hello") }] })),
    ).start(hello, { who: "eu" });

    const id = await createWorkflowArea(
      contextOf(fakeCore({ started: [{ kind: "failed", error: rawIdConflict("hello", "other") }] })),
    ).start(hello, { who: "eu" });

    assert(!command.ok && command.error.code === "WORKFLOW_COMMAND_CONFLICT");
    assert(!id.ok && id.error.code === "WORKFLOW_ID_CONFLICT");
    assert.equal(id.error.existingWorkflowName, "other");
  });

  const refusals: readonly (readonly [string, (area: ReturnType<typeof createWorkflowArea>) => Promise<string>])[] = [
    ["a workflow that the app does not list", async (area) => codeOf(await area.start(unlisted, {}))],
    // SAFETY: the test gives a workflow id that the type refuses.
    [
      "a workflow id that is not a UUID",
      async (area) => codeOf(await area.start(hello, { who: "a" }, { workflowId: "7" as WorkflowId })),
    ],
    // SAFETY: the test gives params of another type than the params declare.
    ["params that do not encode", async (area) => codeOf(await area.start(hello, { who: 7 } as never))],
    ["a build that throws", async (area) => codeOf(await area.start(thrower, {}))],
    ["a build that returns a thenable", async (area) => codeOf(await area.start(later, {}))],
    ["a build that fails its checks", async (area) => codeOf(await area.start(duplicate, {}))],
  ];

  for (const [name, start] of refusals) {
    test(`${name}: VALIDATION_FAILED, and the core is not called`, async () => {
      const core = fakeCore();
      const code = await start(createWorkflowArea(contextOf(core)));

      assert.deepEqual(core.starts, []);
      assert.equal(code, "VALIDATION_FAILED");
    });
  }

  test("a spec string that the writer cannot store: VALIDATION_FAILED with the path", async () => {
    const core = fakeCore({
      refusedCall: {
        kind: "textRefused",
        refused: { kind: "LoneSurrogate", path: "$.name", detail: "a string holds a lone surrogate" },
      },
    });

    const started = await createWorkflowArea(contextOf(core)).start(hello, { who: "eu" });

    assert(!started.ok && started.error.code === "VALIDATION_FAILED");
    assert.match(started.error.message, /no stored text at \$\.name: a string holds a lone surrogate/u);
  });

  test("spec data that the core's parse refuses: VALIDATION_FAILED with the field path", async () => {
    const core = fakeCore({ refusedCall: { kind: "specRefused", fieldPath: "nodes[0].queue", cause: "no class" } });
    const started = await createWorkflowArea(contextOf(core)).start(hello, { who: "eu" });

    assert(!started.ok && started.error.code === "VALIDATION_FAILED");
    assert.match(started.error.message, /the core refuses the spec data at nodes\[0\]\.queue: no class/u);
  });

  test("a VALIDATION_FAILED of the core's start is the start error, with no retry", async () => {
    const core = fakeCore({ started: [{ kind: "failed", error: rawValidationFailed("hello") }] });
    const started = await createWorkflowArea(contextOf(core)).start(hello, { who: "eu" });

    assert(!started.ok && started.error.code === "VALIDATION_FAILED");
    assert.equal(started.error.workflowName, "hello");
    assert.equal(core.starts.length, 1);
  });

  test("the start observer wraps the core's start and gives its carrier", async () => {
    const carrier: TraceArgument = { kind: "carrier", carrier: { traceparent: "00-a-b-01" }, traceIdHex: "a" };
    const seen: string[] = [];

    const observer: StartObserver = {
      observeStart: async (facts, run) => {
        seen.push(`${facts.workflowName} ${facts.workflowId}`);

        return run(carrier);
      },
    };

    const core = fakeCore();

    await createWorkflowArea(contextOf(core, observer)).start(hello, { who: "a" });

    assert.deepEqual(seen, [`hello ${WORKFLOW_ID}`]);
    assert.deepEqual(core.starts[0]?.trace, carrier);
  });
});

describe("app.retryStart", () => {
  test("ENQUEUE_FAILED: the retry sends the same text, id and sent instant again, and gives a handle", async () => {
    const core = fakeCore({
      started: [{ kind: "failed", error: rawStartEnqueueFailed("hello", true) }, { kind: "started" }],
    });

    const area = createWorkflowArea(contextOf(core));
    const started = await area.start(hello, { who: "secret-value" });

    assert(!started.ok && started.error.code === "ENQUEUE_FAILED");
    assert.equal(started.error.retryable, true);
    assert.doesNotMatch(JSON.stringify(started.error), /secret-value/);

    const retried = await area.retryStart(started.error);

    assert(retried.ok);
    assert.deepEqual(core.restarts, [
      {
        specData: SPEC_DATA,
        workflowId: WORKFLOW_ID,
        sentAtMicros: 1_795_687_200_123_456n,
        trace: { kind: "noCarrier" },
      },
    ]);
  });

  test("an ENQUEUE_FAILED that no start of this process gave: VALIDATION_FAILED, and the core is not called", async () => {
    const core = fakeCore();
    const area = createWorkflowArea(contextOf(core));

    const forged = {
      code: "ENQUEUE_FAILED",
      retryable: true,
      message: "m",
      workflowName: "hello",
      workflowId: WORKFLOW_ID,
      exception: { kind: "noCause" },
    } as const;

    // SAFETY: the test gives an error that no start gave; the type of its `exception` is not the point.
    const retried = await area.retryStart(forged as never);

    assert(!retried.ok && retried.error.code === "VALIDATION_FAILED");
    assert.deepEqual(core.restarts, []);
  });
});
