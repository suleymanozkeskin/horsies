// The workflow start end to end, with a built addon and a database: `Horsies.create` with workflows, a start, a start
// whose root node is a sub-workflow (the child spec source builds the child inside the start), the start-once rule,
// and a result wait that times out. No worker runs, so each workflow stays RUNNING.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`) and `HORSIES_TEST_DATABASE_URL` names a
// throwaway PostgreSQL database. The CI runs this file on Node and on Bun.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe, test } from "node:test";

import { defineTask, defineWorkflow, h, Horsies, WorkflowId } from "../../../horsies/src/index.ts";
import { appCoreHandle } from "../../../horsies/src/app/core-access.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { workflowsAddon } from "../../../horsies/src/native/workflows/addon.ts";
import { testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

const greet = defineTask({ name: "workflow_end_to_end_greet", input: { who: h.string() }, output: h.string() });

const hello = defineWorkflow(
  {
    name: "end_to_end_hello",
    definitionKey: "tests.end_to_end_hello.v1",
    output: h.string(),
    params: { who: h.string() },
    checkCases: [{ who: "a" }],
  },
  (w, { who }) => ({ output: w.task(greet, { who }, { id: "greet" }) }),
);

const parent = defineWorkflow(
  { name: "end_to_end_parent", definitionKey: "tests.end_to_end_parent.v1", output: h.string(), params: {} },
  (w) => ({
    output: w.subWorkflow(hello, { who: "child" }, { id: "child" }),
  }),
);

/** The app of the test database, or the failed test with every refusal. */
function testApp(): Horsies {
  const created = Horsies.create({
    broker: { databaseUrl: { kind: "value", value: testDatabaseUrl() } },
    tasks: [greet],
    workflows: [hello, parent],
  });

  if (!created.ok) {
    assert.fail(`Horsies.create refused: ${created.error.refusals.map((refusal) => refusal.message).join("; ")}`);
  }

  return created.value;
}

function newWorkflowId(): WorkflowId {
  const parsed = WorkflowId.parse(randomUUID());

  assert(parsed.ok);

  return parsed.value;
}

describe("the child spec source of the app", () => {
  test("it is registered on the app's one core handle, the handle that the worker runs on", () => {
    const handle = appCoreHandle(appInternals(testApp()).core);
    const again = workflowsAddon().registerChildSpecSource(handle.native, () => undefined, []);

    assert.equal(envelopeValue(again), "alreadySet");
  });
});

describe("the workflow start with a database", () => {
  test("a start gives a handle; RUNNING; a bounded wait is waitTimeout", async () => {
    const app = testApp();
    const started = await app.start(hello, { who: "eu" });

    if (!started.ok) {
      assert.fail(`the start failed: ${started.error.code}: ${started.error.message}`);
    }

    assert.deepEqual(await started.value.status(), { ok: true, value: "RUNNING" });
    assert.deepEqual(await started.value.get({ waitMs: 200 }), { ok: false, error: { kind: "waitTimeout", code: "WAIT_TIMEOUT" } });
  });

  test("a start whose root is a sub-workflow node builds the child inside the start", async () => {
    const app = testApp();
    const started = await app.start(parent, {});

    if (!started.ok) {
      assert.fail(`the start failed: ${started.error.code}: ${started.error.message}`);
    }

    const nodes = await started.value.tasks();

    assert(nodes.ok);
    assert.equal(nodes.value[0]?.subWorkflowId.kind, "recorded");
    assert.deepEqual(await started.value.get({ waitMs: 200 }), { ok: false, error: { kind: "waitTimeout", code: "WAIT_TIMEOUT" } });
  });

  test("start once: the same id and params give the run; other params give WORKFLOW_COMMAND_CONFLICT", async () => {
    const app = testApp();
    const workflowId = newWorkflowId();
    const first = await app.start(hello, { who: "a" }, { workflowId });
    const again = await app.start(hello, { who: "a" }, { workflowId });
    const other = await app.start(hello, { who: "b" }, { workflowId });
    const otherWorkflow = await app.start(parent, {}, { workflowId });

    assert(first.ok && again.ok);
    assert.equal(again.value.workflowId, workflowId);
    assert(!other.ok && other.error.code === "WORKFLOW_COMMAND_CONFLICT");
    assert(!otherWorkflow.ok && otherWorkflow.error.code === "WORKFLOW_ID_CONFLICT");
  });
});
