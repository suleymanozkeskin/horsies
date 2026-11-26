// The workflows of the app hub: `Horsies.create({ workflows })` with its refusals after the contract refusals, on a
// main thread and on a worker thread, and `app.start`, `app.retryStart` and `app.workflowHandle` through the app.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { createHorsies, type Horsies } from "../../../horsies/src/app.ts";
import type { HorsiesConfig } from "../../../horsies/src/app/create.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { WorkflowId } from "../../../horsies/src/contract/ids.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { type AnyWorkflowContract, defineWorkflow } from "../../../horsies/src/workflow/public.ts";
import { DATABASE_URL, fakeEnvironment, fakeProducer } from "../app/support.ts";
import { fakeCore, type FakeCore, rawStartEnqueueFailed, WORKFLOW_ID } from "./core-support.ts";

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

const broken = defineWorkflow(
  { name: "broken", definitionKey: "tests.broken.v1", output: h.string(), params: {} },
  (w) => {
    w.task(greet, { who: "a" }, { id: "same" });

    return { output: w.task(greet, { who: "b" }, { id: "same" }) };
  },
);

function config(workflows: readonly AnyWorkflowContract[], tasks: HorsiesConfig["tasks"] = [greet]): HorsiesConfig {
  return { broker: { databaseUrl: DATABASE_URL }, tasks, workflows };
}

function appWith(core: FakeCore): Horsies {
  const created = createHorsies(config([hello]), fakeEnvironment({ producer: { ...fakeProducer(), workflows: core } }));

  assert(created.ok);

  return created.value;
}

describe("Horsies.create with workflows", () => {
  test("the workflow refusals follow the contract refusals", () => {
    const refusedTask = defineTask({ name: "", queue: "x", input: {}, output: h.null() });
    const created = createHorsies(config([broken], [greet, refusedTask]), fakeEnvironment());

    assert(!created.ok);
    assert.deepEqual(
      created.error.refusals.map((refused) => refused.code),
      ["HRS-200", "HRS-004"],
    );
  });

  test("on a worker thread the workflow checks run, and no core loads", () => {
    const environment = fakeEnvironment({ mainThread: false });
    const created = createHorsies(config([broken]), environment);

    assert(!created.ok);
    assert.deepEqual(
      created.error.refusals.map((refused) => refused.code),
      ["HRS-004"],
    );
    assert.deepEqual(environment.loads, { checks: 0, producers: 0 });
  });
});

describe("app.start, app.retryStart and app.workflowHandle", () => {
  test("a start goes to the producer's workflow core and gives a handle", async () => {
    const core = fakeCore();
    const started = await appWith(core).start(hello, { who: "eu" });

    assert(started.ok);
    assert.equal(core.starts.length, 1);
    assert.equal(core.starts[0]?.spec.name, "hello");
  });

  test("retryStart sends the kept spec again", async () => {
    const core = fakeCore({
      started: [{ kind: "failed", error: rawStartEnqueueFailed("hello", true) }, { kind: "started" }],
    });

    const app = appWith(core);
    const started = await app.start(hello, { who: "eu" });

    assert(!started.ok && started.error.code === "ENQUEUE_FAILED");
    assert((await app.retryStart(started.error)).ok);
    assert.equal(core.restarts.length, 1);
  });

  test("a handle by id reads through the same core, with no I/O before a call", async () => {
    const core = fakeCore();
    const id = WorkflowId.parse(WORKFLOW_ID);

    assert(id.ok);

    const handle = appWith(core).workflowHandle(hello, id.value);

    assert.deepEqual(core.calls, []);
    assert.deepEqual(await handle.status(), { ok: true, value: "RUNNING" });
  });
});
