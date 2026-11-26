// The test helpers: `TaskRuntime.forTest` (with and without a workflow context) and `implementation.run` with no
// worker, no database and no addon; the app members `provide` and `onChildStart` of the hub.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { servedMembers } from "../../../horsies/src/child/app-members.ts";
import { AttemptNumber } from "../../../horsies/src/child/attempt.ts";
import { TaskRuntime, type WorkflowTaskRuntime } from "../../../horsies/src/child/runtime.ts";
import { taskIdFromCore } from "../../../horsies/src/contract/ids.ts";
import { implementationOf } from "../../../horsies/src/contract/implementation.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { StoredWorkflowContext } from "../../../horsies/src/testing/task-runtime.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { contextOf, testApp } from "../worker/support.ts";

const TASK_ID = taskIdFromCore("0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b", "test");

const add = defineTask({ name: "ping", input: { n: h.int() }, output: h.int() });

const charge = defineTask({
  name: "charge",
  input: { amount: h.int() },
  output: h.null(),
  errors: ["CARD_DECLINED", "BANK_DOWN"],
  execution: "SHARED_ASYNC",
  timeoutMs: 5_000,
  cancelGraceMs: 1_000,
});

/** The task of a context node. */
const produce = defineTask({ name: "produce", input: {}, output: h.int() });

/** A task that takes the workflow context. */
const audit = defineTask({ name: "audit", input: {}, output: h.int(), workflowContext: true });

/** The stored form of the context of a node whose `ctxFrom` names the node `produce`, which returned 10. */
const STORED_CONTEXT: StoredWorkflowContext = {
  workflow_id: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5d",
  task_index: 1,
  task_name: "audit",
  results_by_id: { produce: { __h_task_result__: true, ok: 10, err: null } },
};

class CardDeclined extends Error {
  override readonly name = "CardDeclined";
}

function attempt(value: number): AttemptNumber {
  const parsed = AttemptNumber.parse(value);

  return parsed.ok ? parsed.value : assert.fail(parsed.error.reason);
}

function runtime(): WorkflowTaskRuntime {
  return TaskRuntime.forTest({ taskId: TASK_ID, attempt: attempt(3) });
}

describe("TaskRuntime.forTest", () => {
  test("the task id, the fencing token, a signal that never fires, and spawn that runs its work", async () => {
    const forTest = runtime();
    const spawned = forTest.spawn("work", async () => "done");

    assert.equal(forTest.taskId, TASK_ID);
    assert.deepEqual(forTest.fencingToken, { taskId: TASK_ID, value: 3 });
    assert.equal(forTest.signal.aborted, false);
    assert.deepEqual(forTest.cancelState(), { kind: "notRequested" });
    assert.equal(await (spawned.ok ? spawned.value : Promise.reject(new Error("refused"))), "done");
  });

  test("state gives the value of each entry; a key with no entry is a defect", () => {
    const app = testApp();
    const pool = app.provide(() => ({ size: 4 }));
    const region = app.provide(async () => "eu-1");
    const other = app.provide(() => 1);
    const forTest = TaskRuntime.forTest({ taskId: TASK_ID, attempt: attempt(1), state: [[pool, { size: 4 }], [region, "eu-1"]] });

    assert.deepEqual(forTest.state(pool), { size: 4 });
    assert.equal(forTest.state(region), "eu-1");
    assert.throws(() => forTest.state(other), /belongs to another app/u);
  });

  test("a stored workflow context is the runtime's context", () => {
    const forTest = TaskRuntime.forTest({ taskId: TASK_ID, attempt: attempt(1), workflowContext: STORED_CONTEXT });
    const context = forTest.workflowContext.kind === "context" ? forTest.workflowContext.context : assert.fail("no context");

    assert.deepEqual([context.taskIndex, context.taskName], [1, "audit"]);
    assert.deepEqual(context.resultFor("produce", produce), { ok: true, value: { kind: "succeeded", value: 10 } });
  });

  test("no workflow context is noContext, as a run outside a workflow", () => {
    assert.deepEqual(runtime().workflowContext, { kind: "noContext" });
  });

  test("a stored form that the context reader refuses throws TypeError with the member and the rule", () => {
    assert.throws(
      () => TaskRuntime.forTest({ taskId: TASK_ID, attempt: attempt(1), workflowContext: { ...STORED_CONTEXT, task_index: -1 } }),
      { name: "TypeError", message: "horsies: TaskRuntime.forTest: workflowContext.task_index: not an integer of 0 or more" },
    );
  });

  test("the stored workflow context of a test runtime is written by the codec writer, which makes strict JSON only", () => {
    // Each value would break the strict read rule in a text: a float that is not finite, a lone surrogate, a value
    // that is not JSON. The writer refuses each, so the reader never gets such a text.
    const values: readonly unknown[] = [Number.NaN, Number.POSITIVE_INFINITY, "\ud800", undefined];

    for (const value of values) {
      // SAFETY: the test gives a stored result outside `JsonValue` on purpose: the writer refuses it.
      const workflowContext = { ...STORED_CONTEXT, results_by_id: { produce: value as never } };

      assert.throws(() => TaskRuntime.forTest({ taskId: TASK_ID, attempt: attempt(1), workflowContext }), {
        name: "TypeError",
        message: /^horsies: TaskRuntime\.forTest: the workflowContext is not storable: /u,
      });
    }
  });
});

describe("implementation.run", () => {
  test("a value is checked with the output type and given as a task read gives it", async () => {
    const app = testApp();
    const implementation = app.implement(add, ({ n }, { ok }) => ok(n + 1));

    assert.deepEqual(await implementation.run({ n: 41 }, runtime()), { kind: "succeeded", value: 42 });
  });

  test("a task error keeps its code, message and data", async () => {
    const app = testApp();
    const implementation = app.implement(charge, async (_input, { err }) => err({ code: "BANK_DOWN", message: "retry later", data: { wait: 30 } }));

    assert.deepEqual(await implementation.run({ amount: 5 }, runtime()), {
      kind: "failed",
      error: {
        code: { kind: "user", code: "BANK_DOWN" },
        message: "retry later",
        data: { wait: 30 },
        exception: null,
        subWorkflow: null,
      },
    });
  });

  test("a thrown error of a mapped class is stored with the mapped code and its record", async () => {
    const app = testApp();

    const implementation = app.implement(
      charge,
      async () => {
        throw new CardDeclined("declined by the bank");
      },
      { exceptions: [[CardDeclined, "CARD_DECLINED"]] },
    );

    const outcome = await implementation.run({ amount: 5 }, runtime());

    assert.equal(outcome.kind, "failed");

    const error = outcome.kind === "failed" ? outcome.error : assert.fail("the run failed");

    assert.deepEqual(error.code, { kind: "user", code: "CARD_DECLINED" });
    assert.equal(error.message, "Unhandled exception in task charge: CardDeclined: declined by the bank");
    assert.deepEqual(error.data, { exception_type: "CardDeclined" });
    assert.equal(error.exception?.type, "CardDeclined");
  });

  test("an input that does not encode is WORKER_SERIALIZATION_ERROR, and the body does not run", async () => {
    let calls = 0;
    const app = testApp();

    const implementation = app.implement(add, ({ n }, { ok }) => {
      calls += 1;

      return ok(n);
    });
    // SAFETY: the test gives an input outside the parameter type on purpose: the encode refuses it.

    const outcome = await implementation.run({ n: "secret-text" } as never, runtime());

    assert.equal(calls, 0);
    assert.deepEqual(outcome.kind === "failed" ? outcome.error.code : undefined, { kind: "library", code: "WORKER_SERIALIZATION_ERROR" });
    assert.doesNotMatch(JSON.stringify(outcome), /secret-text/u);
  });

  test("the body of a task that takes the context reads the workflow context of the test runtime", async () => {
    const implementation = implementationOf(
      audit,
      (_input, runtime) => {
        const context = runtime.workflowContext;
        const produced = context.kind === "context" ? context.context.resultFor("produce", produce) : undefined;

        return runtime.ok(produced?.ok === true && produced.value.kind === "succeeded" ? produced.value.value + 1 : 0);
      },
      [],
    );

    const withContext = TaskRuntime.forTest({ taskId: TASK_ID, attempt: attempt(1), workflowContext: STORED_CONTEXT });

    assert.deepEqual(await implementation.run({}, withContext), { kind: "succeeded", value: 11 });
    assert.deepEqual(await implementation.run({}, runtime()), { kind: "succeeded", value: 0 });
  });

  test("an output that does not read back is WORKER_SERIALIZATION_ERROR", async () => {
    const app = testApp();
    // SAFETY: the body returns a value outside the output type on purpose: the read-back refuses it.
    const implementation = app.implement(add, (_input, { ok }) => ok("not an int" as never));
    const outcome = await implementation.run({ n: 1 }, runtime());

    assert.deepEqual(outcome.kind === "failed" ? outcome.error.code : undefined, { kind: "library", code: "WORKER_SERIALIZATION_ERROR" });
    assert.match(outcome.kind === "failed" ? (outcome.error.message ?? "") : "", /^task returned a value of type ping\.output/u);
  });
});

describe("the child members of the app hub", () => {
  test("provide returns a key and calls nothing; onChildStart keeps its hook; both keep their order", () => {
    const app = testApp();
    let calls = 0;

    const key = app.provide(function openPool() {
      calls += 1;

      return 1;
    });

    app.onChildStart(function warmCache() {
      calls += 1;
    });

    const members = servedMembers(contextOf(app));

    assert.equal(calls, 0);
    assert.equal(key.name, "provide #1 openPool");
    assert.deepEqual(
      members.hooks.map((hook) => hook.name),
      ["onChildStart #1 warmCache"],
    );
    assert.throws(() => app.provide(() => 2), /before cli\.run serves the child/u);
  });
});
