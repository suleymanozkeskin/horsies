// The second argument of a body: one frozen object per run with the own members of the run's runtime and the
// contract's `ok` and `err`, in a child run and in a test run; and the runtime of `TaskRuntime.forTest`, whose function
// members are own and bound, so a body may destructure them.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { AttemptNumber } from "../../../horsies/src/child/attempt.ts";
import { TaskRuntime } from "../../../horsies/src/child/runtime.ts";
import { stateKeyNamed } from "../../../horsies/src/child/state.ts";
import { implementationOf, implementationParts } from "../../../horsies/src/contract/implementation.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { adapterRun, RUN_TASK_ID } from "./support.ts";

const add = defineTask({ name: "add", input: { a: h.int(), b: h.int() }, output: h.int(), errorCodes: ["TOO_BIG"] });

/** The own member names of the second argument of a body of a task that takes no workflow context. */
const MEMBERS = ["taskId", "fencingToken", "signal", "cancelState", "spawn", "state", "workflowContext", "ok", "err"];

function attemptOne(): AttemptNumber {
  const attempt = AttemptNumber.parse(1);

  return attempt.ok ? attempt.value : assert.fail("1 is an attempt number");
}

describe("the second argument of a body in a child run", () => {
  test("a frozen object with the runtime's members and ok and err as own members", async () => {
    const seen: object[] = [];

    const implementation = implementationOf(
      add,
      ({ a, b }, runtime) => {
        seen.push(runtime);

        return runtime.ok(a + b);
      },
      [],
    );

    const run = await adapterRun(implementationParts(implementation), '{"a":2,"b":3}');

    assert.deepEqual(run.outcome, { kind: "value", json: "5" });
    assert.equal(seen.length, 1);
    assert.equal(Object.isFrozen(seen[0]), true);
    assert.deepEqual(Object.keys(seen[0] ?? {}).toSorted(), MEMBERS.toSorted());
  });

  test("destructured members work apart from their object: ok, err, cancelState, spawn", async () => {
    const implementation = implementationOf(
      add,
      async ({ a, b }, { ok, err, cancelState, spawn, taskId, fencingToken }) => {
        assert.deepEqual(cancelState(), { kind: "notRequested" });
        assert.equal(taskId, RUN_TASK_ID);
        assert.equal(fencingToken.value, 2);

        const spawned = spawn("sum", () => Promise.resolve(a + b));

        if (!spawned.ok) {
          return assert.fail("spawn starts the work");
        }

        const sum = await spawned.value;

        return sum > 10 ? err({ code: "TOO_BIG", message: "the sum is too big" }) : ok(sum);
      },
      [],
    );

    const parts = implementationParts(implementation);

    assert.deepEqual((await adapterRun(parts, '{"a":2,"b":3}')).outcome, { kind: "value", json: "5" });

    const failed = (await adapterRun(parts, '{"a":20,"b":3}')).outcome;

    assert.equal(failed.kind, "error");
    assert.deepEqual(failed.kind === "error" ? failed.parts.code : undefined, { kind: "custom", code: "TOO_BIG" });
  });
});

describe("the second argument of a body in a test run", () => {
  test("implementation.run gives the members of the test runtime and the contract's ok and err", async () => {
    const seen: object[] = [];
    const runtime = TaskRuntime.forTest({ taskId: RUN_TASK_ID, attempt: attemptOne() });

    const implementation = implementationOf(
      add,
      ({ a, b }, argument) => {
        seen.push(argument);

        return argument.err({ code: "TOO_BIG", data: { sum: a + b } });
      },
      [],
    );

    const outcome = await implementation.run({ a: 1, b: 2 }, runtime);

    assert.equal(outcome.kind, "failed");
    assert.deepEqual(outcome.kind === "failed" ? [outcome.error.kind, outcome.error.code] : undefined, ["custom", "TOO_BIG"]);
    assert.deepEqual(outcome.kind === "failed" ? outcome.error.data : undefined, { sum: 3 });
    assert.equal(Object.isFrozen(seen[0]), true);
    assert.deepEqual(Object.keys(seen[0] ?? {}).toSorted(), MEMBERS.toSorted());
  });
});

describe("TaskRuntime.forTest", () => {
  test("the runtime holds no ok and no err: implementation.run adds them", () => {
    const runtime = TaskRuntime.forTest({ taskId: RUN_TASK_ID, attempt: attemptOne() });

    assert.equal(Object.hasOwn(runtime, "ok"), false);
    assert.equal(Object.hasOwn(runtime, "err"), false);
  });

  test("cancelState, spawn and state are own members that work apart from the runtime", async () => {
    const pool = stateKeyNamed<number>("pool");

    const { cancelState, spawn, state } = TaskRuntime.forTest({
      taskId: RUN_TASK_ID,
      attempt: attemptOne(),
      state: [[pool, 4]],
    });

    assert.deepEqual(cancelState(), { kind: "notRequested" });
    assert.equal(state(pool), 4);

    const spawned = spawn("work", () => Promise.resolve(4));

    assert.equal(spawned.ok ? await spawned.value : undefined, 4);
  });
});
