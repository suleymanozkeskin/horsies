// The `runtime` object of a body in a child: the task id and the fencing token, the signal and its reasons, the
// cancel state, `spawn` and its refusals, the signal of spawned work, and the app state.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { StateValues } from "../../../horsies/src/child/state.ts";
import { stateKeyNamed } from "../../../horsies/src/child/state.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { FakeRunNative, RecordingMarks, RUN_TASK_ID, tableRun } from "./support.ts";

describe("the runtime of a run", () => {
  test("the task id and the fencing token come from the run", () => {
    const { runtime } = tableRun();

    assert.equal(runtime.taskId, RUN_TASK_ID);
    assert.deepEqual(runtime.fencingToken, { taskId: RUN_TASK_ID, value: 2 });
  });

  test("the signal fires with DEADLINE or OPERATOR, and the first cause stays", () => {
    const deadline = tableRun();

    deadline.table.cancel(deadline.runKey, "DEADLINE");
    deadline.table.cancel(deadline.runKey, "OPERATOR");
    assert.deepEqual(deadline.runtime.signal.reason, { cause: "DEADLINE" });
    assert.deepEqual(deadline.runtime.cancelState(), { kind: "requested", cause: "DEADLINE" });

    const operator = tableRun();

    operator.table.cancel(operator.runKey, "OPERATOR");
    assert.deepEqual(operator.runtime.signal.reason, { cause: "OPERATOR" });
  });

  test("cancelState before and after a request; a sync read gives the cause before the post", () => {
    const native = new FakeRunNative();
    const { runtime } = tableRun(native);

    assert.deepEqual(runtime.cancelState(), { kind: "notRequested" });

    native.cancel = { kind: "requested", cause: "OPERATOR" };
    assert.deepEqual(runtime.cancelState(), { kind: "requested", cause: "OPERATOR" });
    assert.equal(runtime.signal.aborted, false);
  });

  test("spawn starts the work in the run, with the run held for its synchronous part, and ends it when it settles", async () => {
    const native = new FakeRunNative();
    const marks = new RecordingMarks();
    const { runtime, table } = tableRun(native, marks);
    const started = runtime.spawn("flush", async () => "done");

    assert.equal(started.ok, true);
    assert.equal(await (started.ok ? started.value : Promise.reject(new Error("refused"))), "done");
    assert.deepEqual(native.registered, ["flush"]);
    assert.deepEqual(native.ended, [1n]);
    assert.deepEqual(marks.marks, [`hold ${RUN_TASK_ID}`, "release"]);
    assert.deepEqual(table.size(), { runs: 1, works: 0 });
  });

  test("spawn after the run ended is refused runEnded, and at the bound tooMuchWork; neither starts the work", () => {
    const native = new FakeRunNative();
    const { runtime } = tableRun(native);
    let starts = 0;

    native.registration = { kind: "runEnded" };
    assert.deepEqual(
      runtime.spawn("late", async () => {
        starts += 1;
      }),
      { ok: false, error: { kind: "runEnded" } },
    );

    native.registration = { kind: "tooMuchWork", bound: 10_000 };
    assert.deepEqual(
      runtime.spawn("many", async () => {
        starts += 1;
      }),
      { ok: false, error: { kind: "tooMuchWork", bound: 10_000 } },
    );
    assert.equal(starts, 0);
  });

  test("the signal of spawned work fires at the run's cancel and at the stop of left work", () => {
    const signals: AbortSignal[] = [];
    const native = new FakeRunNative();
    const { runtime, table, runKey } = tableRun(native);

    const capture = async (signal: AbortSignal): Promise<void> => {
      signals.push(signal);
      await new Promise<void>(() => undefined);
    };

    native.registration = { kind: "registered", workKey: 11n };
    void runtime.spawn("left", capture);
    native.registration = { kind: "registered", workKey: 12n };
    void runtime.spawn("cancelled", capture);

    table.leave(runKey);
    table.stopWork(11n, "LEFT_WORK");
    assert.deepEqual(signals[0]?.reason, { cause: "LEFT_WORK" });
    assert.equal(signals[1]?.aborted, false);

    const second = tableRun(native);

    native.registration = { kind: "registered", workKey: 13n };
    void second.runtime.spawn("cancelled", capture);
    second.table.cancel(second.runKey, "OPERATOR");
    assert.deepEqual(signals[2]?.reason, { cause: "OPERATOR" });
  });

  test("state gives the value that the provider built; a key of another app is a defect", () => {
    const key = stateKeyNamed<string>("provide #1 pool");
    const other = stateKeyNamed<string>("provide #1 other");
    const states = new StateValues([key]);

    states.keep(key, "the pool");

    const { runtime } = tableRun(new FakeRunNative(), new RecordingMarks(), states);

    assert.equal(runtime.state(key), "the pool");
    assert.throws(() => runtime.state(other), ClientDefect);
  });
});
