// The loop marks: the turns every 100 ms, the hold and release of the adapter, and the culprit method after an
// `await` (a fake of the Promise hooks; the method of this runtime).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  LOOP_TURN_TICK_MS,
  type LoopNative,
  loopMarksOf,
  type PromiseHookCallbacks,
  runtimeCulpritMethod,
  startCulpritMarks,
  startTurns,
} from "../../../horsies/src/child/loop-marks.ts";
import { runStore } from "../../../horsies/src/child/run-store.ts";
import { FakeRunNative, RUN_TASK_ID, tableRun } from "./support.ts";

/** The core's holder cell: one mark, no nesting (`HostLoop::hold` sets it, `release` clears it). */
class HolderCell implements LoopNative {
  holder = "noRun";

  loopTurned(): void {
    return undefined;
  }

  loopHold(taskId: string): void {
    this.holder = taskId;
  }

  loopRelease(): void {
    this.holder = "noRun";
  }
}

class RecordingLoop implements LoopNative {
  readonly marks: string[] = [];

  loopTurned(): void {
    this.marks.push("turned");
  }

  loopHold(taskId: string): void {
    this.marks.push(`hold ${taskId}`);
  }

  loopRelease(): void {
    this.marks.push("release");
  }
}

describe("the loop marks", () => {
  test("a turn at the start, then one every 100 ms; the stop ends the timer", () => {
    const loop = new RecordingLoop();
    const timers: number[] = [];
    let tick = (): void => undefined;
    let stopped = false;

    const stop = startTurns(loop, (milliseconds, onTick) => {
      timers.push(milliseconds);
      tick = onTick;

      return () => {
        stopped = true;
      };
    });

    tick();
    tick();
    stop();
    assert.deepEqual(timers, [LOOP_TURN_TICK_MS]);
    assert.equal(LOOP_TURN_TICK_MS, 100);
    assert.deepEqual(loop.marks, ["turned", "turned", "turned"]);
    assert.equal(stopped, true);
  });

  test("hold gives the holder before it, and restore puts it back on the addon", () => {
    const loop = new RecordingLoop();
    const marks = loopMarksOf(loop);
    const before = marks.hold(RUN_TASK_ID);

    marks.restore(before);
    assert.deepEqual(before, { kind: "noRun" });
    assert.deepEqual(loop.marks, [`hold ${RUN_TASK_ID}`, "release"]);
  });

  test("a spawn in the synchronous part of a body keeps the body's mark in the core's cell", () => {
    const cell = new HolderCell();
    const marks = loopMarksOf(cell);
    const { runtime } = tableRun(new FakeRunNative(), marks);
    const body = marks.hold(RUN_TASK_ID);

    void runtime.spawn("audit", async () => undefined);
    // A block here names the body's run: every sample of the core reads the body's task id.
    assert.equal(cell.holder, RUN_TASK_ID);
    marks.restore(body);
    assert.equal(cell.holder, "noRun");
  });

  test("the culprit method marks the run of a Promise at each continuation, and no run for another Promise", () => {
    const loop = new RecordingLoop();
    let callbacks: PromiseHookCallbacks | undefined;

    startCulpritMarks(
      {
        kind: "promiseHooks",
        createHook: (given) => {
          callbacks = given;

          return () => undefined;
        },
      },
      loopMarksOf(loop),
    );

    const ofRun = Promise.resolve(1);
    const outside = Promise.resolve(2);

    runStore.run({ taskId: RUN_TASK_ID, taskName: "charge", attempt: 1 }, () => callbacks?.init(ofRun));
    callbacks?.init(outside);
    callbacks?.before(ofRun);
    callbacks?.after(ofRun);
    callbacks?.before(outside);
    callbacks?.after(outside);
    assert.deepEqual(loop.marks, [`hold ${RUN_TASK_ID}`, "release"]);
  });

  test("the method of this runtime: the Promise hooks on Node, none on Bun", () => {
    assert.equal(runtimeCulpritMethod().kind, "bun" in process.versions ? "none" : "promiseHooks");
    assert.equal(startCulpritMarks({ kind: "none" }, loopMarksOf(new RecordingLoop()))(), undefined);
  });
});
