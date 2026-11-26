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
import { RUN_TASK_ID } from "./support.ts";

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

  test("hold and release reach the addon", () => {
    const loop = new RecordingLoop();
    const marks = loopMarksOf(loop);

    marks.hold(RUN_TASK_ID);
    marks.release();
    assert.deepEqual(loop.marks, [`hold ${RUN_TASK_ID}`, "release"]);
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
      loop,
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
    assert.equal(startCulpritMarks({ kind: "none" }, new RecordingLoop())(), undefined);
  });
});
