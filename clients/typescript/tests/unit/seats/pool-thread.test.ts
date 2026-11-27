// The seat pool with real `worker_threads` isolates of the fixture entry: the isolates boot with the heap cap of the
// main isolate; a run, its result and its verdict; `process.exit` in an isolate ends the run WORKER_CRASHED, and a new
// isolate boots and takes the next run; a late throw in code of a run is logged and the run keeps its value (Node
// only). No case allocates without a bound.

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { fileURLToPath } from "node:url";
import { getHeapStatistics } from "node:v8";

import { type SeatPool, type SeatRunPost, startSeatPool } from "../../../horsies/src/seats/pool.ts";
import { seatResourceLimits, workerIsolates } from "../../../horsies/src/seats/node-isolates.ts";
import type { SeatOutcome } from "../../../horsies/src/seats/protocol.ts";
import type { CoreDefect } from "../../../horsies/src/thrown-errors.ts";
import { TASK_ID } from "../app/support.ts";
import { TEST_SEAT_BOUNDS } from "./support.ts";

/** `test` on Node; a skipped case on Bun, whose worker threads end at a throw that a listener of the thread handles. */
const nodeOnly = "bun" in process.versions ? test.skip : test;

/** The longest wait for one settle. */
const SETTLE_BOUND_MS = 15_000;

const ENTRY: string = fileURLToPath(new URL("fixtures/seat-entry.ts", import.meta.url));

function runPost(runKey: bigint, taskName: string): SeatRunPost {
  return { kind: "runBody", runKey, taskName, inputJson: "{}", taskId: TASK_ID, attempt: 1, facts: { kind: "notRecorded" } };
}

/** The settles of a pool of fixture isolates. */
class Settles {
  readonly outcomes = new Map<bigint, SeatOutcome>();
  readonly lines: string[] = [];
  readonly defects: CoreDefect[] = [];
  readonly ends: number[] = [];

  async of(runKey: bigint): Promise<SeatOutcome> {
    const deadline = Date.now() + SETTLE_BOUND_MS;

    while (Date.now() < deadline) {
      const outcome = this.outcomes.get(runKey);

      if (outcome !== undefined) {
        return outcome;
      }

      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    return assert.fail(`the run ${String(runKey)} did not settle; lines: ${this.lines.join(" | ")}`);
  }
}

describe("the memory cap of a seat isolate", () => {
  test("is the heap limit of the child's main isolate, in whole MiB", () => {
    assert.deepEqual(seatResourceLimits(4 * 1024 * 1024 * 1024 + 123), { maxOldGenerationSizeMb: 4096 });
  });
});

describe("the seat pool with worker threads", () => {
  const settles = new Settles();
  const stops: (() => void)[] = [];
  let pool: SeatPool;

  before(async () => {
    const started = startSeatPool(2, {
      start: workerIsolates(ENTRY, seatResourceLimits(getHeapStatistics().heap_size_limit)),
      bounds: () => TEST_SEAT_BOUNDS,
      clock: () => performance.now(),
      now: () => Temporal.Now.instant(),
      timer: (milliseconds, callback) => {
        const timer = setTimeout(callback, milliseconds);

        stops.push(() => {
          clearTimeout(timer);
        });

        return () => {
          clearTimeout(timer);
        };
      },
      settle: (runKey, _taskName, outcome) => {
        settles.outcomes.set(runKey, outcome);
      },
      isolateEnded: () => undefined,
      serve: async () => assert.fail("the fixture runs no operation here"),
      endChild: (code) => settles.ends.push(code),
      writeLine: (line) => settles.lines.push(line),
      spanNotMade: () => assert.fail("the fixture makes no span"),
      reportDefect: (defect) => settles.defects.push(defect),
    });

    pool = started.pool;
    await started.booted;
  });

  after(() => {
    for (const stop of stops) {
      stop();
    }

    // The pool never terminates its isolates, and they do not keep the process alive: the test process ends them.
  });

  test("both isolates boot, and a run settles with its result", async () => {
    assert.deepEqual(pool.stages(), ["free", "free"]);
    pool.run(runPost(1n, "probe"));
    assert.deepEqual(await settles.of(1n), { kind: "value", json: "0" });
  });

  test("process.exit in an isolate ends its run WORKER_CRASHED, and a new isolate takes the next run", async () => {
    pool.run(runPost(2n, "exit_now"));

    const outcome = await settles.of(2n);

    assert.equal(outcome.kind === "error" ? outcome.parts.code.code : "", "WORKER_CRASHED");
    assert.match(
      outcome.kind === "error" && outcome.parts.message.kind === "text" ? outcome.parts.message.text : "",
      /exited with code 3 before the task ended: WorkerThreadExit/,
    );
    pool.run(runPost(3n, "probe"));
    pool.run(runPost(4n, "probe"));
    assert.deepEqual(await settles.of(3n), { kind: "value", json: "0" });
    assert.deepEqual(await settles.of(4n), { kind: "value", json: "0" });
    assert.deepEqual(settles.ends, []);
  });

  nodeOnly("a late throw in code of a run is logged, and the run keeps its value", async () => {
    pool.run(runPost(5n, "throw_later"));
    assert.deepEqual(await settles.of(5n), { kind: "value", json: "5" });
    assert.ok(settles.lines.some((line) => line.includes("a callback threw in the run of task throw_later")));
    assert.deepEqual(settles.defects, []);
  });
});
