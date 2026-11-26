// The seat pool with real `worker_threads` isolates of the fixture entry: the isolates boot with the memory cap of the
// main isolate; a run and its result; `process.exit` in an isolate and a late throw there end the run
// UNHANDLED_EXCEPTION, and a new isolate boots and takes the next run. No case allocates without a bound.

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { fileURLToPath } from "node:url";
import { getHeapStatistics } from "node:v8";

import { type SeatPool, type SeatRunPost, startSeatPool } from "../../../horsies/src/seats/pool.ts";
import { seatResourceLimits, workerIsolates } from "../../../horsies/src/seats/node-isolates.ts";
import type { SeatOutcome } from "../../../horsies/src/seats/protocol.ts";
import type { CoreDefect } from "../../../horsies/src/thrown-errors.ts";
import { TASK_ID } from "../app/support.ts";

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
      clock: () => performance.now(),
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
      serve: async () => assert.fail("the fixture runs no operation here"),
      endChild: (code) => settles.ends.push(code),
      writeLine: (line) => settles.lines.push(line),
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

  test("process.exit in an isolate ends its run WorkerThreadExit, and a new isolate takes the next run", async () => {
    pool.run(runPost(2n, "exit_now"));

    const outcome = await settles.of(2n);

    assert.equal(outcome.kind === "error" && outcome.parts.exception.kind === "record" ? outcome.parts.exception.className : "", "WorkerThreadExit");
    pool.run(runPost(3n, "probe"));
    pool.run(runPost(4n, "probe"));
    assert.deepEqual(await settles.of(3n), { kind: "value", json: "0" });
    assert.deepEqual(await settles.of(4n), { kind: "value", json: "0" });
    assert.deepEqual(settles.ends, []);
  });

  test("a late throw in an isolate ends its run UNHANDLED_EXCEPTION with the record of the throw", async () => {
    pool.run(runPost(5n, "throw_later"));

    const outcome = await settles.of(5n);

    assert.equal(
      outcome.kind === "error" && outcome.parts.message.kind === "text" ? outcome.parts.message.text : "",
      "Unhandled exception in task throw_later: RangeError: a late throw",
    );
    assert.deepEqual(settles.defects, []);
  });
});
