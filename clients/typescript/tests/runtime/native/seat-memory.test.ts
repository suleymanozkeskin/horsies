// The heap cap of a seat isolate, on Node: an isolate whose JavaScript heap reaches its cap ends alone with
// `ERR_WORKER_OUT_OF_MEMORY`; its run ends `WORKER_CRASHED` with that error in its message; a new isolate takes the
// seat and runs the next run; the child lives. The isolate's cap is small (32 MiB), and the task stops at 1 GiB of the
// heap on its own.
//
// Node only: Bun ignores `resourceLimits`, so the case never runs there (a Bun worker refuses `SHARED_THREAD`
// children). Run it under the memory watchdog of the runtime checks (2 GiB, with a wall-time bound). It needs no addon
// and no database.

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

import { startSeatPool } from "../../../horsies/src/seats/pool.ts";
import { workerIsolates } from "../../../horsies/src/seats/node-isolates.ts";
import type { SeatOutcome } from "../../../horsies/src/seats/protocol.ts";

/** The memory cap of the isolate of this case. */
const SMALL_CAP_MB = 32;

/** The longest wait for one settle. */
const SETTLE_BOUND_MS = 30_000;

const ENTRY: string = fileURLToPath(new URL("seat-memory-entry.ts", import.meta.url));

const TASK_ID = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b";

/** `test` on Node; a skipped case on Bun, which ignores the memory cap of an isolate. */
const nodeOnly = "bun" in process.versions ? test.skip : test;

async function settledOutcome(outcomes: Map<bigint, SeatOutcome>, runKey: bigint): Promise<SeatOutcome> {
  const deadline = Date.now() + SETTLE_BOUND_MS;

  while (Date.now() < deadline) {
    const outcome = outcomes.get(runKey);

    if (outcome !== undefined) {
      return outcome;
    }

    await new Promise((resolve) => setTimeout(resolve, 20));
  }

  return assert.fail(`the run ${String(runKey)} did not settle within ${String(SETTLE_BOUND_MS)} ms`);
}

describe("the memory cap of a seat isolate", () => {
  nodeOnly("an isolate at its heap cap ends alone; its run ends WORKER_CRASHED; a new isolate runs the next run", async () => {
    const outcomes = new Map<bigint, SeatOutcome>();
    const ends: number[] = [];

    const started = startSeatPool(1, {
      start: workerIsolates(ENTRY, { maxOldGenerationSizeMb: SMALL_CAP_MB }),
      bounds: () => ({ opsInFlight: 2, opsWaiting: 10_000, replacementBootBoundMs: 10_000 }),
      clock: () => performance.now(),
      now: () => Temporal.Now.instant(),
      timer: (milliseconds, callback) => {
        const timer = setTimeout(callback, milliseconds);

        timer.unref();

        return () => {
          clearTimeout(timer);
        };
      },
      settle: (runKey, _taskName, outcome) => {
        outcomes.set(runKey, outcome);
      },
      isolateEnded: () => undefined,
      serve: async () => assert.fail("the case runs no operation"),
      endChild: (code) => ends.push(code),
      writeLine: () => undefined,
      spanNotMade: () => assert.fail("the case makes no span"),
      reportDefect: (defect) => assert.fail(defect.message),
    });

    await started.booted;

    const facts = { kind: "notRecorded" } as const;

    started.pool.run({ kind: "runBody", runKey: 1n, taskName: "allocate", inputJson: "{}", taskId: TASK_ID, attempt: 1, facts });

    const outcome = await settledOutcome(outcomes, 1n);

    assert.equal(outcome.kind === "error" ? outcome.parts.code.code : "", "WORKER_CRASHED");
    assert.match(
      outcome.kind === "error" && outcome.parts.message.kind === "text" ? outcome.parts.message.text : "",
      /^Seat isolate 0 exited with code \d+ before the task ended: Error: .*memory limit/,
    );
    started.pool.run({ kind: "runBody", runKey: 2n, taskName: "probe", inputJson: "{}", taskId: TASK_ID, attempt: 1, facts });
    assert.deepEqual(await settledOutcome(outcomes, 2n), { kind: "value", json: "1" });
    assert.deepEqual(ends, []);
  });
});
