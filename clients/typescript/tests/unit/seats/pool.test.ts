// The seat pool on the main thread: N isolates boot; a provider failure fails the boot (173); an isolate that does not
// start; a run goes to a free isolate; the result relay and the verdict; a cancel writes the cell and posts; a cancel
// of an ended run is left; the operation relay; the defects and log lines of an isolate; an isolate end during a run
// gives WORKER_CRASHED and a new isolate; a run during the replacement waits and the wait is logged; a replacement past
// the start-hook bound, and a provider failure in a new isolate, end the child with 173; a run that the core posts
// while the child ends is left; the span reports of the isolates; the flush of the isolates at the child's stop.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { BodyFactsRecord, ChildPost } from "../../../horsies/src/native/child/addon.ts";
import type { EndedSeatRun } from "../../../horsies/src/seats/body-observer.ts";
import { cancelStateOf } from "../../../horsies/src/seats/cancel-cells.ts";
import {
  type IsolateEvents,
  type SeatPool,
  type SeatPoolDeps,
  type SeatRunPost,
  startSeatPool,
} from "../../../horsies/src/seats/pool.ts";
import {
  type MainToSeat,
  SEAT_PROTOCOL_VERSION,
  type SeatAnswer,
  type SeatMarker,
  type SeatOutcome,
  type SeatRequest,
  type SeatToMain,
} from "../../../horsies/src/seats/protocol.ts";
import { ClientDefect, type CoreDefect } from "../../../horsies/src/thrown-errors.ts";
import { unreachable } from "../../../horsies/src/wire/unreachable.ts";
import { PREPARED, TASK_ID } from "../app/support.ts";
import { ENDED_AT, settled, TEST_SEAT_BOUNDS } from "./support.ts";

/** The name of the operation of `request`, for the record of the served operations. */
function operationName(request: SeatRequest): string {
  const area = request.area;

  switch (area) {
    case "sends":
      return request.operation.kind;
    case "facades":
      return request.operation.part;
    case "workflows":
      return request.operation.kind;
    default:
      return unreachable(area, "operationName");
  }
}

const REPLACEMENT_BOOT_BOUND_MS = TEST_SEAT_BOUNDS.replacementBootBoundMs;

/** One isolate that the fake started. */
type FakeIsolate = { readonly marker: SeatMarker; readonly events: IsolateEvents; readonly posts: MainToSeat[] };

/** The fakes of a pool and what it did. */
class PoolFakes {
  readonly isolates: FakeIsolate[] = [];
  readonly settles: [bigint, string, SeatOutcome][] = [];
  readonly served: string[] = [];
  readonly lines: string[] = [];
  readonly ends: number[] = [];
  readonly defects: CoreDefect[] = [];
  readonly notMade: string[] = [];
  readonly endedRuns: EndedSeatRun[] = [];
  /** The wall clock that `now` reads. */
  wall: Temporal.Instant = ENDED_AT;
  readonly timers: { readonly milliseconds: number; readonly fire: () => void; stopped: boolean }[] = [];
  now = 0;
  /** The seat whose next start the runtime refuses, if any. */
  refuseStart: number | undefined = undefined;

  deps(): SeatPoolDeps {
    return {
      start: (marker, events) => {
        if (this.refuseStart === marker.seatIndex) {
          return { kind: "notStarted", cause: "Error: the OS refused a thread" };
        }

        const posts: MainToSeat[] = [];

        this.isolates.push({ marker, events, posts });

        return { kind: "started", isolate: { post: (message) => posts.push(message) } };
      },
      bounds: () => TEST_SEAT_BOUNDS,
      clock: () => this.now,
      now: () => this.wall,
      timer: (milliseconds, fire) => {
        const timer = { milliseconds, fire, stopped: false };

        this.timers.push(timer);

        return () => {
          timer.stopped = true;
        };
      },
      settle: (runKey, taskName, outcome) => {
        this.settles.push([runKey, taskName, outcome]);
      },
      isolateEnded: (run) => {
        this.endedRuns.push(run);
      },
      serve: async (request): Promise<SeatAnswer> => {
        this.served.push(operationName(request));

        return { kind: "reply", reply: { area: "sends", reply: { kind: "prepareSend", prepared: PREPARED } } };
      },
      endChild: (code) => {
        this.ends.push(code);
      },
      writeLine: (line) => this.lines.push(line),
      spanNotMade: (span, cause) => this.notMade.push(`${span.kind} ${cause}`),
      reportDefect: (defect) => this.defects.push(defect),
    };
  }

  /** The message `message` of the isolate `index` (in start order). */
  send(index: number, message: SeatToMain): void {
    this.isolate(index).events.message(message);
  }

  isolate(index: number): FakeIsolate {
    return this.isolates[index] ?? assert.fail(`no isolate ${String(index)}`);
  }

  /** Every isolate started so far sends ready for its seat. */
  readyAll(): void {
    for (const [index, isolate] of this.isolates.entries()) {
      this.send(index, { kind: "ready", protocol: SEAT_PROTOCOL_VERSION, seatIndex: isolate.marker.seatIndex });
    }
  }
}

const RECORDED_FACTS: BodyFactsRecord = {
  kind: "recorded",
  taskId: TASK_ID,
  taskName: "crunch",
  queueName: "default",
  attempt: 2,
  workerId: "worker-1",
  execution: "SHARED_THREAD",
  workflow: { kind: "notInWorkflow" },
  carrier: { kind: "noCarrier" },
};

function runPost(runKey: bigint, taskName = "crunch"): SeatRunPost {
  return { kind: "runBody", runKey, taskName, inputJson: '{"n":1}', taskId: TASK_ID, attempt: 2, facts: { kind: "notRecorded" } };
}

function result(runKey: bigint, json: string): SeatToMain {
  return { kind: "result", protocol: SEAT_PROTOCOL_VERSION, runKey, outcome: { kind: "value", json } };
}

async function bootedPool(seats: number): Promise<{ readonly pool: SeatPool; readonly fakes: PoolFakes }> {
  const fakes = new PoolFakes();
  const started = startSeatPool(seats, fakes.deps());

  fakes.readyAll();
  await started.booted;

  return { pool: started.pool, fakes };
}

function kinds(posts: readonly MainToSeat[]): string[] {
  return posts.map((post) => post.kind);
}

describe("the boot of the seat pool", () => {
  test("each seat starts its isolate with its index and the shared cells; the boot ends when all are ready", async () => {
    const fakes = new PoolFakes();
    const started = startSeatPool(3, fakes.deps());
    let booted = false;

    void started.booted.then(() => {
      booted = true;
    });
    assert.deepEqual(fakes.isolates.map((isolate) => isolate.marker.seatIndex), [0, 1, 2]);
    assert.equal(fakes.isolate(0).marker.cancelCells, fakes.isolate(2).marker.cancelCells);
    fakes.send(0, { kind: "ready", protocol: 1, seatIndex: 0 });
    fakes.send(1, { kind: "ready", protocol: 1, seatIndex: 1 });
    await settled();
    assert.equal(booted, false);
    fakes.send(2, { kind: "ready", protocol: 1, seatIndex: 2 });
    await started.booted;
    assert.deepEqual(started.pool.stages(), ["free", "free", "free"]);
  });

  test("a provider failure in an isolate fails the boot with the provider's name: the hook fails, the child ends with 173", async () => {
    const fakes = new PoolFakes();
    const started = startSeatPool(2, fakes.deps());

    fakes.send(1, { kind: "bootFailed", protocol: 1, seatIndex: 1, hookName: "provide #1 openPool", cause: "Error: refused" });
    await assert.rejects(started.booted, /the provider provide #1 openPool failed in the seat isolate 1: Error: refused/);
  });

  test("an isolate that ends before it is ready fails the boot", async () => {
    const fakes = new PoolFakes();
    const started = startSeatPool(1, fakes.deps());

    fakes.isolate(0).events.error(new SyntaxError("Unexpected token"));
    fakes.isolate(0).events.exit(1);
    await assert.rejects(started.booted, /ended with exit code 1: SyntaxError: Unexpected token, before it was ready/);
  });
});

describe("runs on the seat pool", () => {
  test("a run goes to a free isolate with a clear cell; its result settles it and frees the seat", async () => {
    const { pool, fakes } = await bootedPool(2);

    Atomics.store(fakes.isolate(0).marker.cancelCells, 0, 2);
    pool.run(runPost(5n));
    pool.run(runPost(6n));
    assert.deepEqual(kinds(fakes.isolate(0).posts), ["run"]);
    assert.deepEqual(kinds(fakes.isolate(1).posts), ["run"]);
    assert.deepEqual(cancelStateOf(fakes.isolate(0).marker.cancelCells, 0), { kind: "notRequested" });
    fakes.send(1, result(6n, "42"));
    assert.deepEqual(fakes.settles, [[6n, "crunch", { kind: "value", json: "42" }]]);
    assert.deepEqual(pool.stages(), ["running", "free"]);
  });

  test("a cancel writes the cause into the run's cell and posts cancel; a cancel of an ended run is left", async () => {
    const { pool, fakes } = await bootedPool(2);
    const cells = fakes.isolate(0).marker.cancelCells;

    pool.run(runPost(5n));
    pool.run(runPost(6n));
    pool.cancel(6n, "OPERATOR");
    assert.deepEqual(cancelStateOf(cells, 1), { kind: "requested", cause: "OPERATOR" });
    assert.deepEqual(fakes.isolate(1).posts.at(-1), { kind: "cancel", protocol: 1, runKey: 6n, cause: "OPERATOR" });
    fakes.send(0, result(5n, "1"));
    pool.cancel(5n, "DEADLINE");
    assert.deepEqual(kinds(fakes.isolate(0).posts), ["run", "bodyEnded"]);
    assert.deepEqual(cancelStateOf(cells, 0), { kind: "notRequested" });
  });

  test("after each result the isolate gets the verdict of the run, from its cancel cause and its outcome, and the time", async () => {
    const { pool, fakes } = await bootedPool(1);

    const failed: SeatToMain = {
      kind: "result",
      protocol: SEAT_PROTOCOL_VERSION,
      runKey: 2n,
      outcome: {
        kind: "error",
        parts: { code: { kind: "custom", code: "TOO_BIG" }, message: { kind: "none" }, data: { kind: "noData" }, exception: { kind: "noException" } },
      },
    };

    pool.run(runPost(1n));
    fakes.send(0, result(1n, "1"));
    pool.run(runPost(2n));
    fakes.send(0, failed);
    pool.run(runPost(3n));
    pool.cancel(3n, "DEADLINE");
    pool.cancel(3n, "OPERATOR");
    fakes.send(0, result(3n, "1"));
    pool.run(runPost(4n));
    pool.cancel(4n, "OPERATOR");
    fakes.send(0, result(4n, "1"));

    const verdicts = fakes.isolate(0).posts.flatMap((post) => (post.kind === "bodyEnded" ? [post.end] : []));

    assert.deepEqual(verdicts, [{ kind: "returned" }, { kind: "failed", code: "TOO_BIG" }, { kind: "timedOut" }, { kind: "cancelled" }]);
    assert.equal(fakes.isolate(0).posts.find((post) => post.kind === "bodyEnded")?.endedAtEpochNanoseconds, ENDED_AT.epochNanoseconds);
  });

  test("a defect of an isolate goes to the defect rule; a log line of an isolate goes to stderr", async () => {
    const { fakes } = await bootedPool(1);

    fakes.send(0, { kind: "defect", protocol: 1, code: "CLIENT_DEFECT", name: "ClientDefect", message: "seat run: a rule" });
    fakes.send(0, { kind: "log", protocol: 1, line: "horsies child: a callback threw" });
    fakes.isolate(0).events.unreadable(new Error("DataCloneError"));
    assert.match(fakes.defects[0]?.message ?? "", /the seat isolate 0: ClientDefect \(CLIENT_DEFECT\): seat run: a rule/);
    assert.match(fakes.defects[1]?.message ?? "", /does not deserialize/);
    assert.deepEqual(fakes.lines, ["horsies child: a callback threw"]);
  });

  test("an operation of an isolate runs on the main thread, and its answer goes back to that isolate", async () => {
    const { fakes } = await bootedPool(1);

    fakes.send(0, {
      kind: "op",
      protocol: 1,
      requestId: 3,
      request: { area: "sends", operation: { kind: "taskInfo", taskId: TASK_ID } },
    });
    await settled();
    assert.deepEqual(fakes.served, ["taskInfo"]);
    assert.deepEqual(fakes.isolate(0).posts, [
      {
        kind: "opReply",
        protocol: 1,
        requestId: 3,
        answer: { kind: "reply", reply: { area: "sends", reply: { kind: "prepareSend", prepared: PREPARED } } },
      },
    ]);
  });

  test("a result of a run that does not run on the seat is a defect", async () => {
    const { fakes } = await bootedPool(1);

    fakes.send(0, result(9n, "1"));
    assert.ok(fakes.defects[0] instanceof ClientDefect);
  });
});

describe("an isolate that ends", () => {
  test("during a run: the seat body observer gets the run's facts, the time of the run post and the time of the exit", async () => {
    const { pool, fakes } = await bootedPool(1);
    const facts = { ...RECORDED_FACTS, taskId: TASK_ID };
    const startedAt = Temporal.Instant.from("2026-11-26T10:00:00Z");
    const exitedAt = Temporal.Instant.from("2026-11-26T10:00:03Z");

    fakes.wall = startedAt;
    pool.run({ ...runPost(5n), facts });
    fakes.wall = exitedAt;
    fakes.isolate(0).events.exit(1);
    assert.deepEqual(fakes.endedRuns, [{ facts, startedAt, endedAt: exitedAt }]);
  });

  test("while free: no run ended, so the seat body observer gets nothing", async () => {
    const { fakes } = await bootedPool(1);

    fakes.isolate(0).events.exit(1);
    assert.deepEqual(fakes.endedRuns, []);
  });

  test("during a run: the run ends WORKER_CRASHED with the error that ended the isolate, and a new isolate takes the seat", async () => {
    const { pool, fakes } = await bootedPool(2);

    pool.run(runPost(5n));
    fakes.isolate(0).events.error(new RangeError("Worker terminated due to reaching memory limit: JS heap out of memory"));
    fakes.isolate(0).events.exit(1);

    const [settle] = fakes.settles;
    const outcome = settle?.[2];

    assert.equal(settle?.[0], 5n);
    assert.deepEqual(outcome?.kind === "error" ? outcome.parts.code : undefined, { kind: "builtIn", code: "WORKER_CRASHED" });
    assert.equal(
      outcome?.kind === "error" && outcome.parts.message.kind === "text" ? outcome.parts.message.text : "",
      "Seat isolate 0 exited with code 1 before the task ended: RangeError: Worker terminated due to reaching memory limit: JS heap out of memory",
    );
    assert.deepEqual(outcome?.kind === "error" ? [outcome.parts.data, outcome.parts.exception] : [], [
      { kind: "text", text: `{"task_id":"${TASK_ID}"}` },
      { kind: "noException" },
    ]);
    assert.equal(fakes.isolates.length, 3);
    assert.equal(fakes.isolate(2).marker.seatIndex, 0);
    assert.deepEqual(pool.stages(), ["booting", "free"]);
    assert.match(fakes.lines[0] ?? "", /seat isolate 0 ended with exit code 1: RangeError: .*during the run of task crunch/);
  });

  test("with no error: WORKER_CRASHED names WorkerThreadExit and the exit code", async () => {
    const { pool, fakes } = await bootedPool(1);

    pool.run(runPost(5n));
    fakes.isolate(0).events.exit(7);

    const outcome = fakes.settles[0]?.[2];

    assert.equal(
      outcome?.kind === "error" && outcome.parts.message.kind === "text" ? outcome.parts.message.text : "",
      "Seat isolate 0 exited with code 7 before the task ended: WorkerThreadExit: the seat isolate ended with exit code 7",
    );
  });

  test("an isolate that the runtime does not start fails the first boot, and ends the child with 173 as a replacement", async () => {
    const first = new PoolFakes();

    first.refuseStart = 1;

    await assert.rejects(startSeatPool(2, first.deps()).booted, /the seat isolate 1 did not start: Error: the OS refused a thread/);

    const { fakes } = await bootedPool(1);

    fakes.refuseStart = 0;
    fakes.isolate(0).events.exit(1);
    assert.deepEqual(fakes.ends, [173]);
    assert.equal(fakes.timers[0]?.stopped, true);
  });

  test("a run during the replacement waits for the new isolate, then runs there; the wait is logged with its length", async () => {
    const { pool, fakes } = await bootedPool(2);

    pool.run(runPost(5n));
    pool.run(runPost(6n));
    fakes.isolate(0).events.exit(1);
    fakes.now = 1000;
    pool.run(runPost(7n));
    assert.equal(pool.waiting(), 1);
    fakes.now = 1250;
    fakes.send(2, { kind: "ready", protocol: 1, seatIndex: 0 });
    assert.deepEqual(kinds(fakes.isolate(2).posts), ["run"]);
    assert.equal(pool.waiting(), 0);
    assert.ok(fakes.lines.some((line) => line.includes("the run of task crunch") && line.includes("waited 250 ms for the new isolate")));
    assert.equal(fakes.timers[0]?.stopped, true);
  });

  test("a cancel of a run that waits reaches the run when it starts: the cell holds the cause, then cancel follows run", async () => {
    const { pool, fakes } = await bootedPool(1);

    pool.run(runPost(5n));
    fakes.isolate(0).events.exit(1);
    pool.run(runPost(6n));
    pool.cancel(6n, "DEADLINE");
    fakes.send(1, { kind: "ready", protocol: 1, seatIndex: 0 });
    assert.deepEqual(kinds(fakes.isolate(1).posts), ["run", "cancel"]);
    assert.deepEqual(cancelStateOf(fakes.isolate(1).marker.cancelCells, 0), { kind: "requested", cause: "DEADLINE" });
  });

  test("a new isolate that is not ready within 10 s ends the child with 173", async () => {
    const { pool, fakes } = await bootedPool(1);

    fakes.isolate(0).events.exit(1);
    assert.equal(fakes.timers[0]?.milliseconds, REPLACEMENT_BOOT_BOUND_MS);
    fakes.timers[0]?.fire();
    assert.deepEqual(fakes.ends, [173]);
    assert.deepEqual(pool.stages(), ["ended"]);
    assert.match(fakes.lines.at(-1) ?? "", /not ready within 10000 ms; the child ends with 173/);
  });

  test("a run and a cancel that the core posts while the child ends are left; the child ends once", async () => {
    const { pool, fakes } = await bootedPool(1);

    fakes.isolate(0).events.exit(1);
    fakes.timers[0]?.fire();
    pool.run(runPost(6n));
    pool.cancel(6n, "DEADLINE");
    fakes.isolate(1).events.exit(1);
    assert.equal(pool.stage(), "ending");
    assert.deepEqual(fakes.ends, [173]);
    assert.deepEqual(fakes.defects, []);
    assert.equal(fakes.isolates.length, 2);
  });

  test("a provider failure in a new isolate ends the child with 173", async () => {
    const { fakes } = await bootedPool(1);

    fakes.isolate(0).events.exit(1);
    fakes.send(1, { kind: "bootFailed", protocol: 1, seatIndex: 0, hookName: "provide #1 openPool", cause: "Error: refused" });
    assert.deepEqual(fakes.ends, [173]);
    assert.equal(fakes.timers[0]?.stopped, true);
  });

  test("a run with no free isolate and no new isolate on the way is a defect", async () => {
    const { pool, fakes } = await bootedPool(1);
    const posts: ChildPost[] = [runPost(5n), runPost(6n)];

    assert.throws(() => {
      for (const post of posts) {
        if (post.kind === "runBody") {
          pool.run(post);
        }
      }
    }, ClientDefect);
    assert.deepEqual(fakes.ends, []);
  });
});

describe("the span reports of the isolates", () => {
  test("a span that an isolate did not make goes to the reports of the process, with its cause", async () => {
    const { fakes } = await bootedPool(2);

    fakes.send(1, {
      kind: "spanNotMade",
      protocol: SEAT_PROTOCOL_VERSION,
      span: { kind: "seatAttempt", taskId: TASK_ID },
      cause: "TypeError: no exporter",
    });
    fakes.send(0, {
      kind: "spanNotMade",
      protocol: SEAT_PROTOCOL_VERSION,
      span: { kind: "workflowStart", workflowId: TASK_ID },
      cause: "Error: broken",
    });
    assert.deepEqual(fakes.notMade, ["seatAttempt TypeError: no exporter", "workflowStart Error: broken"]);
    assert.deepEqual(fakes.defects, []);
  });
});

describe("the flush of the seat isolates at the child's stop", () => {
  test("each free and running isolate gets flush; the flush ends when each one answered or ended; a booting one is not asked", async () => {
    const { pool, fakes } = await bootedPool(3);
    let flushed = false;

    pool.run(runPost(5n));
    fakes.isolate(2).events.exit(1);
    assert.deepEqual(pool.stages(), ["running", "free", "booting"]);

    void pool.flush().then(() => {
      flushed = true;
    });
    assert.deepEqual(
      fakes.isolates.map((isolate) => kinds(isolate.posts).filter((kind) => kind === "flush").length),
      [1, 1, 0, 0],
    );
    fakes.send(1, { kind: "flushed", protocol: SEAT_PROTOCOL_VERSION });
    await settled();
    assert.equal(flushed, false, "the running isolate has not answered");
    fakes.isolate(0).events.exit(1);
    await settled();
    assert.equal(flushed, true);
  });

  test("a pool with no isolate that took its runs ends its flush at once", async () => {
    const fakes = new PoolFakes();
    const started = startSeatPool(1, fakes.deps());

    await started.pool.flush();
    assert.deepEqual(kinds(fakes.isolate(0).posts), []);
  });
});
