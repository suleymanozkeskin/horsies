// A `SHARED_THREAD` child on the child host: hook 0 boots the seat isolates (a provider failure there is the hook's
// failure, so the core ends the child with 173); the main thread runs no provider; the main-thread watch (the turn
// timer every 100 ms, no run mark); the posts of the core go to the seat pool; `stopWork` is a defect there; an
// isolated child runs a `SHARED_THREAD` body on its main thread. The operations of an isolate on the main thread.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { provideFor } from "../../../horsies/src/child/app-members.ts";
import { type ChildModeDeps, runChildMode } from "../../../horsies/src/child/host.ts";
import { LOOP_TURN_TICK_MS } from "../../../horsies/src/child/loop-marks.ts";
import { thrownError } from "../../../horsies/src/native/call/envelope.ts";
import type { ChildPost } from "../../../horsies/src/native/child/addon.ts";
import type { EnqueueRequest } from "../../../horsies/src/native/sends/addon.ts";
import { relayedRefusal, serveSeatOperation } from "../../../horsies/src/seats/operations.ts";
import type { IsolateEvents, SeatHost } from "../../../horsies/src/seats/pool.ts";
import { type MainToSeat, SEAT_PROTOCOL_VERSION, type SeatMarker } from "../../../horsies/src/seats/protocol.ts";
import { ClientDefect, type CoreDefect, CoreStopped, ForkedAfterCoreStart, TooManyPendingCalls } from "../../../horsies/src/thrown-errors.ts";
import type { CoreAccess, ProducerCore } from "../../../horsies/src/app/core-access.ts";
import { ENQUEUED, fakeCoreHandle, fakeProducer, PREPARED, TASK_ID } from "../app/support.ts";
import { type AddonWorkflowCore, noWorkflowCore } from "../../../horsies/src/workflow/core.ts";
import { FakeChildNative, settled } from "../child/child-native.ts";
import { contextOf, crunch, ENDED_AT, seatApp, TEST_SEAT_BOUNDS } from "./support.ts";

const CHILD_ARGUMENTS = ["--horsies-child", "3", "4242"] as const;

/** The end of a fake process. */
class Ended extends Error {
  readonly code: number;

  constructor(code: number) {
    super(`ended with ${String(code)}`);
    this.code = code;
  }
}

/** The isolates that a fake seat host started. */
type Started = { readonly marker: SeatMarker; readonly events: IsolateEvents; readonly posts: MainToSeat[] };

type Recorded = {
  readonly settlements: string[];
  readonly intervals: number[];
  readonly ticks: (() => void)[];
  readonly isolates: Started[];
  readonly defects: CoreDefect[];
};

function threadDeps(native: FakeChildNative): ChildModeDeps & Recorded {
  const settlements: string[] = [];
  const intervals: number[] = [];
  const ticks: (() => void)[] = [];
  const isolates: Started[] = [];
  const defects: CoreDefect[] = [];

  const seats: SeatHost = {
    start: (marker, events) => {
      const posts: MainToSeat[] = [];

      isolates.push({ marker, events, posts });

      return { kind: "started", isolate: { post: (message) => posts.push(message) } };
    },
    bounds: () => TEST_SEAT_BOUNDS,
    clock: () => 0,
    now: () => ENDED_AT,
    timer: () => () => undefined,
  };

  return {
    settlements,
    intervals,
    ticks,
    isolates,
    defects,
    native,
    events: { onSignal: () => undefined, onUncaught: () => undefined, onRejection: () => undefined },
    timer: (milliseconds, tick) => {
      intervals.push(milliseconds);
      ticks.push(tick);

      return () => undefined;
    },
    culprit: { kind: "none" },
    settlement: {
      returned: (callId, value) => settlements.push(`${String(callId)} returned ${String(value)}`),
      threw: (callId, cause) => settlements.push(`${String(callId)} threw ${cause instanceof Error ? cause.message : String(cause)}`),
    },
    hostname: "host-1",
    seats,
    writeLine: () => undefined,
    wait: async () => undefined,
    exit: (code) => {
      throw new Ended(code);
    },
    reportDefect: (defect) => defects.push(defect),
  };
}

function runBodyPost(runKey: bigint): ChildPost {
  return { kind: "runBody", runKey, taskName: "crunch", inputJson: '{"n":1}', taskId: TASK_ID, attempt: 1, facts: { kind: "notRecorded" } };
}

/** A served thread child of `seats` seats whose hook 0 ran with every isolate ready. */
async function servedThreadChild(seats: number): Promise<{ readonly native: FakeChildNative; readonly deps: ChildModeDeps & Recorded }> {
  const app = seatApp();

  app.implement(crunch, ({ n }, { ok }) => ok(n));

  const native = new FakeChildNative();
  const deps = threadDeps(native);

  void runChildMode(app, CHILD_ARGUMENTS, deps).catch(() => undefined);
  await settled();
  native.serves[0]?.hook(1n, { index: 0, facts: { kind: "sharedThread", seats } });

  for (const isolate of deps.isolates) {
    isolate.events.message({ kind: "ready", protocol: SEAT_PROTOCOL_VERSION, seatIndex: isolate.marker.seatIndex });
  }

  await settled();

  return { native, deps };
}

describe("a SHARED_THREAD child", () => {
  test("hook 0 boots one isolate per seat and ends when every isolate is ready; the main thread runs no provider", async () => {
    const app = seatApp();
    const built: string[] = [];

    provideFor(contextOf(app), () => built.push("on the main thread"));

    const native = new FakeChildNative();
    const deps = threadDeps(native);

    void runChildMode(app, CHILD_ARGUMENTS, deps).catch(() => undefined);
    await settled();
    native.serves[0]?.hook(1n, { index: 0, facts: { kind: "sharedThread", seats: 2 } });
    await settled();
    assert.equal(deps.isolates.length, 2);
    assert.deepEqual(deps.settlements, []);
    deps.isolates[0]?.events.message({ kind: "ready", protocol: 1, seatIndex: 0 });
    deps.isolates[1]?.events.message({ kind: "ready", protocol: 1, seatIndex: 1 });
    await settled();
    native.serves[0]?.hook(2n, { index: 1, facts: { kind: "sharedThread", seats: 2 } });
    await settled();
    assert.deepEqual(deps.settlements, ["1 returned ended", "2 returned ended"]);
    assert.deepEqual(built, []);
  });

  test("a provider failure in an isolate is the failure of hook 0: the core ends the child with 173", async () => {
    const native = new FakeChildNative();
    const deps = threadDeps(native);

    void runChildMode(seatApp(), CHILD_ARGUMENTS, deps).catch(() => undefined);
    await settled();
    native.serves[0]?.hook(1n, { index: 0, facts: { kind: "sharedThread", seats: 1 } });
    deps.isolates[0]?.events.message({ kind: "bootFailed", protocol: 1, seatIndex: 0, hookName: "provide #1 openPool", cause: "Error: refused" });
    await settled();
    assert.deepEqual(deps.settlements, ["1 threw the provider provide #1 openPool failed in the seat isolate 0: Error: refused"]);
  });

  test("the main-thread watch: the turn timer runs every 100 ms, and no run mark is set", async () => {
    const { native, deps } = await servedThreadChild(1);

    assert.deepEqual(deps.intervals, [LOOP_TURN_TICK_MS]);
    assert.equal(LOOP_TURN_TICK_MS, 100);
    deps.ticks[0]?.();
    native.serves[0]?.post(runBodyPost(5n));
    deps.isolates[0]?.events.message({ kind: "result", protocol: 1, runKey: 5n, outcome: { kind: "value", json: "1" } });
    assert.equal(native.calls.filter((call) => call === "loopTurned").length, 2);
    assert.deepEqual(native.calls.filter((call) => call.startsWith("loopHold") || call === "loopRelease"), []);
  });

  test("a run, its result and a cancel go through the seat pool", async () => {
    const { native, deps } = await servedThreadChild(1);

    native.serves[0]?.post(runBodyPost(5n));
    native.serves[0]?.post({ kind: "cancelRun", runKey: 5n, cause: "DEADLINE" });
    assert.deepEqual(deps.isolates[0]?.posts.map((post) => post.kind), ["run", "cancel"]);
    deps.isolates[0]?.events.message({ kind: "result", protocol: 1, runKey: 5n, outcome: { kind: "value", json: "1" } });
    assert.deepEqual(native.values, [[5n, "1"]]);
    assert.deepEqual(deps.isolates[0]?.posts.at(-1), {
      kind: "bodyEnded",
      protocol: 1,
      runKey: 5n,
      end: { kind: "timedOut" },
      endedAtEpochNanoseconds: ENDED_AT.epochNanoseconds,
    });
  });

  test("a post before the start facts is a defect", async () => {
    const native = new FakeChildNative();
    const deps = threadDeps(native);

    void runChildMode(seatApp(), CHILD_ARGUMENTS, deps).catch(() => undefined);
    await settled();
    native.serves[0]?.post(runBodyPost(4n));
    assert.match(deps.defects[0]?.message ?? "", /before the start facts of the child/);
  });

  test("a stopWork post in a thread child is a defect", async () => {
    const { native, deps } = await servedThreadChild(1);

    native.serves[0]?.post({ kind: "stopWork", workKey: 3n, cause: "LEFT_WORK" });
    assert.ok(deps.defects[0] instanceof ClientDefect);
  });

  test("an isolated child runs a SHARED_THREAD body on its main thread", async () => {
    const app = seatApp();

    app.implement(crunch, ({ n }, { ok }) => ok(n + 1));

    const native = new FakeChildNative();
    const deps = threadDeps(native);

    void runChildMode(app, CHILD_ARGUMENTS, deps).catch(() => undefined);
    await settled();
    native.serves[0]?.hook(1n, { index: 0, facts: { kind: "isolated" } });
    await settled();
    native.serves[0]?.post(runBodyPost(6n));
    await settled();
    assert.deepEqual(deps.isolates, []);
    assert.deepEqual(native.values, [[6n, "2"]]);
  });

  test("at the child's end the exit rule asks each seat isolate to flush, and the process ends after their answers", async () => {
    const native = new FakeChildNative();
    const exits: number[] = [];

    const deps: ChildModeDeps & Recorded = {
      ...threadDeps(native),
      // The flush bound never passes here: the end waits for the answers.
      wait: async () => new Promise<void>(() => undefined),
      exit: (code) => {
        exits.push(code);

        throw new Ended(code);
      },
    };

    void runChildMode(seatApp(), CHILD_ARGUMENTS, deps).catch(() => undefined);
    await settled();
    native.serves[0]?.hook(1n, { index: 0, facts: { kind: "sharedThread", seats: 2 } });

    for (const isolate of deps.isolates) {
      isolate.events.message({ kind: "ready", protocol: SEAT_PROTOCOL_VERSION, seatIndex: isolate.marker.seatIndex });
    }

    await settled();
    native.endServe(0);
    await settled();
    assert.deepEqual(deps.isolates.map((isolate) => isolate.posts.at(-1)?.kind), ["flush", "flush"]);
    deps.isolates[0]?.events.message({ kind: "flushed", protocol: SEAT_PROTOCOL_VERSION });
    await settled();
    assert.deepEqual(exits, []);
    deps.isolates[1]?.events.message({ kind: "flushed", protocol: SEAT_PROTOCOL_VERSION });
    await settled();
    assert.deepEqual(exits, [0]);
  });

  test("a seat isolate that does not answer the flush within the flush bound does not hold the end of the child", async () => {
    const native = new FakeChildNative();
    const exits: number[] = [];

    const deps: ChildModeDeps & Recorded = {
      // The flush bound passes at once here.
      ...threadDeps(native),
      exit: (code) => {
        exits.push(code);

        throw new Ended(code);
      },
    };

    void runChildMode(seatApp(), CHILD_ARGUMENTS, deps).catch(() => undefined);
    await settled();
    native.serves[0]?.hook(1n, { index: 0, facts: { kind: "sharedThread", seats: 1 } });
    deps.isolates[0]?.events.message({ kind: "ready", protocol: SEAT_PROTOCOL_VERSION, seatIndex: 0 });
    await settled();
    native.endServe(0);
    await settled();
    assert.equal(deps.isolates[0]?.posts.at(-1)?.kind, "flush");
    assert.deepEqual(exits, [0]);
  });
});

const ENQUEUE_REQUEST: EnqueueRequest = {
  taskId: TASK_ID,
  taskName: "crunch",
  queueName: "default",
  priority: 100,
  inputJson: '{"n":1}',
  sentAtMicros: 1_700_000_000_000_000n,
  deadline: { kind: "noDeadline" },
  delay: { kind: "noDelay" },
  taskOptions: { kind: "none" },
  idempotencyKey: { kind: "none" },
  retention: { kind: "forever" },
};

/** The core access of a main thread whose producer is `producer`. */
function addonAccess(producer: ProducerCore): CoreAccess {
  return { kind: "addon", producer, handle: fakeCoreHandle() };
}

describe("the operations of the isolates on the main thread", () => {
  test("each sends operation runs on the producer of the main thread", async () => {
    const producer = fakeProducer();
    const access = addonAccess(producer);

    assert.deepEqual(
      await serveSeatOperation(access, {
        area: "sends",
        operation: {
          kind: "prepareSend",
          taskName: "crunch",
          facts: { retention: { kind: "queueOrDefault" }, deadline: { kind: "noDeadline" }, inputBytes: 7 },
        },
      }),
      { kind: "reply", reply: { area: "sends", reply: { kind: "prepareSend", prepared: PREPARED } } },
    );
    assert.deepEqual(await serveSeatOperation(access, { area: "sends", operation: { kind: "taskInfo", taskId: TASK_ID } }), {
      kind: "reply",
      reply: { area: "sends", reply: { kind: "taskInfo", read: { kind: "absent", reason: "NOT_CLASSIFIED" } } },
    });
    assert.deepEqual(
      await serveSeatOperation(access, {
        area: "sends",
        operation: { kind: "enqueue", request: ENQUEUE_REQUEST, sendKind: "REPEAT", trace: { kind: "noCarrier" } },
      }),
      { kind: "reply", reply: { area: "sends", reply: { kind: "enqueue", outcome: ENQUEUED } } },
    );
    assert.equal(producer.prepares.length, 1);
    assert.deepEqual(producer.infos, [TASK_ID]);
    assert.deepEqual(
      producer.enqueues.map((call) => call.sendKind),
      ["REPEAT"],
    );
  });

  test("a workflows operation runs on the workflow core of the main thread's producer", async () => {
    const asked: string[] = [];

    const workflows: AddonWorkflowCore = {
      ...noWorkflowCore(),
      status: async (workflowId) => {
        asked.push(workflowId);

        return { kind: "status", status: "RUNNING" };
      },
    };

    const access = addonAccess({ ...fakeProducer(), workflows });

    assert.deepEqual(await serveSeatOperation(access, { area: "workflows", operation: { kind: "status", workflowId: "wf-1" } }), {
      kind: "reply",
      reply: { area: "workflows", reply: { kind: "status", read: { kind: "status", status: "RUNNING" } } },
    });
    assert.deepEqual(asked, ["wf-1"]);
  });

  test("a CoreError of the main thread answers refused, and the isolate rebuilds the same class", async () => {
    const producer = fakeProducer();

    producer.taskInfo = async () => {
      throw new CoreStopped({ subject: "task_info", cause: "panic", stateAfter: "noChange" });
    };

    const answer = await serveSeatOperation(addonAccess(producer), { area: "sends", operation: { kind: "taskInfo", taskId: TASK_ID } });

    assert.ok(answer.kind === "refused" && thrownError(answer.refusal) instanceof CoreStopped);

    for (const error of [new TooManyPendingCalls({ subject: "enqueue", bound: 65_536 }), new ForkedAfterCoreStart({ corePid: 1, thisPid: 2 })]) {
      assert.deepEqual(thrownError(relayedRefusal(error)), error);
    }
  });

  test("a defect of the main thread rejects; a main thread with no addon core is a defect", async () => {
    const producer = fakeProducer();

    producer.taskInfo = async () => {
      throw new ClientDefect({ subject: "task_info", rule: "a rule" });
    };

    await assert.rejects(serveSeatOperation(addonAccess(producer), { area: "sends", operation: { kind: "taskInfo", taskId: TASK_ID } }), ClientDefect);
    await assert.rejects(serveSeatOperation({ kind: "noCore" }, { area: "sends", operation: { kind: "taskInfo", taskId: TASK_ID } }), ClientDefect);
  });
});
