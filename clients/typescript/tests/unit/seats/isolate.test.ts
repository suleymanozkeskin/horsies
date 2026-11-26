// The seat isolate side: the providers and `ready`, a provider failure, a run and its result, the reader's input, the
// seat body observer (enter at the run, end at the main thread's verdict), cancel in a seat (a sync body reads the
// cell, an async body sees the signal), a cancel of another run, the process events of an isolate (a detached throw in
// a run, a throw outside every run, an unhandled rejection), and each defect path (to the main thread, then the
// throw).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { BodyEnd } from "../../../horsies/src/child/body-observer.ts";
import { runStore } from "../../../horsies/src/child/run-store.ts";
import type { TaskRuntime } from "../../../horsies/src/child/runtime.ts";
import type { BodyFactsRecord } from "../../../horsies/src/native/child/addon.ts";
import { type SeatBodyObserver, setSeatBodyObserver } from "../../../horsies/src/seats/body-observer.ts";
import { clearCancel, requestCancel } from "../../../horsies/src/seats/cancel-cells.ts";
import { serveSeat } from "../../../horsies/src/seats/isolate.ts";
import type { SeatOutcome } from "../../../horsies/src/seats/protocol.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { PREPARED, TASK_ID } from "../app/support.ts";
import { RUN_TASK_ID } from "../child/support.ts";
import {
  bodyEndedMessage,
  cancelMessage,
  contextOf,
  crunch,
  ENDED_AT,
  fakeChannel,
  FakeSeatEvents,
  type FakeIsolatePort,
  fetchPage,
  replyMessage,
  runMessage,
  seatApp,
  settled,
} from "./support.ts";

const RECORDED: BodyFactsRecord = {
  kind: "recorded",
  taskId: TASK_ID,
  taskName: "crunch",
  queueName: "default",
  attempt: 1,
  workerId: "worker-1",
  execution: "SHARED_THREAD",
  workflow: { kind: "notInWorkflow" },
  carrier: { kind: "noCarrier" },
};

function outcomes(port: FakeIsolatePort): SeatOutcome[] {
  return port.postsOf("result").map((post) => post.outcome);
}

/** The rule texts of the defects that the isolate sent to the main thread. */
function defects(port: FakeIsolatePort): string[] {
  return port.postsOf("defect").map((post) => `${post.name}: ${post.message}`);
}

/** A served isolate of `app` on a fake channel, after its boot. */
async function served(
  app: ReturnType<typeof seatApp>,
  seatIndex = 0,
): Promise<{ readonly port: FakeIsolatePort; readonly events: FakeSeatEvents; readonly channel: ReturnType<typeof fakeChannel>["channel"] }> {
  const { channel, port } = fakeChannel(seatIndex);
  const events = new FakeSeatEvents();

  void serveSeat(contextOf(app), channel, events.deps());
  await settled();

  return { port, events, channel };
}

describe("the boot of a seat isolate", () => {
  test("each provider runs in registration order and keeps its value; then the isolate is ready", async () => {
    const app = seatApp();
    const order: string[] = [];

    const pool = app.provide(() => {
      order.push("pool");

      return "pool-1";
    });

    app.provide(async () => {
      order.push("cache");

      return 2;
    });
    app.implement(crunch, ({ n }, runtime) => runtime.ok(n + runtime.state(pool).length));

    const { port } = await served(app, 1);

    assert.deepEqual(order, ["pool", "cache"]);
    assert.deepEqual(port.posts, [{ kind: "ready", protocol: 1, seatIndex: 1 }]);
    port.deliver(runMessage(3n, "crunch", '{"n":1}'));
    await settled();
    assert.deepEqual(outcomes(port), [{ kind: "value", json: "7" }]);
  });

  test("a provider that throws: bootFailed with its name and the record, no later provider, no ready", async () => {
    const app = seatApp();
    const order: string[] = [];

    app.provide(function openPool() {
      throw new RangeError("the pool is full");
    });
    app.provide(() => order.push("later"));

    const { port } = await served(app);

    assert.deepEqual(order, []);
    assert.deepEqual(port.posts, [
      { kind: "bootFailed", protocol: 1, seatIndex: 0, hookName: "provide #1 openPool", cause: "RangeError: the pool is full" },
    ]);
  });
});

describe("a run in a seat isolate", () => {
  test("the reader of a seat isolate gets only the input text that the core posts after its strict check", async () => {
    // The core's child host refuses a run input that breaks its strict read rule (`build_task_input`) and ends that
    // run itself, so the post holds checked text only; the main thread and the isolate give it to the reader as it came.
    const app = seatApp();
    const seen: unknown[] = [];

    app.implement(crunch, (input, { ok }) => {
      seen.push(input);

      return ok(1);
    });

    const { port } = await served(app);

    port.deliver(runMessage(4n, "crunch", '{"n":9007199254740991}'));
    await settled();
    assert.deepEqual(seen, [{ n: 9_007_199_254_740_991 }]);
    assert.deepEqual(outcomes(port), [{ kind: "value", json: "1" }]);
  });

  test("a body that throws gives the stored error of the body adapter", async () => {
    const app = seatApp();

    app.implement(crunch, () => {
      throw new TypeError("bad");
    });

    const { port } = await served(app);

    port.deliver(runMessage(5n, "crunch", '{"n":1}'));
    await settled();

    const [outcome] = outcomes(port);

    assert.equal(outcome?.kind === "error" ? outcome.parts.code.code : "", "UNHANDLED_EXCEPTION");
    assert.equal(
      outcome?.kind === "error" && outcome.parts.message.kind === "text" ? outcome.parts.message.text : "",
      "Unhandled exception in task crunch: TypeError: bad",
    );
  });

  test("the seat body observer: enter at the run with its facts; end at the main thread's verdict with its time", async () => {
    const app = seatApp();
    const calls: string[] = [];

    const observer: SeatBodyObserver = {
      enter: (facts) => {
        calls.push(`enter ${facts.kind}`);

        return {
          run: (call) => {
            calls.push("run");

            return call();
          },
          thrown: () => undefined,
          end: (end: BodyEnd, endedAt) => {
            calls.push(`end ${end.kind} ${endedAt.toString()}`);
          },
          cancelled: () => undefined,
        };
      },
    };

    setSeatBodyObserver(contextOf(app), observer);
    app.implement(crunch, ({ n }, { ok }) => {
      calls.push("body");

      return ok(n);
    });

    const { port } = await served(app);

    port.deliver(runMessage(6n, "crunch", '{"n":1}', RECORDED));
    await settled();
    assert.deepEqual(calls, ["enter recorded", "run", "body"]);
    port.deliver(bodyEndedMessage(6n, { kind: "timedOut" }));
    assert.deepEqual(calls, ["enter recorded", "run", "body", `end timedOut ${ENDED_AT.toString()}`]);
    port.deliver(runMessage(7n, "crunch", '{"n":2}', RECORDED));
    await settled();
    assert.equal(outcomes(port).length, 2);
  });
});

describe("cancel in a seat", () => {
  test("a sync body reads the cancel cell of its seat", async () => {
    const app = seatApp();

    app.implement(crunch, (_input, runtime) => {
      const state = runtime.cancelState();

      return runtime.ok(state.kind === "requested" && state.cause === "DEADLINE" ? 1 : 0);
    });

    const { port, channel } = await served(app, 1);

    clearCancel(channel.marker.cancelCells, 1);
    requestCancel(channel.marker.cancelCells, 1, "DEADLINE");
    port.deliver(runMessage(7n, "crunch", '{"n":1}'));
    await settled();
    assert.deepEqual(outcomes(port), [{ kind: "value", json: "1" }]);
  });

  test("an async body sees its signal fire with the cause; a cancel of another run is left", async () => {
    const app = seatApp();

    app.implement(fetchPage, async (_input, runtime) => {
      const reason = await new Promise<unknown>((resolve) => {
        runtime.signal.addEventListener("abort", () => {
          resolve(runtime.signal.reason);
        });
      });

      return runtime.ok(JSON.stringify(reason));
    });

    const { port } = await served(app);

    port.deliver(runMessage(8n, "fetch_page", '{"url":"https://example.org"}'));
    port.deliver(cancelMessage(99n, "DEADLINE"));
    await settled();
    assert.deepEqual(outcomes(port), []);
    port.deliver(cancelMessage(8n, "OPERATOR"));
    await settled();
    assert.deepEqual(outcomes(port), [{ kind: "value", json: JSON.stringify(JSON.stringify({ cause: "OPERATOR" })) }]);
  });

  test("the seat body observer gets the cause of the run's cancel once; a cancel of another run is left", async () => {
    const app = seatApp();
    const causes: string[] = [];

    const observer: SeatBodyObserver = {
      enter: () => ({
        run: (call) => call(),
        thrown: () => undefined,
        end: () => undefined,
        cancelled: (cause) => {
          causes.push(cause);
        },
      }),
    };

    setSeatBodyObserver(contextOf(app), observer);
    app.implement(fetchPage, async (_input, runtime) => {
      await new Promise<void>((resolve) => {
        runtime.signal.addEventListener("abort", () => {
          resolve();
        });
      });

      return runtime.ok("cancelled");
    });

    const { port } = await served(app);

    port.deliver(runMessage(9n, "fetch_page", '{"url":"https://example.org"}'));
    port.deliver(cancelMessage(98n, "OPERATOR"));
    port.deliver(cancelMessage(9n, "DEADLINE"));
    port.deliver(cancelMessage(9n, "OPERATOR"));
    await settled();
    assert.deepEqual(causes, ["DEADLINE"]);
  });
});

describe("the process events of a seat isolate", () => {
  test("a detached throw in code of a run is logged with the run; the isolate lives", async () => {
    const { port, events } = await served(seatApp());

    runStore.run({ taskId: RUN_TASK_ID, taskName: "crunch", attempt: 2 }, () => {
      events.uncaught[0]?.(new TypeError("late"));
    });
    assert.match(port.postsOf("log")[0]?.line ?? "", /a callback threw in the run of task crunch .*attempt 2.*TypeError: late.*; the child lives/s);
    assert.deepEqual(events.ends, []);
  });

  test("a throw outside every run is logged and ends the isolate", async () => {
    const { port, events } = await served(seatApp());

    events.uncaught[0]?.(new RangeError("outside"));
    assert.match(port.postsOf("log")[0]?.line ?? "", /code outside every run threw: RangeError: outside/);
    assert.deepEqual(events.ends, [1]);
  });

  test("a rejection that no body awaits is logged; the isolate lives", async () => {
    const { port, events } = await served(seatApp());

    events.rejections[0]?.(new TypeError("not awaited"));
    assert.match(port.postsOf("log")[0]?.line ?? "", /a Promise that no body awaits rejected: TypeError: not awaited/);
    assert.deepEqual(events.ends, []);
  });
});

describe("the defects of a seat isolate go to the main thread, then the isolate throws", () => {
  test("a message outside the protocol", async () => {
    const { port } = await served(seatApp());

    assert.throws(() => {
      port.deliverRaw({ ...cancelMessage(1n, "DEADLINE"), protocol: 9 });
    }, ClientDefect);
    assert.match(defects(port)[0] ?? "", /^ClientDefect: .*protocol version 9/);
  });

  test("a run before the isolate serves", () => {
    const { port } = fakeChannel();

    assert.throws(() => {
      port.deliver(runMessage(12n, "crunch", '{"n":1}'));
    }, /before the isolate served its runs/);
    assert.match(defects(port)[0] ?? "", /before the isolate served its runs/);
  });

  test("a run while a run runs", async () => {
    const app = seatApp();

    app.implement(fetchPage, async () => new Promise(() => undefined));

    const { port } = await served(app);

    port.deliver(runMessage(10n, "fetch_page", '{"url":"a"}'));
    assert.throws(() => {
      port.deliver(runMessage(11n, "fetch_page", '{"url":"b"}'));
    }, ClientDefect);
    assert.match(defects(port)[0] ?? "", /whose run has not ended/);
  });

  test("a run of a task with no implementation", async () => {
    const { port } = await served(seatApp());

    assert.throws(() => {
      port.deliver(runMessage(13n, "crunch", '{"n":1}'));
    }, ClientDefect);
    assert.match(defects(port)[0] ?? "", /which has no implementation/);
  });

  test("an answer to an operation that does not wait", async () => {
    const { port } = await served(seatApp());

    assert.throws(() => {
      port.deliver(replyMessage(42, { kind: "reply", reply: { area: "sends", reply: { kind: "prepareSend", prepared: PREPARED } } }));
    }, ClientDefect);
    assert.match(defects(port)[0] ?? "", /answered the operation 42, which does not wait/);
  });

  test("a verdict of a run whose result did not go", async () => {
    const { port } = await served(seatApp());

    assert.throws(() => {
      port.deliver(bodyEndedMessage(14n, { kind: "returned" }));
    }, ClientDefect);
    assert.match(defects(port)[0] ?? "", /whose result did not go/);
  });

  test("a rejection of the body adapter (a seat body observer that threw)", async () => {
    const app = seatApp();

    setSeatBodyObserver(contextOf(app), {
      enter: () => ({
        run: (call) => call(),
        thrown: () => {
          throw new TypeError("the observer broke");
        },
        end: () => undefined,
        cancelled: () => undefined,
      }),
    });
    app.implement(crunch, () => {
      throw new RangeError("body");
    });

    const { port } = await served(app);

    port.deliver(runMessage(15n, "crunch", '{"n":1}'));
    await settled();
    assert.match(defects(port)[0] ?? "", /the body adapter rejected with TypeError: the observer broke/);
    assert.deepEqual(outcomes(port), []);
  });

  test("spawn in the runtime of a seat body", async () => {
    const app = seatApp();
    const thrown: unknown[] = [];

    app.implement(crunch, (_input, runtime) => {
      try {
        // SAFETY: a JavaScript body can call a member that the type of its runtime does not hold.
        void (runtime as unknown as TaskRuntime).spawn("work", async () => 1);
      } catch (cause) {
        thrown.push(cause);
      }

      return runtime.ok(0);
    });

    const { port } = await served(app);

    port.deliver(runMessage(9n, "crunch", '{"n":1}'));
    await settled();
    assert.ok(thrown[0] instanceof ClientDefect);
    assert.match(defects(port)[0] ?? "", /has no spawn/);
  });

  test("an answer goes to the relay", async () => {
    const { channel, port } = fakeChannel();

    const replied = channel.relay.request("sends", {
      kind: "prepareSend",
      taskName: "crunch",
      facts: { retention: { kind: "queueOrDefault" }, deadline: { kind: "noDeadline" }, inputBytes: 7 },
    });

    port.deliver(replyMessage(1, { kind: "reply", reply: { area: "sends", reply: { kind: "prepareSend", prepared: PREPARED } } }));
    assert.deepEqual(await replied, { kind: "reply", reply: { kind: "prepareSend", prepared: PREPARED } });
  });
});
