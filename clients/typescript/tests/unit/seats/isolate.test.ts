// The seat isolate side: the providers and `ready`, a provider failure, a run and its result, the reader's input, the
// seat body observer, cancel in a seat (a sync body reads the cell, an async body sees the signal), a cancel of another
// run, `spawn`, a run while a run runs, a run before the serve, and the answers of the relay.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { BodyEnd } from "../../../horsies/src/child/body-observer.ts";
import type { TaskRuntime } from "../../../horsies/src/child/runtime.ts";
import type { BodyFactsRecord } from "../../../horsies/src/native/child/addon.ts";
import { type SeatBodyObserver, setSeatBodyObserver } from "../../../horsies/src/seats/body-observer.ts";
import { clearCancel, requestCancel } from "../../../horsies/src/seats/cancel-cells.ts";
import { type SeatIsolateDeps, serveSeat } from "../../../horsies/src/seats/isolate.ts";
import type { SeatOutcome } from "../../../horsies/src/seats/protocol.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { PREPARED } from "../app/support.ts";
import {
  cancelMessage,
  contextOf,
  crunch,
  fakeChannel,
  type FakeIsolatePort,
  fetchPage,
  replyMessage,
  runMessage,
  seatApp,
  settled,
} from "./support.ts";

const ENDED_AT: Temporal.Instant = Temporal.Instant.from("2026-11-26T10:00:00Z");

const DEPS: SeatIsolateDeps = { now: () => ENDED_AT };

const RECORDED: BodyFactsRecord = {
  kind: "recorded",
  taskId: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b",
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
    app.implement(crunch, ({ n }, runtime) => crunch.ok(n + runtime.state(pool).length));

    const { channel, port } = fakeChannel(1);

    void serveSeat(contextOf(app), channel, DEPS);
    await settled();
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

    const { channel, port } = fakeChannel(0);

    void serveSeat(contextOf(app), channel, DEPS);
    await settled();
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

    app.implement(crunch, (input) => {
      seen.push(input);

      return crunch.ok(1);
    });

    const { channel, port } = fakeChannel();

    void serveSeat(contextOf(app), channel, DEPS);
    await settled();
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

    const { channel, port } = fakeChannel();

    void serveSeat(contextOf(app), channel, DEPS);
    await settled();
    port.deliver(runMessage(5n, "crunch", '{"n":1}'));
    await settled();

    const [outcome] = outcomes(port);

    assert.equal(outcome?.kind === "error" ? outcome.parts.code.code : "", "UNHANDLED_EXCEPTION");
    assert.equal(
      outcome?.kind === "error" && outcome.parts.message.kind === "text" ? outcome.parts.message.text : "",
      "Unhandled exception in task crunch: TypeError: bad",
    );
  });

  test("the seat body observer: enter at the run with its facts, end at the body end with endedAt", async () => {
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
        };
      },
    };

    setSeatBodyObserver(contextOf(app), observer);
    app.implement(crunch, ({ n }) => {
      calls.push("body");

      return crunch.ok(n);
    });

    const { channel, port } = fakeChannel();

    void serveSeat(contextOf(app), channel, DEPS);
    await settled();
    port.deliver(runMessage(6n, "crunch", '{"n":1}', RECORDED));
    await settled();
    assert.deepEqual(calls, ["enter recorded", "run", "body", `end returned ${ENDED_AT.toString()}`]);
  });
});

describe("cancel in a seat", () => {
  test("a sync body reads the cancel cell of its seat", async () => {
    const app = seatApp();

    app.implement(crunch, (_input, runtime) => {
      const state = runtime.cancelState();

      return crunch.ok(state.kind === "requested" && state.cause === "DEADLINE" ? 1 : 0);
    });

    const { channel, port } = fakeChannel(1);

    void serveSeat(contextOf(app), channel, DEPS);
    await settled();
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

      return fetchPage.ok(JSON.stringify(reason));
    });

    const { channel, port } = fakeChannel();

    void serveSeat(contextOf(app), channel, DEPS);
    await settled();
    port.deliver(runMessage(8n, "fetch_page", '{"url":"https://example.org"}'));
    port.deliver(cancelMessage(99n, "DEADLINE"));
    await settled();
    assert.deepEqual(outcomes(port), []);
    port.deliver(cancelMessage(8n, "OPERATOR"));
    await settled();
    assert.deepEqual(outcomes(port), [{ kind: "value", json: JSON.stringify(JSON.stringify({ cause: "OPERATOR" })) }]);
  });

  test("the runtime of a seat body has no spawn: a call is a defect", async () => {
    const app = seatApp();
    const thrown: unknown[] = [];

    app.implement(crunch, (_input, runtime) => {
      try {
        // SAFETY: a JavaScript body can call a member that the type of its runtime does not hold.
        (runtime as unknown as TaskRuntime).spawn("work", async () => 1);
      } catch (cause) {
        thrown.push(cause);
      }

      return crunch.ok(0);
    });

    const { channel, port } = fakeChannel();

    void serveSeat(contextOf(app), channel, DEPS);
    await settled();
    port.deliver(runMessage(9n, "crunch", '{"n":1}'));
    await settled();
    assert.ok(thrown[0] instanceof ClientDefect);
  });
});

describe("the defects of a seat isolate", () => {
  test("a run while a run runs is a defect", async () => {
    const app = seatApp();

    app.implement(fetchPage, async () => new Promise(() => undefined));

    const { channel, port } = fakeChannel();

    void serveSeat(contextOf(app), channel, DEPS);
    await settled();
    port.deliver(runMessage(10n, "fetch_page", '{"url":"a"}'));
    assert.throws(() => {
      port.deliver(runMessage(11n, "fetch_page", '{"url":"b"}'));
    }, ClientDefect);
  });

  test("a run before the isolate serves is a defect; an answer goes to the relay", async () => {
    const { channel, port } = fakeChannel();

    assert.throws(() => {
      port.deliver(runMessage(12n, "crunch", '{"n":1}'));
    }, /before the isolate served its runs/);

    const replied = channel.relay.request("sends", {
      kind: "prepareSend",
      taskName: "crunch",
      facts: { retention: { kind: "queueOrDefault" }, deadline: { kind: "noDeadline" }, inputBytes: 7 },
    });

    port.deliver(replyMessage(1, { kind: "reply", reply: { area: "sends", reply: { kind: "prepareSend", prepared: PREPARED } } }));
    assert.deepEqual(await replied, { kind: "prepareSend", prepared: PREPARED });
  });
});
