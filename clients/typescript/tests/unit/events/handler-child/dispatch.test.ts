// The handler child: the event dispatcher (two handlers of one type both get the event; a throwing handler answers
// `failed` and takes the next event; a text that does not decode answers `failed`), the hook dispatcher, the plan,
// and the fill of the handler slot before the serve of child mode.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { AnyEvent } from "../../../../horsies/src/_catalog/events.ts";
import { appInternals } from "../../../../horsies/src/app/internals.ts";
import type { ChildModeDeps } from "../../../../horsies/src/child/host.ts";
import { runChildMode } from "../../../../horsies/src/child/host.ts";
import { eventDispatch, fillHandlerChildOf, hookDispatch } from "../../../../horsies/src/events/handler-child.ts";
import type { RegisteredHandler } from "../../../../horsies/src/events/register.ts";
import { wireItemsOf } from "../../../../horsies/src/events/wire-text.ts";
import type { HandlerChildPlan } from "../../../../horsies/src/native/handler-child/addon.ts";
import { FakeChildNative, settled } from "../../child/child-native.ts";
import { unusedSeatHost } from "../../seats/support.ts";
import { implementedApp } from "../../worker/support.ts";

/** The wire text of the core's vector `task_claimed`: its time is above 2^53 nanoseconds. */
const TASK_CLAIMED_TEXT =
  '["TaskClaimed",1700000000123456789,"0192f6a1-7b3c-7d4e-8f00-0123456789ab","send_email","default","SHARED_ASYNC",2,50,1500000]';

const SLOT_RECOVERED_TEXT = '["SlotRecovered",1700000000123456789,"SHARED_ASYNC",1]';

function handlerOf(name: string, eventType: RegisteredHandler["eventType"], handler: RegisteredHandler["handler"]): RegisteredHandler {
  return { name, eventType, handler };
}

describe("the wire text of an event", () => {
  test("an integer above 2^53 is an exact bigint", () => {
    const read = wireItemsOf(TASK_CLAIMED_TEXT);

    assert.equal(read.kind === "items" ? read.items[1] : undefined, 1_700_000_000_123_456_789n);
  });

  test("a text that is not a JSON array is notItems", () => {
    assert.equal(wireItemsOf("{").kind, "notItems");
    assert.equal(wireItemsOf('"TaskClaimed"').kind, "notItems");
  });
});

describe("the event dispatcher", () => {
  test("two handlers of one type both get the decoded event", async () => {
    const seen: [string, AnyEvent][] = [];

    const dispatch = eventDispatch([
      handlerOf("alerts", "TaskClaimed", (event) => {
        seen.push(["alerts", event]);
      }),
      handlerOf("audit", "TaskClaimed", async (event) => {
        seen.push(["audit", event]);
      }),
    ]);

    assert.deepEqual(await dispatch({ index: 0, wireJson: TASK_CLAIMED_TEXT }), { kind: "handled" });
    assert.deepEqual(await dispatch({ index: 1, wireJson: TASK_CLAIMED_TEXT }), { kind: "handled" });
    assert.deepEqual(
      seen.map(([name, event]) => [name, event.kind, event.kind === "TaskClaimed" ? event.attempt : 0]),
      [
        ["alerts", "TaskClaimed", 2],
        ["audit", "TaskClaimed", 2],
      ],
    );
  });

  test("a throwing handler answers failed with the type and message, and takes the next event", async () => {
    let calls = 0;

    const dispatch = eventDispatch([
      handlerOf("flaky", "SlotRecovered", () => {
        calls += 1;

        if (calls === 1) {
          throw new TypeError("the client is closed");
        }
      }),
    ]);

    assert.deepEqual(await dispatch({ index: 0, wireJson: SLOT_RECOVERED_TEXT }), {
      kind: "failed",
      cause: "TypeError: the client is closed",
    });
    assert.deepEqual(await dispatch({ index: 0, wireJson: SLOT_RECOVERED_TEXT }), { kind: "handled" });
  });

  test("a rejected Promise answers failed", async () => {
    const dispatch = eventDispatch([
      handlerOf("rejects", "SlotRecovered", async () => Promise.reject(new RangeError("no room"))),
    ]);

    assert.deepEqual(await dispatch({ index: 0, wireJson: SLOT_RECOVERED_TEXT }), { kind: "failed", cause: "RangeError: no room" });
  });

  test("a text that does not decode, and an event of another type, answer failed and reach no handler", async () => {
    let calls = 0;

    const dispatch = eventDispatch([
      handlerOf("claims", "TaskClaimed", () => {
        calls += 1;
      }),
    ]);

    const answers = [
      await dispatch({ index: 0, wireJson: "not json" }),
      await dispatch({ index: 0, wireJson: '["AttemptFinished"]' }),
      await dispatch({ index: 0, wireJson: SLOT_RECOVERED_TEXT }),
    ];

    assert.deepEqual(
      answers.map((answer) => answer.kind),
      ["failed", "failed", "failed"],
    );
    assert.match(answers[1]?.kind === "failed" ? answers[1].cause : "", /AttemptFinished is not in this contract version/u);
    assert.match(answers[2]?.kind === "failed" ? answers[2].cause : "", /the event is a SlotRecovered; the handler takes TaskClaimed/u);
    assert.equal(calls, 0);
  });
});

describe("the hook dispatcher of the handler child", () => {
  test("the hook at the index runs; a throwing hook rejects, so the core ends the child", async () => {
    const ran: string[] = [];

    const dispatch = hookDispatch([
      { name: "onHandlerChildStart #1 open", hook: () => void ran.push("open") },
      {
        name: "onHandlerChildStart #2 broken",
        hook: () => {
          throw new Error("the SDK does not start");
        },
      },
    ]);

    assert.equal(await dispatch({ index: 0, facts: { kind: "handler" } }), "ended");
    await assert.rejects(dispatch({ index: 1, facts: { kind: "handler" } }), /the SDK does not start/u);
    assert.deepEqual(ran, ["open"]);
  });
});

describe("the fill of the handler slot", () => {
  const settlement = { returned: (): void => undefined, threw: (): void => undefined };

  function recorder(): { readonly plans: HandlerChildPlan[]; readonly native: { fillHandlerChild: (plan: HandlerChildPlan) => void } } {
    const plans: HandlerChildPlan[] = [];

    return { plans, native: { fillHandlerChild: (plan) => void plans.push(plan) } };
  }

  test("an app with no handler, no hook and no worker-ended spans fills nothing", () => {
    const { plans, native } = recorder();

    fillHandlerChildOf(appInternals(implementedApp()), native, settlement);
    assert.deepEqual(plans, []);
  });

  test("the plan holds each handler, each hook and the span mode, in registration order", () => {
    const app = implementedApp();
    const { plans, native } = recorder();

    app.onEvent("AttemptEnded", { name: "alerts" }, () => undefined);
    app.onEvent("TaskClaimed", { name: "claims" }, () => undefined);
    app.onHandlerChildStart(function openClient() {
      return undefined;
    });
    fillHandlerChildOf(appInternals(app), native, settlement);

    assert.deepEqual(plans, [
      {
        handlers: [
          { name: "alerts", eventType: "AttemptEnded" },
          { name: "claims", eventType: "TaskClaimed" },
        ],
        startHooks: ["onHandlerChildStart #1 openClient"],
        endedSpans: "NO_SPANS",
      },
    ]);
  });

  test("child mode fills the handler slot before the serve", async () => {
    const native = new FakeChildNative();
    const app = implementedApp();

    app.onEvent("SlotDown", { name: "slots" }, () => undefined);

    const deps: ChildModeDeps = {
      native,
      events: { onSignal: () => undefined, onUncaught: () => undefined, onRejection: () => undefined },
      timer: () => () => undefined,
      culprit: { kind: "none" },
      settlement,
      hostname: "host-1",
      writeLine: () => undefined,
      wait: async () => undefined,
      exit: (code) => {
        throw new Error(`ended with ${String(code)}`);
      },
      reportDefect: (defect) => {
        throw defect;
      },
      seats: unusedSeatHost(),
    };

    void runChildMode(app, ["--horsies-child", "3", "4242"], deps).catch(() => undefined);
    await settled();

    const filled = native.calls.indexOf("fillHandlerChild");

    assert.ok(filled > 0 && filled < native.calls.indexOf("serveChild"), native.calls.join(", "));
    assert.deepEqual(
      native.handlerPlans.map((plan) => plan.handlers),
      [[{ name: "slots", eventType: "SlotDown" }]],
    );
    native.endServe(0);
  });
});
