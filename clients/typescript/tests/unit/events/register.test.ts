// The event registration: handlers of one type are kept in order and reach the worker manifest's handler list; each
// refusal lands in the refusal list (area `events`) and the check reports it; the start hooks of the handler child
// are kept in order; a member after the serve of the handler child is a defect.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { createHorsies, type Horsies } from "../../../horsies/src/app.ts";
import type { AppContext } from "../../../horsies/src/app/context.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import type { CoreChecks } from "../../../horsies/src/app/core-checks.ts";
import { staticCheck } from "../../../horsies/src/check/static-check.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { MAX_EVENT_HANDLERS } from "../../../horsies/src/events/bounds.ts";
import { onEventFor, registeredHandlers, servedEventMembers } from "../../../horsies/src/events/register.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { DATABASE_URL, fakeChecks, fakeEnvironment } from "../app/support.ts";

const listed = defineTask({ name: "listed", input: {}, output: h.null() });

function app(): Horsies {
  const created = createHorsies({ broker: { databaseUrl: DATABASE_URL }, tasks: [listed] }, fakeEnvironment());

  if (!created.ok) {
    throw new Error("the app of the event tests is refused");
  }

  created.value.implement(listed, () => listed.ok(null));

  return created.value;
}

function refusalCodes(context: AppContext): readonly [string, string][] {
  return context.refusals.recorded().map((recorded) => [recorded.area, recorded.refusal.code]);
}

const noop = (): undefined => undefined;

describe("app.onEvent", () => {
  test("two handlers of one type are kept in order, and the worker manifest lists both", () => {
    const hub = app();

    hub.onEvent("AttemptEnded", { name: "alerts" }, noop);
    hub.onEvent("AttemptEnded", { name: "audit" }, noop);
    hub.onEvent("TaskClaimed", { name: "claims" }, noop);

    const context = appInternals(hub);

    assert.deepEqual(context.telemetry.handlers(), [
      { name: "alerts", eventType: "AttemptEnded" },
      { name: "audit", eventType: "AttemptEnded" },
      { name: "claims", eventType: "TaskClaimed" },
    ]);
    assert.deepEqual(registeredHandlers(context), context.telemetry.handlers());
    assert.deepEqual(context.refusals.recorded(), []);
  });

  test("a name of 255 characters outside the Basic Multilingual Plane is taken: the rule counts characters", () => {
    const hub = app();

    hub.onEvent("SlotDown", { name: "\u{1F40E}".repeat(255) }, noop);
    assert.deepEqual(refusalCodes(appInternals(hub)), []);
  });

  test("each refusal lands in the refusal list with its code, and nothing is registered", () => {
    const hub = app();
    const context = appInternals(hub);

    hub.onEvent("TaskClaimed", { name: "first" }, noop);
    // A JavaScript caller can pass a type and a handler that the static types refuse.
    onEventFor(context, "AttemptFinished", { name: "unknown_type" }, noop);
    onEventFor(context, "TaskClaimed", { name: "not_a_function" }, { handle: noop });
    hub.onEvent("TaskClaimed", { name: "" }, noop);
    hub.onEvent("TaskClaimed", { name: "x".repeat(256) }, noop);
    hub.onEvent("TaskClaimed", { name: "(unknown)" }, noop);
    hub.onEvent("TaskClaimed", { name: "first" }, noop);

    assert.deepEqual(refusalCodes(context), [
      ["events", "HRS-222"],
      ["events", "HRS-223"],
      ["events", "HRS-225"],
      ["events", "HRS-225"],
      ["events", "HRS-225"],
      ["events", "HRS-224"],
    ]);
    assert.deepEqual(
      context.telemetry.handlers().map((handler) => handler.name),
      ["first"],
    );
  });

  test("a handler past the most handlers is HRS-226", () => {
    const hub = app();
    const context = appInternals(hub);

    for (let index = 0; index <= MAX_EVENT_HANDLERS; index += 1) {
      hub.onEvent("TaskClaimed", { name: `handler_${String(index)}` }, noop);
    }

    assert.equal(context.telemetry.handlers().length, MAX_EVENT_HANDLERS);
    assert.deepEqual(refusalCodes(context), [["events", "HRS-226"]]);
    assert.match(context.refusals.recorded()[0]?.refusal.message ?? "", /^onEvent TaskClaimed handler_32: /u);
  });

  test("the check reports a refused registration, in both scopes", () => {
    const hub = app();
    const checks: CoreChecks = fakeChecks();

    hub.onEvent("TaskClaimed", { name: "" }, noop);

    for (const scope of ["withSchedules", "withoutSchedules"] as const) {
      const outcome = staticCheck(appInternals(hub), checks, scope, "check");

      assert.equal(outcome.kind, "refused", scope);
    }
  });

  test("a handler after the serve of the handler child is a defect", () => {
    const hub = app();

    servedEventMembers(appInternals(hub));
    assert.throws(() => {
      hub.onEvent("TaskClaimed", { name: "late" }, noop);
    }, ClientDefect);
  });
});

describe("app.onHandlerChildStart", () => {
  test("the hooks are kept in registration order, each with its name", () => {
    const hub = app();

    function openClient(): void {
      return undefined;
    }

    hub.onHandlerChildStart(openClient);
    hub.onHandlerChildStart(async () => undefined);

    const { hooks } = servedEventMembers(appInternals(hub));

    assert.deepEqual(
      hooks.map((member) => member.name),
      ["onHandlerChildStart #1 openClient", "onHandlerChildStart #2 (anonymous)"],
    );
  });

  test("a hook after the serve of the handler child is a defect: ClientDefect", () => {
    const hub = app();

    servedEventMembers(appInternals(hub));
    assert.throws(() => {
      hub.onHandlerChildStart(noop);
    }, ClientDefect);
  });
});
