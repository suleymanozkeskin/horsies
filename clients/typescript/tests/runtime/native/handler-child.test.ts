// The fill of the handler slot through the addon: a plan with an event type that the core does not have is a defect
// and fills nothing; a plan of every event type fills the slot; a second fill is a defect.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`). One process fills its slot once, so the cases
// run in this order in one test.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { EVENT_TYPES } from "../../../horsies/src/_catalog/events.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { fillHandlerChild } from "../../../horsies/src/native/handler-child/handler-child.ts";
import type { HandlerChildPlan } from "../../../horsies/src/native/handler-child/addon.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

const noop = (): void => undefined;

function planOf(eventTypes: readonly string[]): HandlerChildPlan {
  return {
    handlers: eventTypes.map((eventType, index) => ({ name: `handler_${String(index)}`, eventType })),
    startHooks: ["onHandlerChildStart #1 open"],
    endedSpans: "MAKE_SPANS",
  };
}

describe("the fill of the handler slot", () => {
  test("an unknown event type is refused, a plan of every event type fills, a second fill is refused", () => {
    assert.throws(() => {
      fillHandlerChild(planOf(["AttemptFinished"]), noop, noop, noop);
    }, ClientDefect);

    assert.doesNotThrow(() => {
      fillHandlerChild(planOf(EVENT_TYPES), noop, noop, noop);
    });

    assert.throws(() => {
      fillHandlerChild(planOf(["TaskClaimed"]), noop, noop, noop);
    }, ClientDefect);
  });
});
