// The app hub: a refusal after `Horsies.create` reaches the refusal sink, the default observer gives no carrier, and
// the internal view is not an export of the main entry point.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { createHorsies, Horsies } from "../../../horsies/src/app.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import { NO_OP_OBSERVER } from "../../../horsies/src/app/observers.ts";
import { defineTask, type ResultConstructors } from "../../../horsies/src/contract/public.ts";
import { taskIdFromCore } from "../../../horsies/src/contract/ids.ts";
import * as entry from "../../../horsies/src/index.ts";
import type { TraceArgument } from "../../../horsies/src/native/values/records.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { type Ok, ok } from "../../../horsies/src/wire/result.ts";
import { DATABASE_URL, fakeEnvironment } from "./support.ts";

const listed = defineTask({ name: "listed", input: { n: h.int() }, output: h.int() });
const unlisted = defineTask({ name: "unlisted", input: {}, output: h.null() });

function app(): Horsies {
  const created = createHorsies({ broker: { databaseUrl: DATABASE_URL }, tasks: [listed] }, fakeEnvironment());

  if (!created.ok) {
    throw new Error("the app of the hub tests is refused");
  }

  return created.value;
}

describe("app.implement and the refusal sink", () => {
  test("the implementation of a listed contract is kept by task name, with no refusal", () => {
    const hub = app();
    const implementation = hub.implement(listed, ({ n }, { ok }) => ok(n + 1));

    assert.equal(implementation.kind, "implementation");
    assert.equal(implementation.contract, listed);
    assert.deepEqual([...appInternals(hub).implementations.keys()], ["listed"]);
    assert.deepEqual(appInternals(hub).refusals.recorded(), []);
  });

  test("an implementation of a contract that the app does not list reaches the sink as HRS-300, with no throw", () => {
    const hub = app();

    assert.doesNotThrow(() => hub.implement(unlisted, (_input, { ok }) => ok(null)));
    assert.deepEqual(
      appInternals(hub)
        .refusals.recorded()
        .map((recorded) => [recorded.area, recorded.refusal.code, recorded.refusal.path]),
      [["contracts", "HRS-300", "implement unlisted"]],
    );
    assert.equal(appInternals(hub).implementations.has("unlisted"), false);
  });

  test("a second implementation reaches the sink as HRS-301, and the first one stays", () => {
    const hub = app();
    const first = (_input: object, runtime: ResultConstructors<ReturnType<typeof h.int>, string>): Ok<number> => runtime.ok(1);

    hub.implement(listed, first);
    assert.doesNotThrow(() => hub.implement(listed, (_input, { ok }) => ok(2)));
    assert.deepEqual(
      appInternals(hub)
        .refusals.recorded()
        .map((recorded) => recorded.refusal.code),
      ["HRS-301"],
    );
    assert.equal(appInternals(hub).implementations.get("listed")?.body, first);
  });
});

describe("the send observer", () => {
  test("the no-op observer runs the send with no carrier", async () => {
    const seen: TraceArgument[] = [];
    const sent = ok(taskIdFromCore("0192f6a1-7b3c-7d4e-8f00-0123456789ab", "the hub test"));

    const value = await NO_OP_OBSERVER.observeSend({ taskName: "t", queueName: "default", taskId: "id" }, async (trace) => {
      seen.push(trace);

      return sent;
    });

    assert.equal(value, sent);
    assert.deepEqual(seen, [{ kind: "noCarrier" }]);
  });

  test("the no-op observer has no active carrier", () => {
    assert.deepEqual(NO_OP_OBSERVER.activeCarrier(), { kind: "noCarrier" });
  });

  test("an app of Horsies.create uses the no-op observer", () => {
    assert.equal(appInternals(app()).observer, NO_OP_OBSERVER);
  });
});

describe("the entry points", () => {
  test("appInternals and createHorsies are not exported from the main entry point", () => {
    assert.equal("appInternals" in entry, false);
    assert.equal("createHorsies" in entry, false);
    assert.equal("Horsies" in entry, true);
  });

  test("an app of createHorsies is a Horsies, with the members of this package", () => {
    const hub = app();

    assert.equal(hub instanceof Horsies, true);
    assert.equal(hub.implement instanceof Function, true);
    assert.equal(hub.send instanceof Function, true);
    assert.equal(hub.resend instanceof Function, true);
    assert.equal(hub.taskHandle instanceof Function, true);
  });

  test("appInternals of a value that is not an app is a defect: TypeError", () => {
    assert.throws(() => appInternals({}), TypeError);
  });
});
