// The core handle of an app: the handle of the addon core on the main thread; `AddonOutsideMainThread` off it; a
// defect for an app of the main thread with no addon core.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  appCoreHandle,
  type CoreAccess,
  coreHandleOnThread,
  type SeatRelay,
} from "../../../horsies/src/app/core-access.ts";
import { AddonOutsideMainThread, ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { fakeCoreHandle, fakeProducer } from "./support.ts";

/** The thread of these tests. */
const MAIN = { kind: "main" } as const;

/** A relay that the tests never call. */
const RELAY: SeatRelay = {
  request: async () => assert.fail("the relay is not called"),
};

describe("coreHandleOnThread", () => {
  test("the addon core on the main thread gives the app's handle", () => {
    const handle = fakeCoreHandle();
    const core: CoreAccess = { kind: "addon", handle, producer: fakeProducer() };

    assert.equal(coreHandleOnThread(core, MAIN), handle);
  });

  test("a call off the main thread is AddonOutsideMainThread with the thread id", () => {
    const core: CoreAccess = { kind: "addon", handle: fakeCoreHandle(), producer: fakeProducer() };

    assert.throws(() => coreHandleOnThread(core, { kind: "worker", threadId: 7 }), (thrown: Error) => {
      assert.ok(thrown instanceof AddonOutsideMainThread);
      assert.equal(thrown.threadId, 7);

      return true;
    });
  });

  test("an app of the main thread with no addon core is a defect, for the seat relay and for no core", () => {
    for (const core of [{ kind: "noCore" }, { kind: "seatRelay", relay: RELAY }] satisfies CoreAccess[]) {
      assert.throws(() => coreHandleOnThread(core, MAIN), ClientDefect, core.kind);
    }
  });
});

describe("appCoreHandle", () => {
  test("reads the thread of the call: this test runs on the main thread", () => {
    const handle = fakeCoreHandle();

    assert.equal(appCoreHandle({ kind: "addon", handle, producer: fakeProducer() }), handle);
    assert.throws(() => appCoreHandle({ kind: "noCore" }), ClientDefect);
  });
});
