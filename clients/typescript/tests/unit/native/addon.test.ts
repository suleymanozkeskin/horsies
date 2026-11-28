// The addon object of a load, or the thrown error of a failed load: one case per load outcome.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { addonOrThrow } from "../../../horsies/src/native/call/addon.ts";

describe("addonOrThrow", () => {
  test("gives the export object of a loaded addon", () => {
    const exports = {};

    assert.equal(addonOrThrow({ kind: "loaded", exports, source: { kind: "devBuild", path: "/a.node" } }), exports);
  });

  test("throws AddonOutsideMainThread with the thread of a worker", () => {
    assert.throws(() => addonOrThrow({ kind: "outsideMainThread", threadId: 3 }), {
      name: "AddonOutsideMainThread",
      code: "ADDON_OUTSIDE_MAIN_THREAD",
      threadId: 3,
    });
  });

  test("throws RuntimeStartFailed that names an unsupported host", () => {
    assert.throws(() => addonOrThrow({ kind: "platformUnsupported", host: "win32-x64" }), {
      name: "RuntimeStartFailed",
      code: "RUNTIME_START_FAILED",
      cause: "no native addon of horsies is built for win32-x64",
    });
  });

  test("throws RuntimeStartFailed that names the source and the cause of a failed load", () => {
    assert.throws(
      () =>
        addonOrThrow({
          kind: "loadFailed",
          source: { kind: "platformPackage", packageName: "@horsies/native-linux-x64-gnu" },
          cause: "Cannot find module",
        }),
      {
        name: "RuntimeStartFailed",
        cause: "the native addon did not load from the package @horsies/native-linux-x64-gnu: Cannot find module",
      },
    );
  });

  test("throws RuntimeStartFailed that names the dev build of a failed dev load", () => {
    assert.throws(
      () =>
        addonOrThrow({
          kind: "loadFailed",
          source: { kind: "devBuild", path: "/builds/horsies.node" },
          cause: "no such file",
        }),
      {
        name: "RuntimeStartFailed",
        cause: "the native addon did not load from the dev build /builds/horsies.node: no such file",
      },
    );
  });
});
