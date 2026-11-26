// The load check of the addon: it loads a built addon through the package loader.
// Runs only where a built addon exists, with `HORSIES_NATIVE_ADDON_PATH` naming the file
// (`bun run --cwd bridge test:native` and `test:native:node`).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { ADDON_PATH_VARIABLE, loadAddon } from "../../../horsies/src/native/load.ts";

describe("the addon loader with a built addon", () => {
  test("loads the addon file that the path variable names, on the main thread", () => {
    const path = process.env[ADDON_PATH_VARIABLE];

    assert.ok(path !== undefined, `set ${ADDON_PATH_VARIABLE} to the built addon file`);

    const load = loadAddon();

    assert.deepEqual(
      load.kind === "loaded" ? load.source : load,
      { kind: "pathVariable", path },
    );
  });

  test("gives the same export object at a later call", () => {
    const first = loadAddon();
    const second = loadAddon();

    if (first.kind !== "loaded" || second.kind !== "loaded") {
      assert.fail(`the addon did not load: ${JSON.stringify(first)}`);
    }

    assert.equal(second.exports, first.exports);
  });
});
