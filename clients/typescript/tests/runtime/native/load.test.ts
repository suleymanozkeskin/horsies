// The load check of the addon: the dev-only entry loads a built addon, and the package loader keeps
// that load. Runs only where a built addon exists, with `HORSIES_NATIVE_ADDON_PATH` naming the file
// (`bun run --cwd bridge test:native` and `test:native:node`).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { DEV_ADDON_PATH_VARIABLE, loadDevAddon, requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { loadAddon } from "../../../horsies/src/native/load.ts";

const exports = requireDevAddon();

describe("the addon loader with a built addon", () => {
  test("the dev entry loads the file that the variable names, on the main thread", () => {
    const path = process.env[DEV_ADDON_PATH_VARIABLE];
    const load = loadDevAddon(path);

    assert.ok(load.kind === "loaded", JSON.stringify(load));
    assert.equal(load.path, path);
    assert.equal(load.exports, exports);
  });

  test("the package loader gives the kept load of the dev build", () => {
    const load = loadAddon();

    assert.ok(load.kind === "loaded", JSON.stringify(load));
    assert.deepEqual(load.source, { kind: "devBuild", path: process.env[DEV_ADDON_PATH_VARIABLE] });
    assert.equal(load.exports, exports);
  });
});
