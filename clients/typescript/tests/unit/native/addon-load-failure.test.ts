// A failed addon load inside a call wrapper caller throws its own error, `RuntimeStartFailed`, and not
// `ClientDefect`: each caller takes the addon before the call wrapper. One case per caller.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { keptLoad } from "../../../horsies/src/native/load.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import {
  coreState,
  environmentCount,
  migrationSqlFiles,
  mintTaskId,
  setPanicRule,
} from "../../../horsies/src/native/process-core/process-core.ts";

// The first load of this process is a failed one, unless an earlier test of the process loaded: no test
// here depends on an addon file of the host.
keptLoad(() => ({
  kind: "loadFailed",
  source: { kind: "devBuild", path: "/builds/horsies.node" },
  cause: "no such file",
}));

const LOAD_FAILED = {
  name: "RuntimeStartFailed",
  code: "RUNTIME_START_FAILED",
};

describe("a failed addon load in a caller of the call wrapper", () => {
  test("buildCoreHandle throws RuntimeStartFailed", () => {
    assert.throws(() => buildCoreHandle("{}", "PRODUCER"), LOAD_FAILED);
  });

  test("coreState throws RuntimeStartFailed", () => {
    assert.throws(() => coreState(), LOAD_FAILED);
  });

  test("setPanicRule throws RuntimeStartFailed", () => {
    assert.throws(() => {
      setPanicRule("STOP_CORE");
    }, LOAD_FAILED);
  });

  test("mintTaskId throws RuntimeStartFailed", () => {
    assert.throws(() => mintTaskId(), LOAD_FAILED);
  });

  test("migrationSqlFiles throws RuntimeStartFailed", () => {
    assert.throws(() => migrationSqlFiles(), LOAD_FAILED);
  });

  test("environmentCount throws RuntimeStartFailed", () => {
    assert.throws(() => environmentCount(), LOAD_FAILED);
  });
});
