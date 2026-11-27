// The test-only exports of the addon and the core state of a loaded addon.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { coreStateProblem, TEST_ONLY_EXPORTS, testOnlyExportsIn, testOnlyNamesInBytes } from "./addon-exports.ts";

describe("testOnlyExportsIn", () => {
  test("names each test-only export of an export object", () => {
    assert.deepEqual(testOnlyExportsIn({ coreState: () => null, probeCallback: () => null }), ["probeCallback"]);
    assert.deepEqual(
      testOnlyExportsIn({ coreState: () => null, setProbeChildSpecSource: () => null }),
      ["setProbeChildSpecSource"],
    );
  });

  test("a published export object has none", () => {
    assert.deepEqual(testOnlyExportsIn({ coreState: () => null }), []);
  });
});

describe("testOnlyNamesInBytes", () => {
  test("finds the name of a test-only export in the bytes of a file", () => {
    const bytes = new TextEncoder().encode(`\u0000\u0001${TEST_ONLY_EXPORTS[0] ?? ""}\u0000coreState\u0000`);

    assert.deepEqual(testOnlyNamesInBytes(bytes), ["probeCallback"]);
  });

  test("bytes without the names give none", () => {
    assert.deepEqual(testOnlyNamesInBytes(new TextEncoder().encode("coreState buildCoreHandle")), []);
  });
});

describe("coreStateProblem", () => {
  test("not_started is no problem", () => {
    assert.equal(coreStateProblem({ kind: "value", value: "not_started" }), "none");
  });

  test("another state is a problem", () => {
    assert.equal(coreStateProblem({ kind: "value", value: "running" }), "coreState() is running, not not_started");
  });

  test("a thrown answer is a problem", () => {
    assert.equal(
      coreStateProblem({ kind: "thrown", thrown: { code: "RUNTIME_START_FAILED", cause: "no runtime" } }),
      "coreState() threw RUNTIME_START_FAILED",
    );
  });
});
