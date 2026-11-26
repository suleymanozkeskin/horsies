// The zod adapter that the contracts load: its declarations fit the interface that the loader asserts, and the load
// gives the adapter's own functions.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { zodEntry, type ZodKindEntry } from "../../../horsies/src/contract/zod-entry.ts";
import { zodMemberWire, zodValueWire } from "../../../horsies/src/zod.ts";

// A type check: the adapter's functions are assignable to the interface of the loader.
const declared: ZodKindEntry = { zodMemberWire, zodValueWire };

describe("zodEntry", () => {
  test("the load gives the adapter's functions, the same module instance as an import", () => {
    const load = zodEntry();

    assert.equal(load.kind, "loaded");
    assert.equal(load.kind === "loaded" ? load.entry.zodMemberWire : undefined, declared.zodMemberWire);
    assert.equal(load.kind === "loaded" ? load.entry.zodValueWire : undefined, declared.zodValueWire);
  });

  test("a second call gives the kept outcome", () => {
    assert.equal(zodEntry(), zodEntry());
  });
});
