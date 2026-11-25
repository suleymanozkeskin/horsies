// The workflow spec data documents of workflow_spec_data.json through the writer. The product takes the document type
// from the codegen output; this test declares it with h.* in the member order of the generated Python dataclasses
// (`kind` first in each tagged union), which the vectors pin.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";
import { show, specDataFile } from "./support/vectors.ts";

const join = h.union("kind", [
  h.object({ kind: h.literal("ALL") }),
  h.object({ kind: h.literal("ANY") }),
  h.object({ kind: h.literal("QUORUM"), min_success: h.int() }),
]);

const taskOptions = h.union("kind", [
  h.object({ kind: h.literal("ENCODED"), json: h.string() }),
  h.object({ kind: h.literal("NO_OPTIONS") }),
]);

const body = h.union("kind", [
  h.object({ kind: h.literal("TASK"), queue: h.string(), priority: h.int(), task_options: taskOptions }),
  h.object({ kind: h.literal("SUB_WORKFLOW"), definition_key: h.string(), params: h.enum(["GIVEN", "NOT_GIVEN"]) }),
]);

const node = h.object({
  index: h.int(),
  node_id: h.string(),
  task_name: h.string(),
  input_json: h.string(),
  dependencies: h.array(h.int()),
  args_from: h.record(h.int()),
  workflow_ctx_from: h.array(h.string()),
  allow_failed_deps: h.boolean(),
  join,
  body,
});

const SPEC_DATA = h.object({
  data_version: h.int(),
  name: h.string(),
  definition_key: h.union("kind", [
    h.object({ kind: h.literal("KEYED"), key: h.string() }),
    h.object({ kind: h.literal("UNKEYED") }),
  ]),
  on_error: h.enum(["FAIL", "PAUSE"]),
  output: h.union("kind", [
    h.object({ kind: h.literal("NODE"), index: h.int() }),
    h.object({ kind: h.literal("NO_OUTPUT") }),
  ]),
  success_policy: h.union("kind", [
    h.object({
      kind: h.literal("CASES"),
      cases: h.array(h.object({ required: h.array(h.int()) })),
      optional: h.array(h.int()),
    }),
    h.object({ kind: h.literal("NO_POLICY") }),
  ]),
  queue_classes: h.record(h.string()),
  nodes: h.array(node),
});

describe("workflow_spec_data.json documents", () => {
  for (const vector of specDataFile().vectors) {
    test(`${vector.name}: the document writes Python's text`, () => {
      const written = encodeValue(kindOf(SPEC_DATA), vector.document);

      assert(written.ok, show(written));
      assert.equal(written.value, vector.text);
    });
  }
});
