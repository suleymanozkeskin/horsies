import { describe, expect, test } from "bun:test";

import specDataVectors from "../../../../../core/tests/fixtures/stored_forms/workflow_spec_data.json" with { type: "json" };
import { workflowSpecDataToWire } from "../../../horsies/src/_catalog/workflow_spec.ts";
import type { SpecNode, WorkflowSpecData } from "../../../horsies/src/_catalog/workflow_spec.ts";
import { wireText } from "./wire-text.ts";

// Each expected text is the text of a vector of the core's stored-form
// fixture: Python's text of the same spec data. The two documents hold every
// variant of every tagged type and choice.

/** The text of the spec data vector `name`. */
function vectorText(name: string): string {
  const vector = specDataVectors.vectors.find((row) => row.name === name);

  if (vector === undefined) {
    throw new Error(`the vector ${name} is not in workflow_spec_data.json`);
  }

  return vector.text;
}

const CHARGE_INPUT = String.raw`{"order_id":7,"amount":9.5}`;

const FIRST_NODE: SpecNode = {
  index: 0,
  node_id: "first",
  task_name: "app.charge",
  input_json: CHARGE_INPUT,
  dependencies: [],
  args_from: new Map(),
  workflow_ctx_from: [],
  allow_failed_deps: false,
  join: { kind: "ALL" },
  body: { kind: "TASK", queue: "default", priority: 100, task_options: { kind: "NO_OPTIONS" } },
};

const POLICY_AND_QUORUM: WorkflowSpecData = {
  data_version: 1,
  name: "checkout",
  definition_key: { kind: "KEYED", key: "tests.checkout.v1" },
  on_error: "PAUSE",
  output: { kind: "NODE", index: 1 },
  success_policy: { kind: "CASES", cases: [{ required: [1] }], optional: [0] },
  queue_classes: new Map([["default", "standard_30d"]]),
  nodes: [
    FIRST_NODE,
    {
      ...FIRST_NODE,
      index: 1,
      node_id: "second",
      dependencies: [0],
      join: { kind: "QUORUM", min_success: 1 },
      body: { kind: "TASK", queue: "default", priority: 3, task_options: { kind: "NO_OPTIONS" } },
    },
  ],
};

const DAY_INPUT = String.raw`{"day":"2026-11-17"}`;

const FETCH_OPTIONS = String.raw`{"retry_policy":null,"good_until":"2026-11-18T00:00:00+00:00","timeout_ms":5000,"execution":"ISOLATED","cancel_grace_ms":null}`;

const EVERY_KIND: WorkflowSpecData = {
  data_version: 1,
  name: "nightly",
  definition_key: { kind: "UNKEYED" },
  on_error: "FAIL",
  output: { kind: "NO_OUTPUT" },
  success_policy: { kind: "NO_POLICY" },
  queue_classes: new Map([["default", "standard_30d"]]),
  nodes: [
    {
      index: 0,
      node_id: "fetch",
      task_name: "fetch_rows",
      input_json: DAY_INPUT,
      dependencies: [],
      args_from: new Map(),
      workflow_ctx_from: [],
      allow_failed_deps: false,
      join: { kind: "ALL" },
      body: {
        kind: "TASK",
        queue: "default",
        priority: 10,
        task_options: { kind: "ENCODED", json: FETCH_OPTIONS },
      },
    },
    {
      index: 1,
      node_id: "report",
      task_name: "report_workflow",
      input_json: DAY_INPUT,
      dependencies: [0],
      args_from: new Map([["rows", 0]]),
      workflow_ctx_from: [],
      allow_failed_deps: false,
      join: { kind: "ALL" },
      body: { kind: "SUB_WORKFLOW", definition_key: "tests.report.v1", params: "GIVEN" },
    },
    {
      index: 2,
      node_id: "publish",
      task_name: "publish",
      input_json: "{}",
      dependencies: [0, 1],
      args_from: new Map(),
      workflow_ctx_from: ["fetch", "report"],
      allow_failed_deps: true,
      join: { kind: "ANY" },
      body: { kind: "TASK", queue: "default", priority: 100, task_options: { kind: "NO_OPTIONS" } },
    },
  ],
};

describe("workflowSpecDataToWire", () => {
  test("gives Python's text of a spec with a success policy and a quorum join", () => {
    expect(wireText(workflowSpecDataToWire(POLICY_AND_QUORUM))).toBe(
      vectorText("two_nodes_with_a_policy_and_a_quorum"),
    );
  });

  test("gives Python's text of a spec with every member kind", () => {
    expect(wireText(workflowSpecDataToWire(EVERY_KIND))).toBe(vectorText("members_of_every_kind"));
  });

  test("throws a defect for a number that is not a safe integer, with its field", () => {
    const spec: WorkflowSpecData = {
      ...EVERY_KIND,
      nodes: [{ ...FIRST_NODE, args_from: new Map([["rows", -0.5]]) }],
    };

    expect(() => workflowSpecDataToWire(spec)).toThrow(
      "integerToWire: SpecNode.args_from is -0.5, not a safe integer",
    );
  });
});
