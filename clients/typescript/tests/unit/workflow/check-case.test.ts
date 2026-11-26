// The params that `check` gives a build for a check case: each outcome of `checkCaseParams`, and the check of a
// workflow with a required `h.taskOutcome` param (a check case gives the outcome, as in the Python and Rust clients).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { compileContract } from "../../../horsies/src/contract/compile.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { ParameterSchemas } from "../../../horsies/src/contract/schema.ts";
import type { ObjectKind } from "../../../horsies/src/wire/kinds.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import type { TaskError } from "../../../horsies/src/wire/task-error.ts";
import { checkWorkflows } from "../../../horsies/src/workflow/check-cases.ts";
import { checkCaseParams } from "../../../horsies/src/workflow/check-case.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";
import { checkedAtCreate, ping } from "./create-workflows.ts";

/** The upstream task of the params: its output has a member with a default, so the worker form fills it. */
const priced = defineTask({
  name: "priced",
  input: {},
  output: h.object({ total: h.int(), currency: h.string().default("EUR") }),
  errors: ["NO_PRICE"],
});

const FAILURE: TaskError<"NO_PRICE"> = Object.freeze({
  code: Object.freeze({ kind: "user", code: "NO_PRICE" }),
  message: "no price",
  data: null,
  exception: null,
  subWorkflow: null,
});

/** The compiled params of a params contract with a plain `region` and the outcome params of `outcomes`. */
function paramsOf(outcomes: ParameterSchemas): ObjectKind {
  const contract = defineTask({ name: "params", input: { region: h.string(), ...outcomes }, output: h.null() });
  const compiled = compileContract(contract);

  return compiled.ok ? compiled.value.parameters : assert.fail("the params contract compiles");
}

describe("checkCaseParams", () => {
  test("a success value is encoded with the upstream output kind and decoded in the worker form", () => {
    const params = paramsOf({ price: h.taskOutcome(priced) });
    const built = checkCaseParams(params, { region: "eu", price: { kind: "succeeded", value: { total: 3 } } });

    assert.deepEqual(built, {
      ok: true,
      value: { region: "eu", price: { kind: "succeeded", value: { total: 3, currency: "EUR" } } },
    });
    assert.equal(built.ok && Object.isFrozen(built.value), true);
  });

  test("a failure is given to the build as the case holds it", () => {
    const params = paramsOf({ price: h.taskOutcome(priced) });
    const built = checkCaseParams(params, { region: "eu", price: { kind: "failed", error: FAILURE } });

    assert.equal(built.ok, true);
    assert.equal(built.ok ? Reflect.get(Reflect.get(built.value, "price"), "error") : undefined, FAILURE);
  });

  test("the members follow the declaration order of the params", () => {
    const params = paramsOf({ price: h.taskOutcome(priced) });
    const built = checkCaseParams(params, { price: { kind: "failed", error: FAILURE }, region: "eu" });

    assert.deepEqual(built.ok ? Object.keys(built.value) : [], ["region", "price"]);
  });

  test("a required outcome param that the case leaves out: MissingMember at its place", () => {
    const params = paramsOf({ price: h.taskOutcome(priced) });
    const built = checkCaseParams(params, { region: "eu" });

    assert.deepEqual(built, {
      ok: false,
      error: { side: "decode", kind: "MissingMember", path: "$.price", detail: "a required member is missing" },
    });
  });

  test("an optional outcome param that the case leaves out is left out", () => {
    const params = paramsOf({ price: h.taskOutcome(priced).optional() });
    const built = checkCaseParams(params, { region: "eu" });

    assert.deepEqual(built, { ok: true, value: { region: "eu" } });
  });

  test("a nullable outcome param takes null", () => {
    const params = paramsOf({ price: h.taskOutcome(priced).nullable() });
    const built = checkCaseParams(params, { region: "eu", price: null });

    assert.deepEqual(built, { ok: true, value: { region: "eu", price: null } });
  });

  test("a value that is not an outcome: WrongType at its place", () => {
    const params = paramsOf({ price: h.taskOutcome(priced) });

    for (const value of [3, { kind: "succeeded" }, { kind: "failed", error: "NO_PRICE" }, { kind: "skipped" }]) {
      const built = checkCaseParams(params, { region: "eu", price: value });

      assert.equal(built.ok ? undefined : built.error.kind, "WrongType");
      assert.equal(built.ok ? undefined : built.error.path, "$.price");
    }
  });

  test("a success value that does not encode with the upstream output kind: the writer's refusal", () => {
    const params = paramsOf({ price: h.taskOutcome(priced) });
    const built = checkCaseParams(params, { region: "eu", price: { kind: "succeeded", value: { total: "3" } } });

    assert.equal(built.ok ? undefined : built.error.side, "encode");
  });

  test("a case that is not a plain object: the writer's refusal", () => {
    const params = paramsOf({ price: h.taskOutcome(priced) });
    const built = checkCaseParams(params, new Map([["region", "eu"]]));

    assert.equal(built.ok ? undefined : built.error.side, "encode");
  });

  test("a plain param that does not encode: the writer's refusal, before the outcome params", () => {
    const params = paramsOf({ price: h.taskOutcome(priced) });
    const built = checkCaseParams(params, { region: 7 });

    assert.equal(built.ok ? undefined : built.error.side, "encode");
  });
});

describe("check of a workflow with a required h.taskOutcome param", () => {
  /** The child of a sub-workflow node whose param is the result of an upstream node. */
  const child = defineWorkflow(
    {
      name: "child",
      definitionKey: "tests.child.v1",
      output: h.int(),
      params: { price: h.taskOutcome(priced) },
      checkCases: [
        { price: { kind: "succeeded", value: { total: 3, currency: "EUR" } } },
        { price: { kind: "failed", error: FAILURE } },
      ],
    },
    (w, { price }) => ({ output: w.task(ping, {}, { id: price.kind === "succeeded" ? "priced" : "unpriced" }) }),
  );

  test("check builds each case with its outcome: no refusal", () => {
    const checked = checkedAtCreate([child]);

    assert.deepEqual(checked.refusals, []);
    assert.deepEqual(checkWorkflows(checked.registry), []);
  });

  test("a case that leaves out the outcome param: HRS-028 that names the param", () => {
    const leftOut = defineWorkflow(
      {
        name: "left_out",
        definitionKey: "tests.left_out.v1",
        output: h.int(),
        params: { price: h.taskOutcome(priced) },
        // SAFETY: the types require the outcome param; a JavaScript caller can leave it out.
        checkCases: [{} as never],
      },
      (w) => ({ output: w.task(ping, {}, { id: "a" }) }),
    );

    const checked = checkedAtCreate([leftOut]);
    const refusals = checkWorkflows(checked.registry);

    assert.deepEqual(
      refusals.map((refused) => refused.code),
      ["HRS-028"],
    );
    assert.match(refusals[0]?.message ?? "", /\$\.price: a required member is missing/);
  });
});
