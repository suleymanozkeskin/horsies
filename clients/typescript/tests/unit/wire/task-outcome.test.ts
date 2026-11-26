// `h.taskOutcome(contract)`: the kind of the parameter from a task contract and from a workflow contract, the refusal
// of an upstream whose output has no stored form, the placement rule, and the decode of a stored outcome.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import { writtenJsonText } from "../../../horsies/src/codec/json-text.ts";
import { decodeParameters } from "../../../horsies/src/codec/reader.ts";
import { compileContract } from "../../../horsies/src/contract/compile.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { ParameterSchemas } from "../../../horsies/src/contract/schema.ts";
import type { ObjectKind } from "../../../horsies/src/wire/kinds.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const Total = h.object({ total: h.int() });

const priced = defineTask({ name: "priced", input: {}, output: Total, errorCodes: ["NO_PRICE"] });

const pricing = defineWorkflow(
  { name: "pricing", definitionKey: "tests.pricing.v1", params: {}, output: Total },
  (w) => ({ output: w.task(priced, {}, { nodeId: "price" }) }),
);

/** The compile of a task whose one parameter `price` is `schema`. */
function compiledWith(schema: ParameterSchemas[string]): ReturnType<typeof compileContract> {
  return compileContract(defineTask({ name: "charge", input: { price: schema }, output: h.null() }));
}

function parametersWith(schema: ParameterSchemas[string]): ObjectKind {
  const compiled = compiledWith(schema);

  return compiled.ok ? compiled.value.parameters : assert.fail("the contract compiles");
}

describe("h.taskOutcome", () => {
  test("of a task contract: the outcome kind holds the task's output kind", () => {
    assert.deepEqual(kindOf(h.taskOutcome(priced)), {
      node: "taskOutcome",
      output: { state: "compiled", kind: kindOf(Total) },
    });
  });

  test("of a workflow contract: the outcome kind holds the workflow's declared output kind", () => {
    assert.deepEqual(kindOf(h.taskOutcome(pricing)), {
      node: "taskOutcome",
      output: { state: "compiled", kind: kindOf(Total) },
    });
  });

  test("of a task with a zod output: the outcome kind holds the zod output's kind", () => {
    const named = defineTask({ name: "named", input: {}, output: z.string() });

    assert.deepEqual(kindOf(h.taskOutcome(named)), {
      node: "taskOutcome",
      output: { state: "compiled", kind: { node: "string" } },
    });
  });

  test("a parameter of it is left out of the send form: only a workflow node runs the task", () => {
    const compiled = compiledWith(h.taskOutcome(priced));

    assert.deepEqual(compiled.ok ? compiled.value.send : undefined, { kind: "workflowNodeOnly", upstream: ["price"] });
  });

  test("an upstream whose output has no stored form: the downstream compile refuses the parameter", () => {
    const dated = defineTask({ name: "dated", input: {}, output: z.date() });

    for (const schema of [h.taskOutcome(dated), h.taskOutcome(dated).optional(), h.taskOutcome(dated).nullable()]) {
      const compiled = compiledWith(schema);

      assert.equal(compiled.ok, false);
      assert.deepEqual(
        compiled.ok ? [] : compiled.error.map((refused) => [refused.rule, refused.path]),
        [["WireKind", "input.price"]],
      );
      assert.match(
        compiled.ok ? "" : (compiled.error[0]?.detail ?? ""),
        /^the output of the task dated has no stored form: output: .*\[UpstreamOutputRefused\]$/,
      );
    }
  });

  test("inside a value type: TypeError that names h.taskOutcome", () => {
    // SAFETY: the types refuse an upstream outcome as an object member; the builder refuses it at run time too.
    assert.throws(() => h.object({ price: h.taskOutcome(priced) } as never), {
      name: "TypeError",
      message: "h.object: h.taskOutcome is valid only as a whole task input parameter",
    });
  });
});

describe("the decode of an h.taskOutcome parameter", () => {
  const parameters = parametersWith(h.taskOutcome(priced));

  test("a stored success decodes with the upstream output kind", () => {
    const text = writtenJsonText('{"price":{"__h_task_result__":true,"ok":{"total":3},"err":null}}');

    assert.deepEqual(decodeParameters(parameters, text, "worker"), {
      ok: true,
      value: { price: { kind: "succeeded", value: { total: 3 } } },
    });
  });

  test("a stored success that the upstream output kind refuses: the decode refuses it", () => {
    const text = writtenJsonText('{"price":{"__h_task_result__":true,"ok":{"total":"3"},"err":null}}');
    const decoded = decodeParameters(parameters, text, "worker");

    assert.equal(decoded.ok ? undefined : decoded.error.path, "$.price.ok.total");
  });

  test("a stored custom code that the contract does not list decodes as a custom code (#2557)", () => {
    const stored = '{"error_code":"OTHER","message":null,"data":null,"exception":null}';
    const text = writtenJsonText(`{"price":{"__h_task_result__":true,"ok":null,"err":${stored}}}`);

    const failed = {
      kind: "failed",
      error: { kind: "custom", code: "OTHER", message: null, data: null, exception: null, subWorkflow: null },
    };

    assert.deepEqual(decodeParameters(parameters, text, "worker"), { ok: true, value: { price: failed } });
  });
});
