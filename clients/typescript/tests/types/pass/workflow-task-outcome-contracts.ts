// `h.taskOutcome(contract)` with a task contract and with a workflow contract: the value and the code of the outcome
// are typed by the contract; a node fits when its output and its codes fit; a sub-workflow node feeds a parameter
// declared with its workflow contract; a check case gives the outcome of each such param.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import type { TaskOutcomeWire } from "../../../horsies/src/wire/public.ts";
import { defineWorkflow, type UpstreamCode, type UpstreamValue } from "../../../horsies/src/workflow/public.ts";

declare const app: Horsies;

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

type Expect<T extends true> = T;

const validateOrder = defineTask({
  name: "validate_order",
  input: {},
  output: h.object({ total: h.int() }),
  errors: ["EMPTY_ORDER", "INVALID_EMAIL"],
});

const emptyOnly = defineTask({ name: "empty_only", input: {}, output: h.object({ total: h.int() }), errors: ["EMPTY_ORDER"] });

const regionTotal = defineWorkflow(
  {
    name: "region_total",
    definitionKey: "tests.region_total.v1",
    params: { region: h.string() },
    output: h.object({ total: h.int() }),
    checkCases: [{ region: "eu" }],
  },
  (w) => ({ output: w.task(validateOrder, {}, { id: "validate" }) }),
);

const chargeOrder = defineTask({
  name: "charge_order",
  input: { order: h.taskOutcome(validateOrder) },
  output: h.int(),
  errors: ["UPSTREAM_FAILED"],
});

const report = defineTask({ name: "report", input: { total: h.int() }, output: h.int() });
const sumRegions = defineTask({ name: "sum_regions", input: { region: h.taskOutcome(regionTotal) }, output: h.int() });

export type Checks = [
  Expect<Equal<UpstreamValue<TaskOutcomeWire<typeof validateOrder>>, { total: number }>>,
  Expect<Equal<UpstreamCode<TaskOutcomeWire<typeof validateOrder>>, "EMPTY_ORDER" | "INVALID_EMAIL">>,
  Expect<Equal<UpstreamValue<TaskOutcomeWire<typeof regionTotal>>, { total: number }>>,
  Expect<Equal<UpstreamCode<TaskOutcomeWire<typeof regionTotal>>, string>>,
];

export const charge = app.implement(chargeOrder, ({ order }, { ok, err }) => {
  if (order.kind === "failed") {
    const code: string = order.error.code.code;

    if (order.error.code.kind === "user") {
      const custom: "EMPTY_ORDER" | "INVALID_EMAIL" = order.error.code.code;

      return err({ code: "UPSTREAM_FAILED", message: custom });
    }

    return err({ code: "UPSTREAM_FAILED", message: code });
  }

  return ok(order.value.total);
});

export const sum = app.implement(sumRegions, ({ region }, { ok }) =>
  ok(region.kind === "succeeded" ? region.value.total : 0),
);

export const fulfil = defineWorkflow(
  { name: "fulfil", definitionKey: "tests.fulfil.v1", params: {}, output: h.int() },
  (w) => {
    const validated = w.task(validateOrder, {}, { id: "validate" });
    const emptyChecked = w.task(emptyOnly, {}, { id: "empty_only" });
    const eu = w.subWorkflow(regionTotal, { region: "eu" }, { id: "eu" });

    w.task(chargeOrder, { order: validated }, { id: "charge" });
    w.task(chargeOrder, { order: emptyChecked }, { id: "charge_empty_only" });

    return { output: w.task(sumRegions, { region: eu }, { id: "sum" }) };
  },
);

export const child = defineWorkflow(
  {
    name: "child",
    definitionKey: "tests.child.v1",
    params: { order: h.taskOutcome(validateOrder), note: h.string().optional() },
    output: h.int(),
    checkCases: [
      { order: { kind: "succeeded", value: { total: 3 } } },
      {
        order: {
          kind: "failed",
          error: {
            code: { kind: "user", code: "EMPTY_ORDER" },
            message: null,
            data: null,
            exception: null,
            subWorkflow: null,
          },
        },
        note: "empty",
      },
    ],
  },
  (w, { order }) => ({
    output: w.task(report, { total: order.kind === "succeeded" ? order.value.total : 0 }, { id: "report" }),
  }),
);
