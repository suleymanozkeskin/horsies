// Compile-pass cases of the sketch's workflow definitions (sections 5.1 and 5.2): a parameter takes a value or, when
// declared h.taskOutcome(contract), an upstream node; joins; a success policy; sub-workflows with and without params; a
// task that takes the workflow context names ctxFrom.

import { defineTask, h, type ParameterSchemas } from "../../../horsies/src/index.ts";
import {
  defineWorkflow,
  type NodeRef,
  type ReturnedNodes,
  type WorkflowContract,
  type WorkflowOutput,
} from "../../../horsies/src/workflow/public.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const Order = h.object({ orderId: h.uuid(), lines: h.array(h.string()) });
const ValidatedOrder = h.object({ orderId: h.uuid(), total: h.decimal() });
const Receipt = h.object({ receiptId: h.string() });

const validateOrder = defineTask({ name: "validate_order", input: { order: Order }, output: ValidatedOrder });

const reserveStock = defineTask({
  name: "reserve_stock",
  input: { order: h.taskOutcome(validateOrder), warehouse: h.string() },
  output: h.null(),
});

const chargePayment = defineTask({
  name: "charge_payment",
  input: { order: h.taskOutcome(validateOrder) },
  output: Receipt,
  errors: ["PAYMENT_DECLINED"],
});

const sendReceipt = defineTask({ name: "send_receipt", input: { receipt: h.taskOutcome(chargePayment) }, output: h.null() });

const quoteShipping = defineTask({
  name: "quote_shipping",
  input: { carrier: h.string(), order: Order },
  output: h.int(),
});

const pickCarrier = defineTask({ name: "pick_carrier", input: {}, output: h.string() });
const audit = defineTask({ name: "audit", input: {}, output: h.null(), workflowContext: true });

const sampleOrder = { orderId: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b", lines: ["a"] };

export const fulfilOrder = defineWorkflow(
  {
    name: "fulfil_order",
    definitionKey: "shop.fulfil_order.v1",
    output: Receipt,
    params: { order: Order },
    onError: "fail",
    checkCases: [{ order: sampleOrder }],
  },
  (w, { order }) => {
    const validated = w.task(validateOrder, { order }, { id: "validate" });
    const reserved = w.task(reserveStock, { order: validated, warehouse: "eu-1" }, { id: "reserve" });
    const charged = w.task(chargePayment, { order: validated }, { id: "charge", waitsFor: [reserved] });

    w.task(sendReceipt, { receipt: charged }, { id: "notify", allowFailedDeps: true });
    w.task(audit, {}, { id: "audit", waitsFor: [charged], ctxFrom: [charged] });

    return { output: charged };
  },
);

/** The output type of a workflow contract. */
type OutputOf<W> = W extends WorkflowContract<ParameterSchemas, infer R> ? WorkflowOutput<R> : never;

expectType<Equal<OutputOf<typeof fulfilOrder>, { receiptId: string }>>();

export const shipping = defineWorkflow(
  {
    name: "shipping",
    definitionKey: "shop.shipping.v1",
    output: h.string(),
    params: { order: Order },
    checkCases: [{ order: sampleOrder }],
  },
  (w, { order }) => {
    const quotes = ["dhl", "ups", "fedex"].map((carrier) =>
      w.task(quoteShipping, { carrier, order }, { id: `quote-${carrier}` }),
    );

    const pick = w.task(pickCarrier, {}, { id: "pick", waitsFor: quotes, join: { kind: "quorum", minSuccess: 2 } });

    return { output: pick, successPolicy: { cases: [{ required: [pick] }], optional: quotes } };
  },
);

const syncRegion = defineWorkflow(
  {
    name: "sync_region",
    definitionKey: "shop.sync_region.v1",
    output: h.int(),
    params: { region: h.string() },
    checkCases: [{ region: "eu" }],
  },
  (w, { region }) => ({ output: w.task(quoteShipping, { carrier: region, order: sampleOrder }, { id: "last" }) }),
);

const buildReport = defineTask({ name: "build_report", input: {}, output: h.string() });

export const nightly = defineWorkflow(
  { name: "nightly", definitionKey: "shop.nightly.v1", output: h.string(), params: {} },
  (w) => {
    const regions = ["eu", "us", "apac"].map((region) =>
      w.subWorkflow(syncRegion, { region }, { id: `sync-${region}` }),
    );

    const report = w.task(buildReport, {}, { id: "report", waitsFor: regions, allowFailedDeps: true });

    expectType<Equal<(typeof regions)[number], NodeRef<number, string, `sync-${string}`>>>();

    return { output: report };
  },
);

/** A node-fed parameter declared optional runs under an any join. */
const tally = defineTask({ name: "tally", input: { first: h.taskOutcome(quoteShipping).optional() }, output: h.int() });

export const anyJoin = defineWorkflow(
  { name: "any_join", definitionKey: "shop.any_join.v1", output: h.int(), params: {} },
  (w) => {
    const one = w.task(pickCarrier, {}, { id: "one" });
    const count = w.task(quoteShipping, { carrier: "x", order: sampleOrder }, { id: "count" });
    const summed = w.task(tally, { first: count }, { id: "sum", waitsFor: [one], join: { kind: "any" } });

    return { output: summed };
  },
);

/** The typed set of resultFor: the nodes of the returned object. */
type NodesOf<W> = W extends WorkflowContract<ParameterSchemas, infer R> ? ReturnedNodes<R>["id"] : never;

expectType<Equal<NodesOf<typeof shipping>, "pick" | `quote-${string}`>>();
