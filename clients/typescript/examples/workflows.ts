// A workflow: a graph of task nodes whose results feed later nodes. `load_prices` runs first; `sum_prices` and
// `count_items` take its result and run side by side; `summarize` takes both. The types check the graph: a parameter
// that does not exist, or a source of another type, fails typecheck at its line, and `Horsies.create` checks the
// build again.
//
//   node examples/workflows.ts worker --profile default
//   node examples/workflows.ts demo

import { defineTask, defineWorkflow, h, Horsies, type Result, type TaskError, type TaskOutcome } from "horsies";

import {
  DATABASE_URL,
  DEFAULT_PROFILE,
  DEMO_DONE,
  demoFailed,
  failureText,
  ISOLATED_PROFILE,
  LONG_RESULT_WAIT_MS,
  outcomeLine,
  printLine,
  refusalLines,
  runExample,
  sendErrorText,
  workflowReadErrorText,
} from "./support.ts";

/** The order that the demo prices. */
const ORDER_ID = 42;

/** The prices of the order besides its id, in cents. */
const OTHER_PRICES_CENTS: readonly number[] = [250, 1_200];

const Prices = h.array(h.int());

const loadPrices = defineTask({ name: "load_prices", input: { orderId: h.int() }, output: Prices });

/** A node's dependency result arrives as an outcome, bound by the parameter's name. */
const sumPrices = defineTask({
  name: "sum_prices",
  input: { prices: h.taskOutcome(loadPrices) },
  output: h.int(),
  errors: ["UPSTREAM_FAILED"],
});

const countItems = defineTask({
  name: "count_items",
  input: { prices: h.taskOutcome(loadPrices) },
  output: h.int(),
  errors: ["UPSTREAM_FAILED"],
});

const summarize = defineTask({
  name: "summarize",
  input: { total: h.taskOutcome(sumPrices), count: h.taskOutcome(countItems) },
  output: h.string(),
  errors: ["UPSTREAM_FAILED"],
});

/** The workflow: its nodes, and the node whose result is the workflow's. It has no params, so no check cases. */
const orderSummary = defineWorkflow(
  { name: "order_summary", definitionKey: "examples.order_summary.v1", params: {}, output: h.string() },
  (w) => {
    const load = w.task(loadPrices, { orderId: ORDER_ID }, { id: "load_prices" });
    const total = w.task(sumPrices, { prices: load }, { id: "sum_prices" });
    const count = w.task(countItems, { prices: load }, { id: "count_items" });
    const summary = w.task(summarize, { total, count }, { id: "summarize" });

    return { output: summary };
  },
);

const created = Horsies.create({
  broker: { databaseUrl: DATABASE_URL },
  workerProfiles: { [DEFAULT_PROFILE]: ISOLATED_PROFILE },
  tasks: [loadPrices, sumPrices, countItems, summarize],
  workflows: [orderSummary],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

/** The message of the error that a node gives when a dependency did not succeed. Pure. */
function upstreamMessage(error: TaskError): string {
  return `a dependency failed: ${failureText(error)}`;
}

/** The value of a dependency outcome, or the error to return when it failed. Pure. */
function succeededValue<T>(outcome: TaskOutcome<T>): Result<T, string> {
  switch (outcome.kind) {
    case "succeeded":
      return { ok: true, value: outcome.value };
    case "failed":
      return { ok: false, error: upstreamMessage(outcome.error) };
    default: {
      const unexpected: never = outcome;

      throw new TypeError(`succeededValue: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

app.implement(loadPrices, async ({ orderId }, { ok }) => ok([orderId, ...OTHER_PRICES_CENTS]));

app.implement(sumPrices, async ({ prices }, { ok, err }) => {
  const loaded = succeededValue(prices);

  return loaded.ok
    ? ok(loaded.value.reduce((sum, price) => sum + price, 0))
    : err({ code: "UPSTREAM_FAILED", message: loaded.error });
});

app.implement(countItems, async ({ prices }, { ok, err }) => {
  const loaded = succeededValue(prices);

  return loaded.ok ? ok(loaded.value.length) : err({ code: "UPSTREAM_FAILED", message: loaded.error });
});

app.implement(summarize, async ({ total, count }, { ok, err }) => {
  const sum = succeededValue(total);
  const items = succeededValue(count);

  if (!sum.ok) {
    return err({ code: "UPSTREAM_FAILED", message: sum.error });
  }

  if (!items.ok) {
    return err({ code: "UPSTREAM_FAILED", message: items.error });
  }

  return ok(`${String(items.value)} items, ${String(sum.value)} cents`);
});

async function demo(): Promise<Result<void, string>> {
  const started = await app.start(orderSummary, {});

  if (!started.ok) {
    return demoFailed(`start order_summary: ${sendErrorText(started.error)}`);
  }

  printLine(`started workflow ${started.value.workflowId}`);

  const read = await started.value.get({ waitMs: LONG_RESULT_WAIT_MS });

  if (!read.ok) {
    return demoFailed(`read order_summary: ${workflowReadErrorText(read.error)}`);
  }

  printLine(outcomeLine("summary", read.value, (summary) => summary));

  return DEMO_DONE;
}

process.exitCode = await runExample(app, demo);
