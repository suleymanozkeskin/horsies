// `problem-child`: the business failures of the demo. Orders whose card declines, one order whose first line takes a
// discontinued SKU, and a return of each line of the newest captured orders.

import type { Result } from "horsies";

import { err, ok } from "../result.ts";
import { storeFailureText } from "../store/connection.ts";
import type { CatalogEntry } from "../store/catalog.ts";
import { PROBLEM_CHILD_DECLINES, PROBLEM_CHILD_RETURNS } from "../tuning.ts";
import { cardDeclines, readCatalog, reserveOrderId, stockFallsShort, storeOrder } from "./catalog.ts";
import { type ScenarioContext, startOrder, withScenario } from "./context.ts";
import { bullet, heading } from "./output.ts";
import { listReturnableLines } from "../store/orders.ts";
import { startReturn } from "./returns.ts";

/** Find an order id whose draws `wanted` accepts, write its order, and start its workflow: the order id. */
async function placeDrawnOrder(
  context: ScenarioContext,
  catalog: readonly CatalogEntry[],
  label: string,
  wanted: (orderId: string) => boolean,
): Promise<Result<string, string>> {
  const search = await reserveOrderId(context.store, wanted);

  if (!search.ok) {
    return search;
  }

  switch (search.value.kind) {
    case "notFound":
      return err(`no ${label} order id within the search bound`);
    case "found": {
      const order = await storeOrder(context.store, search.value.orderId, catalog);
      const started = order.ok ? await startOrder(context, order.value) : order;

      return started.ok ? ok(search.value.orderId) : started;
    }

    default: {
      const unexpected: never = search.value;

      throw new TypeError(`placeDrawnOrder: a search outside its type: ${String(unexpected)}`);
    }
  }
}

async function startReturns(context: ScenarioContext): Promise<Result<void, string>> {
  const lines = await listReturnableLines(context.store, PROBLEM_CHILD_RETURNS);

  if (!lines.ok) {
    return err(storeFailureText(lines.error));
  }

  if (lines.value.length === 0) {
    bullet("no captured orders available for returns; run steady first");
  }

  for (const line of lines.value) {
    const started = await startReturn(context, line.orderId, line.sku, line.quantity);

    if (!started.ok) {
      return started;
    }

    bullet(`return opened: ${started.value.returnId} for ${line.orderId}`);
  }

  return ok(undefined);
}

export async function runProblemChild(): Promise<Result<void, string>> {
  heading("Acme Clothing — problem child");

  return withScenario(async (context) => {
    const catalog = await readCatalog(context.store);

    if (!catalog.ok) {
      return catalog;
    }

    for (let index = 0; index < PROBLEM_CHILD_DECLINES; index += 1) {
      const declined = await placeDrawnOrder(context, catalog.value, "card-decline", cardDeclines);

      if (!declined.ok) {
        return declined;
      }

      bullet(`declined order ${String(index + 1)}/${String(PROBLEM_CHILD_DECLINES)}: ${declined.value}`);
    }

    const short = await placeDrawnOrder(context, catalog.value, "stock-shortfall", stockFallsShort);

    if (!short.ok) {
      return short;
    }

    bullet(`stock-shortfall order: ${short.value}`);

    return startReturns(context);
  });
}
