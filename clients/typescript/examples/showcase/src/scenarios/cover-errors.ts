// `steady --cover-errors`: one run of each row of the failure table before the steady orders. Each row is a domain id
// whose draw gives the fault: a bundle pricing crash, a size code fault, a loyalty engine fault, a declined card, a
// short stock line, a provider outage that a retry outlives, and a courier flake that a retry outlives.

import type { Result } from "horsies";

import { applyPromotions, computeLoyaltyPoints } from "../contracts/promotions.ts";
import { err, ok } from "../result.ts";
import { draw } from "../simulate.ts";
import type { CatalogEntry } from "../store/catalog.ts";
import {
  COURIER_FLAKE_RATE,
  LOYALTY_ENGINE_BUG_RATE,
  PROMOTION_BUNDLE_BUG_RATE,
  PROMOTION_SIZE_CODE_BUG_RATE,
  PSP_UNAVAILABLE_RATE,
} from "../tuning.ts";
import { cardDeclines, reserveOrderId, stockFallsShort, storeOrder } from "./catalog.ts";
import { type ScenarioContext, sendErrorText, startOrder } from "./context.ts";

/** The most ids that one search for a drawn fault tries. */
const DRAW_SEARCH_BOUND = 100_000;

/** The digits of the number of a searched id. */
const DRAW_ID_DIGITS = 5;

/** The order id of the loyalty row: the fault is a draw over the customer id. */
const LOYALTY_ORDER_ID = "S4-LOYALTY";

/** The first id `S4-<label>-<n>` whose draw of `rate` over `[id, label]` hits and that `accepted` takes. Pure. */
function idForDraw(rate: number, label: string, accepted: (id: string) => boolean): Result<string, string> {
  for (let index = 0; index < DRAW_SEARCH_BOUND; index += 1) {
    const id = `S4-${label}-${String(index).padStart(DRAW_ID_DIGITS, "0")}`;

    if (draw(rate, [id, label]) && accepted(id)) {
      return ok(id);
    }
  }

  return err(`could not find a ${label} identity within ${String(DRAW_SEARCH_BOUND)} ids`);
}

/** Send the three rows of the promotion story. */
async function sendPromotionRows(context: ScenarioContext): Promise<Result<void, string>> {
  const app = context.showcase.app;
  const bundle = idForDraw(PROMOTION_BUNDLE_BUG_RATE, "bundle-bug", () => true);
  const size = idForDraw(PROMOTION_SIZE_CODE_BUG_RATE, "size-code", (id) => !draw(PROMOTION_BUNDLE_BUG_RATE, [id, "bundle-bug"]));
  const loyalty = idForDraw(LOYALTY_ENGINE_BUG_RATE, "lifetime-bug", () => true);

  for (const orderId of [bundle, size]) {
    if (!orderId.ok) {
      return orderId;
    }

    const sent = await app.send(applyPromotions, { orderId: orderId.value });

    if (!sent.ok) {
      return err(`send apply_promotions: ${sendErrorText(sent.error)}`);
    }
  }

  if (!loyalty.ok) {
    return loyalty;
  }

  const sent = await app.send(computeLoyaltyPoints, { customerId: loyalty.value, orderId: LOYALTY_ORDER_ID });

  return sent.ok ? ok(undefined) : err(`send compute_loyalty_points: ${sendErrorText(sent.error)}`);
}

/** The order rows: each one a predicate over the order id of the order sequence. */
const ORDER_ROWS: readonly { readonly label: string; readonly wanted: (orderId: string) => boolean }[] = [
  { label: "card-decline", wanted: (id) => cardDeclines(id) && !stockFallsShort(id) },
  { label: "stock-shortfall", wanted: stockFallsShort },
  {
    label: "PSP-unavailable",
    wanted: (id) => draw(PSP_UNAVAILABLE_RATE, [id, "psp"]) && !cardDeclines(id) && !stockFallsShort(id),
  },
  {
    label: "courier-flake",
    wanted: (id) => draw(COURIER_FLAKE_RATE, [id, "courier"]) && !cardDeclines(id) && !stockFallsShort(id),
  },
];

/** Run each row of the failure table once. */
export async function coverFailureTable(context: ScenarioContext, catalog: readonly CatalogEntry[]): Promise<Result<void, string>> {
  const promotions = await sendPromotionRows(context);

  if (!promotions.ok) {
    return promotions;
  }

  for (const row of ORDER_ROWS) {
    const search = await reserveOrderId(context.store, row.wanted);

    if (!search.ok) {
      return search;
    }

    if (search.value.kind === "notFound") {
      return err(`could not find a ${row.label} order id`);
    }

    const order = await storeOrder(context.store, search.value.orderId, catalog);
    const started = order.ok ? await startOrder(context, order.value) : order;

    if (!started.ok) {
      return err(`${row.label} workflow: ${started.error}`);
    }
  }

  return ok(undefined);
}

