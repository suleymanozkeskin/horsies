// The implementations of the promotion, loyalty and price tasks, with the three error paths of the promotion story:
//
// 1. The bundle pricing of a drawn order divides by zero earning units. The pricing throws a `RangeError`, which no
//    exception map names: the worker stores `UNHANDLED_EXCEPTION`, with the error's name, message and stack.
// 2. The size table of a drawn order meets a corrupt size code and throws `SizeCodeError`. The implementation's
//    exception map stores that class as `DATA_CORRUPTION`. Before the throw goes on, the body fails the order and
//    releases its reservations.
// 3. The loyalty engine of a drawn customer throws `LoyaltyEngineFault`. The body catches that class and returns
//    `LOYALTY_ENGINE_BUG`.

import type { BodyResult, Horsies } from "horsies";

import {
  applyPromotions,
  computeLoyaltyPoints,
  prewarmSearch,
  publishCdn,
  publishOrigin,
  updatePrice,
  warmCacheEdge,
} from "../contracts/promotions.ts";
import { type CacheWarm, LoyaltyPoints, type OrderLine, type PricePush, type PriceUpdate, PromotionOutcome, type SearchPrewarm } from "../domain.ts";
import { draw, integer } from "../simulate.ts";
import type { Store } from "../store/connection.ts";
import { failOrderAndReleaseReservations, getOrder } from "../store/orders.ts";
import {
  BUNDLE_MIN_QUANTITY,
  BUNDLE_POT_DIVISOR,
  BUNDLE_PRICE_FLOOR_CENTS,
  CLEARANCE_PRICE_CENTS,
  CORRUPT_SIZE_CODE,
  FLASH_SALE_DISCOUNT_PERCENT,
  LOYALTY_ENGINE_BUG_RATE,
  LOYALTY_LIFETIME_MAX,
  LOYALTY_POINTS_PER_EURO,
  MAX_PRICE_CENTS,
  MIN_PRICE_CENTS,
  PROMOTION_BUNDLE_BUG_RATE,
  PROMOTION_CODES,
  PROMOTION_SIZE_CODE_BUG_RATE,
} from "../tuning.ts";
import { type ResultsOf, storeFailure, type StoreKey } from "./support.ts";

/** The result of the promotion body. */
export type PromotionResult = BodyResult<typeof PromotionOutcome, "ORDER_NOT_FOUND" | "DATA_CORRUPTION" | "STORE_UNAVAILABLE">;

/** The result of the loyalty body. */
export type LoyaltyResult = BodyResult<typeof LoyaltyPoints, "LOYALTY_ENGINE_BUG">;

/** The price multiplier of each size code, in percent. */
const SIZE_MULTIPLIER_PERCENT: ReadonlyMap<string, number> = new Map([
  ["XS", 90],
  ["S", 95],
  ["M", 100],
  ["L", 105],
  ["XL", 110],
]);

/** The size code that the promotion prices when its order draws no size fault. */
export const PROMOTION_SIZE_CODE = "M";

/** 100 percent. */
const WHOLE_PERCENT = 100;

/** The loyalty tier of every customer of the demo. */
export const LOYALTY_TIER = "standard";

/** The message of the loyalty engine fault. */
export const LOYALTY_FAULT_MESSAGE = "loyalty tier row is a bare label";

/** A size code that the size table does not hold. The exception map of `apply_promotions` stores it as `DATA_CORRUPTION`. */
export class SizeCodeError extends Error {
  readonly sizeCode: string;

  constructor(sizeCode: string) {
    super(`unknown promotion size code ${sizeCode}`);
    this.name = "SizeCodeError";
    this.sizeCode = sizeCode;
  }
}

/** A fault of the loyalty engine. `compute_loyalty_points` catches it and returns `LOYALTY_ENGINE_BUG`. */
export class LoyaltyEngineFault extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LoyaltyEngineFault";
  }
}

/** The price multiplier of `sizeCode`, in percent. A code outside the table throws `SizeCodeError`, as a library does. */
export function sizeMultiplierPercent(sizeCode: string): number {
  const percent = SIZE_MULTIPLIER_PERCENT.get(sizeCode);

  if (percent === undefined) {
    throw new SizeCodeError(sizeCode);
  }

  return percent;
}

/**
 * The bundle discount of `lines`: a tenth of the total of the bundled lines (a quantity of at least
 * `BUNDLE_MIN_QUANTITY`), shared by their units at or above the price floor. Bundled lines that are all below the floor
 * have no earning units: the division by zero is the demo's pricing bug, and it throws `RangeError`. Pure.
 */
export function bundleDiscountCents(lines: readonly OrderLine[]): number {
  const bundled = lines.filter((line) => line.quantity >= BUNDLE_MIN_QUANTITY);

  if (bundled.length === 0) {
    return 0;
  }

  const potCents = Math.floor(bundled.reduce((sum, line) => sum + line.quantity * line.unitPriceCents, 0) / BUNDLE_POT_DIVISOR);

  const earningUnits = bundled
    .filter((line) => line.unitPriceCents >= BUNDLE_PRICE_FLOOR_CENTS)
    .reduce((sum, line) => sum + line.quantity, 0);

  if (earningUnits === 0) {
    throw new RangeError(`bundle pricing: a pot of ${String(potCents)} cents is divided by 0 earning units`);
  }

  return Math.floor(potCents / earningUnits);
}

/** Price a clearance bundle for an order that the bug rate draws: the pricing throws `RangeError` for it. Pure. */
export function priceDrawnClearanceBundle(orderId: string): void {
  if (!draw(PROMOTION_BUNDLE_BUG_RATE, [orderId, "bundle-bug"])) {
    return;
  }

  const clearance: OrderLine = {
    lineNo: 1,
    sku: "clearance-bundle",
    sizeCode: PROMOTION_SIZE_CODE,
    quantity: BUNDLE_MIN_QUANTITY,
    unitPriceCents: CLEARANCE_PRICE_CENTS,
  };

  void bundleDiscountCents([clearance]);
}

/** The size code that the promotion of `orderId` prices: the corrupt code for an order that the fault rate draws. Pure. */
export function promotionSizeCode(orderId: string): string {
  return draw(PROMOTION_SIZE_CODE_BUG_RATE, [orderId, "size-code"]) ? CORRUPT_SIZE_CODE : PROMOTION_SIZE_CODE;
}

/**
 * The size multiplier of the promotion of `orderId`. For a corrupt size code, fail the order and release its
 * reservations, then throw the `SizeCodeError` on. A store failure of that step is written to stderr: the nightly
 * stocktake clears what stays reserved.
 */
async function promotionMultiplier(store: Store, orderId: string): Promise<number> {
  try {
    return sizeMultiplierPercent(promotionSizeCode(orderId));
  } catch (cause) {
    if (cause instanceof SizeCodeError) {
      const released = await failOrderAndReleaseReservations(store, orderId);

      if (!released.ok) {
        process.stderr.write(`apply_promotions: order ${orderId} was not compensated: ${released.error.cause}\n`);
      }
    }

    throw cause;
  }
}

/**
 * The promotions of `orderId`: the bundle discount of its lines times the size multiplier, and the code that applied.
 * Throws `RangeError` for an order that the bundle fault draws and `SizeCodeError` for one that the size fault draws.
 */
export async function applyPromotionsWith(
  store: Store,
  orderId: string,
  { ok, err }: ResultsOf<typeof applyPromotions>,
): Promise<PromotionResult> {
  priceDrawnClearanceBundle(orderId);

  const percent = await promotionMultiplier(store, orderId);
  const order = await getOrder(store, orderId);

  if (!order.ok) {
    return err({ code: "STORE_UNAVAILABLE", ...storeFailure(order.error) });
  }

  switch (order.value.kind) {
    case "notFound":
      return err({ code: "ORDER_NOT_FOUND", message: `no order ${orderId}` });
    case "found":
      return ok({
        orderId,
        discountCents: Math.floor((bundleDiscountCents(order.value.value.lines) * percent) / WHOLE_PERCENT),
        appliedCodes: [PROMOTION_CODES[0]],
      });
    default: {
      const unexpected: never = order.value;

      throw new TypeError(`applyPromotionsWith: a lookup outside its type: ${String(unexpected)}`);
    }
  }
}

/** The lifetime spend of `customerId` in euros. The engine throws `LoyaltyEngineFault` for a drawn customer. Pure. */
export function loyaltyLifetimeEuros(customerId: string): number {
  if (draw(LOYALTY_ENGINE_BUG_RATE, [customerId, "lifetime-bug"])) {
    throw new LoyaltyEngineFault(LOYALTY_FAULT_MESSAGE);
  }

  return integer(0, LOYALTY_LIFETIME_MAX, [customerId, "lifetime"]);
}

/** The loyalty points that `orderId` earns `customerId`. A fault of the engine is `LOYALTY_ENGINE_BUG`. Pure. */
export function loyaltyPoints(customerId: string, orderId: string, { ok, err }: ResultsOf<typeof computeLoyaltyPoints>): LoyaltyResult {
  let lifetimeEuros: number;

  try {
    lifetimeEuros = loyaltyLifetimeEuros(customerId);
  } catch (cause) {
    if (cause instanceof LoyaltyEngineFault) {
      return err({ code: "LOYALTY_ENGINE_BUG", message: cause.message });
    }

    throw cause;
  }

  return ok({ customerId, orderId, points: lifetimeEuros * LOYALTY_POINTS_PER_EURO, tier: LOYALTY_TIER });
}

/** The list price of `sku` in the range of the demo catalog. Pure. */
export function listPriceCents(sku: string): number {
  return integer(MIN_PRICE_CENTS, MAX_PRICE_CENTS, [sku, "price"]);
}

/** The price push of `sku` to `target`. Pure. */
export function pricePush(sku: string, target: string): PricePush {
  return { sku, priceCents: listPriceCents(sku), target };
}

/** The search index of `campaignId`. Pure. */
export function searchPrewarm(campaignId: string): SearchPrewarm {
  return { documents: integer(100, 5_000, [campaignId, "documents"]), indexName: `products-${campaignId}` };
}

/** The edge cache keys of `campaignId`. Pure. */
export function cacheWarm(campaignId: string): CacheWarm {
  return { target: campaignId, keysWarmed: integer(50, 2_000, [campaignId, "keys"]) };
}

/** The discount of `campaignId` in percent: the flash sale discount, or 0 for no campaign (an empty id). Pure. */
export function campaignDiscountPercent(campaignId: string): number {
  return campaignId === "" ? 0 : FLASH_SALE_DISCOUNT_PERCENT;
}

/** The new price of `sku` in the campaign `campaignId`. Pure. */
export function priceUpdate(sku: string, campaignId: string): PriceUpdate {
  const wasCents = listPriceCents(sku);

  return { sku, wasCents, nowCents: Math.floor((wasCents * (WHOLE_PERCENT - campaignDiscountPercent(campaignId))) / WHOLE_PERCENT) };
}

/** Implement the promotion, loyalty and price tasks on `app`; `apply_promotions` uses the store of `store`. */
export function implementPromotionTasks(app: Horsies, store: StoreKey): void {
  app.implement(applyPromotions, async ({ orderId }, runtime) => applyPromotionsWith(runtime.state(store), orderId, runtime), {
    exceptions: [[SizeCodeError, "DATA_CORRUPTION"]],
  });
  app.implement(computeLoyaltyPoints, async ({ customerId, orderId }, results) => loyaltyPoints(customerId, orderId, results));
  app.implement(publishCdn, async ({ campaignId, sku }, { ok }) => ok(pricePush(sku, `cdn:${campaignId}`)));
  app.implement(publishOrigin, async ({ campaignId, sku }, { ok }) => ok(pricePush(sku, `origin:${campaignId}`)));
  app.implement(prewarmSearch, async ({ campaignId }, { ok }) => ok(searchPrewarm(campaignId)));
  app.implement(warmCacheEdge, async ({ campaignId }, { ok }) => ok(cacheWarm(campaignId)));
  app.implement(updatePrice, async ({ sku, campaignId }, { ok }) => ok(priceUpdate(sku, campaignId)));
}
