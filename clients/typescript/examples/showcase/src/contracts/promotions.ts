// The contracts of the promotion, loyalty and price tasks.
//
// The promotion story has three error paths: a thrown error that no map names (`UNHANDLED_EXCEPTION`), a thrown error
// that the implementation's exception map stores as `DATA_CORRUPTION`, and a caught engine fault that the body
// returns as `LOYALTY_ENGINE_BUG`.

import { defineTask, h } from "horsies";

import { CacheWarm, LoyaltyPoints, PricePush, PriceUpdate, PromotionOutcome, SearchPrewarm } from "../domain.ts";
import { SHARED_TASK_TIMEOUT_MS } from "../tuning.ts";
import { CRASH_RETRY, QUEUE_ANALYTICS, QUEUE_FULFILLMENT } from "./policies.ts";

/** Price the promotions of the order. */
export const applyPromotions = defineTask({
  name: "apply_promotions",
  queue: QUEUE_FULFILLMENT,
  input: { orderId: h.string() },
  output: PromotionOutcome,
  errorCodes: ["ORDER_NOT_FOUND", "DATA_CORRUPTION", "STORE_UNAVAILABLE"],
  retryPolicy: CRASH_RETRY,
});

/** The loyalty points of one order of a customer. */
export const computeLoyaltyPoints = defineTask({
  name: "compute_loyalty_points",
  queue: QUEUE_ANALYTICS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { customerId: h.string(), orderId: h.string() },
  output: LoyaltyPoints,
  errorCodes: ["LOYALTY_ENGINE_BUG"],
  retryPolicy: CRASH_RETRY,
});

/** Push the campaign price of a SKU to the CDN. */
export const publishCdn = defineTask({
  name: "publish_cdn",
  queue: QUEUE_FULFILLMENT,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { campaignId: h.string(), sku: h.string() },
  output: PricePush,
  errorCodes: [],
  retryPolicy: CRASH_RETRY,
});

/** Push the campaign price of a SKU to the origin servers. */
export const publishOrigin = defineTask({
  name: "publish_origin",
  queue: QUEUE_FULFILLMENT,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { campaignId: h.string(), sku: h.string() },
  output: PricePush,
  errorCodes: [],
  retryPolicy: CRASH_RETRY,
});

/** Build the search index of a campaign before its start. */
export const prewarmSearch = defineTask({
  name: "prewarm_search",
  queue: QUEUE_ANALYTICS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { campaignId: h.string() },
  output: SearchPrewarm,
  errorCodes: [],
  retryPolicy: CRASH_RETRY,
});

/** Fill the edge cache of a campaign. */
export const warmCacheEdge = defineTask({
  name: "warm_cache_edge",
  queue: QUEUE_FULFILLMENT,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { campaignId: h.string() },
  output: CacheWarm,
  errorCodes: [],
  retryPolicy: CRASH_RETRY,
});

/** Change the price of a SKU for a campaign. A flash sale sends it with a short `goodUntil`. */
export const updatePrice = defineTask({
  name: "update_price",
  queue: QUEUE_ANALYTICS,
  execution: "SHARED_ASYNC",
  timeoutMs: SHARED_TASK_TIMEOUT_MS,
  input: { sku: h.string(), campaignId: h.string() },
  output: PriceUpdate,
  errorCodes: [],
  retryPolicy: CRASH_RETRY,
});

export const PROMOTION_TASKS = [
  applyPromotions,
  computeLoyaltyPoints,
  publishCdn,
  publishOrigin,
  prewarmSearch,
  warmCacheEdge,
  updatePrice,
] as const;
