// The price sync: push the price to the CDN and to the origin, then fill the edge cache when both completed. Its
// result is the cache warm-up.

import { defineWorkflow } from "horsies";

import { publishCdn, publishOrigin, warmCacheEdge } from "../contracts/promotions.ts";
import { CacheWarm } from "../domain.ts";

/** The campaign that the workflow publishes. */
export const SYNC_CAMPAIGN_ID = "SYNC-CHECK";

/** The SKU that the workflow publishes. */
export const SYNC_SKU = "ACME-SKU-0001";

export const priceSync = defineWorkflow(
  { name: "price_sync", definitionKey: "acme.price_sync.v1", params: {}, output: CacheWarm },
  (w) => {
    const cdn = w.task(publishCdn, { campaignId: SYNC_CAMPAIGN_ID, sku: SYNC_SKU }, { id: "publish_cdn" });
    const origin = w.task(publishOrigin, { campaignId: SYNC_CAMPAIGN_ID, sku: SYNC_SKU }, { id: "publish_origin" });
    const warm = w.task(warmCacheEdge, { campaignId: SYNC_CAMPAIGN_ID }, { id: "warm_cache_edge", waitsFor: [cdn, origin] });

    return { output: warm };
  },
);
