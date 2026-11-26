// The flash sale: push the campaign price to the CDN and to the origin, build the search index, and fill the edge
// cache when one of the two pushes completed (`any` join). Its result is the cache warm-up.
//
// The workflow succeeds when the CDN push or the origin push completed; the search index may fail.

import { defineWorkflow } from "horsies";

import { prewarmSearch, publishCdn, publishOrigin, warmCacheEdge } from "../contracts/promotions.ts";
import { CacheWarm } from "../domain.ts";

/** The campaign that the workflow publishes. */
export const FLASH_CAMPAIGN_ID = "FLASH-CHECK";

/** The SKU that the workflow publishes. */
export const FLASH_SKU = "ACME-SKU-0001";

export const flashSale = defineWorkflow(
  { name: "flash_sale", definitionKey: "acme.flash_sale.v1", params: {}, output: CacheWarm },
  (w) => {
    const cdn = w.task(publishCdn, { campaignId: FLASH_CAMPAIGN_ID, sku: FLASH_SKU }, { id: "publish_cdn" });
    const origin = w.task(publishOrigin, { campaignId: FLASH_CAMPAIGN_ID, sku: FLASH_SKU }, { id: "publish_origin" });
    const search = w.task(prewarmSearch, { campaignId: FLASH_CAMPAIGN_ID }, { id: "prewarm_search" });

    const warm = w.task(
      warmCacheEdge,
      { campaignId: FLASH_CAMPAIGN_ID },
      { id: "warm_cache_edge", waitsFor: [cdn, origin], join: { kind: "any" } },
    );

    return {
      output: warm,
      successPolicy: { cases: [{ required: [cdn] }, { required: [origin] }], optional: [search] },
    };
  },
);
