// The seasonal markdown: change the price of each SKU of the campaign side by side, then roll the sales up when every
// change completed. Its result is the rollup.

import { defineWorkflow } from "horsies";

import { salesRollup } from "../contracts/analytics.ts";
import { updatePrice } from "../contracts/promotions.ts";
import { SalesRollup } from "../domain.ts";

/** The campaign that the workflow marks down. */
export const MARKDOWN_CAMPAIGN_ID = "MARKDOWN-CHECK";

/** The SKUs that the workflow marks down: `ACME-SKU-0001` to `ACME-SKU-0006`. */
export const MARKDOWN_SKU_COUNT = 6;

export const seasonalMarkdown = defineWorkflow(
  { name: "seasonal_markdown", definitionKey: "acme.seasonal_markdown.v1", params: {}, output: SalesRollup },
  (w) => {
    const markdowns = Array.from({ length: MARKDOWN_SKU_COUNT }, (_unused, index) =>
      w.task(
        updatePrice,
        { sku: `ACME-SKU-${String(index + 1).padStart(4, "0")}`, campaignId: MARKDOWN_CAMPAIGN_ID },
        { id: `update_price_${String(index).padStart(2, "0")}` },
      ),
    );

    const rollup = w.task(
      salesRollup,
      { window: `markdown-${MARKDOWN_CAMPAIGN_ID}` },
      { id: "sales_rollup", waitsFor: markdowns },
    );

    return { output: rollup };
  },
);
