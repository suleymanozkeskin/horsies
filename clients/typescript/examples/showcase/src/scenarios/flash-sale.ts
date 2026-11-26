// `flash-sale`: start two flash sale workflows, then send expiring price updates: each one expires if no worker starts
// it within `PRICE_GOOD_UNTIL_SECONDS`.

import type { Result } from "horsies";

import { updatePrice } from "../contracts/promotions.ts";
import { err, ok } from "../result.ts";
import { EXPIRING_PRICE_SENDS, PRICE_GOOD_UNTIL_SECONDS } from "../tuning.ts";
import { flashSale } from "../workflows/flash-sale.ts";
import { readCatalog } from "./catalog.ts";
import { type ScenarioContext, sendErrorText, withScenario } from "./context.ts";
import { bullet, heading, say } from "./output.ts";

/** The campaign of the expiring price updates. */
const PRICE_CAMPAIGN = "flash-sale";

/** The labels of the two flash sale workflows. */
const FLASH_CAMPAIGNS: readonly string[] = ["flash-a", "flash-b"];

async function startCampaigns(context: ScenarioContext): Promise<Result<void, string>> {
  for (const campaign of FLASH_CAMPAIGNS) {
    const started = await context.showcase.app.start(flashSale, {});

    if (!started.ok) {
      return err(`start flash_sale: ${sendErrorText(started.error)}`);
    }

    bullet(`${campaign}: ${started.value.workflowId}`);
  }

  return ok(undefined);
}

export async function runFlashSale(): Promise<Result<void, string>> {
  heading("Acme Clothing — flash sale");

  return withScenario(async (context) => {
    const catalog = await readCatalog(context.store);

    if (!catalog.ok) {
      return catalog;
    }

    const campaigns = await startCampaigns(context);

    if (!campaigns.ok) {
      return campaigns;
    }

    const goodUntil = Temporal.Now.instant().add({ seconds: PRICE_GOOD_UNTIL_SECONDS });

    for (let index = 0; index < EXPIRING_PRICE_SENDS; index += 1) {
      const entry = catalog.value[index % catalog.value.length];

      if (entry === undefined) {
        return err("the catalog is empty; run the seed scenario first");
      }

      const sent = await context.showcase.app.send(updatePrice, { sku: entry.product.sku, campaignId: PRICE_CAMPAIGN }, { goodUntil });

      if (!sent.ok) {
        return err(`send update_price: ${sendErrorText(sent.error)}`);
      }
    }

    say(`submitted ${String(EXPIRING_PRICE_SENDS)} expiring price updates`);

    return ok(undefined);
  });
}
