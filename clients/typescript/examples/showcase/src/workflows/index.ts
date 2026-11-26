// Every workflow contract of the app, in one list: the `workflows` of `Horsies.create`.

import { catalogImport } from "./catalog-import.ts";
import { customerWinback } from "./customer-winback.ts";
import { dailyReport } from "./daily-report.ts";
import { flashSale } from "./flash-sale.ts";
import { fraudReview } from "./fraud-review.ts";
import { orderFulfillment } from "./order-fulfillment.ts";
import { priceSync } from "./price-sync.ts";
import { restock } from "./restock.ts";
import { returnsReview } from "./returns-review.ts";
import { seasonalMarkdown } from "./seasonal-markdown.ts";
import { shipping } from "./shipping.ts";
import { warehouseTransfer } from "./warehouse-transfer.ts";

export const ALL_WORKFLOWS = [
  orderFulfillment,
  shipping,
  returnsReview,
  catalogImport,
  customerWinback,
  dailyReport,
  flashSale,
  fraudReview,
  priceSync,
  restock,
  seasonalMarkdown,
  warehouseTransfer,
] as const;

/** The name of every workflow of the app. */
export const ALL_WORKFLOW_NAMES: readonly string[] = ALL_WORKFLOWS.map((workflow) => workflow.name);
