// The daily report: the sales rollup, then the abandoned cart sweep, then the win-back send of the sweep. Its result
// is the sweep.

import { defineWorkflow } from "horsies";

import { abandonedCartSweep, salesRollup } from "../contracts/analytics.ts";
import { winbackBlast } from "../contracts/notify.ts";
import { AbandonedCartSweep } from "../domain.ts";
import { ABANDONED_CART_AGE_MINUTES } from "../tuning.ts";

/** The window that the workflow reports. */
export const REPORT_WINDOW = "CHECK";

export const dailyReport = defineWorkflow(
  { name: "daily_report", definitionKey: "acme.daily_report.v1", params: {}, output: AbandonedCartSweep },
  (w) => {
    const rollup = w.task(salesRollup, { window: REPORT_WINDOW }, { id: "sales_rollup" });

    const sweep = w.task(
      abandonedCartSweep,
      { olderThanMinutes: ABANDONED_CART_AGE_MINUTES },
      { id: "abandoned_cart_sweep", waitsFor: [rollup] },
    );

    w.task(winbackBlast, { segment: `winback-${REPORT_WINDOW}`, sweep }, { id: "winback_blast" });

    return { output: sweep };
  },
);
