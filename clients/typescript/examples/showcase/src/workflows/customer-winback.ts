// The win-back: sweep the abandoned carts, then send the win-back message to the customers that the sweep found. Its
// result is the send.

import { defineWorkflow } from "horsies";

import { abandonedCartSweep } from "../contracts/analytics.ts";
import { winbackBlast } from "../contracts/notify.ts";
import { MarketingBlast } from "../domain.ts";
import { ABANDONED_CART_AGE_MINUTES } from "../tuning.ts";

/** The segment that the workflow sends to. */
export const WINBACK_SEGMENT = "CHECK-SEGMENT";

export const customerWinback = defineWorkflow(
  { name: "customer_winback", definitionKey: "acme.customer_winback.v1", params: {}, output: MarketingBlast },
  (w) => {
    const sweep = w.task(abandonedCartSweep, { olderThanMinutes: ABANDONED_CART_AGE_MINUTES }, { nodeId: "abandoned_cart_sweep" });
    const blast = w.task(winbackBlast, { segment: WINBACK_SEGMENT, sweep }, { nodeId: "winback_blast" });

    return { output: blast };
  },
);
