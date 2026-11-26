// `rush`: place `RUSH_ORDER_COUNT` orders over `RUSH_WINDOW_SECONDS`, each with its order workflow and its promotion
// and loyalty tasks.

import { setTimeout as sleep } from "node:timers/promises";

import type { Result } from "horsies";

import { ok } from "../result.ts";
import { RUSH_ORDER_COUNT, RUSH_WINDOW_SECONDS } from "../tuning.ts";
import { nextOrder, readCatalog } from "./catalog.ts";
import { sendStandalone, startOrder, withScenario } from "./context.ts";
import { bullet, heading, say } from "./output.ts";

/** The pause between two orders of the rush, in milliseconds. */
const RUSH_SPACING_MS = (RUSH_WINDOW_SECONDS * 1_000) / RUSH_ORDER_COUNT;

export async function runRush(): Promise<Result<void, string>> {
  heading("Acme Clothing — rush");

  return withScenario(async (context) => {
    const catalog = await readCatalog(context.store);

    if (!catalog.ok) {
      return catalog;
    }

    bullet(`placing ${String(RUSH_ORDER_COUNT)} orders over ${String(RUSH_WINDOW_SECONDS)} seconds`);

    for (let placed = 0; placed < RUSH_ORDER_COUNT; placed += 1) {
      const order = await nextOrder(context.store, catalog.value);

      if (!order.ok) {
        return order;
      }

      const started = await startOrder(context, order.value);
      const standalone = started.ok ? await sendStandalone(context, order.value) : started;

      if (!standalone.ok) {
        return standalone;
      }

      await sleep(RUSH_SPACING_MS);
    }

    say("rush orders submitted");

    return ok(undefined);
  });
}
