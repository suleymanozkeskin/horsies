// `simulate`: print deterministic samples of the simulation, with no database.

import type { Result } from "horsies";

import { ok } from "../result.ts";
import { demandFactor } from "../simulate.ts";
import { CATALOG_SIZE, RUSH_ORDER_COUNT } from "../tuning.ts";
import { say } from "./output.ts";

/** The digits after the point of the printed demand factor. */
const DEMAND_DIGITS = 3;

/** The instant of the printed demand factor: the Unix epoch. */
const SAMPLE_INSTANT = Temporal.Instant.fromEpochMilliseconds(0);

export async function runSimulate(): Promise<Result<void, string>> {
  say(
    `demand factor: ${demandFactor(SAMPLE_INSTANT).toFixed(DEMAND_DIGITS)}; ` +
      `catalog=${String(CATALOG_SIZE)} products; rush=${String(RUSH_ORDER_COUNT)} orders`,
  );

  return ok(undefined);
}
