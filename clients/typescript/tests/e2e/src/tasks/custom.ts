// Tasks of the custom queue instance (Rust `clients/rust/tests/worker/src/tasks/custom.rs`, Python
// `clients/python/tests/e2e/tasks/queues_custom.py`). In the custom queue mode every task names a configured queue.

import { defineTask, h, type Horsies } from "#horsies";
import { holdThread } from "./waits.ts";

/** The readiness task of a worker of the custom queue instance. */
export const customHealthcheck = defineTask({ name: "e2e_custom_healthcheck", queue: "high", input: {}, output: h.string() });

export const high = defineTask({ name: "e2e_high", queue: "high", input: {}, output: h.string() });

export const normal = defineTask({ name: "e2e_normal", queue: "normal", input: {}, output: h.string() });

export const low = defineTask({ name: "e2e_low", queue: "low", input: {}, output: h.string() });

/** Holds its thread for `durationMs` on the `high` queue (cap 5). */
export const slowHigh = defineTask({ name: "e2e_slow_high", queue: "high", input: { durationMs: h.int() }, output: h.string() });

/** Holds its thread for `durationMs` on the `normal` queue (cap 10). */
export const slowNormal = defineTask({ name: "e2e_slow_normal", queue: "normal", input: { durationMs: h.int() }, output: h.string() });

/** Holds its thread for `durationMs` on the `low` queue (cap 20). */
export const slowLow = defineTask({ name: "e2e_slow_low", queue: "low", input: { durationMs: h.int() }, output: h.string() });

/** Every contract of this module. */
export const CUSTOM_CONTRACTS = [customHealthcheck, high, normal, low, slowHigh, slowNormal, slowLow] as const;

/** The implementations of this module on `app`. */
export function implementCustom(app: Horsies): void {
  app.implement(customHealthcheck, (_input, { ok }) => ok("ready"));
  app.implement(high, (_input, { ok }) => ok("high"));
  app.implement(normal, (_input, { ok }) => ok("normal"));
  app.implement(low, (_input, { ok }) => ok("low"));

  for (const contract of [slowHigh, slowNormal, slowLow]) {
    app.implement(contract, ({ durationMs }, { ok }) => {
      holdThread(durationMs);

      return ok(`slept_${String(durationMs)}`);
    });
  }
}
