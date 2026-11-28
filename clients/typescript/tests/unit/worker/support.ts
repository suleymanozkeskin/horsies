// Apps for the tests of the worker run and the check: built with the fakes of the app hub, so no addon loads.

import assert from "node:assert/strict";

import { createHorsies, type Horsies } from "../../../horsies/src/app.ts";
import type { AppContext } from "../../../horsies/src/app/context.ts";
import type { HorsiesConfig } from "../../../horsies/src/app/create.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { DATABASE_URL, fakeEnvironment } from "../app/support.ts";

/** An isolated task with open codes. */
export const ping = defineTask({ name: "ping", input: { n: h.int() }, output: h.int() });

/** A shared async task that declares its codes. */
export const charge = defineTask({
  name: "charge",
  input: { amount: h.int() },
  output: h.null(),
  errorCodes: ["CARD_DECLINED", "BANK_DOWN"],
  execution: "SHARED_ASYNC",
  timeoutMs: 5_000,
  cancelGraceMs: 1_000,
});

/** A task that the app does not list. */
export const unlisted = defineTask({ name: "unlisted", input: {}, output: h.null() });

/** The settings of a test app that the core's config does not hold. */
export type ProcessSettings = Pick<HorsiesConfig, "schemaMode" | "logFormat">;

/** The app of `ping` and `charge` with `settings`, built with the fakes; a failed create fails the test. */
export function testApp(settings: ProcessSettings = {}): Horsies {
  const created = createHorsies({ database: { databaseUrl: DATABASE_URL }, tasks: [ping, charge], ...settings }, fakeEnvironment());

  if (!created.ok) {
    assert.fail(`the test app was refused: ${created.error.refusals.map((refused) => refused.message).join("; ")}`);
  }

  return created.value;
}

/** The app of `testApp` with both tasks implemented. */
export function implementedApp(settings: ProcessSettings = {}): Horsies {
  const app = testApp(settings);

  app.implement(ping, ({ n }, { ok }) => ok(n));
  app.implement(charge, async (_input, { ok }) => ok(null));

  return app;
}

/** The context of `app`. */
export function contextOf(app: Horsies): AppContext {
  return appInternals(app);
}
