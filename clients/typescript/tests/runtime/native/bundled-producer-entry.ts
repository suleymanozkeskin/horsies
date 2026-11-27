// The producer that `bundled-producer.test.ts` builds with `bun build` into one file and runs: one contract with zod
// schemas, one send and the task info. It writes one JSON line with the outcome on stdout.

import { z } from "zod";

import { defineTask, Horsies } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { testDatabaseUrl } from "./manifest.ts";

// The bundle runs in a process of its own: its addon is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

const placeOrder = defineTask({
  name: "bundled_producer_place_order",
  input: { order: z.object({ id: z.int(), lines: z.array(z.string()).min(1) }), note: z.string().optional() },
  output: z.object({ total: z.number() }),
});

/** The outcome of the producer, as one JSON line: the task info, or the step that failed. */
async function outcome(): Promise<string> {
  const created = Horsies.create({
    database: { databaseUrl: { kind: "value", value: testDatabaseUrl() } },
    tasks: [placeOrder],
  });

  if (!created.ok) {
    return JSON.stringify({ outcome: "refused", messages: created.error.refusals.map((refusal) => refusal.message) });
  }

  const sent = await created.value.send(placeOrder, { order: { id: 1, lines: ["sku-1"] } });

  if (!sent.ok) {
    return JSON.stringify({ outcome: "sendFailed", code: sent.error.code });
  }

  const info = await sent.value.info();

  return info.ok
    ? JSON.stringify({ outcome: "read", taskName: info.value.taskName, status: info.value.status })
    : JSON.stringify({ outcome: "infoFailed", kind: info.error.kind });
}

process.stdout.write(`${await outcome()}\n`);
