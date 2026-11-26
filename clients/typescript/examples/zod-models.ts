// Contracts whose input and output are zod 4 schemas. The `hz` codecs of `horsies/zod` give the stored forms that
// zod has no type for: an instant (with microseconds) and an exact decimal. A send checks the input with the schema
// before it writes, so an input that the schema refuses is never stored.
//
// The adapter refuses, at `Horsies.create`, a zod form that has no stored form, with a named refusal. For example
// `placedAt: z.date()` is refused (a `Date` cannot hold microseconds; use `hz.instant()`), and so are `z.any()`,
// `z.unknown()` (use `h.json()`) and `.transform(...)` (use a codec).
//
//   node examples/zod-models.ts worker --profile default
//   node examples/zod-models.ts demo

import { Decimal, defineTask, Horsies, type Result, type SchemaSent } from "horsies";
import { hz } from "horsies/zod";
import { z } from "zod";

import {
  DATABASE_URL,
  DEFAULT_PROFILE,
  DEMO_DONE,
  demoFailed,
  ISOLATED_PROFILE,
  printLine,
  readLine,
  refusalLines,
  RESULT_WAIT_MS,
  runExample,
  sendErrorText,
} from "./support.ts";

/** The cents of one unit of currency. */
const CENTS_PER_UNIT = 100n;

/** The digits of the cents part of a price text. */
const CENTS_DIGITS = 2;

const OrderItem = z.object({
  sku: z.string().max(64),
  // An integer in the safe JavaScript range.
  quantity: z.int().min(1),
  // An integer of any size, exact.
  unitCents: z.bigint(),
});

const Order = z.object({
  orderId: z.uuid(),
  customerEmail: z.email(),
  items: z.array(OrderItem).min(1),
  placedAt: hz.instant(),
  // Left out means "no note"; it is never confused with `null`.
  note: z.string().optional(),
});

const PricedOrder = z.object({
  orderId: z.uuid(),
  itemCount: z.int(),
  // An exact decimal text such as "38.70", never a float.
  total: hz.decimal(),
  placedAt: hz.instant(),
});

const priceOrder = defineTask({ name: "price_order", input: { order: Order }, output: PricedOrder });

const created = Horsies.create({
  broker: { databaseUrl: DATABASE_URL },
  workerProfiles: { [DEFAULT_PROFILE]: ISOLATED_PROFILE },
  tasks: [priceOrder],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

/**
 * The decimal of `cents` (not negative), such as "38.70" for 3870. Throws `RangeError` for a text that `Decimal`
 * refuses: the text is built from an integer, so a refusal is a defect, and the worker stores it as
 * `UNHANDLED_EXCEPTION`.
 */
function decimalOfCents(cents: bigint): Decimal {
  const text = `${String(cents / CENTS_PER_UNIT)}.${String(cents % CENTS_PER_UNIT).padStart(CENTS_DIGITS, "0")}`;
  const parsed = Decimal.parse(text);

  if (!parsed.ok) {
    throw new RangeError(`decimalOfCents: ${text} is not a decimal: ${parsed.error.detail}`);
  }

  return parsed.value;
}

app.implement(priceOrder, async ({ order }, { ok }) => {
  const totalCents = order.items.reduce((sum, item) => sum + BigInt(item.quantity) * item.unitCents, 0n);

  return ok({
    orderId: order.orderId,
    itemCount: order.items.length,
    total: decimalOfCents(totalCents),
    placedAt: order.placedAt,
  });
});

/** The order of the demo, placed now, as a sender gives it: the codecs take their values, not their stored texts. */
function sampleOrder(customerEmail: string): SchemaSent<typeof Order> {
  return {
    orderId: crypto.randomUUID(),
    customerEmail,
    items: [
      { sku: "TEE-BONE-M", quantity: 2, unitCents: 1_290n },
      { sku: "JEAN-INK-32", quantity: 1, unitCents: 1_290n },
    ],
    placedAt: Temporal.Now.instant(),
  };
}

async function demo(): Promise<Result<void, string>> {
  const sent = await app.send(priceOrder, { order: sampleOrder("ada@example.com") });

  if (!sent.ok) {
    return demoFailed(`send price_order: ${sendErrorText(sent.error)}`);
  }

  const read = await sent.value.get({ timeoutMs: RESULT_WAIT_MS });

  printLine(readLine("price_order", read, (priced) => `${String(priced.itemCount)} items, total ${priced.total.toString()}`));

  // The schema refuses the e-mail text, so the send writes nothing.
  const refused = await app.send(priceOrder, { order: sampleOrder("not-an-email") });

  printLine(refused.ok ? "price_order: the send was accepted" : `price_order refused at send: ${refused.error.code}`);

  return DEMO_DONE;
}

process.exitCode = await runExample(app, demo);
