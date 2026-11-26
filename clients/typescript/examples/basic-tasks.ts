// Async and sync task bodies, a task with codes of its own, and the reads of their outcomes.
//
//   node examples/basic-tasks.ts worker --profile default
//   node examples/basic-tasks.ts demo

import { defineTask, h, Horsies, type Result } from "horsies";

import {
  DATABASE_URL,
  DEFAULT_PROFILE,
  DEMO_DONE,
  demoFailed,
  failureCodeText,
  ISOLATED_PROFILE,
  printLine,
  readLine,
  refusalLines,
  RESULT_WAIT_MS,
  runExample,
  sendErrorText,
} from "./support.ts";

/** The largest charge that `charge` accepts, in cents. */
const CHARGE_LIMIT_CENTS = 100_000;

/** An async body. Stored input: `{"a":2,"b":3}`. */
const add = defineTask({ name: "add", input: { a: h.int(), b: h.int() }, output: h.int() });

/** A sync body: an `ISOLATED` task may hold its child's thread, because it runs one task at a time. */
const wordCount = defineTask({ name: "word_count", input: { text: h.string() }, output: h.int() });

/** A task that fails with a code of its own and data for the reader. */
const charge = defineTask({
  name: "charge",
  input: { amountCents: h.int() },
  output: h.string(),
  errors: ["OVER_LIMIT"],
});

const created = Horsies.create({
  broker: { databaseUrl: DATABASE_URL },
  workerProfiles: { [DEFAULT_PROFILE]: ISOLATED_PROFILE },
  tasks: [add, wordCount, charge],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

app.implement(add, async ({ a, b }, { ok }) => ok(a + b));

app.implement(wordCount, ({ text }, { ok }) => ok(text.split(/\s+/u).filter((word) => word !== "").length));

app.implement(charge, async ({ amountCents }, { ok, err }) =>
  amountCents > CHARGE_LIMIT_CENTS
    ? err({ code: "OVER_LIMIT", message: "the amount is over the limit", data: { limitCents: CHARGE_LIMIT_CENTS } })
    : ok(`charged ${String(amountCents)} cents`),
);

/** The value of a read as JSON text, as the Rust example prints it. Pure. */
function jsonText(value: string | number): string {
  return JSON.stringify(value);
}

async function demo(): Promise<Result<void, string>> {
  const sum = await app.send(add, { a: 2, b: 3 });
  const words = await app.send(wordCount, { text: "the quick brown fox" });
  const small = await app.send(charge, { amountCents: 500 });
  const large = await app.send(charge, { amountCents: 500_000 });

  if (!sum.ok || !words.ok || !small.ok || !large.ok) {
    const failed = [sum, words, small, large].flatMap((sent) => (sent.ok ? [] : [sendErrorText(sent.error)]));

    return demoFailed(`a send failed: ${failed.join("; ")}`);
  }

  printLine(readLine("add", await sum.value.get({ timeoutMs: RESULT_WAIT_MS }), jsonText));
  printLine(readLine("word_count", await words.value.get({ timeoutMs: RESULT_WAIT_MS }), jsonText));
  printLine(readLine("charge 500", await small.value.get({ timeoutMs: RESULT_WAIT_MS }), jsonText));

  const refused = await large.value.get({ timeoutMs: RESULT_WAIT_MS });

  if (refused.ok && refused.value.kind === "failed") {
    const error = refused.value.error;

    printLine(`charge 500000: code ${failureCodeText(error)}, data ${JSON.stringify(error.data)}`);
  }

  printLine(readLine("charge 500000", refused, jsonText));

  return DEMO_DONE;
}

process.exitCode = await runExample(app, demo);
