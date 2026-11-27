// The smallest horsies app: one task, one worker profile, and a demo that sends the task and reads its result.
//
//   node examples/quick-start.ts worker --profile default
//   node examples/quick-start.ts demo

import { defineTask, h, Horsies, type Result } from "horsies";

import {
  DATABASE_URL,
  DEFAULT_PROFILE,
  DEMO_DONE,
  demoFailed,
  failureText,
  ISOLATED_PROFILE,
  printLine,
  readErrorText,
  refusalLines,
  RESULT_WAIT_MS,
  runExample,
} from "./support.ts";

/** Stored input: `{"name":"world"}`. */
const greet = defineTask({ name: "greet", input: { name: h.string() }, output: h.string() });

// Every process builds the same app: the demo, the worker and each child.
const created = Horsies.create({
  database: { databaseUrl: DATABASE_URL },
  workerProfiles: { [DEFAULT_PROFILE]: ISOLATED_PROFILE },
  tasks: [greet],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

app.implement(greet, ({ name }, { ok }) => ok(`hello, ${name}`));

async function demo(): Promise<Result<void, string>> {
  const sent = await app.send(greet, { name: "world" });

  if (!sent.ok) {
    return demoFailed(`send greet: ${sent.error.code}: ${sent.error.message}`);
  }

  const read = await sent.value.get({ waitMs: RESULT_WAIT_MS });

  if (!read.ok) {
    return demoFailed(`read greet: ${readErrorText(read.error)}`);
  }

  switch (read.value.kind) {
    case "succeeded":
      printLine(read.value.value);
      break;
    case "failed":
      printLine(`greet failed: ${failureText(read.value.error)}`);
      break;
    default: {
      const unexpected: never = read.value;

      throw new TypeError(`an outcome outside its type: ${String(unexpected)}`);
    }
  }

  return DEMO_DONE;
}

process.exitCode = await runExample(app, demo);
