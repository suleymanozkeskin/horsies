// Automatic retries: a `retryPolicy` with `retryFor`. A retry starts when the task ends with one of the listed
// codes: a custom code that the task declares, or a built-in code such as `TASK_TIMEOUT`.
//
//   node examples/retries.ts worker --profile default
//   node examples/retries.ts demo

import { defineTask, h, Horsies, type Result } from "horsies";

import {
  DATABASE_URL,
  DEFAULT_PROFILE,
  DEMO_DONE,
  demoFailed,
  ISOLATED_PROFILE,
  LONG_RESULT_WAIT_MS,
  printLine,
  readErrorText,
  readLine,
  refusalLines,
  runExample,
  sendErrorText,
} from "./support.ts";

/** The requests of the demo: one that the partner rate-limits, and an empty one that it rejects. */
const REQUESTS: readonly string[] = ["sync-orders", ""];

/**
 * Fails with `RATE_LIMITED` every time, so the demo sees each retry: one run and two retries, 1 s and 2 s apart, then
 * the final failure. `REJECTED` is not in `retryFor`, so an empty request fails at its first run.
 */
const callPartner = defineTask({
  name: "call_partner",
  input: { request: h.string() },
  output: h.string(),
  errorCodes: ["RATE_LIMITED", "REJECTED"],
  timeoutMs: 5_000,
  retryPolicy: {
    kind: "fixed",
    intervalsSeconds: [1, 2],
    jitter: false,
    retryFor: ["RATE_LIMITED", "TASK_TIMEOUT"],
  },
});

const created = Horsies.create({
  broker: { databaseUrl: DATABASE_URL },
  workerProfiles: { [DEFAULT_PROFILE]: ISOLATED_PROFILE },
  tasks: [callPartner],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

app.implement(callPartner, async ({ request }, { err }) =>
  request === ""
    ? err({ code: "REJECTED", message: "an empty request" })
    : err({ code: "RATE_LIMITED", message: `the partner is busy: ${request}` }),
);

/** Send `request`, read its outcome, then read how many retries ran. */
async function callOnce(request: string): Promise<Result<void, string>> {
  const label = JSON.stringify(request);
  const sent = await app.send(callPartner, { request });

  if (!sent.ok) {
    return demoFailed(`send call_partner ${label}: ${sendErrorText(sent.error)}`);
  }

  printLine(readLine(label, await sent.value.get({ waitMs: LONG_RESULT_WAIT_MS }), (answer) => answer));

  const info = await sent.value.info();

  if (!info.ok) {
    return demoFailed(`info of call_partner ${label}: ${readErrorText(info.error)}`);
  }

  printLine(`${label}: ${String(info.value.retryCount)} retries of ${String(info.value.maxRetries)} allowed`);

  return DEMO_DONE;
}

async function demo(): Promise<Result<void, string>> {
  for (const request of REQUESTS) {
    const ran = await callOnce(request);

    if (!ran.ok) {
      return ran;
    }
  }

  return DEMO_DONE;
}

process.exitCode = await runExample(app, demo);
