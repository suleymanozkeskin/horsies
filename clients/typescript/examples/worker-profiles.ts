// One worker profile with two execution classes: isolated children and shared async children. Each task declares
// its class; a shared class needs `timeoutMs`.
//
// The third class, `SHARED_THREAD` (thread seats in a child), is not in this profile: the client refuses a profile
// with `sharedThread` children until thread seats are released.
//
//   node examples/worker-profiles.ts worker --profile mixed
//   node examples/worker-profiles.ts demo

import { defineTask, h, Horsies, type Result, type TaskHandle } from "horsies";

import {
  DATABASE_URL,
  DEMO_DONE,
  demoFailed,
  printLine,
  readLine,
  refusalLines,
  RESULT_WAIT_MS,
  runExample,
  sendErrorText,
} from "./support.ts";

/** The profile that `worker --profile mixed` runs. */
const PROFILE = "mixed";

/** How long one page fetch takes, in milliseconds. */
const FETCH_PAGE_MS = 100;

/** The pages of the demo. */
const PAGE_PATHS: readonly string[] = ["/", "/docs", "/docs/typescript"];

/** One task at a time in its own process (the default class): for code that may crash or leak. A sync body. */
const checksum = defineTask({ name: "checksum", input: { data: h.array(h.int()) }, output: h.int() });

/** Many runs on one event loop of a child. At `timeoutMs` the run's signal fires, then it has `cancelGraceMs` to end. */
const fetchPage = defineTask({
  name: "fetch_page",
  execution: "SHARED_ASYNC",
  timeoutMs: 10_000,
  cancelGraceMs: 500,
  input: { path: h.string() },
  output: h.int(),
  errorCodes: ["INTERRUPTED"],
});

const created = Horsies.create({
  database: { databaseUrl: DATABASE_URL },
  workerProfiles: {
    [PROFILE]: {
      isolated: { kind: "isolatedChildren", count: 1 },
      sharedAsync: { kind: "sharedAsyncChildren", count: 1, seats: 20 },
    },
  },
  tasks: [checksum, fetchPage],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

app.implement(checksum, ({ data }, { ok }) => ok(data.reduce((sum, value) => sum + value, 0)));

/** How a wait ended: its time passed, or the signal fired first. */
type WaitEnd = "elapsed" | "cancelled";

/** Wait `ms` milliseconds, or until `signal` fires. Effects: a timer and an abort listener, both removed at the end. */
async function waitOrCancel(ms: number, signal: AbortSignal): Promise<WaitEnd> {
  const { promise, resolve } = Promise.withResolvers<WaitEnd>();

  if (signal.aborted) {
    resolve("cancelled");

    return promise;
  }

  const onAbort = (): void => {
    clearTimeout(timer);
    resolve("cancelled");
  };

  const timer = setTimeout(() => {
    signal.removeEventListener("abort", onAbort);
    resolve("elapsed");
  }, ms);

  signal.addEventListener("abort", onAbort, { once: true });

  return promise;
}

app.implement(fetchPage, async ({ path }, { ok, err, signal }) => {
  const ended = await waitOrCancel(FETCH_PAGE_MS, signal);

  switch (ended) {
    case "elapsed":
      return ok(path.length);
    case "cancelled":
      return err({ code: "INTERRUPTED", message: "cancelled before the page arrived" });
    default: {
      const unexpected: never = ended;

      throw new TypeError(`fetch_page: a wait end outside its type: ${String(unexpected)}`);
    }
  }
});

async function demo(): Promise<Result<void, string>> {
  const sum = await app.send(checksum, { data: [1, 2, 3] });

  if (!sum.ok) {
    return demoFailed(`send checksum: ${sendErrorText(sum.error)}`);
  }

  const pages: TaskHandle<number, "INTERRUPTED">[] = [];

  for (const path of PAGE_PATHS) {
    const sent = await app.send(fetchPage, { path });

    if (!sent.ok) {
      return demoFailed(`send fetch_page ${path}: ${sendErrorText(sent.error)}`);
    }

    pages.push(sent.value);
  }

  printLine(readLine("checksum", await sum.value.get({ waitMs: RESULT_WAIT_MS }), String));

  for (const page of pages) {
    printLine(readLine("fetch_page", await page.get({ waitMs: RESULT_WAIT_MS }), String));
  }

  return DEMO_DONE;
}

process.exitCode = await runExample(app, demo);
