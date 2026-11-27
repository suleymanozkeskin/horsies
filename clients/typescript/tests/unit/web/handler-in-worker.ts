// Runs in a `worker_threads` isolate of `app-handler.test.ts`: it builds an app with `Horsies.create` (on a worker
// thread only the checks that are TypeScript code run, and no addon loads), asks for its monitoring handler and posts
// the name of the error that the call throws.

import { parentPort } from "node:worker_threads";

import { allowAll, createMonitoringHandler, Horsies } from "../../../horsies/src/index.ts";

/** A database URL that no test connects to: the handler call throws before any connect. */
const DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** The name of the error that `createMonitoringHandler` throws for `app`, or `returned` when it returns. */
function handlerCallOutcome(app: Horsies): string {
  try {
    void createMonitoringHandler(app, { authorizationPolicy: allowAll() });

    return "returned";
  } catch (error) {
    return error instanceof Error ? error.name : "notAnError";
  }
}

const created = Horsies.create({ database: { databaseUrl: { kind: "value", value: DATABASE_URL } }, tasks: [] });

parentPort?.postMessage(created.ok ? handlerCallOutcome(created.value) : "createRefused");
