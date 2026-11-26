// Runs in a `worker_threads` isolate of `create-checks.test.ts`: it runs the create checks of a workflow list and posts
// the codes of the refusals. No addon loads in this thread.

import { parentPort } from "node:worker_threads";

import { checkedAtCreate, duplicateIds, plain } from "./create-workflows.ts";

parentPort?.postMessage(checkedAtCreate([plain, duplicateIds]).refusals.map((refused) => refused.code));
