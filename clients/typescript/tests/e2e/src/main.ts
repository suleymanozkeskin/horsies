// The worker entry of the end-to-end tests: the worker, its children and `check` of an instance.
//
//   HORSIES_TS_E2E_INSTANCE=default HORSIES_TS_E2E_DATABASE_URL=postgresql://... \
//       <node|bun> clients/typescript/tests/e2e/src/main.ts worker --profile isolated-1
//
// Every line before `cli.run` runs in each child too: the worker starts each child with this entry and
// `--horsies-child`, and the child builds the same app from the same environment. The first step of that code writes
// the marker `entry-<child|parent>-<pid>`, so a test can count the starts of this entry.
//
// A seat isolate of a `SHARED_THREAD` child runs this entry on a worker thread, where no addon loads. So the marker
// and the load of the addon run on the main thread of each process only (`entry-thread.ts`).
//
// The entry imports the source entry of the package and loads the dev build of the addon; the form through the built
// package and its platform package belongs to the CI lanes (#2493).

import { isMainThread } from "node:worker_threads";

import { cli, type Horsies } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import { buildApp } from "./app.ts";
import { connectionFromEnv } from "./connection.ts";
import { entryThread, runProcessSteps } from "./entry-thread.ts";
import { INSTANCE_ENV, parseInstance } from "./instances.ts";
import { createOnce, markerErrorText } from "./markers.ts";

/** The argument after the entry script of a process that the worker started as a child. */
const CHILD_FLAG = "--horsies-child";

/** The steps that run once per process: the marker of this start, then the load of the addon. */
function processSteps(): void {
  const role = process.argv[2] === CHILD_FLAG ? "child" : "parent";
  const marked = createOnce(`entry-${role}-${String(process.pid)}`, process.argv.slice(2).join(" "));

  if (!marked.ok) {
    process.stderr.write(`horsies e2e entry: the entry marker: ${markerErrorText(marked.error)}\n`);
  }

  // The addon of these processes is the dev build that HORSIES_NATIVE_ADDON_PATH names.
  void requireDevAddon();
}

void runProcessSteps(entryThread(isMainThread), processSteps);

/** The app of the instance and the connection that the environment names. */
function entryApp(): Result<Horsies, string> {
  const instance = parseInstance(process.env[INSTANCE_ENV] ?? "");

  if (!instance.ok) {
    return instance;
  }

  const connection = connectionFromEnv((name) => process.env[name]);

  return connection.ok ? buildApp(instance.value, connection.value) : connection;
}

const app = entryApp();

if (app.ok) {
  process.exitCode = await cli.run(app.value);
} else {
  process.stderr.write(`horsies e2e entry: ${app.error}\n`);
  process.exitCode = 1;
}
