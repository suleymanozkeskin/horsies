// The log setup of the commands that load the app: the worker, the scheduler and `check` each set the threshold of
// their `--log-level` with the app's `logFormat`.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { WorkerLogLevel } from "../../../horsies/src/_catalog/worker_manifest.ts";
import { processCheckCommandDeps } from "../../../horsies/src/cli/check-command.ts";
import type { LogFormat } from "../../../horsies/src/config/process-settings.ts";
import type { CommandLogging } from "../../../horsies/src/logging.ts";
import { NATIVE_SCHEDULER_CALLS, processSchedulerRunDeps } from "../../../horsies/src/scheduler/run.ts";
import { NATIVE_WORKER_CALLS, processWorkerRunDeps } from "../../../horsies/src/worker/run.ts";
import { contextOf, testApp } from "../worker/support.ts";

/** A command log setup that records each call as `level format`. */
function recordingLogging(): { readonly calls: string[]; readonly logging: CommandLogging } {
  const calls: string[] = [];

  return {
    calls,
    logging: (level: WorkerLogLevel, format: LogFormat) => {
      calls.push(`${level} ${format.kind}`);
    },
  };
}

/** The log setup call of each command for an app with `format`, in the order worker, scheduler, check. */
function commandCalls(format: LogFormat): readonly string[] {
  const context = contextOf(testApp({ logFormat: format }));
  const noSignals = (): void => undefined;
  const worker = recordingLogging();
  const scheduler = recordingLogging();
  const check = recordingLogging();

  processWorkerRunDeps(context, noSignals, NATIVE_WORKER_CALLS, worker.logging).setLogLevel("INFO");
  processSchedulerRunDeps(context, noSignals, NATIVE_SCHEDULER_CALLS, scheduler.logging).setLogLevel("DEBUG");
  processCheckCommandDeps(context, check.logging).setLogLevel("WARNING");

  return [...worker.calls, ...scheduler.calls, ...check.calls];
}

describe("the log setup of the commands that load the app", () => {
  test("an app with jsonLogs: the worker, the scheduler and check each apply jsonLogs", () => {
    assert.deepEqual(commandCalls({ kind: "jsonLogs" }), ["INFO jsonLogs", "DEBUG jsonLogs", "WARNING jsonLogs"]);
  });

  test("an app with textLogs: each command applies textLogs", () => {
    assert.deepEqual(commandCalls({ kind: "textLogs" }), ["INFO textLogs", "DEBUG textLogs", "WARNING textLogs"]);
  });
});
