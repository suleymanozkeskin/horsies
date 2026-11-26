// A child process of `process-core.test.ts`: it runs one case that sets a process-wide rule, and
// writes its outcome as one line on stdout. The panic rule and the environment count are per
// process, so each such case runs in its own process.

import { Worker } from "node:worker_threads";

import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { DEV_ADDON_PATH_VARIABLE, requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { coreState, environmentCount, setPanicRule } from "../../../horsies/src/native/process-core/process-core.ts";
import { manifestJson } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** Run the case that the first argument names. */
async function runCase(name: string | undefined): Promise<string> {
  switch (name) {
    case "panic-rule-fixed":
      return panicRuleFixed();
    case "defect-under-stop-core":
      return defect();
    case "defect-under-abort-process":
      setPanicRule("ABORT_PROCESS");

      return defect();
    case "second-environment":
      return secondEnvironment();
    case "never-connected-close":
      return neverConnectedClose();
    case undefined:
    default:
      return `unknown case ${String(name)}`;
  }
}

/**
 * The refusal `PanicRuleFixed` is a defect. Under `STOP_CORE` it is thrown; under `ABORT_PROCESS` a
 * defect aborts the process (the case `defect-under-abort-process`).
 */
function panicRuleFixed(): string {
  setPanicRule("STOP_CORE");
  setPanicRule("STOP_CORE");

  try {
    setPanicRule("ABORT_PROCESS");

    return "no throw";
  } catch (cause) {
    return cause instanceof Error && "current" in cause ? `${cause.name} ${String(cause.current)}` : String(cause);
  }
}

function defect(): string {
  try {
    envelopeValue({ kind: "thrown", thrown: { code: "CLIENT_DEFECT", subject: "taskId", rule: "a UUID" } });

    return "no throw";
  } catch (cause) {
    return cause instanceof Error ? cause.name : String(cause);
  }
}

/** A handle that never connected closes at once, and the core does not start. */
async function neverConnectedClose(): Promise<string> {
  const handle = buildCoreHandle(manifestJson("postgresql://horsies@127.0.0.1:1/none", false), "PRODUCER");
  const closed = await handle.close();

  return `${String(closed)} ${coreState()}`;
}

/** A worker thread opens the addon file itself; its registration must throw. */
async function secondEnvironment(): Promise<string> {
  const before = environmentCount();

  const worker = new Worker(
    `const m = { exports: {} };
     try { process.dlopen(m, process.env.${DEV_ADDON_PATH_VARIABLE}); require("node:worker_threads").parentPort.postMessage("opened"); }
     catch (error) { require("node:worker_threads").parentPort.postMessage(String(error.message)); }`,
    { eval: true },
  );

  const answer = await new Promise<string>((resolve) => {
    worker.once("message", resolve);
  });

  return `${before} ${environmentCount()} ${answer}`;
}

process.stdout.write(`${await runCase(process.argv[2])}\n`);
