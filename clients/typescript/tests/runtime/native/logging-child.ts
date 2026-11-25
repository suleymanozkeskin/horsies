// A child process of `logging.test.ts`: one case of the JavaScript log sink that needs a process of
// its own (the process exit, stderr, an abort). It writes its outcome on stdout.

import { writeSync } from "node:fs";

import { configureLogging } from "../../../horsies/src/logging.ts";
import { addonSettlement, settlingCallback } from "../../../horsies/src/native/call/callback.ts";
import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { setPanicRule } from "../../../horsies/src/native/process-core/process-core.ts";
import { probeAddon } from "./callback-probe.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

/** How long the throwing-sink case waits after the sink ran, so the forwarder writes its stderr lines. */
const STDERR_WRITE_WAIT_MS = 500;

/** The longest wait for the first call of the sink. */
const SINK_CALL_WAIT_MS = 5_000;

/** The pause between two looks at the sink calls. */
const SINK_CALL_POLL_MS = 20;

/** Run the case that the first argument names. */
async function runCase(name: string | undefined): Promise<string> {
  switch (name) {
    case "idle-exit-with-sink":
      return idleExitWithSink();
    case "throwing-sink":
      return throwingSink();
    case "defect-record-to-sink":
      return defectRecordToSink();
    default:
      return `unknown case ${String(name)}`;
  }
}

/**
 * A sink stays set, the core started (the probe call starts it) and a callback ran: the process
 * must still exit when its loop is empty.
 */
async function idleExitWithSink(): Promise<string> {
  configureLogging({ levels: { default: "WARNING" }, sink: () => undefined });

  const probe = envelopeValue(
    await probeAddon().probeCallback(settlingCallback((argument: string) => argument, addonSettlement), "ping"),
  );

  return `probed ${probe.kind}`;
}

/** A sink that throws: the batch goes to stderr, and the process goes on. */
async function throwingSink(): Promise<string> {
  let calls = 0;

  configureLogging({
    levels: { default: "WARNING", loggers: { "horsies.sql": "INFO" } },
    sink: () => {
      calls += 1;

      throw new Error("planned sink failure");
    },
  });

  const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), true), "PRODUCER");
  const ping = envelopeValue(await handle.pingDatabase());

  for (let waited = 0; waited < SINK_CALL_WAIT_MS && calls === 0; waited += SINK_CALL_POLL_MS) {
    await sleep(SINK_CALL_POLL_MS);
  }

  await sleep(STDERR_WRITE_WAIT_MS);
  envelopeValue(await handle.close());

  return `pinged ${ping.kind}; sink calls ${calls > 0 ? "some" : "none"}`;
}

/** A defect under ABORT_PROCESS: the record reaches the sink (written with a sync write), then the abort. */
function defectRecordToSink(): string {
  configureLogging({
    sink: (records) => {
      for (const record of records) {
        writeSync(1, `SINK ${record.level} ${record.logger} ${record.message}\n`);
      }
    },
  });
  setPanicRule("ABORT_PROCESS");
  envelopeValue({ kind: "thrown", thrown: { code: "CLIENT_DEFECT", subject: "taskId", rule: "a UUID" } });

  return "no abort";
}

async function sleep(milliseconds: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

process.stdout.write(`${await runCase(process.argv[2])}\n`);
