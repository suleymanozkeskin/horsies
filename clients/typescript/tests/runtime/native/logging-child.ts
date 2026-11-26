// A child process of `logging.test.ts`: one case of the JavaScript log sink that needs a process of
// its own (the process exit, stderr, an abort). It writes its outcome on stdout.

import { writeSync } from "node:fs";

import { DEFAULT_QUEUE_ROUTE } from "../../../horsies/src/app/contract-list.ts";
import { taskOptionsFacts } from "../../../horsies/src/app/core-checks.ts";
import { parseAppConfig } from "../../../horsies/src/config/app-config.ts";
import { contractRecord } from "../../../horsies/src/contract/definition.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { configureLogging } from "../../../horsies/src/logging.ts";
import { addonSettlement, settlingCallback } from "../../../horsies/src/native/call/callback.ts";
import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { coreState, setPanicRule } from "../../../horsies/src/native/process-core/process-core.ts";
import type { NativeSendRules, NativeTaskOptions } from "../../../horsies/src/native/sends/addon.ts";
import { checkAppConfig, checkTaskOptions, prepareSendFacts } from "../../../horsies/src/native/sends/sends.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { probeAddon } from "./callback-probe.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** How long the throwing-sink case waits after the sink ran, so the forwarder writes its stderr lines. */
const STDERR_WRITE_WAIT_MS = 500;

/** The longest wait for the first call of the sink. */
const SINK_CALL_WAIT_MS = 5_000;

/** The pause between two looks at the sink calls. */
const SINK_CALL_POLL_MS = 20;

/** A database URL of the config of the payload case: the prepare step opens no connection. */
const UNUSED_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** The task of the payload case. No other case sends it: the core warns once per task name in a process. */
const PAYLOAD_TASK_NAME = "first_payload_warning";

/** The warn bound of the payload case, and an input one byte above it. */
const WARN_BYTES = 10;
const INPUT_BYTES = WARN_BYTES + 1;

/** The start of the core's payload warning, as a sink line of `firstPayloadWarning`. */
const PAYLOAD_WARNING_LINE = "WARNING horsies.app payload size guardrail";

/** Run the case that the first argument names. */
async function runCase(name: string | undefined): Promise<string> {
  switch (name) {
    case "idle-exit-with-sink":
      return idleExitWithSink();
    case "throwing-sink":
      return throwingSink();
    case "defect-record-to-sink":
      return defectRecordToSink();
    case "first-payload-warning":
      return firstPayloadWarning();
    case undefined:
    default:
      return `unknown case ${String(name)}`;
  }
}

/**
 * A sink stays set, the core started (the probe call starts it) and a callback ran: the process
 * must still exit when its loop is empty.
 */
async function idleExitWithSink(): Promise<string> {
  configureLogging({ levels: { default: "WARNING" }, sink: { kind: "function", sink: () => undefined } });

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
    sink: {
      kind: "function",
      sink: () => {
        calls += 1;

        throw new Error("planned sink failure");
      },
    },
  });

  const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), true), "PRODUCER");
  const ping = await handle.pingDatabase();

  for (let waited = 0; waited < SINK_CALL_WAIT_MS && calls === 0; waited += SINK_CALL_POLL_MS) {
    await sleep(SINK_CALL_POLL_MS);
  }

  await sleep(STDERR_WRITE_WAIT_MS);
  await handle.close();

  return `pinged ${ping.kind}; sink calls ${calls > 0 ? "some" : "none"}`;
}

/** A defect under ABORT_PROCESS: the record reaches the sink (written with a sync write), then the abort. */
function defectRecordToSink(): string {
  configureLogging({
    sink: {
      kind: "function",
      sink: (records) => {
        for (const record of records) {
          writeSync(1, `SINK ${record.level} ${record.logger} ${record.message}\n`);
        }
      },
    },
  });
  setPanicRule("ABORT_PROCESS");
  envelopeValue({ kind: "thrown", thrown: { code: "CLIENT_DEFECT", subject: "taskId", rule: "a UUID" } });

  return "no abort";
}

/**
 * The first input of a task above the warn bound, before any core call: the core's warning reaches the sink, and the
 * core does not start (the prepare step starts the log forwarder only).
 */
async function firstPayloadWarning(): Promise<string> {
  const lines: string[] = [];

  configureLogging({
    levels: { default: "WARNING" },
    sink: {
      kind: "function",
      sink: (records) => {
        for (const record of records) {
          lines.push(`${record.level} ${record.logger} ${record.message}`);
        }
      },
    },
  });

  const prepared = prepareSendFacts(
    warningRules(),
    {
      retention: { kind: "queueOrDefault" },
      deadline: { kind: "noDeadline" },
      inputBytes: INPUT_BYTES,
    },
    payloadTaskOptions(),
  );

  const warning = (): string | undefined => lines.find((line) => line.startsWith(PAYLOAD_WARNING_LINE));

  for (let waited = 0; waited < SINK_CALL_WAIT_MS && warning() === undefined; waited += SINK_CALL_POLL_MS) {
    await sleep(SINK_CALL_POLL_MS);
  }

  return `${prepared.kind}; core ${coreState()}; ${warning() ?? `no warning in: ${lines.join(" | ")}`}`;
}

/** The send rules of a config whose payload warn bound is `WARN_BYTES`. */
function warningRules(): NativeSendRules {
  const config = parseAppConfig(
    {
      broker: { databaseUrl: { kind: "value", value: UNUSED_DATABASE_URL } },
      payload: { warnBytes: { kind: "bytes", bytes: WARN_BYTES } },
    },
    () => undefined,
  );

  if (!config.ok) {
    throw new Error(`the config is refused: ${JSON.stringify(config.error)}`);
  }

  const checked = checkAppConfig(config.value.coreConfigJson);

  if (checked.kind !== "accepted") {
    throw new Error(`the core refuses the config: ${JSON.stringify(checked.refusals)}`);
  }

  return checked.rules;
}

/** The checked options of the task of the payload case. */
function payloadTaskOptions(): NativeTaskOptions {
  const contract = defineTask({ name: PAYLOAD_TASK_NAME, input: {}, output: h.null() });
  const checked = checkTaskOptions(taskOptionsFacts(contractRecord(contract), DEFAULT_QUEUE_ROUTE));

  if (checked.kind !== "accepted") {
    throw new Error(`the core refuses the options: ${JSON.stringify(checked.refusals)}`);
  }

  return checked.options;
}

async function sleep(milliseconds: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

process.stdout.write(`${await runCase(process.argv[2])}\n`);
