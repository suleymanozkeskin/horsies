// The steps of the task child rule cases of `child-host-child.ts` and `process-core-child.ts`: the read of the core
// that the child host of a task child makes, and a panic of the log forwarder, through the test exports of the dev
// addon (`probeReadTaskChildCore`, `probePanicLogForwarder`; a build with the cargo feature `test-probes`).

import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { setPanicRule } from "../../../horsies/src/native/process-core/process-core.ts";
import { logWarning } from "../../../horsies/src/logging.ts";
import type { ForwarderPanicAddon, TaskChildCoreProbe, TaskChildCoreRead } from "../../e2e/src/forwarder-panic.ts";
import { manifestJson } from "./manifest.ts";

/** The panic text of the cases. */
export const TASK_CHILD_PANIC = "planned forwarder panic of a task child rule case";

/** The longest wait for the stop of the core after the panic. */
const STOP_WAIT_MS = 5_000;

/** The interval of the reads of that wait. */
const STOP_POLL_MS = 10;

/** A database that no case reaches: the stopped core refuses the call before a connection. */
const UNUSED_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

type Probes = ForwarderPanicAddon & TaskChildCoreProbe;

/** The test exports of the dev addon. Throws when the build has no probes: the case cannot run. */
function probes(): Probes {
  const exports = requireDevAddon();

  if (!("probeReadTaskChildCore" in exports) || !("probePanicLogForwarder" in exports)) {
    throw new Error("the dev addon exports no probes: build it with the cargo feature test-probes (build:dev)");
  }

  // SAFETY: a build with `test-probes` registers these functions with these signatures; the declaration test of the
  // `logging` area compares the interfaces with the declarations of that build.
  return exports as Probes;
}

/** The worker process rule of a task child, and a running log forwarder. */
export function startTaskChild(): void {
  setPanicRule("ABORT_PROCESS");
  logWarning("horsies.app", "a task child rule case starts the log forwarder");
}

/** The read of the core by the child host: the first one enters the task child rule. */
export function readCore(): TaskChildCoreRead {
  return envelopeValue(probes().probeReadTaskChildCore());
}

/** Make the log forwarder panic at its next batch. */
export function panicForwarder(): void {
  envelopeValue(probes().probePanicLogForwarder(TASK_CHILD_PANIC));
}

/** Wait until the read of the core gives `stopped`, at most `STOP_WAIT_MS`; the last read. */
export async function untilCoreStopped(): Promise<TaskChildCoreRead> {
  for (let waited = 0; waited < STOP_WAIT_MS; waited += STOP_POLL_MS) {
    if (readCore() === "stopped") {
      return "stopped";
    }

    await new Promise((resolve) => {
      setTimeout(resolve, STOP_POLL_MS);
    });
  }

  return readCore();
}

/** Wait `STOP_WAIT_MS`: a case whose panic aborts the process ends before the wait does. */
export async function waitForTheEnd(): Promise<void> {
  await new Promise((resolve) => {
    setTimeout(resolve, STOP_WAIT_MS);
  });
}

/** A core call: under the task child rule a stopped core ends the process with 80 here. */
export async function coreCall(): Promise<string> {
  const handle = buildCoreHandle(manifestJson(UNUSED_DATABASE_URL, false), "PRODUCER");
  const ping = await handle.pingDatabase();

  return `the core call returned ${ping.kind}`;
}
