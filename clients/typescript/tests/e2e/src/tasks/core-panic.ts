// Tasks that make a core background task of their child panic (Python
// `clients/python/tests/e2e/tasks/instance_core_stopped_child.py`): the log forwarder of the child panics through the
// test export of the dev addon (`forwarder-panic.ts`). Each is `ISOLATED`, so its body runs on the main thread of its
// child, where the addon is loaded.
//
// A task child runs under the panic rule `ABORT_PROCESS`: at the panic the child writes the abort line and aborts, so
// no code of the child runs after it. (A Python child runs under `STOP_CORE`: its core stops, and the child ends
// itself at its next core call with exit code 80.)
//
// - `e2e_core_panic_after_return`: a timer of the child arms the panic `PANIC_DELAY_MS` after the body returned, while
//   the child holds no task.
// - `e2e_core_panic_in_body`: on its first run the panic comes while the body waits; a body that reaches its end
//   appends its process id to `<token>-returned`. Each run appends its process id to `<token>-runs`.
// - `e2e_core_panic_then_send`: on its first run the panic comes while the body waits before a send. Later runs
//   return their process id.

import { defineTask, h, type Horsies } from "../../../../horsies/src/index.ts";
import type { Result } from "../../../../horsies/src/wire/result.ts";
import { armForwarderPanic, type ForwarderPanicAddon, forwarderPanicAddon, type ProbeRefused, probeRefusedText } from "../forwarder-panic.ts";
import { appendLine, createOnce, markerErrorText } from "../markers.ts";
import { pid } from "./basic.ts";
import { sleepFor } from "./waits.ts";

/** The panic text of every task of this module. */
export const PANIC_TEXT = "planned panic in the log forwarder of an e2e task";

/** How long after the return of `e2e_core_panic_after_return` the panic comes: after the end of its run (Python `STOP_DELAY_S`). */
export const PANIC_DELAY_MS = 2_000;

/** How long a body waits after the panic of its own run: far past the abort, which comes at once (Python `STOP_WAIT_S`). */
const AFTER_PANIC_WAIT_MS = 10_000;

const PANIC_CODES = ["PROBE_FAILED", "MARKER_FAILED", "SEND_FAILED"] as const;

/** Arms the panic, which comes `PANIC_DELAY_MS` later, and returns the process id of its child. */
export const panicAfterReturn = defineTask({
  name: "e2e_core_panic_after_return",
  input: {},
  output: h.int(),
  errors: PANIC_CODES,
});

/** The first run of `token` panics while its body waits; every run that reaches its end returns its process id. */
export const panicInBody = defineTask({
  name: "e2e_core_panic_in_body",
  input: { token: h.string() },
  output: h.int(),
  errors: PANIC_CODES,
  retry: { kind: "fixed", intervalsSeconds: [1], jitter: false, autoRetryFor: ["WORKER_CRASHED"] },
});

/** The first run of `token` panics while its body waits before a send of `e2e_pid`; later runs return their process id. */
export const panicThenSend = defineTask({
  name: "e2e_core_panic_then_send",
  input: { token: h.string() },
  output: h.int(),
  errors: PANIC_CODES,
  retry: { kind: "fixed", intervalsSeconds: [1], jitter: false, autoRetryFor: ["WORKER_CRASHED"] },
});

/** Every contract of this module. */
export const CORE_PANIC_CONTRACTS = [panicAfterReturn, panicInBody, panicThenSend] as const;

/** Arm the forwarder panic of this process now. */
function armNow(): Result<undefined, ProbeRefused> {
  const addon = forwarderPanicAddon();

  return addon.ok ? armForwarderPanic(addon.value, PANIC_TEXT) : addon;
}

/** Arm the forwarder panic of this process `PANIC_DELAY_MS` from now; a refusal then goes to stderr (the worker log). */
function armLater(addon: ForwarderPanicAddon): void {
  setTimeout(() => {
    const armed = armForwarderPanic(addon, PANIC_TEXT);

    if (!armed.ok) {
      process.stderr.write(`e2e_core_panic_after_return: ${probeRefusedText(armed.error)}\n`);
    }
  }, PANIC_DELAY_MS);
}

/** The implementations of this module on `app`. */
export function implementCorePanic(app: Horsies): void {
  app.implement(panicAfterReturn, () => {
    const addon = forwarderPanicAddon();

    if (!addon.ok) {
      return panicAfterReturn.err({ code: "PROBE_FAILED", message: probeRefusedText(addon.error) });
    }

    armLater(addon.value);

    return panicAfterReturn.ok(process.pid);
  });
  app.implement(panicInBody, async ({ token }) => {
    const ran = appendLine(`${token}-runs`, String(process.pid));

    if (!ran.ok) {
      return panicInBody.err({ code: "MARKER_FAILED", message: markerErrorText(ran.error) });
    }

    const first = createOnce(`${token}-first-run`, String(process.pid));

    if (!first.ok) {
      return panicInBody.err({ code: "MARKER_FAILED", message: markerErrorText(first.error) });
    }

    if (first.value === "created") {
      const armed = armNow();

      if (!armed.ok) {
        return panicInBody.err({ code: "PROBE_FAILED", message: probeRefusedText(armed.error) });
      }

      await sleepFor(AFTER_PANIC_WAIT_MS, "noSignal");
    }

    const returned = appendLine(`${token}-returned`, String(process.pid));

    return returned.ok ? panicInBody.ok(process.pid) : panicInBody.err({ code: "MARKER_FAILED", message: markerErrorText(returned.error) });
  });
  app.implement(panicThenSend, async ({ token }) => {
    const first = createOnce(`${token}-first-run`, String(process.pid));

    if (!first.ok) {
      return panicThenSend.err({ code: "MARKER_FAILED", message: markerErrorText(first.error) });
    }

    if (first.value === "existed") {
      return panicThenSend.ok(process.pid);
    }

    const armed = armNow();

    if (!armed.ok) {
      return panicThenSend.err({ code: "PROBE_FAILED", message: probeRefusedText(armed.error) });
    }

    await sleepFor(AFTER_PANIC_WAIT_MS, "noSignal");

    const sent = await app.send(pid, { sleepMs: 0 });

    return sent.ok ? panicThenSend.ok(process.pid) : panicThenSend.err({ code: "SEND_FAILED", message: `${sent.error.code}: ${sent.error.message}` });
  });
}
