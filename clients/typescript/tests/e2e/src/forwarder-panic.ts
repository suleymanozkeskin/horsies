// The test export of the addon that makes the log forwarder of this process panic at its next batch:
// `probePanicLogForwarder` (`bridge/src/logging/forwarder_probe.rs`). Only a build with the cargo feature `test-probes`
// exports it (`bun run --cwd bridge build:dev`); a published addon does not.
//
// The panic is a panic in a core background task, the mechanism of Python
// `clients/python/tests/e2e/tasks/instance_core_stopped_child.py` (a log handler that panics in the forwarder). The
// process panic rule decides the end: a task child runs under `ABORT_PROCESS`, so it writes the abort line to stderr
// and aborts. A body calls the probe on the main thread of an isolated child, where the dev addon is loaded
// (`entry-thread.ts`).

import type { NativeEnvelope, NativeThrown } from "../../../horsies/src/native/call/envelope.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";

/** The thread of the forwarder, as the abort line names it (`FORWARDER_THREAD_NAME` of the bridge). */
const FORWARDER_THREAD = "horsies-log-forwarder";

/** The test export of the forwarder panic. */
export interface ForwarderPanicAddon {
  probePanicLogForwarder(text: string): NativeEnvelope<undefined>;
}

/** Why the forwarder was not armed: the addon has no probe, or the probe threw (no log forwarder runs). */
export type ProbeRefused = { readonly kind: "notInBuild" } | { readonly kind: "thrown"; readonly thrown: NativeThrown };

/** The text of a probe refusal, for a task error message or a log line. Pure. */
export function probeRefusedText(refused: ProbeRefused): string {
  switch (refused.kind) {
    case "notInBuild":
      return "the addon exports no probePanicLogForwarder: build it with the cargo feature test-probes (build:dev)";
    case "thrown":
      return `probePanicLogForwarder threw ${refused.thrown.code}: ${JSON.stringify(refused.thrown)}`;
  }
}

/** The probe of the dev addon of this process, or `notInBuild`. Effects: loads the dev addon at the first call. */
export function forwarderPanicAddon(): Result<ForwarderPanicAddon, ProbeRefused> {
  const exports = requireDevAddon();

  if (!("probePanicLogForwarder" in exports)) {
    return err({ kind: "notInBuild" });
  }

  // SAFETY: a build with `test-probes` registers this function with this signature; the declaration test of the
  // `logging` area compares this interface with the declarations that the build writes.
  return ok(exports as ForwarderPanicAddon);
}

/** Make the log forwarder of this process panic with `text` at its next batch. Returns at once. */
export function armForwarderPanic(addon: ForwarderPanicAddon, text: string): Result<undefined, ProbeRefused> {
  const answer = addon.probePanicLogForwarder(text);

  switch (answer.kind) {
    case "value":
      return ok(undefined);
    case "thrown":
      return err({ kind: "thrown", thrown: answer.thrown });
  }
}

/** The stderr line of child `pid` that aborts after its forwarder panicked with `text` (`write_abort_line` of the bridge). */
export function abortLine(pid: number, text: string): string {
  return `horsies: worker process ${String(pid)} aborts: panic in core background task thread ${FORWARDER_THREAD}: ${text}`;
}
