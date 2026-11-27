// The test export of the addon that makes the log forwarder of this process panic at its next batch:
// `probePanicLogForwarder` (`bridge/src/logging/forwarder_probe.rs`). Only a build with the cargo feature `test-probes`
// exports it (`bun run --cwd bridge build:dev`); a published addon does not.
//
// The panic is a panic in a core background task, the mechanism of Python
// `clients/python/tests/e2e/tasks/instance_core_stopped_child.py` (a log handler that panics in the forwarder). The
// process panic rule decides the end: a task child whose child host read its core runs under the task child rule, so
// its core stops (the stop line on stderr) and the child ends itself with exit code 80 at its next task start, after
// a body that returned, or at a refused core call. A body calls the probe on the main thread of an isolated child,
// where the dev addon is loaded (`entry-thread.ts`).

import type { NativeEnvelope, NativeThrown } from "../../../horsies/src/native/call/envelope.ts";
import { loadAddon } from "#horsies";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";

/** The thread of the forwarder, as the stop line names it (`FORWARDER_THREAD_NAME` of the bridge). */
const FORWARDER_THREAD = "horsies-log-forwarder";

/** The test export of the forwarder panic. */
export interface ForwarderPanicAddon {
  probePanicLogForwarder(text: string): NativeEnvelope<undefined>;
}

/** The state of the core that the child host of a task child reads (`TaskChildCoreRead` of the bridge). */
export type TaskChildCoreRead = "running" | "stopped";

/** The test export of the read of the core that the child host of a task child makes; the first read enters the task child rule. */
export interface TaskChildCoreProbe {
  probeReadTaskChildCore(): NativeEnvelope<TaskChildCoreRead>;
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
  const exports = loadAddon();

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

/** The stderr line of child `pid` whose core stopped after its forwarder panicked with `text` (`write_stop_line` of the bridge). */
export function stopLine(pid: number, text: string): string {
  return `horsies: the core stopped in process ${String(pid)}: panic in thread ${FORWARDER_THREAD}: ${text}`;
}

/** The stderr line of a child whose stopped core refused a core call of a body (`refused_call_line` of the bridge). */
export function refusedCallLine(text: string): string {
  return `horsies: At a core call of this child: the core of this child stopped after a panic in a core background task (thread ${FORWARDER_THREAD}: ${text}`;
}

/** The last log line of a child whose run met its stopped core (`CoreStopEnd::line` of the core's child host). */
export function childLastLine(moment: "At the start of task" | "After the body of task", taskId: string, text: string): string {
  return `${moment} ${taskId}: the core of this child stopped after a panic in a core background task (thread ${FORWARDER_THREAD}: ${text}`;
}

/** The worker's line for child `pid` that ended itself with exit code 80 while it held no task (`log_core_stopped_child`). */
export function endedItselfLine(pid: number): string {
  return `child ${String(pid)} ended itself with exit code 80`;
}
