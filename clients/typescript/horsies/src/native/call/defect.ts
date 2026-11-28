// The rule of a defect of the package.
//
// A `CoreDefect` crashes loud. Under the panic rule `ABORT_PROCESS` (a worker process) the defect goes
// to stderr first, then to the log sink, and the process aborts:
// 1. `reportDefect` of the addon writes the stderr line and gives the record;
// 2. this module gives the record to the package's log sink, on this thread;
// 3. `abortForDefect` sends the records still in the channel to stderr, drains, and aborts.
// Under `STOP_CORE` the defect is thrown. No step here throws: a step that fails leaves the throw.
//
// A thread with no addon (a `worker_threads` isolate) has no rule of its own. A seat isolate of a `SHARED_THREAD`
// child sends each defect to the main thread of its child, which applies this rule (in a worker process it aborts);
// the seat registers that report (`reportDefectsToMainThread`). Another worker thread throws the defect.

import { ClientDefect } from "../../thrown-errors.ts";
import type { CoreDefect } from "../../thrown-errors.ts";
import { loadAddon } from "../load.ts";
import type { LogRecordOut } from "../logging/addon.ts";
import { currentLogSink } from "../logging/sink-slot.ts";
import type { NativeEnvelope } from "./envelope.ts";

/** How the package goes on after a defect report. */
export type DefectAnswer =
  | { readonly kind: "throw" }
  | { readonly kind: "abortWithRecord"; readonly record: LogRecordOut }
  | { readonly kind: "abortWithNoRecord" };

/** Whether the package gave the defect record to its log sink. */
export type RecordHandoff = "givenToSink" | "notGiven";

/** The functions of the addon that the defect rule calls. */
export interface DefectAddon {
  reportDefect(code: string, message: string): NativeEnvelope<DefectAnswer>;
  abortForDefect(handoff: RecordHandoff): NativeEnvelope<undefined>;
}

/** Where a defect of a thread with no addon goes: nowhere (it is thrown), or to the main thread of the process. */
type ThreadDefectReport =
  | { readonly kind: "throwOnly" }
  | { readonly kind: "toMainThread"; readonly report: (defect: CoreDefect) => void };

let threadDefectReport: ThreadDefectReport = { kind: "throwOnly" };

/**
 * Send each defect of this thread to the main thread of the process with `report`: the seat isolate of a
 * `SHARED_THREAD` child calls it once, when its channel opens. `report` must not throw.
 */
export function reportDefectsToMainThread(report: (defect: CoreDefect) => void): void {
  threadDefectReport = { kind: "toMainThread", report };
}

/** Apply the defect rule to `defect`, then throw it. In a worker process the throw is not reached. */
export function raiseDefect(defect: CoreDefect): never {
  reportDefect(defect);

  throw defect;
}

/** Apply the defect rule to `defect` with no throw: a caller that must not throw (a callback wrapper). */
export function reportDefect(defect: CoreDefect): void {
  const load = loadAddon();

  switch (load.kind) {
    case "loaded":
      // SAFETY: the addon registers these functions with these signatures; the declaration test of
      // the process-core area compares this interface with the declarations of the native build.
      applyDefectRule(load.exports as DefectAddon, defect);

      return;
    case "outsideMainThread":
      reportFromThread(defect);

      return;
    case "platformUnsupported":
    case "loadFailed":
      // No addon in this process: the defect is thrown.
      return;
    default: {
      const unexpected: never = load;

      throw new ClientDefect({ subject: "exhaustive switch", rule: `a load outcome outside its type: ${String(unexpected)}` });
    }
  }
}

/** A defect of a thread with no addon: to the main thread when this thread reports there, else only the throw. */
function reportFromThread(defect: CoreDefect): void {
  switch (threadDefectReport.kind) {
    case "throwOnly":
      return;
    case "toMainThread":
      threadDefectReport.report(defect);

      return;
    default: {
      const unexpected: never = threadDefectReport;

      throw new ClientDefect({ subject: "exhaustive switch", rule: `a thread report outside its type: ${String(unexpected)}` });
    }
  }
}

function applyDefectRule(addon: DefectAddon, defect: CoreDefect): void {
  const answer = defectAnswer(addon, defect);

  switch (answer.kind) {
    case "throw":
      return;
    case "abortWithNoRecord":
      abort(addon, "notGiven");

      return;
    case "abortWithRecord":
      abort(addon, giveToSink(answer.record));

      return;
    default: {
      const unexpected: never = answer;

      throw new ClientDefect({ subject: "exhaustive switch", rule: `a defect answer outside its type: ${String(unexpected)}` });
    }
  }
}

/** The answer of the addon; `throw` when the report call fails. */
function defectAnswer(addon: DefectAddon, defect: CoreDefect): DefectAnswer {
  try {
    const envelope = addon.reportDefect(defect.code, defect.message);

    return envelope.kind === "value" ? envelope.value : { kind: "throw" };
  } catch {
    return { kind: "throw" };
  }
}

/** Give the defect record to the package's log sink, on this thread. */
function giveToSink(record: LogRecordOut): RecordHandoff {
  const slot = currentLogSink();

  switch (slot.kind) {
    case "stderr":
      return "notGiven";
    case "function":
      // The sink wrapper never throws; `false` means the user's sink threw.
      return slot.sink([record]) ? "givenToSink" : "notGiven";
    default: {
      const unexpected: never = slot;

      throw new ClientDefect({ subject: "exhaustive switch", rule: `a sink slot outside its type: ${String(unexpected)}` });
    }
  }
}

/** End the process. A failed call returns, and the caller throws the defect. */
function abort(addon: DefectAddon, handoff: RecordHandoff): void {
  try {
    addon.abortForDefect(handoff);
  } catch {
    // The abort did not run; the defect is thrown.
  }
}
