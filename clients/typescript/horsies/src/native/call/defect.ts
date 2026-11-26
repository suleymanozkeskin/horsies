// The rule of a defect of the package.
//
// A `CoreDefect` crashes loud. Under the panic rule `ABORT_PROCESS` (a worker process) the defect goes
// to stderr first, then to the log sink, and the process aborts:
// 1. `reportDefect` of the addon writes the stderr line and gives the record;
// 2. this module gives the record to the package's log sink, on this thread;
// 3. `abortForDefect` sends the records still in the channel to stderr, drains, and aborts.
// Under `STOP_CORE` the defect is thrown. No step here throws: a step that fails leaves the throw.

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
    case "platformUnsupported":
    case "loadFailed":
      // No addon in this thread: the defect is thrown.
      return;
    default: {
      const unexpected: never = load;

      throw new ClientDefect({ subject: "exhaustive switch", rule: `a load outcome outside its type: ${String(unexpected)}` });
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
