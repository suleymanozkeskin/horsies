// The log sink that the package gave the addon, kept for the defect rule: the record of a defect in a
// worker process goes to this sink on the JavaScript thread.

import type { NativeLogSink } from "./addon.ts";

/** The sink in force: one stderr line per record (the addon's default), or a function. */
export type LogSinkSlot = { readonly kind: "stderr" } | { readonly kind: "function"; readonly sink: NativeLogSink };

let slot: LogSinkSlot = { kind: "stderr" };

/** The sink in force. */
export function currentLogSink(): LogSinkSlot {
  return slot;
}

/** Keep the sink that the package gave the addon. */
export function keepLogSink(next: LogSinkSlot): void {
  slot = next;
}
