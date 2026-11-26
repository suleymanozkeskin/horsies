// The cancel function of the `facades` area of the addon, its record, and the accessor.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { NativeCoreHandle } from "../handle/addon.ts";
import type { NativeOutcome } from "../values/records.ts";

/** Which statuses a cancel moves to `CANCELLED`. */
export type CancelPolicy = "includeRunning" | "pendingOrClaimedOnly";

/** The task that the cancel moved to `CANCELLED`; `wasStatus` is the wire value of the status it left. */
export interface TaskCancelledRecord {
  readonly taskId: string;
  readonly wasStatus: string;
}

/** The cancel function of the addon. */
export interface CancelAddon {
  cancelTask(
    handle: NativeCoreHandle,
    taskId: string,
    policy: CancelPolicy,
  ): Promise<NativeEnvelope<NativeOutcome<TaskCancelledRecord>>>;
}

/** The cancel function of the addon. Throws as `requireAddon` does. */
export function cancelAddon(): CancelAddon {
  // SAFETY: the addon registers this function with this signature; the declaration test of the facades area compares
  // this interface with the declarations that the native build writes.
  return requireAddon() as CancelAddon;
}
