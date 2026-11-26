// The operator cancel of one task that no workflow holds.
//
// - `includeRunning`: `PENDING`, `CLAIMED` and `RUNNING` move to `CANCELLED`. A running body sees `runtime.signal`
//   abort with the cause `OPERATOR`; the statement records the running attempt with the outcome `CANCELLED`.
// - `pendingOrClaimedOnly`: `PENDING` and `CLAIMED` only; no user code ran.
// - One statement. `DB_OPERATION_FAILED` leaves the outcome unknown: nothing written, or committed with the reply lost.
//   A repeat tells which: a committed cancel answers `TASK_NOT_CANCELLABLE` with `CANCELLED`.
// - A task of a workflow is refused (`TASK_IS_WORKFLOW_TASK`): use the workflow handle.

import { nativeValueAsync } from "../native/call/envelope.ts";
import { unreachable } from "../native/call/never.ts";
import { raiseDefect } from "../native/call/defect.ts";
import type { CancelAddon, CancelPolicy } from "../native/facades/cancel.ts";
import type { RelayedAddon } from "./seat-calls.ts";
import type { NativeCoreHandle } from "../native/handle/addon.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { TaskId } from "../contract/ids.ts";
import { type CancelTaskError, taskActionError } from "./errors.ts";
import type { TaskCancelled } from "./health-types.ts";

/** The options of a cancel. The policy has no default: the caller names whether a running body is cancelled. */
export interface CancelTaskOptions {
  readonly policy: CancelPolicy;
}

/** The addon and the handle that the cancel calls. */
export interface CancelTarget {
  readonly addon: RelayedAddon<CancelAddon>;
  readonly handle: NativeCoreHandle;
}

/** Cancel one task that no workflow holds. Writes, in one statement. */
export async function cancelTask(
  target: CancelTarget,
  taskId: TaskId,
  options: CancelTaskOptions,
): Promise<Result<TaskCancelled, CancelTaskError>> {
  const outcome = await nativeValueAsync("cancelTask", () =>
    target.addon.cancelTask(target.handle, taskId, options.policy),
  );

  switch (outcome.kind) {
    case "done":
      return ok(Object.freeze({ taskId, wasStatus: leftStatus(outcome.record.wasStatus) }));
    case "failed":
      return err(taskActionError(outcome.error));
    case "seatRelayFull":
      return err(outcome.error);
    default:
      return unreachable(outcome);
  }
}

/** The status that a cancel left. Another text is a defect of the core: only these three can move to `CANCELLED`. */
function leftStatus(text: string): TaskCancelled["wasStatus"] {
  switch (text) {
    case "PENDING":
    case "CLAIMED":
    case "RUNNING":
      return text;
    default:
      return raiseDefect(
        new ClientDefect({ subject: "cancelTask", rule: `the core cancelled a task from status ${text}` }),
      );
  }
}
