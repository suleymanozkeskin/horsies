// `cancel <task-id> [--policy include-running|pending-or-claimed-only]`: the operator cancel of one task. A running
// body sees `runtime.signal` fire with the cause `OPERATOR`.

import { parseArgs } from "node:util";

import { type CancelTaskError, type CancelTaskOptions, type Horsies, type Result, type TaskCancelled, TaskId } from "horsies";

import { err, ok } from "../result.ts";

/** The cancel policy of the command line, by its flag text. */
const POLICY_BY_FLAG: Readonly<Record<string, CancelTaskOptions["policy"]>> = {
  "include-running": "includeRunning",
  "pending-or-claimed-only": "pendingOrClaimedOnly",
};

/** The policy of a cancel that names none: a running task is cancelled too. */
export const DEFAULT_CANCEL_POLICY: CancelTaskOptions["policy"] = "includeRunning";

/** The arguments of `cancel`. */
export type CancelArguments = { readonly taskId: TaskId; readonly policy: CancelTaskOptions["policy"] };

/** The arguments of `cancel`, or the usage error. Pure. */
export function parseCancelArguments(args: readonly string[]): Result<CancelArguments, string> {
  let parsed: { readonly values: { readonly policy?: string | undefined }; readonly positionals: readonly string[] };

  try {
    parsed = parseArgs({ args: [...args], options: { policy: { type: "string" } }, strict: true, allowPositionals: true });
  } catch (cause) {
    return err(cause instanceof Error ? cause.message : String(cause));
  }

  const [text, ...rest] = parsed.positionals;

  if (text === undefined || rest.length > 0) {
    return err("cancel takes one task id");
  }

  const taskId = TaskId.parse(text);

  if (!taskId.ok) {
    return err(`${JSON.stringify(text)} is not a task id: ${taskId.error.detail}`);
  }

  const policyText = parsed.values.policy;
  const policy = policyText === undefined ? DEFAULT_CANCEL_POLICY : POLICY_BY_FLAG[policyText];

  if (policy === undefined) {
    return err(`--policy must be one of ${Object.keys(POLICY_BY_FLAG).join(", ")}`);
  }

  return ok({ taskId: taskId.value, policy });
}

/** Cancel the task of `args`. Effects: the cancel write. */
export async function cancelTask(app: Horsies, args: CancelArguments): Promise<Result<TaskCancelled, CancelTaskError>> {
  return app.monitoring.cancelTask(args.taskId, { policy: args.policy });
}

/** The line of a cancel. Pure. */
export function cancelLine(cancelled: Result<TaskCancelled, CancelTaskError>): string {
  return cancelled.ok
    ? `cancelled ${cancelled.value.taskId} (it was ${cancelled.value.wasStatus})`
    : `not cancelled: ${cancelled.error.code}`;
}
