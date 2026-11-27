// `rerun <task-id> [--reason TEXT]`: run a retained failed task again, in a transaction of the Acme store, with the
// store's audit row of the rerun in the same transaction (`app.rerunTaskIn` with the `pg` adapter of `horsies/rerun/pg`).
// The rerun and the audit row commit together, or neither is written.

import { parseArgs } from "node:util";

import { type CallerRerunError, type RerunCommand, type RerunOutcome, type RerunEnqueuePolicy, type Result, TaskId } from "horsies";
import { pgTransaction } from "horsies/rerun/pg";

import type { ShowcaseApp } from "../app.ts";
import { err, ok } from "../result.ts";
import { inTransactionUntilFailure, type Store, type StoreClient, type StoreFailure, storeFailureText } from "../store/connection.ts";
import { RERUN_RETENTION_DAYS } from "../tuning.ts";

/** How long the caller key of a rerun holds it: a second rerun of the same source in this window is a replay. */
export const RERUN_KEY_WINDOW_HOURS = 24;

/** The longest `--reason` text. */
export const RERUN_REASON_MAX_LENGTH = 200;

/** The reason of a rerun that names none. */
export const DEFAULT_RERUN_REASON = "operator rerun";

/** The enqueue policy of every showcase rerun. */
export const RERUN_POLICY: RerunEnqueuePolicy = {
  retention: { kind: "period", days: RERUN_RETENTION_DAYS },
  retainRerunInput: "keep",
  reservationWindow: Temporal.Duration.from({ hours: RERUN_KEY_WINDOW_HOURS }),
};

/** Why a rerun wrote nothing. */
export type RerunFailure =
  | { readonly kind: "store"; readonly failure: StoreFailure }
  | { readonly kind: "rerun"; readonly error: CallerRerunError };

/** The caller key of a rerun of `sourceTaskId`: one rerun of a source per key window. Pure. */
export function rerunCallerKey(sourceTaskId: TaskId): string {
  return `acme-rerun:${sourceTaskId}`;
}

/** The rerun of `sourceTaskId`, and its audit row when it enqueued a task, in one transaction of `store`. */
export async function rerunWithAudit(
  showcase: ShowcaseApp,
  store: Store,
  sourceTaskId: TaskId,
  reason: string,
): Promise<Result<RerunOutcome, RerunFailure>> {
  const command: RerunCommand = { sourceTaskId, goodUntil: { kind: "none" }, idempotencyKey: rerunCallerKey(sourceTaskId) };

  const ran = await inTransactionUntilFailure(store, "rerun_with_audit", async (client) => {
    const outcome = await showcase.app.rerunTaskIn(pgTransaction(client), command, RERUN_POLICY);

    if (!outcome.ok) {
      return err(outcome.error);
    }

    if (outcome.value.kind === "enqueued") {
      await recordRerun(client, outcome.value.newTaskId, sourceTaskId, reason);
    }

    return ok(outcome.value);
  });

  if (!ran.ok) {
    return err({ kind: "store", failure: ran.error });
  }

  return ran.value.ok ? ok(ran.value.value) : err({ kind: "rerun", error: ran.value.error });
}

async function recordRerun(client: StoreClient, newTaskId: TaskId, sourceTaskId: TaskId, reason: string): Promise<void> {
  await client.query("INSERT INTO acme_reruns (rerun_id, source_task_id, new_task_id, reason) VALUES ($1, $2, $3, $4)", [
    `rerun_${newTaskId}`,
    sourceTaskId,
    newTaskId,
    reason,
  ]);
}

/** The line of a rerun outcome. Pure. */
export function rerunOutcomeLine(outcome: RerunOutcome): string {
  switch (outcome.kind) {
    case "enqueued":
      return `enqueued ${outcome.newTaskId} (source ${outcome.sourceTaskId}, chain root ${outcome.rerunRootTaskId}); audit row written`;
    case "sourceLive":
      return `${outcome.taskId} is still live: its own retries own it; nothing written`;
    case "sourceAbsent":
      return `no record of ${outcome.taskId} (${outcome.reason}); nothing written`;
    case "sourceCorrupt":
      return `the history row of ${outcome.taskId} does not decode: ${outcome.detail}; nothing written`;
    case "notEligible":
      return `${outcome.taskId} is not eligible (${outcome.reason}); nothing written`;
    case "inputUnavailable":
      return `${outcome.taskId} stores no input (${outcome.disposition}); nothing written`;
    case "inputCorrupt":
      return `the stored input of ${outcome.taskId} fails its checks: ${outcome.detail}; nothing written`;
    case "keyConflict":
      return `the caller key of ${outcome.taskId} holds the rerun ${outcome.ownerTaskId}; nothing written`;
    case "keyReplay":
      return `this rerun exists already as ${outcome.existingTaskId}; nothing written`;
    default: {
      const unexpected: never = outcome;

      throw new TypeError(`rerunOutcomeLine: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** The line of a rerun failure. Pure. */
export function rerunFailureLine(failure: RerunFailure): string {
  switch (failure.kind) {
    case "store":
      return `the store transaction failed: ${storeFailureText(failure.failure)}; nothing written`;
    case "rerun":
      return `the rerun failed (${failure.error.code}); the transaction rolled back`;
    default: {
      const unexpected: never = failure;

      throw new TypeError(`rerunFailureLine: a failure outside its type: ${String(unexpected)}`);
    }
  }
}

/** The arguments of `rerun`. */
export type RerunArguments = { readonly sourceTaskId: TaskId; readonly reason: string };

/** The arguments of `rerun`, or the usage error. Pure. */
export function parseRerunArguments(args: readonly string[]): Result<RerunArguments, string> {
  let parsed: { readonly values: { readonly reason?: string | undefined }; readonly positionals: readonly string[] };

  try {
    parsed = parseArgs({ args: [...args], options: { reason: { type: "string" } }, strict: true, allowPositionals: true });
  } catch (cause) {
    return err(cause instanceof Error ? cause.message : String(cause));
  }

  const [text, ...rest] = parsed.positionals;

  if (text === undefined || rest.length > 0) {
    return err("rerun takes one task id");
  }

  const taskId = TaskId.parse(text);

  if (!taskId.ok) {
    return err(`${JSON.stringify(text)} is not a task id: ${taskId.error.detail}`);
  }

  const reason = parsed.values.reason ?? DEFAULT_RERUN_REASON;

  if (reason.length === 0 || reason.length > RERUN_REASON_MAX_LENGTH) {
    return err(`--reason must have 1 to ${String(RERUN_REASON_MAX_LENGTH)} characters`);
  }

  return ok({ sourceTaskId: taskId.value, reason });
}
