// The handle of a sent task: its id, the result read (`get`) and the info read (`info`). Built by a send, by
// `retrySend`, or by `app.taskHandle` from a stored id; it does no I/O until a read.

import { threadId } from "node:worker_threads";

import type { CoreAccess } from "../app/core-access.ts";
import { jsonTextCheckedByCore } from "../codec/json-text.ts";
import { decodeEnvelope } from "../codec/reader.ts";
import { refusalText } from "../codec/refusal.ts";
import { type CompiledContract, compileContract } from "../contract/compile.ts";
import type { DeclaredCodes } from "../contract/definition.ts";
import type { TaskId } from "../contract/ids.ts";
import type { ResultRead, ResultRecord, TaskStatusName, WaitBound } from "../native/sends/addon.ts";
import { taskInfo } from "../send/core-calls.ts";
import { AddonOutsideMainThread } from "../thrown-errors.ts";
import type { Kind } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { TaskError, TaskOutcome } from "../wire/task-error.ts";
import { MAX_WAIT_LIMIT_MS } from "../send/bounds.ts";
import { brokerReadError, type FinishedStatus, type ReadError, taskAbsence, type UnfinishedStatus } from "./errors.ts";
import { type TaskInfo, taskInfoOf } from "./info.ts";

/** The options of a result read. */
export interface GetOptions {
  /** The longest wait for a task that has not finished: whole milliseconds, 0 to 4,294,967,295. Left out: no bound. */
  readonly timeoutMs?: number;
}

/** The handle of a sent task. `O` is the output type, `E` the type of the task's codes. */
export interface TaskHandle<O, E extends string> {
  readonly taskId: TaskId;
  /** The task's outcome. The first final read is kept: a later call gives it with no I/O. */
  get(options?: GetOptions): Promise<Result<TaskOutcome<O, E>, ReadError>>;
  /** The status, times, failed reason and attempts. Not kept: a status changes. */
  info(): Promise<Result<TaskInfo, ReadError>>;
}

/** How the stored result of a contract reads: with its output kind and codes, or not at all (a refused contract). */
export type ReadForm =
  | { readonly kind: "readable"; readonly output: Kind; readonly codes: DeclaredCodes }
  | { readonly kind: "unreadable"; readonly cause: string };

/** The read form of a contract. Effects: compiles the contract at its first use (see `compileContract`). */
export function readFormOf(contract: object): ReadForm {
  const compiled = compileContract(contract);

  if (compiled.ok) {
    return readFormOfCompiled(compiled.value);
  }

  const causes = compiled.error.map((refused) => `${refused.path}: ${refused.detail}`).join("; ");

  return Object.freeze({ kind: "unreadable", cause: `the contract is refused: ${causes}` });
}

/** The read form of a compiled contract. Pure. */
export function readFormOfCompiled(compiled: CompiledContract): ReadForm {
  return Object.freeze({ kind: "readable", output: compiled.output, codes: compiled.record.codes });
}

/** A handle of `taskId` that reads through `core` with `form`. Pure. */
export function taskHandleOf<O, E extends string>(core: CoreAccess, form: ReadForm, taskId: TaskId): TaskHandle<O, E> {
  return new Handle<O, E>(core, form, taskId);
}

type Outcome = Result<TaskOutcome<unknown, string>, ReadError>;

/** The kept read of a handle: none yet, or the first final one. */
type KeptRead = { readonly kind: "none" } | { readonly kind: "kept"; readonly read: Outcome };

class Handle<O, E extends string> implements TaskHandle<O, E> {
  readonly taskId: TaskId;
  readonly #core: CoreAccess;
  readonly #form: ReadForm;
  #kept: KeptRead = { kind: "none" };

  constructor(core: CoreAccess, form: ReadForm, taskId: TaskId) {
    this.taskId = taskId;
    this.#core = core;
    this.#form = form;
  }

  async get(options: GetOptions = {}): Promise<Result<TaskOutcome<O, E>, ReadError>> {
    // SAFETY: the reader built the value from the contract's output kind, whose value type is `O`, and a user code is
    // one of the contract's codes (`E`) or the read is `unknownUserCode`.
    return (await this.#read(options)) as Result<TaskOutcome<O, E>, ReadError>;
  }

  async #read(options: GetOptions): Promise<Outcome> {
    const wait = waitBound(options.timeoutMs);

    if (!wait.ok) {
      return wait;
    }

    if (this.#kept.kind === "kept") {
      return this.#kept.read;
    }

    const read = await resultRead(this.#core, this.taskId, wait.value);

    if (read.kind === "waitInsideTask") {
      return err(Object.freeze({ kind: "waitInsideTask" }));
    }

    const outcome = outcomeOfRead(read.read, this.#form);

    if (!isFinal(outcome)) {
      return outcome;
    }

    // The first final read is kept: a final read of a concurrent call that ended first stays.
    if (this.#kept.kind === "none") {
      this.#kept = { kind: "kept", read: outcome };
    }

    return this.#kept.read;
  }

  async info(): Promise<Result<TaskInfo, ReadError>> {
    const read = await taskInfo(this.#core, this.taskId);

    switch (read.kind) {
      case "found":
        return ok(taskInfoOf(read.record));
      case "absent":
        return err(Object.freeze({ kind: "taskNotFound", absence: taskAbsence(read.reason) }));
      case "failed":
        return err(brokerReadError(read.error));
      default: {
        const unexpected: never = read;

        throw new TypeError(`info: a read outside its type: ${String(unexpected)}`);
      }
    }
  }
}

/** The wait of a `timeoutMs`, or `invalidWait`. Pure. */
export function waitBound(timeoutMs: number | undefined): Result<WaitBound, ReadError> {
  if (timeoutMs === undefined) {
    return ok({ kind: "untilEnd" });
  }

  return Number.isSafeInteger(timeoutMs) && timeoutMs >= 0 && timeoutMs <= MAX_WAIT_LIMIT_MS
    ? ok({ kind: "upTo", milliseconds: timeoutMs })
    : err(Object.freeze({ kind: "invalidWait", maxMs: MAX_WAIT_LIMIT_MS }));
}

/** A result read, or `waitInsideTask` when this process runs task bodies (decided before any statement). */
async function resultRead(
  core: CoreAccess,
  taskId: string,
  wait: WaitBound,
): Promise<{ readonly kind: "read"; readonly read: ResultRead } | { readonly kind: "waitInsideTask" }> {
  switch (core.kind) {
    case "seatRelay":
      return { kind: "waitInsideTask" };
    case "noCore":
      throw new AddonOutsideMainThread({ threadId });
    case "addon":
      return (await core.producer.processRole()) === "RUNS_TASK_BODIES"
        ? { kind: "waitInsideTask" }
        : { kind: "read", read: await core.producer.getResult(taskId, wait) };
    default: {
      const unexpected: never = core;

      throw new TypeError(`get: a core access outside its type: ${String(unexpected)}`);
    }
  }
}

/** The outcome of one result read under a read form. Pure. */
export function outcomeOfRead(read: ResultRead, form: ReadForm): Outcome {
  switch (read.kind) {
    case "found":
      return outcomeOfRecord(read.record, form);
    case "absent":
      return err(Object.freeze({ kind: "taskNotFound", absence: taskAbsence(read.reason) }));
    case "failed":
      return err(brokerReadError(read.error));
    default: {
      const unexpected: never = read;

      throw new TypeError(`get: a read outside its type: ${String(unexpected)}`);
    }
  }
}

function outcomeOfRecord(record: ResultRecord, form: ReadForm): Outcome {
  const status = record.status;

  if (status === "CANCELLED") {
    return ok(cancelledOutcome(record.taskId));
  }

  switch (record.envelope.kind) {
    case "notFinished":
      return err(notFinished(status));
    case "stored":
      return decodedOutcome(record.envelope.text, form);
    default: {
      const unexpected: never = record.envelope;

      throw new TypeError(`get: an envelope outside its type: ${String(unexpected)}`);
    }
  }
}

/**
 * The outcome of a cancelled task, whatever the row stores: the library code `TASK_CANCELLED`, as the Python and Rust
 * clients read it (`Task <id> was cancelled before completion`, data `{ task_id }`). Pure.
 */
function cancelledOutcome(taskId: string): TaskOutcome<unknown, string> {
  const error: TaskError<string> = {
    code: Object.freeze({ kind: "library", code: "TASK_CANCELLED" }),
    message: `Task ${taskId} was cancelled before completion`,
    data: Object.freeze({ task_id: taskId }),
    exception: null,
    subWorkflow: null,
  };

  return Object.freeze({ kind: "failed", error: Object.freeze(error) });
}

/** The error of a read that found no stored result: the wait ended first, or a finished task stores none. */
function notFinished(status: Exclude<TaskStatusName, "CANCELLED">): ReadError {
  const finished = finishedStatus(status);

  return finished.kind === "finished"
    ? Object.freeze({ kind: "resultNotAvailable", status: finished.status })
    : Object.freeze({ kind: "waitTimedOut", status: finished.status });
}

function finishedStatus(
  status: Exclude<TaskStatusName, "CANCELLED">,
): { readonly kind: "finished"; readonly status: FinishedStatus } | { readonly kind: "unfinished"; readonly status: UnfinishedStatus } {
  switch (status) {
    case "PENDING":
    case "CLAIMED":
    case "RUNNING":
      return { kind: "unfinished", status };
    case "COMPLETED":
    case "FAILED":
    case "EXPIRED":
      return { kind: "finished", status };
    default: {
      const unexpected: never = status;

      throw new TypeError(`get: a status outside its type: ${String(unexpected)}`);
    }
  }
}

/** The outcome of a stored envelope (a text that the core's strict read rule passed). Pure. */
function decodedOutcome(text: string, form: ReadForm): Outcome {
  switch (form.kind) {
    case "unreadable":
      return err(Object.freeze({ kind: "storedResultUnreadable", cause: form.cause }));
    case "readable": {
      const decoded = decodeEnvelope(form.output, jsonTextCheckedByCore(text), "worker");

      if (!decoded.ok) {
        return err(Object.freeze({ kind: "storedResultUnreadable", cause: refusalText(decoded.error) }));
      }

      return checkedUserCode(decoded.value, form.codes);
    }

    default: {
      const unexpected: never = form;

      throw new TypeError(`get: a read form outside its type: ${String(unexpected)}`);
    }
  }
}

/** The outcome, or `unknownUserCode` for a user code that a declared list does not hold. Pure. */
function checkedUserCode(outcome: TaskOutcome<unknown, string>, codes: DeclaredCodes): Outcome {
  if (outcome.kind === "succeeded" || outcome.error.code.kind === "library" || codes.kind === "open") {
    return ok(outcome);
  }

  return codes.codes.includes(outcome.error.code.code)
    ? ok(outcome)
    : err(Object.freeze({ kind: "unknownUserCode", code: outcome.error.code.code }));
}

/** Whether a read is final, so the handle keeps it: an outcome, or a stored result that never reads. Pure. */
function isFinal(read: Outcome): boolean {
  if (read.ok) {
    return true;
  }

  switch (read.error.kind) {
    case "resultNotAvailable":
    case "storedResultUnreadable":
    case "unknownUserCode":
      return true;
    case "waitTimedOut":
    case "taskNotFound":
    case "waitInsideTask":
    case "database":
    case "invalidWait":
      return false;
    default: {
      const unexpected: never = read.error;

      throw new TypeError(`get: a read error outside its type: ${String(unexpected)}`);
    }
  }
}
