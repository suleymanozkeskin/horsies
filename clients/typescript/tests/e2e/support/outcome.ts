// The reads of the tests: the outcome of a task handle, its value, its error, and the code of an error. A read that
// fails, or an outcome of the other kind, fails the test with the task id and the cause.
//
// `errorCode` is the one place that reads the code of a stored error, so a change of the error form changes only it.

import assert from "node:assert/strict";

import type { JsonValue, ReadError, TaskError, TaskHandle, TaskInfo, TaskOutcome, TaskSendError } from "../../../horsies/src/index.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import { pause } from "./process.ts";

/** The interval of `pollUntilInfo`. */
const INFO_POLL_MS = 200;

/** Who chose the code of an error: the task (a custom code) or horsies (a built-in code). */
export type CodeSource = "custom" | "builtIn";

/** The code of a stored error and who chose it. */
export type HorsiesErrorCode = { readonly source: CodeSource; readonly code: string };

/** The code of `error`. The one reader of the code form of a stored error. Pure. */
export function errorCode(error: TaskError): HorsiesErrorCode {
  return { source: error.kind, code: error.code };
}

/** The text of a read error, for a message. Pure. */
export function readErrorText(error: ReadError): string {
  switch (error.kind) {
    case "waitTimeout":
      return `the wait ended with the task ${error.status}`;
    case "taskNotFound":
      return `no task (${error.reason})`;
    case "resultNotAvailable":
      return `the task is ${error.status} and stores no result`;
    case "storedResultUnreadable":
      return `the stored result does not read: ${error.cause}`;
    case "unknownCustomCode":
      return `the stored error has the undeclared code ${error.storedCode}`;
    case "waitInsideTask":
      return "a wait inside a task";
    case "databaseError":
      return `the database read failed: ${error.databaseCode}: ${error.message}`;
    case "invalidInput":
      return `the wait is not 0 to ${String(error.maxMs)} ms`;
    case "seatRelayFull":
      return `the seat relay did not send the read: ${error.message}`;
  }
}

/** The handle of a send of `subject`. Fails the test when the send fails. */
export async function sentHandle<O, E extends string>(
  subject: string,
  send: Promise<Result<TaskHandle<O, E>, TaskSendError<O, E>>>,
): Promise<TaskHandle<O, E>> {
  const result = await send;

  if (!result.ok) {
    assert.fail(`send ${subject}: ${result.error.code}: ${result.error.message}`);
  }

  return result.value;
}

/** The outcome of `handle` within `limitMs`. Fails the test when the read fails. */
export async function outcomeOf<O, E extends string>(handle: TaskHandle<O, E>, limitMs: number): Promise<TaskOutcome<O, E>> {
  const read = await handle.get({ waitMs: limitMs });

  if (!read.ok) {
    assert.fail(`read the outcome of task ${handle.taskId}: ${readErrorText(read.error)}`);
  }

  return read.value;
}

/** The value of a task that succeeded within `limitMs`. Fails the test for a failed task, with its error. */
export async function succeeded<O, E extends string>(handle: TaskHandle<O, E>, limitMs: number): Promise<O> {
  const outcome = await outcomeOf(handle, limitMs);

  if (outcome.kind === "failed") {
    assert.fail(`task ${handle.taskId} failed: ${errorText(outcome.error)}`);
  }

  return outcome.value;
}

/** The error of a task that failed within `limitMs`. Fails the test for a task that succeeded. */
export async function failed<O, E extends string>(handle: TaskHandle<O, E>, limitMs: number): Promise<TaskError<E>> {
  const outcome = await outcomeOf(handle, limitMs);

  if (outcome.kind === "succeeded") {
    assert.fail(`task ${handle.taskId} succeeded with ${JSON.stringify(outcome.value)}`);
  }

  return outcome.error;
}

/** The code text of a task that failed within `limitMs`. */
export async function failedCode<O, E extends string>(handle: TaskHandle<O, E>, limitMs: number): Promise<string> {
  return errorCode(await failed(handle, limitMs)).code;
}

/** The info of `handle`. Fails the test when the read fails. */
export async function infoOf<O, E extends string>(handle: TaskHandle<O, E>): Promise<TaskInfo> {
  const read = await handle.info();

  if (!read.ok) {
    assert.fail(`read the info of task ${handle.taskId}: ${readErrorText(read.error)}`);
  }

  return read.value;
}

/** Read the info of `handle` every 200 ms until `reached` answers true. Fails the test when `limitMs` passes first. */
export async function pollUntilInfo<O, E extends string>(
  handle: TaskHandle<O, E>,
  limitMs: number,
  subject: string,
  reached: (info: TaskInfo) => boolean,
): Promise<TaskInfo> {
  const deadline = performance.now() + limitMs;

  for (;;) {
    const read = await handle.info();

    if (read.ok && reached(read.value)) {
      return read.value;
    }

    if (performance.now() >= deadline) {
      assert.fail(`${subject}: not reached within ${String(limitMs)} ms (${read.ok ? `status ${read.value.status}` : readErrorText(read.error)})`);
    }

    await pause(INFO_POLL_MS);
  }
}

/** The members of the data of `error`. Fails the test when the data is not a JSON object. */
export function dataMembers(error: TaskError): Readonly<Record<string, JsonValue>> {
  const data = error.data;

  if (data === null || isJsonArray(data) || !(data instanceof Object)) {
    assert.fail(`the error data is not an object: ${JSON.stringify(data)}`);
  }

  return data;
}

function isJsonArray(value: JsonValue): value is readonly JsonValue[] {
  return Array.isArray(value);
}

/** The text of an error, for a message. Pure. */
export function errorText(error: TaskError): string {
  const code = errorCode(error);

  return `${code.source} code ${code.code}: ${error.message ?? "no message"}; data ${JSON.stringify(error.data)}`;
}
