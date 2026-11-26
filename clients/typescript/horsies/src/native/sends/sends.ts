// The functions of the `sends` area of the addon through the call wrapper: a throw or a rejection of a call is a
// defect under the defect rule, and a thrown envelope is its error.
//
// The caller checks each value first: user text with `checkText` (`native/values/text.ts`), task ids in their
// lower-case hyphenated form, and numbers in the bounds that the declarations name. A value that breaks such a rule
// is a `ClientDefect` of the addon.

import { nativeValue, nativeValueAsync } from "../call/envelope.ts";
import type { CoreHandle } from "../handle/core-handle.ts";
import type { TraceArgument } from "../values/records.ts";
import { sendsAddon } from "./addon.ts";
import type {
  AppConfigCheck,
  DeclaredCodesCheck,
  EnqueueOutcome,
  EnqueueRequest,
  NativeSendRules,
  NativeTaskError,
  NativeTaskOptions,
  PreparedSend,
  ProcessRoleName,
  ResultRead,
  SendFacts,
  SendKind,
  TaskErrorBuild,
  TaskErrorParts,
  TaskInfoRead,
  TaskOptionsCheck,
  TaskOptionsFacts,
  WaitBound,
} from "./addon.ts";

/**
 * The check of the JSON text of the core's `AppConfig`: the send rules, the producer manifest text and the coverage
 * values of an accepted config, or every refusal. Pure: starts no core.
 */
export function checkAppConfig(appConfigJson: string): AppConfigCheck {
  return nativeValue("checkAppConfig", () => sendsAddon().checkAppConfig(appConfigJson));
}

/** The check of a declared error code list: one refusal per refused entry, in list order. Pure. */
export function parseDeclaredErrorCodes(codes: readonly string[]): DeclaredCodesCheck {
  return nativeValue("parseDeclaredErrorCodes", () => sendsAddon().parseDeclaredErrorCodes([...codes]));
}

/** The check of the options of a task: the checked options, or every refusal in the core's order. Pure. */
export function checkTaskOptions(facts: TaskOptionsFacts): TaskOptionsCheck {
  return nativeValue("checkTaskOptions", () => sendsAddon().checkTaskOptions(facts));
}

/** The stored options text of a send of a task with no deadline. Pure. */
export function storedOptionsText(options: NativeTaskOptions): string {
  return nativeValue("storedOptionsText", () => options.storedText());
}

/**
 * The prepared facts of one send: a new task id, the options text, the retention and the priority, or the send error
 * of a refused fact. Mints a task id and starts the log forwarder of the process, so the payload warning reaches the
 * log sink; starts no core. Throws `CoreStopped` in a process whose core stopped, and `CoreStartFailed` when the log
 * forwarder cannot start.
 */
export function prepareSendFacts(rules: NativeSendRules, facts: SendFacts, options: NativeTaskOptions): PreparedSend {
  return nativeValue("prepareSendFacts", () => rules.prepareSendFacts(facts, options));
}

/**
 * One send on `handle`: the stored task id, or the send error. Starts the core of the process at the first call.
 * Writes: when a stop of the core ends it early, the thrown `CoreStopped` has the state `outcomeUnknown`.
 */
export async function enqueue(
  handle: CoreHandle,
  request: EnqueueRequest,
  sendKind: SendKind,
  trace: TraceArgument,
): Promise<EnqueueOutcome> {
  return nativeValueAsync("enqueue", () => sendsAddon().enqueue(handle.native, request, sendKind, trace));
}

/** The result read of one task, with its wait. Installs the schema once per handle first. Changes no task. */
export async function getResult(handle: CoreHandle, taskId: string, wait: WaitBound): Promise<ResultRead> {
  return nativeValueAsync("getResult", () => sendsAddon().getResult(handle.native, taskId, wait));
}

/** The info of one task: status, times, failed reason and every attempt, newest first. Changes no task. */
export async function taskInfo(handle: CoreHandle, taskId: string): Promise<TaskInfoRead> {
  return nativeValueAsync("taskInfo", () => sendsAddon().taskInfo(handle.native, taskId));
}

/** Whether this process runs task bodies. Pure. */
export function processRole(): ProcessRoleName {
  return nativeValue("processRole", () => sendsAddon().processRole());
}

/**
 * The core's task error of a body, or the refusal of its custom code. Starts the log forwarder of the process, so the
 * record of data that the codec cannot write reaches the log sink; starts no core. Throws `CoreStartFailed` when the
 * log forwarder cannot start.
 */
export function buildTaskError(parts: TaskErrorParts): TaskErrorBuild {
  return nativeValue("buildTaskError", () => sendsAddon().buildTaskError(parts));
}

/** The stored error object text of a task error, as the core writes it into a result envelope. Pure. */
export function storedErrorText(error: NativeTaskError): string {
  return nativeValue("storedErrorText", () => error.storedText());
}
