// The body adapter: one run of a task body, from the stored input text to the stored outcome. The child and
// `implementation.run` share it.
//
// 1. The input decodes with the contract's parameters, in the worker form (defaults filled). A refusal is
//    `WORKER_SERIALIZATION_ERROR`: its text names the input, the field path and the kind of the failure, never a
//    value.
// 2. The body runs inside the run store, with the run held in the loop marks for its synchronous part, through the
//    body observer. A returned Promise is awaited.
// 3. The outcome:
//    - `ok(value)`: the value encodes with the output kind and reads back; a refusal is `WORKER_SERIALIZATION_ERROR`;
//    - `err({ code, message, data })`: the stored error with the user code;
//    - a thrown value (or a rejection): the observer's `thrown`, then the exception map: the first entry whose class
//      the value is an instance of gives its user code, else `UNHANDLED_EXCEPTION`; the message
//      `Unhandled exception in task <name>: <type>: <text>`, `data.exception_type`, and the exception record;
//    - another return: `RETURN_TYPE_MISMATCH`.
// 4. The observer's `end`.
// The adapter never throws and never rejects: a value that the body throws is an outcome.

import { encodeWithReadBack } from "../codec/read-back.ts";
import { decodeParameters } from "../codec/reader.ts";
import { errorDataText } from "../codec/error-data.ts";
import type { JsonText } from "../codec/json-text.ts";
import { refusalText, type CodecRefused } from "../codec/refusal.ts";
import { isObjectLike, isString } from "../codec/values.ts";
import type { CompiledContract } from "../contract/compile.ts";
import type { ImplementationParts } from "../contract/implementation.ts";
import type { OptionalText, TaskErrorParts } from "../native/sends/addon.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { BodyEnd, BodyObservation } from "./body-observer.ts";
import type { LoopMarks } from "./run-table.ts";
import { type RunMark, runStore } from "./run-store.ts";
import type { TaskRuntime } from "./runtime.ts";
import { thrownRecord } from "./thrown-error.ts";

/** The stored outcome of a body: the text of its value, or the parts of its task error. */
export type StoredBodyOutcome =
  | { readonly kind: "value"; readonly json: JsonText }
  | { readonly kind: "error"; readonly parts: TaskErrorParts };

/** One run of a body. */
export interface BodyRun {
  readonly inputJson: JsonText;
  readonly implementation: ImplementationParts;
  readonly compiled: CompiledContract;
  readonly runtime: TaskRuntime;
  readonly mark: RunMark;
  readonly marks: LoopMarks;
  readonly observation: BodyObservation;
}

/** The parameters of a task input, decoded with the contract's parameter kinds. */
type DecodedParameters = Readonly<Record<string, unknown>>;

/** What a body returned (or its Promise resolved with), before its read. */
type BodyReturn = { readonly returned: unknown };

/** The body of an implementation, with its types erased: the adapter decodes its input with the contract's kinds. */
type ErasedBody = (input: DecodedParameters, runtime: TaskRuntime) => unknown;

/** Run the body of `run` and give its stored outcome. Never rejects. */
export async function runBody(run: BodyRun): Promise<StoredBodyOutcome> {
  const taskName = run.mark.taskName;
  const input = decodeParameters(run.compiled.parameters, run.inputJson, "worker");

  if (!input.ok) {
    return ended(run, serializationError(taskName, "input", `task input does not decode as ${taskName}.input`, input.error));
  }

  // SAFETY: the parameters kind of a contract is an object kind, so its decode gives an object of the parameters.
  const outcome = await calledBody(run, input.value as DecodedParameters);

  return ended(run, outcome);
}

/** The outcome of the body call with the decoded `input`. */
async function calledBody(run: BodyRun, input: DecodedParameters): Promise<StoredBodyOutcome> {
  // SAFETY: the parts keep the body with its types erased; the input decoded with the contract's parameters and
  // the runtime is the one of the run, so the body gets the values its static types name.
  const body = run.implementation.body as ErasedBody;
  let returned: unknown;

  run.marks.hold(run.mark.taskId);

  try {
    returned = runStore.run(run.mark, () => run.observation.run(() => body(input, run.runtime)));
  } catch (cause) {
    return thrownOutcome(run, cause);
  } finally {
    run.marks.release();
  }

  try {
    return returnedOutcome(run, { returned: await returned });
  } catch (cause) {
    return thrownOutcome(run, cause);
  }
}

/** Give the observer how the body ended, then give the outcome. */
function ended(run: BodyRun, outcome: StoredBodyOutcome): StoredBodyOutcome {
  run.observation.end(bodyEnd(run, outcome));

  return outcome;
}

/** How the body ended: the run's cancel cause when its signal fired, else the outcome. */
function bodyEnd(run: BodyRun, outcome: StoredBodyOutcome): BodyEnd {
  const state = run.runtime.cancelState();

  switch (state.kind) {
    case "requested":
      return state.cause === "DEADLINE" ? { kind: "timedOut" } : { kind: "cancelled" };
    case "notRequested":
      return outcome.kind === "value" ? { kind: "returned" } : { kind: "failed", code: outcome.parts.code.code };
    default:
      return unreachable(state, "bodyEnd");
  }
}

/** The outcome of a value that the body returned: `ok(value)`, `err(failure)`, or another value. */
function returnedOutcome(run: BodyRun, body: BodyReturn): StoredBodyOutcome {
  const taskName = run.mark.taskName;
  const result = resultOf(body);

  switch (result.kind) {
    case "ok": {
      const written = encodeWithReadBack(run.compiled.output, result.value);

      return written.ok
        ? { kind: "value", json: written.value }
        : serializationError(taskName, "output", `task returned a value of type ${taskName}.output`, written.error);
    }

    case "err":
      return { kind: "error", parts: userFailureParts(result.failure) };
    case "notAResult":
      return notAResult(taskName, result.actualType);
    default:
      return unreachable(result, "returnedOutcome");
  }
}

/** A user failure as the body gave it to `err`. */
type UserFailureRead = { readonly code: string; readonly message: OptionalText; readonly data: unknown };

/** What a body returned. */
type ResultRead =
  | { readonly kind: "ok"; readonly value: unknown }
  | { readonly kind: "err"; readonly failure: UserFailureRead }
  | { readonly kind: "notAResult"; readonly actualType: string };

/** The read of a returned value: `ok(value)`, `err({ code, message?, data? })`, or another value. Pure. */
function resultOf(body: BodyReturn): ResultRead {
  const returned = body.returned;

  if (!isObjectLike(returned)) {
    return { kind: "notAResult", actualType: actualTypeOf(returned) };
  }

  const flag: unknown = Reflect.get(returned, "ok");

  if (flag === true && Object.hasOwn(returned, "value")) {
    return { kind: "ok", value: Reflect.get(returned, "value") };
  }

  if (flag === false && Object.hasOwn(returned, "error")) {
    const failure = failureOf(Reflect.get(returned, "error"));

    return failure === undefined ? { kind: "notAResult", actualType: "Err" } : { kind: "err", failure };
  }

  return { kind: "notAResult", actualType: actualTypeOf(returned) };
}

/** The user failure of `err(failure)`, or `undefined` when its code or message is not a text. */
function failureOf(cause: unknown): UserFailureRead | undefined {
  if (!isObjectLike(cause)) {
    return undefined;
  }

  const code: unknown = Reflect.get(cause, "code");
  const message: unknown = Reflect.get(cause, "message");

  if (!isString(code) || !(message === undefined || isString(message))) {
    return undefined;
  }

  return {
    code,
    message: message === undefined ? NO_TEXT : { kind: "text", text: message },
    data: Reflect.get(cause, "data") ?? null,
  };
}

/** The type of a returned value for a refusal text: the type rule of a thrown value. */
function actualTypeOf(cause: unknown): string {
  return thrownRecord(cause).type;
}

const NO_TEXT: OptionalText = Object.freeze({ kind: "none" });

/** The parts of the stored error of a user failure. Pure. */
function userFailureParts(failure: UserFailureRead): TaskErrorParts {
  return {
    code: { kind: "user", code: failure.code },
    message: failure.message,
    dataJson: errorDataText(failure.data).text,
    exception: { kind: "noException" },
  };
}

/** The outcome of a thrown value: the observer's `thrown`, then the exception map. */
function thrownOutcome(run: BodyRun, cause: unknown): StoredBodyOutcome {
  run.observation.thrown(cause);

  const record = thrownRecord(cause);
  const mapped = mappedCode(run.implementation, cause);

  return {
    kind: "error",
    parts: {
      code: mapped === undefined ? { kind: "library", code: "UNHANDLED_EXCEPTION" } : { kind: "user", code: mapped },
      message: { kind: "text", text: `Unhandled exception in task ${run.mark.taskName}: ${record.type}: ${record.message}` },
      dataJson: errorDataText({ exception_type: record.type }).text,
      exception: {
        kind: "record",
        className: record.type,
        module: "",
        message: record.message,
        repr: record.repr,
        traceback: record.traceback,
      },
    },
  };
}

/** The user code of the first entry of the exception map whose class `cause` is an instance of. */
function mappedCode(implementation: ImplementationParts, cause: unknown): string | undefined {
  for (const [errorClass, code] of implementation.exceptions) {
    if (isInstance(cause, errorClass)) {
      return code;
    }
  }

  return undefined;
}

/** Whether `cause` is an instance of `errorClass`; `false` when the check throws (a `Proxy`). */
function isInstance(cause: unknown, errorClass: ImplementationParts["exceptions"][number][0]): boolean {
  try {
    return cause instanceof errorClass;
  } catch {
    return false;
  }
}

/** `WORKER_SERIALIZATION_ERROR` for the input or the output of a task: the text and the data name no value. Pure. */
function serializationError(
  taskName: string,
  side: "input" | "output",
  subject: string,
  refused: CodecRefused,
): StoredBodyOutcome {
  const message = `${subject}: ${refusalText(refused)}`;

  return {
    kind: "error",
    parts: {
      code: { kind: "library", code: "WORKER_SERIALIZATION_ERROR" },
      message: { kind: "text", text: message },
      dataJson: errorDataText({ expected_type: `${taskName}.${side}`, validation_error: message }).text,
      exception: { kind: "noException" },
    },
  };
}

/** `RETURN_TYPE_MISMATCH` of a return that is neither `ok(value)` nor `err(failure)` (the Python client's form). */
function notAResult(taskName: string, actualType: string): StoredBodyOutcome {
  return {
    kind: "error",
    parts: {
      code: { kind: "library", code: "RETURN_TYPE_MISMATCH" },
      message: {
        kind: "text",
        text: `Task ${taskName} returned ${actualType} but must return ok(value) or err({ code })`,
      },
      dataJson: errorDataText({
        variant: "not_a_task_result",
        expected_type: "Result",
        actual_type: actualType,
        validation_errors: [],
      }).text,
      exception: { kind: "noException" },
    },
  };
}

/**
 * The parts of `RETURN_TYPE_MISMATCH` for an `err` whose user code the core refuses (`cause` names the rule): the
 * stored error then names the task and the rule. Pure.
 */
export function refusedCodeParts(taskName: string, code: string, cause: string): TaskErrorParts {
  const message = `Task ${taskName} returned err with the error code ${JSON.stringify(code)}, which is refused: ${cause}`;

  return {
    code: { kind: "library", code: "RETURN_TYPE_MISMATCH" },
    message: { kind: "text", text: message },
    dataJson: errorDataText({
      variant: "err",
      expected_type: "a code of the task",
      actual_type: "Err",
      validation_errors: [{ loc: ["code"], msg: cause, type: "user_code" }],
    }).text,
    exception: { kind: "noException" },
  };
}
