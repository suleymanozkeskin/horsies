// The body adapter: one run of a task body, from the stored input text to the stored outcome. The child and
// `implementation.run` share it.
//
// 1. The input decodes with the contract's parameters, in the worker form (defaults filled). A refusal is
//    `WORKER_SERIALIZATION_ERROR`: its text names the input, the field path and the kind of the failure, never a
//    value. In a child, the `workflow_ctx` member of the input of a task with `workflowContext: true` is taken out
//    first and given as the runtime's `workflowContext` (`workflow-input.ts`); a member that does not read is
//    `WORKER_SERIALIZATION_ERROR` too, as the Rust client's input decode gives it.
// 2. The body runs inside the run store, with the run held in the loop marks for its synchronous part, through the
//    body observer. Its second argument is one frozen object: the own members of the run's runtime and the
//    contract's `ok` and `err`. A returned Promise is awaited.
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
import { readTree, type TreeValue } from "../codec/number-text.ts";
import { decodeParameterTree } from "../codec/reader.ts";
import { errorDataText, type StoredErrorData } from "../codec/error-data.ts";
import type { JsonText } from "../codec/json-text.ts";
import { refusalText, type CodecRefused } from "../codec/refusal.ts";
import { isObjectLike, isString } from "../codec/values.ts";
import type { CompiledContract } from "../contract/compile.ts";
import type { ResultConstructors } from "../contract/define-task.ts";
import type { ImplementationParts } from "../contract/implementation.ts";
import type { OutputSchema } from "../contract/schema.ts";
import type { ErrorDataPart, OptionalText, TaskErrorParts } from "../native/sends/addon.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { ContextRefused } from "../workflow/context.ts";
import type { BodyEnd, BodyObservation } from "./body-observer.ts";
import type { LoopMarks } from "./run-table.ts";
import { type RunMark, runStore } from "./run-store.ts";
import { type CancelState, type RunControls, taskRuntimeOf, type ThreadTaskRuntime } from "./runtime.ts";
import { thrownRecord } from "./thrown-error.ts";
import { workflowInputOf } from "./workflow-input.ts";

/** The stored outcome of a body: the text of its value, or the parts of its task error and the text of its data. */
export type StoredBodyOutcome =
  | { readonly kind: "value"; readonly json: JsonText }
  | { readonly kind: "error"; readonly parts: TaskErrorParts; readonly data: JsonText };

/** Where the input and the runtime of a run come from. */
export type BodyStart =
  /**
   * A run of a child: the input that the core posted, and the controls of the run. The runtime holds the workflow
   * context of the input (`noContext` for a task that takes none).
   */
  | { readonly kind: "posted"; readonly inputJson: JsonText; readonly controls: RunControls }
  /** A test run (`implementation.run`): the input that the contract wrote, and the runtime of the test. */
  | { readonly kind: "given"; readonly inputJson: JsonText; readonly runtime: ThreadTaskRuntime };

/** One run of a body. */
export interface BodyRun {
  readonly start: BodyStart;
  readonly implementation: ImplementationParts;
  readonly compiled: CompiledContract;
  readonly mark: RunMark;
  readonly marks: LoopMarks;
  readonly observation: BodyObservation;
}

/** The decoded input of a body and its runtime. */
type PreparedBody = { readonly input: DecodedParameters; readonly runtime: ThreadTaskRuntime };

/** The parameters tree of a run input and the runtime of the run. */
type InputParts = { readonly parameters: TreeValue; readonly runtime: ThreadTaskRuntime };

/** The parameters of a task input, decoded with the contract's parameter kinds. */
type DecodedParameters = Readonly<Record<string, unknown>>;

/** What a body returned (or its Promise resolved with), before its read. */
type BodyReturn = { readonly returned: unknown };

/**
 * The second argument of a body: the members of its runtime and the result constructors of its contract. The type of
 * the run's runtime is erased to the members of every class.
 */
type BodyArgument = ThreadTaskRuntime & ResultConstructors<OutputSchema, string>;

/** The body of an implementation, with its types erased: the adapter decodes its input with the contract's kinds. */
type ErasedBody = (input: DecodedParameters, runtime: BodyArgument) => unknown;

/** Run the body of `run` and give its stored outcome. Never rejects. */
export async function runBody(run: BodyRun): Promise<StoredBodyOutcome> {
  const prepared = preparedBody(run);

  if (!prepared.ok) {
    return ended(run, prepared.error);
  }

  const outcome = await calledBody(run, prepared.value);

  return ended(run, outcome);
}

/** The decoded input and the runtime of `run`, or the `WORKER_SERIALIZATION_ERROR` of an input that does not decode. */
function preparedBody(run: BodyRun): Result<PreparedBody, StoredBodyOutcome> {
  const taskName = run.mark.taskName;
  const parts = inputParts(run);

  if (!parts.ok) {
    return parts;
  }

  const input = decodeParameterTree(run.compiled.parameters, parts.value.parameters, "worker");

  if (!input.ok) {
    return err(serializationError(taskName, "input", `task input does not decode as ${taskName}.input`, input.error));
  }

  // SAFETY: the parameters kind of a contract is an object kind, so its decode gives an object of the parameters.
  return ok({ input: input.value as DecodedParameters, runtime: parts.value.runtime });
}

/**
 * The parameters of the input of `run` and its runtime: a test gives the runtime; a child builds it with the workflow
 * context that the input holds.
 */
function inputParts(run: BodyRun): Result<InputParts, StoredBodyOutcome> {
  const start = run.start;
  const tree = readTree(start.inputJson);

  switch (start.kind) {
    case "given":
      return ok({ parameters: tree, runtime: start.runtime });
    case "posted": {
      const split = workflowInputOf(tree, run.compiled.record.context);

      return split.ok
        ? ok({ parameters: split.value.parameters, runtime: taskRuntimeOf(start.controls, split.value.context) })
        : err(contextRefusedOutcome(run.mark.taskName, split.error));
    }

    default:
      return unreachable(start, "inputParts");
  }
}

/** The outcome of the body call with the decoded input and the runtime of `prepared`. */
async function calledBody(run: BodyRun, prepared: PreparedBody): Promise<StoredBodyOutcome> {
  // SAFETY: the parts keep the body with its types erased; the input decoded with the contract's parameters and
  // the runtime is the one of the run, so the body gets the values its static types name.
  const body = run.implementation.body as ErasedBody;
  const argument = bodyArgumentOf(prepared.runtime, run.implementation.resultConstructors);
  let returned: unknown;

  const before = run.marks.hold(run.mark.taskId);

  try {
    returned = runStore.run(run.mark, () => run.observation.run(() => body(prepared.input, argument)));
  } catch (cause) {
    return thrownOutcome(run, cause);
  } finally {
    run.marks.restore(before);
  }

  try {
    return returnedOutcome(run, { returned: await returned });
  } catch (cause) {
    return thrownOutcome(run, cause);
  }
}

/**
 * The second argument of a body: the own members of `runtime` (a child's runtime, or the runtime of a test) and the
 * result constructors of the contract, in one frozen object. Pure.
 */
function bodyArgumentOf(
  runtime: ThreadTaskRuntime,
  constructors: ResultConstructors<OutputSchema, string>,
): BodyArgument {
  return Object.freeze({ ...runtime, ok: constructors.ok, err: constructors.err });
}

/** Give the observer how the body ended, then give the outcome. */
function ended(run: BodyRun, outcome: StoredBodyOutcome): StoredBodyOutcome {
  run.observation.end(bodyEnd(run, outcome));

  return outcome;
}

/** How the body ended: the run's cancel cause when its signal fired, else the outcome. */
function bodyEnd(run: BodyRun, outcome: StoredBodyOutcome): BodyEnd {
  const state = cancelStateOf(run.start);

  switch (state.kind) {
    case "requested":
      return state.cause === "DEADLINE" ? { kind: "timedOut" } : { kind: "cancelled" };
    case "notRequested":
      return outcome.kind === "value" ? { kind: "returned" } : { kind: "failed", code: outcome.parts.code.code };
    default:
      return unreachable(state, "bodyEnd");
  }
}

/** The cancel state of the run of `start`: its controls in a child, the given runtime in a test. */
function cancelStateOf(start: BodyStart): CancelState {
  switch (start.kind) {
    case "posted":
      return start.controls.cancelState();
    case "given":
      return start.runtime.cancelState();
    default:
      return unreachable(start, "cancelStateOf");
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
      return userFailureOutcome(result.failure);
    case "notAResult":
      return notAResult(taskName, result.actualType);
    default:
      return unreachable(result, "returnedOutcome");
  }
}

/** A user failure as the body gave it to `err`. */
type UserFailureRead = { readonly code: string; readonly message: OptionalText; readonly data: BodyErrorData };

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
    data: dataOfFailure(cause),
  };
}

/** The type of a returned value for a refusal text: the type rule of a thrown value. */
function actualTypeOf(cause: unknown): string {
  return thrownRecord(cause).type;
}

const NO_TEXT: OptionalText = Object.freeze({ kind: "none" });

/** The data of a stored error: the part that the core builds the error with, and the text that a test run reads. */
type BodyErrorData = { readonly part: ErrorDataPart; readonly text: JsonText };

/** The data of an `err` with no `data`: no part; a test run reads `null`. */
const NO_ERROR_DATA: BodyErrorData = Object.freeze({ part: Object.freeze({ kind: "noData" }), text: errorDataText(null).text });

/** The data of the failure `cause` that `err` got: none when it has no `data`. */
function dataOfFailure(cause: object): BodyErrorData {
  const data: unknown = Reflect.get(cause, "data");

  return data === undefined ? NO_ERROR_DATA : storedErrorData(data);
}

/** The data of the stored error of the data `cause`: its part and the codec's text. Pure. */
function storedErrorData(cause: unknown): BodyErrorData {
  const stored = errorDataText(cause);

  return { part: partOfStored(stored), text: stored.text };
}

/**
 * The data part of the stored error of the data `cause`: the codec's text; or, for data that the codec replaces, the
 * reason that the bridge stores with Python's cause (and logs on `horsies.codec`). A replacement for data deeper than
 * Pydantic's dump writes has no reason in the bridge: its replacement text is stored as it is.
 */
export function errorDataPart(cause: unknown): ErrorDataPart {
  return partOfStored(errorDataText(cause));
}

function partOfStored(stored: StoredErrorData): ErrorDataPart {
  switch (stored.state) {
    case "stored":
      return { kind: "text", text: stored.text };
    case "replaced":
      switch (stored.cause) {
        case "StrictJsonError":
          return { kind: "notStorable", reason: "notJsonNative" };
        case "SerializationError":
          return { kind: "notStorable", reason: "integerTooLong" };
        case "ValueValidationError":
          return { kind: "text", text: stored.text };
        default:
          return unreachable(stored.cause, "errorDataPart");
      }

    default:
      return unreachable(stored, "errorDataPart");
  }
}

/** The parts of a stored error with its data, and the text of the data. Pure. */
function errorOutcome(parts: Omit<TaskErrorParts, "data">, data: BodyErrorData): StoredBodyOutcome {
  return { kind: "error", parts: { ...parts, data: data.part }, data: data.text };
}

/** The outcome of a user failure. Pure. */
function userFailureOutcome(failure: UserFailureRead): StoredBodyOutcome {
  return errorOutcome(
    { code: { kind: "user", code: failure.code }, message: failure.message, exception: { kind: "noException" } },
    failure.data,
  );
}

/** The outcome of a thrown value: the observer's `thrown`, then the exception map. */
function thrownOutcome(run: BodyRun, cause: unknown): StoredBodyOutcome {
  run.observation.thrown(cause);

  const record = thrownRecord(cause);
  const mapped = mappedCode(run.implementation, cause);

  return errorOutcome(
    {
      code: mapped === undefined ? { kind: "library", code: "UNHANDLED_EXCEPTION" } : { kind: "user", code: mapped },
      message: { kind: "text", text: `Unhandled exception in task ${run.mark.taskName}: ${record.type}: ${record.message}` },
      exception: {
        kind: "record",
        className: record.type,
        module: "",
        message: record.message,
        repr: record.repr,
        traceback: record.traceback,
      },
    },
    storedErrorData({ exception_type: record.type }),
  );
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

  return errorOutcome(
    {
      code: { kind: "library", code: "WORKER_SERIALIZATION_ERROR" },
      message: { kind: "text", text: message },
      exception: { kind: "noException" },
    },
    storedErrorData({ expected_type: `${taskName}.${side}`, validation_error: message }),
  );
}

/**
 * `WORKER_SERIALIZATION_ERROR` for an input whose `workflow_ctx` member does not read as a workflow context: the text
 * names the member and the rule, never a value. Pure.
 */
function contextRefusedOutcome(taskName: string, refused: ContextRefused): StoredBodyOutcome {
  const message = `task input does not decode as ${taskName}.input: the workflow context ${refused.member}: ${refused.detail}`;

  return errorOutcome(
    {
      code: { kind: "library", code: "WORKER_SERIALIZATION_ERROR" },
      message: { kind: "text", text: message },
      exception: { kind: "noException" },
    },
    storedErrorData({ expected_type: `${taskName}.input`, validation_error: message }),
  );
}

/** `RETURN_TYPE_MISMATCH` of a return that is neither `ok(value)` nor `err(failure)` (the Python client's form). */
function notAResult(taskName: string, actualType: string): StoredBodyOutcome {
  return errorOutcome(
    {
      code: { kind: "library", code: "RETURN_TYPE_MISMATCH" },
      message: {
        kind: "text",
        text: `Task ${taskName} returned ${actualType} but must return ok(value) or err({ code })`,
      },
      exception: { kind: "noException" },
    },
    storedErrorData({ variant: "not_a_task_result", expected_type: "Result", actual_type: actualType, validation_errors: [] }),
  );
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
    data: errorDataPart({
      variant: "err",
      expected_type: "a code of the task",
      actual_type: "Err",
      validation_errors: [{ loc: ["code"], msg: cause, type: "user_code" }],
    }),
    exception: { kind: "noException" },
  };
}
