// `implementation.run(input, runtime)`: a run of a task body with no worker, no database and no addon. It checks the
// input and the output with the contract's types, as a worker does:
// 1. the input encodes with the contract's parameters and reads back (as a send does); a refusal is the outcome
//    `WORKER_SERIALIZATION_ERROR`, with no body call;
// 2. the body runs through the body adapter of the child (the input decode, the body, the read-back of the value,
//    the error mapping);
// 3. the stored outcome reads as a task read gives it: the value, or the task error.
// A user code that the core's rules would refuse is not checked here: that check needs the addon.

import { LIBRARY_TASK_CODES, MAX_JSON_DEPTH } from "../codec/bounds.ts";
import { encodeParametersWithReadBack } from "../codec/read-back.ts";
import { decodeValue } from "../codec/reader.ts";
import { decodeJsonNode } from "../codec/json-value.ts";
import { readTree } from "../codec/number-text.ts";
import { refusalText, ROOT_PATH } from "../codec/refusal.ts";
import { runBody, type StoredBodyOutcome } from "../child/body.ts";
import { NO_BODY_OBSERVER } from "../child/body-observer.ts";
import type { LoopHolder, LoopMarks } from "../child/run-table.ts";
import type { ThreadTaskRuntime } from "../child/runtime.ts";
import { type CompiledContract, compileContract } from "../contract/compile.ts";
import type { ImplementationParts } from "../contract/implementation.ts";
import { ClientDefect } from "../thrown-errors.ts";
import type { LibraryTaskCode, TaskError, TaskOutcome } from "../wire/task-error.ts";
import { unreachable } from "../wire/unreachable.ts";

/** The loop marks of a test run: no loop watch runs. */
const NO_LOOP_MARKS: LoopMarks = Object.freeze({
  hold: (): LoopHolder => NO_HOLDER,
  restore: (): void => undefined,
});

const NO_HOLDER: LoopHolder = Object.freeze({ kind: "noRun" });

/** Run the body of `implementation` with `input` and `runtime`; give its outcome as a task read gives it. */
export async function runImplementation(
  implementation: ImplementationParts,
  input: object,
  runtime: ThreadTaskRuntime,
): Promise<TaskOutcome<unknown>> {
  const compiled = compileContract(implementation.contract);

  if (!compiled.ok) {
    throw new ClientDefect({ subject: "implementation.run", rule: "the contract of the implementation does not compile" });
  }

  const taskName = compiled.value.record.name;
  const written = encodeParametersWithReadBack(compiled.value.parameters, input);

  if (!written.ok) {
    const message = `task input does not encode as ${taskName}.input: ${refusalText(written.error)}`;

    return failed({
      code: { kind: "library", code: "WORKER_SERIALIZATION_ERROR" },
      message,
      data: { expected_type: `${taskName}.input`, validation_error: message },
      exception: null,
      subWorkflow: null,
    });
  }

  const outcome = await runBody({
    inputJson: written.value,
    implementation,
    compiled: compiled.value,
    runtime,
    mark: { taskId: runtime.taskId, taskName, attempt: runtime.fencingToken.value },
    marks: NO_LOOP_MARKS,
    observation: NO_BODY_OBSERVER.enter({ kind: "notRecorded" }),
  });

  return outcomeOf(compiled.value, outcome);
}

function failed(error: TaskError): TaskOutcome<unknown> {
  return Object.freeze({ kind: "failed", error: Object.freeze(error) });
}

/** The read of a stored outcome. A text that the adapter wrote and that does not read again is a defect. */
function outcomeOf(compiled: CompiledContract, outcome: StoredBodyOutcome): TaskOutcome<unknown> {
  switch (outcome.kind) {
    case "value": {
      const value = decodeValue(compiled.output, outcome.json, "worker");

      if (!value.ok) {
        throw new ClientDefect({ subject: "implementation.run", rule: "a value that read back does not read again" });
      }

      return Object.freeze({ kind: "succeeded", value: value.value });
    }

    case "error":
      return failed(storedError(outcome));
    default:
      return unreachable(outcome, "outcomeOf");
  }
}

/** The task error of a stored error outcome. */
function storedError(outcome: Extract<StoredBodyOutcome, { kind: "error" }>): TaskError {
  const data = decodeJsonNode(readTree(outcome.data), ROOT_PATH, 0, MAX_JSON_DEPTH);

  if (!data.ok) {
    throw new ClientDefect({ subject: "implementation.run", rule: "the error data that the codec wrote does not read again" });
  }

  const parts = outcome.parts;

  return {
    code: parts.code.kind === "user" ? { kind: "user", code: parts.code.code } : { kind: "library", code: libraryCode(parts.code.code) },
    message: parts.message.kind === "text" ? parts.message.text : null,
    data: data.value,
    exception:
      parts.exception.kind === "record"
        ? {
            type: parts.exception.className,
            module: parts.exception.module,
            message: parts.exception.message,
            repr: parts.exception.repr,
            traceback: parts.exception.traceback,
          }
        : null,
    subWorkflow: null,
  };
}

/** A library code of the adapter. Another text is a defect. */
function libraryCode(code: string): LibraryTaskCode {
  const found = LIBRARY_TASK_CODES.find((known) => known === code);

  if (found === undefined) {
    throw new ClientDefect({ subject: "implementation.run", rule: `the adapter gave the library code ${code}, which the catalog does not hold` });
  }

  return found;
}
