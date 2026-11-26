// Fakes of the child tests: the addon calls of a run, the loop marks, and one run of a body through the adapter.

import assert from "node:assert/strict";

import { runBody, type StoredBodyOutcome } from "../../../horsies/src/child/body.ts";
import type { BodyEnd, BodyObservation } from "../../../horsies/src/child/body-observer.ts";
import { attemptFromCore } from "../../../horsies/src/child/attempt.ts";
import { type LoopHolder, type LoopMarks, type RunNative, RunTable } from "../../../horsies/src/child/run-table.ts";
import { taskRuntimeOf, type TaskRuntime } from "../../../horsies/src/child/runtime.ts";
import { StateValues } from "../../../horsies/src/child/state.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { compileContract } from "../../../horsies/src/contract/compile.ts";
import { type TaskId, taskIdFromCore } from "../../../horsies/src/contract/ids.ts";
import type { ImplementationParts } from "../../../horsies/src/contract/implementation.ts";
import type {
  CancelCauseName,
  RunCancelStateRecord,
  SpawnEnd,
  SpawnRegistration,
} from "../../../horsies/src/native/child/addon.ts";

/** The task id of the runs of the tests. */
export const RUN_TASK_ID: TaskId = taskIdFromCore("0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b", "the child tests");

/** A fake of the addon calls of a run: answers that a test sets, and the calls it made. */
export class FakeRunNative implements RunNative {
  cancel: RunCancelStateRecord = { kind: "notRequested" };
  registration: SpawnRegistration = { kind: "registered", workKey: 1n };
  readonly ended: bigint[] = [];
  readonly registered: string[] = [];

  runCancelStateOf(): RunCancelStateRecord {
    return this.cancel;
  }

  registerSpawn(_runKey: bigint, name: string): SpawnRegistration {
    this.registered.push(name);

    return this.registration;
  }

  endSpawn(workKey: bigint): SpawnEnd {
    this.ended.push(workKey);

    return "ended";
  }
}

/** Loop marks that record each mark, and keep the holder in force as the child's marks do. */
export class RecordingMarks implements LoopMarks {
  readonly marks: string[] = [];
  #current: LoopHolder = { kind: "noRun" };

  hold(taskId: string): LoopHolder {
    const before = this.#current;

    this.#current = { kind: "run", taskId };
    this.marks.push(`hold ${taskId}`);

    return before;
  }

  restore(holder: LoopHolder): void {
    this.#current = holder;
    this.marks.push(holder.kind === "run" ? `hold ${holder.taskId}` : "release");
  }
}

/** An observation that records its calls in order. */
export class RecordingObservation implements BodyObservation {
  readonly calls: string[] = [];
  readonly thrownValues: unknown[] = [];
  readonly ends: BodyEnd[] = [];

  run<T>(call: () => T): T {
    this.calls.push("run");

    return call();
  }

  thrown(cause: unknown): void {
    this.calls.push("thrown");
    this.thrownValues.push(cause);
  }

  end(end: BodyEnd): void {
    this.calls.push("end");
    this.ends.push(end);
  }

  cancelled(cause: CancelCauseName): void {
    this.calls.push(`cancelled ${cause}`);
  }
}

/** A run table with the fakes, and one run of it. */
export function tableRun(
  native: FakeRunNative = new FakeRunNative(),
  marks: LoopMarks = new RecordingMarks(),
  states: StateValues = new StateValues([]),
): { readonly table: RunTable; readonly runtime: TaskRuntime; readonly runKey: bigint } {
  const table = new RunTable(native, marks, states);
  const runKey = 7n;

  const controls = table.enter({
    runKey,
    taskId: taskIdFromCore(RUN_TASK_ID, "test"),
    taskName: "test_task",
    attempt: attemptFromCore(2, "test"),
  });

  return { table, runtime: taskRuntimeOf(controls), runKey };
}

/** One run of the body of `implementation` with the stored input `inputJson`, and what the fakes saw. */
export async function adapterRun(
  implementation: ImplementationParts,
  inputJson: string,
  cancel?: CancelCauseName,
): Promise<{
  readonly outcome: StoredBodyOutcome;
  readonly observation: RecordingObservation;
  readonly marks: RecordingMarks;
}> {
  const compiled = compileContract(implementation.contract);

  if (!compiled.ok) {
    assert.fail(`the test contract does not compile: ${compiled.error.map((refused) => refused.detail).join("; ")}`);
  }

  const marks = new RecordingMarks();
  const observation = new RecordingObservation();
  const { table, runtime, runKey } = tableRun(new FakeRunNative(), marks);

  if (cancel !== undefined) {
    table.cancel(runKey, cancel);
  }

  const outcome = await runBody({
    inputJson: jsonTextCheckedByCore(inputJson),
    implementation,
    compiled: compiled.value,
    runtime,
    mark: { taskId: RUN_TASK_ID, taskName: compiled.value.record.name, attempt: 2 },
    marks,
    observation,
  });

  return { outcome, observation, marks };
}

/** The stored error parts of an outcome; a value fails the test. */
export function errorParts(outcome: StoredBodyOutcome): Extract<StoredBodyOutcome, { kind: "error" }>["parts"] {
  if (outcome.kind !== "error") {
    return assert.fail(`the outcome is a value: ${outcome.json}`);
  }

  return outcome.parts;
}
