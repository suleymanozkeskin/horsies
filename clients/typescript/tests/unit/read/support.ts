// Fakes of the core at the boundary of the read path: a producer with scripted reads and call counters, a seat relay
// that counts its requests, raw broker catalog records, and stored result envelope texts in the forms of
// `core/tests/fixtures/stored_forms/result_envelope.json`.

import type { CoreAccess, ProducerCore, SeatRelay, SendsSeatReply } from "../../../horsies/src/app/core-access.ts";
import { type TaskId, TaskId as TaskIdParser } from "../../../horsies/src/contract/ids.ts";
import type {
  FinishedStatusName,
  LiveStatusName,
  ProcessRoleName,
  ResultRead,
  TaskInfoRead,
  TaskStatusName,
  WaitBound,
} from "../../../horsies/src/native/sends/addon.ts";
import type { RawCatalogError } from "../../../horsies/src/native/values/records.ts";
import { noWorkflowCore } from "../../../horsies/src/workflow/core.ts";
import { fakeCoreHandle } from "../app/support.ts";

/** A task id of the tests (a UUIDv7 text). */
export const TASK_ID: TaskId = taskId("0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b");

/** The task id of a canonical text. Throws for another text: a defect of the test. */
export function taskId(text: string): TaskId {
  const parsed = TaskIdParser.parse(text);

  if (!parsed.ok) {
    throw new TypeError(`not a task id: ${text}`);
  }

  return parsed.value;
}

/** The calls that a fake producer received. */
export type ProducerCalls = {
  getResult: number;
  taskInfo: number;
  processRole: number;
  prepareSend: number;
  enqueue: number;
  readonly waits: WaitBound[];
};

/** The scripted answers of a fake producer. A list gives its items in order and then repeats its last item. */
export type ProducerScript = {
  readonly results?: readonly ResultRead[];
  readonly infos?: readonly TaskInfoRead[];
  readonly role?: ProcessRoleName;
};

/** A fake producer and its call counters. */
export type FakeProducer = { readonly producer: ProducerCore; readonly calls: ProducerCalls };

/** A fake producer with scripted reads. A send operation is a defect of the test: it throws. */
export function fakeProducer(script: ProducerScript): FakeProducer {
  const calls: ProducerCalls = { getResult: 0, taskInfo: 0, processRole: 0, prepareSend: 0, enqueue: 0, waits: [] };

  const producer: ProducerCore = {
    workflows: noWorkflowCore(),
    prepareSend: async () => {
      calls.prepareSend += 1;
      throw new TypeError("the read tests make no send");
    },
    enqueue: async () => {
      calls.enqueue += 1;
      throw new TypeError("the read tests make no send");
    },
    getResult: async (_taskId, wait) => {
      calls.waits.push(wait);
      calls.getResult += 1;

      return scripted(script.results, calls.getResult, "getResult");
    },
    taskInfo: async () => {
      calls.taskInfo += 1;

      return scripted(script.infos, calls.taskInfo, "taskInfo");
    },
    processRole: async () => {
      calls.processRole += 1;

      return script.role ?? "RUNS_NO_TASK_BODIES";
    },
  };

  return { producer, calls };
}

function scripted<T>(answers: readonly T[] | undefined, call: number, operation: string): T {
  const answer = answers?.[Math.min(call, answers.length) - 1];

  if (answer === undefined) {
    throw new TypeError(`the test scripts no answer of ${operation}`);
  }

  return answer;
}

/** The core access of a main thread with a fake producer. */
export function addonAccess(fake: FakeProducer): CoreAccess {
  return { kind: "addon", producer: fake.producer, handle: fakeCoreHandle() };
}

/** A seat relay that answers `taskInfo` with `read` and counts its requests. */
export function countingRelay(read: TaskInfoRead): { readonly access: CoreAccess; readonly requests: () => number } {
  let requests = 0;

  const relay: SeatRelay = {
    request: async (_area, operation) => {
      requests += 1;

      const reply: SendsSeatReply =
        "kind" in operation && operation.kind === "taskInfo"
          ? { kind: "taskInfo", read }
          : { kind: "taskInfo", read: { kind: "absent", reason: "NOT_CLASSIFIED" } };

      return { kind: "reply", reply };
    },
  };

  return { access: { kind: "seatRelay", relay }, requests: () => requests };
}

/** A read that found the task with a stored envelope text. */
export function storedRead(status: TaskStatusName, text: string): ResultRead {
  return { kind: "found", record: { taskId: TASK_ID, taskName: "validate_order", envelope: { kind: "stored", status, text } } };
}

/** A read whose wait ended before the task finished: no stored envelope yet. */
export function waitEndedRead(status: LiveStatusName): ResultRead {
  return { kind: "found", record: { taskId: TASK_ID, taskName: "validate_order", envelope: { kind: "waitEnded", status } } };
}

/** A read that found a finished task that stores no result. */
export function finishedWithoutResultRead(status: FinishedStatusName): ResultRead {
  return {
    kind: "found",
    record: { taskId: TASK_ID, taskName: "validate_order", envelope: { kind: "finishedWithoutResult", status } },
  };
}

/** The stored envelope of an `ok` value text. */
export function okEnvelope(valueText: string): string {
  return `{"__h_task_result__":true,"ok":${valueText},"err":null}`;
}

/** The stored envelope of an error with a custom code (the form of `err_user_code_without_message`). */
export function customErrorEnvelope(code: string): string {
  return `{"__h_task_result__":true,"ok":null,"err":{"exception":null,"error_code":"${code}","data":null,"message":null}}`;
}

/** The stored envelope of an error with a built-in code (the form of `err_built_in_code`). */
export function builtInErrorEnvelope(code: string): string {
  return `{"__h_task_result__":true,"ok":null,"err":{"exception":null,"error_code":{"__builtin_task_code__":"${code}"},"data":null,"message":"the broker refused the write"}}`;
}

/** `DatabaseOperationFailed` with `code`: the slot list of `NativeDatabaseOperationFailed`. */
export function operationFailedRaw(code: string, retryable: boolean): RawCatalogError {
  return {
    class: "DatabaseOperationFailed",
    slots: [
      { name: "code", value: { kind: "codeMember", catalog: "DatabaseOperationFailedCode", value: code } },
      { name: "message", value: { kind: "text", text: "connection failed: refused" } },
      { name: "retryable", value: { kind: "flag", value: retryable } },
      {
        name: "cause",
        value: {
          kind: "exception",
          exception: {
            kind: "coreFailure",
            cause: { kind: "connection", subject: "raw_result_record", message: "connection failed: refused", retryable },
          },
        },
      },
    ],
  };
}

/** `DatabaseInvalidJsonPayload`: the slot list of `NativeDatabaseInvalidJsonPayload`. */
export function invalidJsonPayloadRaw(): RawCatalogError {
  return {
    class: "DatabaseInvalidJsonPayload",
    slots: [
      { name: "message", value: { kind: "text", text: "result JSON is refused: duplicate key" } },
      {
        name: "cause",
        value: { kind: "exception", exception: { kind: "clientClass", class: "serializationError", text: "duplicate key" } },
      },
    ],
  };
}
