// Fakes at the addon boundary for the tests of the app hub and the send path: the core's check functions, the core
// operations of sends and reads, the process facts of `Horsies.create`, and raw records of the task send catalog.

import type { CoreChecks } from "../../../horsies/src/app/core-checks.ts";
import type { ProducerCore } from "../../../horsies/src/app/core-access.ts";
import { noWorkflowCore } from "../../../horsies/src/workflow/core.ts";
import { CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { CreateEnvironment } from "../../../horsies/src/app/create.ts";
import type { ProcessStart } from "../../../horsies/src/app/suppression.ts";
import type {
  AppConfigCheck,
  CoreRefusal,
  DeclaredCodesCheck,
  EnqueueOutcome,
  EnqueueRequest,
  NativeSendRules,
  NativeTaskOptions,
  PreparedSend,
  ProcessRoleName,
  ResultRead,
  SendFacts,
  SendKind,
  SendPriority,
  TaskInfoRead,
  TaskOptionsCheck,
  TaskOptionsFacts,
  WaitBound,
} from "../../../horsies/src/native/sends/addon.ts";
import type {
  ExceptionRecord,
  RawCatalogError,
  RawRecordField,
  TraceArgument,
} from "../../../horsies/src/native/values/records.ts";

/** A canonical task id of the fakes. */
export const TASK_ID = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b";

/** A second canonical task id. */
export const OWNER_TASK_ID = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5c";

/** 2023-11-14T22:13:20.123456Z in epoch microseconds. */
export const SENT_AT_MICROS = 1_700_000_000_123_456n;

/** The options text that the fake core gives. */
export const OPTIONS_TEXT = '{"task_name":"fake"}';

/** A fake task options record of the core. */
export function fakeTaskOptions(): NativeTaskOptions {
  return { storedText: () => ({ kind: "value", value: OPTIONS_TEXT }) };
}

/** A fake send rules record: never called; the fake producer prepares sends. */
export function fakeSendRules(): NativeSendRules {
  return {
    prepareSendFacts: () => {
      throw new Error("fakeSendRules: the fake producer prepares sends");
    },
  };
}

/** The scripted answers and the call counters of a fake `CoreChecks`. */
export type FakeChecks = CoreChecks & {
  readonly calls: { configs: number; codes: number; options: number };
  readonly optionsFacts: TaskOptionsFacts[];
};

/** The scripted answers of the fake core checks. Each answer left out accepts. */
export type CheckScript = {
  readonly config?: readonly CoreRefusal[];
  readonly codes?: readonly CoreRefusal[];
  readonly options?: readonly CoreRefusal[];
};

/** A fake of the core's check functions with call counters. */
export function fakeChecks(script: CheckScript = {}): FakeChecks {
  const calls = { configs: 0, codes: 0, options: 0 };
  const optionsFacts: TaskOptionsFacts[] = [];

  return {
    calls,
    optionsFacts,
    checkAppConfig: (_json: string): AppConfigCheck => {
      calls.configs += 1;

      return script.config === undefined
        ? {
            kind: "accepted",
            rules: fakeSendRules(),
            manifestJson: "{}",
            installCoverage: { historyHorizonDays: 7, heartbeatHorizonHours: 24, declaredPeriodDays: [] },
          }
        : { kind: "refused", refusals: [...script.config] };
    },
    parseDeclaredErrorCodes: (_codes: readonly string[]): DeclaredCodesCheck => {
      calls.codes += 1;

      return script.codes === undefined ? { kind: "accepted" } : { kind: "refused", refusals: [...script.codes] };
    },
    checkTaskOptions: (facts: TaskOptionsFacts): TaskOptionsCheck => {
      calls.options += 1;
      optionsFacts.push(facts);

      return script.options === undefined
        ? { kind: "accepted", options: fakeTaskOptions() }
        : { kind: "refused", refusals: [...script.options] };
    },
  };
}

/** A core refusal with a catalog code and no help. */
export function catalogRefusal(code: string, message: string): CoreRefusal {
  return { code, message, notes: [], help: { kind: "noHelp" } };
}

/** One recorded enqueue of the fake producer. */
export type EnqueueCall = { readonly request: EnqueueRequest; readonly sendKind: SendKind; readonly trace: TraceArgument };

/** The scripted answers of a fake producer: each list is used in order; the last answer repeats. */
export type ProducerScript = {
  readonly prepared?: readonly PreparedSend[];
  readonly enqueued?: readonly EnqueueOutcome[];
  readonly results?: readonly ResultRead[];
  readonly infos?: readonly TaskInfoRead[];
  readonly role?: ProcessRoleName;
};

/** One recorded prepare of the fake producer: the task name and the facts of the send. */
export type PrepareCall = { readonly taskName: string; readonly facts: SendFacts };

/** A fake producer with recorded calls. */
export type FakeProducer = ProducerCore & {
  readonly prepares: PrepareCall[];
  readonly enqueues: EnqueueCall[];
  readonly results: { readonly taskId: string; readonly wait: WaitBound }[];
  readonly infos: string[];
  readonly roles: number[];
};

/** The prepared facts of an accepted send with `priority`. */
export function preparedWith(priority: SendPriority): PreparedSend {
  return {
    kind: "prepared",
    taskId: TASK_ID,
    taskOptions: OPTIONS_TEXT,
    retention: { kind: "classKey", key: "standard_30d" },
    priority,
  };
}

/** The prepared facts of an accepted send on the queue of the default queue mode. */
export const PREPARED: PreparedSend = preparedWith({ kind: "defaultQueue" });

/** The outcome of an accepted enqueue. */
export const ENQUEUED: EnqueueOutcome = { kind: "enqueued", taskId: TASK_ID };

function nth<T>(list: readonly T[] | undefined, index: number, fallback: T): T {
  if (list === undefined || list.length === 0) {
    return fallback;
  }

  return list[Math.min(index, list.length - 1)] ?? fallback;
}

/** A fake of the core operations of sends and reads. */
export function fakeProducer(script: ProducerScript = {}): FakeProducer {
  const prepares: PrepareCall[] = [];
  const enqueues: EnqueueCall[] = [];
  const results: { readonly taskId: string; readonly wait: WaitBound }[] = [];
  const infos: string[] = [];
  const roles: number[] = [];

  return {
    prepares,
    enqueues,
    results,
    infos,
    roles,
    workflows: noWorkflowCore(),
    prepareSend: async (taskName, facts) => {
      prepares.push({ taskName, facts });

      return nth(script.prepared, prepares.length - 1, PREPARED);
    },
    enqueue: async (request, sendKind, trace) => {
      enqueues.push({ request, sendKind, trace });

      return nth(script.enqueued, enqueues.length - 1, ENQUEUED);
    },
    getResult: async (taskId, wait) => {
      results.push({ taskId, wait });

      return nth(script.results, results.length - 1, { kind: "absent", reason: "NOT_CLASSIFIED" });
    },
    taskInfo: async (taskId) => {
      infos.push(taskId);

      return nth(script.infos, infos.length - 1, { kind: "absent", reason: "NOT_CLASSIFIED" });
    },
    processRole: async () => {
      roles.push(1);

      return script.role ?? "RUNS_NO_TASK_BODIES";
    },
  };
}

/** A core handle whose operations fail the test: the fakes of the app hub call none of them. */
export function fakeCoreHandle(): CoreHandle {
  const notCalled = (operation: string): never => {
    throw new TypeError(`fakeCoreHandle: ${operation} is not called by the fakes`);
  };

  return new CoreHandle({
    manifestVersion: () => notCalled("manifestVersion"),
    withInstallCoverage: () => notCalled("withInstallCoverage"),
    close: async () => notCalled("close"),
    pingDatabase: async () => notCalled("pingDatabase"),
    installSchema: async () => notCalled("installSchema"),
    schemaStatus: async () => notCalled("schemaStatus"),
  });
}

/** The start facts of a plain producer process. */
export const PLAIN_START: ProcessStart = { argumentAfterScript: undefined, seatIsolate: false };

/** The parts of a fake create environment. */
export type EnvironmentParts = {
  readonly mainThread?: boolean;
  readonly variables?: Readonly<Record<string, string>>;
  readonly start?: ProcessStart;
  readonly checks?: FakeChecks;
  readonly producer?: FakeProducer;
  readonly handle?: CoreHandle;
};

/** The environment of a fake create, and the counter of the loads of the core checks. */
export type FakeEnvironment = CreateEnvironment & {
  readonly loads: { checks: number; producers: number };
  readonly checks: FakeChecks;
  readonly fakeProducer: FakeProducer;
  /** The core handle that the fake addon core gives the app. */
  readonly handle: CoreHandle;
};

/** A create environment with fakes: a main thread, no variables, a plain start, accepting checks. */
export function fakeEnvironment(parts: EnvironmentParts = {}): FakeEnvironment {
  const loads = { checks: 0, producers: 0 };
  const checks = parts.checks ?? fakeChecks();
  const producer = parts.producer ?? fakeProducer();
  const handle = parts.handle ?? fakeCoreHandle();
  const variables = parts.variables ?? {};

  return {
    loads,
    checks,
    fakeProducer: producer,
    handle,
    mainThread: parts.mainThread ?? true,
    environment: (name) => variables[name],
    processStart: parts.start ?? PLAIN_START,
    coreChecks: () => {
      loads.checks += 1;

      return checks;
    },
    addonCore: () => {
      loads.producers += 1;

      return { producer, handle };
    },
  };
}

/** A database URL secret given as a value. */
export const DATABASE_URL = { kind: "value", value: "postgresql://postgres@127.0.0.1:55432/horsies" } as const;

/** The exception record of no cause. */
export const NO_CAUSE: ExceptionRecord = { kind: "noCause" };

/** The fields of a send payload record, as the bridge gives them. */
export function payloadFields(taskName: string): RawRecordField[] {
  return [
    { name: "task_name", value: { kind: "text", text: taskName } },
    { name: "queue_name", value: { kind: "text", text: "default" } },
    { name: "priority", value: { kind: "integer", value: 100n } },
    { name: "input_json", value: { kind: "text", text: '{"n":1}' } },
    { name: "sent_at", value: { kind: "instant", epochMicros: SENT_AT_MICROS } },
    { name: "good_until", value: { kind: "instant", epochMicros: SENT_AT_MICROS + 60_000_000n } },
    { name: "enqueue_delay_seconds", value: { kind: "integer", value: 30n } },
    { name: "task_options", value: { kind: "text", text: OPTIONS_TEXT } },
    { name: "idempotency_key", value: { kind: "text", text: "key-1" } },
    { name: "retention_class_key", value: { kind: "retainForever" } },
  ];
}

/** A raw `TaskSendEnqueueFailed`. */
export function rawEnqueueFailed(taskName: string, retryable: boolean): RawCatalogError {
  return {
    class: "TaskSendEnqueueFailed",
    slots: [
      { name: "task_id", value: { kind: "uuid", text: TASK_ID } },
      { name: "payload", value: { kind: "sendPayload", fields: payloadFields(taskName) } },
      { name: "message", value: { kind: "text", text: "connection refused" } },
      { name: "retryable", value: { kind: "flag", value: retryable } },
      {
        name: "exception",
        value: {
          kind: "exception",
          exception: {
            kind: "coreFailure",
            cause: { kind: "connection", subject: "enqueue", message: "connection refused", retryable },
          },
        },
      },
    ],
  };
}

/** A raw `TaskSendPayloadMismatch`. */
export function rawPayloadMismatch(taskName: string): RawCatalogError {
  return {
    class: "TaskSendPayloadMismatch",
    slots: [
      { name: "task_id", value: { kind: "uuid", text: TASK_ID } },
      { name: "payload", value: { kind: "sendPayload", fields: payloadFields(taskName) } },
      { name: "message", value: { kind: "text", text: "the task id holds another payload" } },
    ],
  };
}

/** A raw `TaskSendIdempotencyKeyConflict`. */
export function rawKeyConflict(taskName: string): RawCatalogError {
  return {
    class: "TaskSendIdempotencyKeyConflict",
    slots: [
      { name: "task_id", value: { kind: "uuid", text: TASK_ID } },
      { name: "payload", value: { kind: "sendPayload", fields: payloadFields(taskName) } },
      { name: "message", value: { kind: "text", text: "the key is reserved by another task" } },
      { name: "owner_task_id", value: { kind: "uuid", text: OWNER_TASK_ID } },
    ],
  };
}

/** A raw `TaskSendPayloadTooLarge`. */
export function rawPayloadTooLarge(): RawCatalogError {
  return {
    class: "TaskSendPayloadTooLarge",
    slots: [
      { name: "message", value: { kind: "text", text: "the payload is above the reject bound" } },
      { name: "size_bytes", value: { kind: "integer", value: 2_000_000n } },
      { name: "limit_bytes", value: { kind: "integer", value: 1_000_000n } },
    ],
  };
}

/** A raw `TaskSendValidationFailed` with a rule cause. */
export function rawValidationFailed(message: string): RawCatalogError {
  return {
    class: "TaskSendValidationFailed",
    slots: [
      { name: "message", value: { kind: "text", text: message } },
      { name: "exception", value: { kind: "exception", exception: { kind: "clientClass", class: "valueError", text: message } } },
    ],
  };
}
