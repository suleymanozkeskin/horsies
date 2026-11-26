// Fakes of the addon for the monitoring tests: a handle, envelopes, raw catalog records and the core's samples.

import { readFileSync } from "node:fs";

import type { WireJson } from "../../../../horsies/src/_catalog/wire_json.ts";
import type { CoreAccess } from "../../../../horsies/src/app/core-access.ts";
import { TaskId, WorkflowId } from "../../../../horsies/src/contract/ids.ts";
import { CoreHandle } from "../../../../horsies/src/native/handle/core-handle.ts";
import { listItems, wireJsonOfCoreText } from "../../../../horsies/src/monitoring/model-json.ts";
import type { NativeEnvelope } from "../../../../horsies/src/native/call/envelope.ts";
import type { NativeCoreHandle } from "../../../../horsies/src/native/handle/addon.ts";
import type { CauseRecord, RawCatalogError, RawSlot } from "../../../../horsies/src/native/values/records.ts";
import { fakeProducer } from "../../app/support.ts";

/** One sample of each monitoring model, as the core writes it. */
const SAMPLES_URL = new URL("../../../runtime/generated/monitoring-model-samples.json", import.meta.url);

/** The core's JSON of each variant of the attempt phases and of the node and run timings. */
const TIMING_EXAMPLES_URL = new URL("../../../../../../core/src/monitoring/timing_examples.json", import.meta.url);

/** The deepest nesting of a fixture value that `coreText` writes. */
const MAX_FIXTURE_DEPTH = 64;

/** The task id of a canonical UUID text. */
export function taskIdOf(text: string): TaskId {
  const parsed = TaskId.parse(text);

  if (!parsed.ok) {
    throw new Error(`not a task id: ${text}`);
  }

  return parsed.value;
}

/** The workflow id of a canonical UUID text. */
export function workflowIdOf(text: string): WorkflowId {
  const parsed = WorkflowId.parse(text);

  if (!parsed.ok) {
    throw new Error(`not a workflow id: ${text}`);
  }

  return parsed.value;
}

/** The core access of a main thread with the app's handle `native`; the producer is never called. */
export function mainThreadAccess(native: NativeCoreHandle): CoreAccess {
  return { kind: "addon", handle: new CoreHandle(native), producer: fakeProducer() };
}

/** A handle that the fake addon functions take; the tests never call its methods. */
export function fakeHandle(): NativeCoreHandle {
  // SAFETY: the fake addon functions only compare the handle by identity; no test calls a method of it.
  return Object.freeze({ fake: "handle" }) as unknown as NativeCoreHandle;
}

/** A Promise of a value envelope. */
export async function valueEnvelope<T>(value: T): Promise<NativeEnvelope<T>> {
  return { kind: "value", value };
}

/** The core's sample of the model `name`. */
export function sample(name: string): WireJson {
  return member(wireJsonOfCoreText("samples", readFileSync(SAMPLES_URL, "utf8")), name);
}

/** The core's timing examples of `group`. */
export function timingExamples(group: "attempt_phases" | "node_timings" | "run_timings"): ReadonlyArray<WireJson> {
  return listItems("examples", member(wireJsonOfCoreText("examples", readFileSync(TIMING_EXAMPLES_URL, "utf8")), group));
}

/** The member `key` of an object value. */
export function member(value: WireJson, key: string): WireJson {
  const found = value instanceof Map ? value.get(key) : undefined;

  if (found === undefined) {
    throw new Error(`the value holds no member ${key}`);
  }

  return found;
}

/** A copy of the object value `value` with `key` set to `replacement`. */
export function withMember(value: WireJson, key: string, replacement: WireJson): WireJson {
  if (!(value instanceof Map)) {
    throw new Error(`the value that takes ${key} is not an object`);
  }

  return new Map(value).set(key, replacement);
}

/**
 * The JSON text of a fixture value as the core writes it: a `bigint` as its digits, a `number` as float text (`2.0`
 * for an integral value), a map with its members in order.
 */
export function coreText(value: WireJson, depth = 0): string {
  if (depth > MAX_FIXTURE_DEPTH) {
    throw new Error("the fixture value nests too deep");
  }

  if (value === null) {
    return "null";
  }

  if (Array.isArray(value)) {
    return `[${listItems("fixture", value).map((item) => coreText(item, depth + 1)).join(",")}]`;
  }

  if (value instanceof Map) {
    return `{${Array.from(value, ([key, item]) => `${JSON.stringify(key)}:${coreText(item, depth + 1)}`).join(",")}}`;
  }

  const boxed: object = Object(value);

  if (boxed instanceof Number) {
    return Number.isInteger(value) ? `${String(value)}.0` : String(value);
  }

  return boxed instanceof BigInt ? String(value) : JSON.stringify(value);
}

/** A text slot. */
export function textSlot(name: string, text: string): RawSlot {
  return { name, value: { kind: "text", text } };
}

/** A flag slot. */
export function flagSlot(name: string, value: boolean): RawSlot {
  return { name, value: { kind: "flag", value } };
}

/** A UUID slot. */
export function uuidSlot(name: string, text: string): RawSlot {
  return { name, value: { kind: "uuid", text } };
}

/** A code member slot. */
export function codeSlot(name: string, catalog: string, value: string): RawSlot {
  return { name, value: { kind: "codeMember", catalog, value } };
}

/** The `cause` slot of a core failure. */
export function causeSlot(cause: CauseRecord): RawSlot {
  return { name: "cause", value: { kind: "exception", exception: { kind: "coreFailure", cause } } };
}

/** A connection cause of the operation `subject`. */
export function connectionCause(subject: string, retryable: boolean): CauseRecord {
  return { kind: "connection", subject, message: "connection refused", retryable };
}

/** The raw record of `MonitoringQueryDatabaseOperationFailed`. */
export function monitoringDbFailed(subject: string, retryable: boolean): RawCatalogError {
  return {
    class: "MonitoringQueryDatabaseOperationFailed",
    slots: [
      textSlot("message", `${subject} failed: connection refused`),
      flagSlot("retryable", retryable),
      causeSlot(connectionCause(subject, retryable)),
    ],
  };
}

/** The raw record of `DatabaseOperationFailed` with `code`. */
export function brokerOperationFailed(code: string, subject: string): RawCatalogError {
  return {
    class: "DatabaseOperationFailed",
    slots: [
      codeSlot("code", "DatabaseErrorCode", code),
      textSlot("message", `${subject} failed: connection refused`),
      flagSlot("retryable", true),
      causeSlot(connectionCause(subject, true)),
    ],
  };
}

/** The raw record of `DatabaseInvalidInput`. */
export function brokerInvalidInput(message: string): RawCatalogError {
  return {
    class: "DatabaseInvalidInput",
    slots: [textSlot("message", message), { name: "cause", value: { kind: "exception", exception: { kind: "noCause" } } }],
  };
}

/** The raw record of `DatabaseListenerDisabled`. */
export function brokerListenerDisabled(): RawCatalogError {
  return { class: "DatabaseListenerDisabled", slots: [textSlot("message", "ping_workers listener unavailable")] };
}
