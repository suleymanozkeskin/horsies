// The value of a core catalog error that the generated converters take, from the raw record of the bridge.
//
// The bridge gives `{ class, slots }` with the slots as a list of typed values (`RawCatalogError`). The generated
// converters of each catalog (`src/_catalog/*_errors.ts`) take the class and the slots by name, each slot in the form
// that the generated `Native*` type names:
// - text, a UUID and a code member as a string; a flag as a boolean; an integer as a `bigint`;
// - the payload of a send as its fields by name: an instant as a `bigint` of epoch microseconds, an integer as a
//   `number` (a priority, a delay), an absent field and "retain forever" as `null`;
// - the `exception` slot in its public form (`CoreCause | NoCause`, `src/_catalog/error_detail.ts`).

import type { CoreCause, CoreFailure, NoCause } from "../_catalog/error_detail.ts";
import type {
  CauseRecord,
  ClientClassName,
  ExceptionRecord, RawCatalogError, RawRecordValue, RawSlotValue } from "../native/values/records.ts";
import { ClientDefect } from "../thrown-errors.ts";

/** The value of one slot, as a generated `Native*` type names it. */
type NativeSlotValue =
  | string
  | boolean
  | bigint
  | CoreCause
  | NoCause
  | Readonly<Record<string, NativeFieldValue>>;

/** The value of one field of a send payload, as `NativeTaskSendPayload` names it. */
type NativeFieldValue = string | number | bigint | null;

/** A catalog value with its slots by name: the input of a generated converter, before its type is asserted. */
export type NativeCatalogValue = {
  readonly class: string;
  readonly slots: Readonly<Record<string, NativeSlotValue>>;
};

/** The catalog value of a raw record, its slots by name. A repeated slot name is a defect: `ClientDefect`. Pure. */
export function nativeCatalogValue(raw: RawCatalogError): NativeCatalogValue {
  const slots: Record<string, NativeSlotValue> = {};

  for (const slot of raw.slots) {
    if (Object.hasOwn(slots, slot.name)) {
      throw new ClientDefect({ subject: raw.class, rule: `the bridge gave the slot ${slot.name} twice` });
    }

    slots[slot.name] = slotValue(slot.value);
  }

  return Object.freeze({ class: raw.class, slots: Object.freeze(slots) });
}

function slotValue(value: RawSlotValue): NativeSlotValue {
  switch (value.kind) {
    case "text":
    case "uuid":
      return value.text;
    case "flag":
    case "integer":
    case "codeMember":
      return value.value;
    case "sendPayload":
      return Object.freeze(Object.fromEntries(value.fields.map((field) => [field.name, fieldValue(field.value)])));
    case "exception":
      return publicExceptionCause(value.exception);
    default: {
      const unexpected: never = value;

      throw new ClientDefect({ subject: "catalog slot", rule: `a slot value outside its type: ${String(unexpected)}` });
    }
  }
}

function fieldValue(value: RawRecordValue): NativeFieldValue {
  switch (value.kind) {
    case "text":
      return value.text;
    case "integer":
      return Number(value.value);
    case "instant":
      return value.epochMicros;
    case "absent":
    case "retainForever":
      return null;
    default: {
      const unexpected: never = value;

      throw new ClientDefect({ subject: "send payload", rule: `a field value outside its type: ${String(unexpected)}` });
    }
  }
}

/** The core exception class of an outcome decode failure. */
const OUTCOME_DECODE_CLASS = "OutcomeDecodeError";

/** The core exception class of a history read that breaks its contract. */
const HISTORY_CONTRACT_CLASS = "HistoryContractError";

/** The public form of the `cause` slot of a catalog error. An exception class that this build does not know is a defect. Pure. */
export function publicExceptionCause(record: ExceptionRecord): CoreCause | NoCause {
  switch (record.kind) {
    case "noCause":
      return Object.freeze({ kind: "noCause" });
    case "coreFailure":
      return coreFailureOf(record.cause);
    case "generated":
      return generatedCause(record.class, record.text);
    case "clientClass":
      return clientClassCause(record.class, record.text);

    default: {
      const unexpected: never = record;

      throw new ClientDefect({ subject: "exception slot", rule: `an exception record outside its type: ${String(unexpected)}` });
    }
  }
}

function clientClassCause(clientClass: ClientClassName, message: string): CoreCause {
  switch (clientClass) {
    case "valueError":
      return Object.freeze({ kind: "ruleRefused", message });
    case "serializationError":
      return Object.freeze({ kind: "jsonUnreadable", message });
    default: {
      const unexpected: never = clientClass;

      throw new ClientDefect({ subject: "exception slot", rule: `a client class outside its type: ${String(unexpected)}` });
    }
  }
}

function generatedCause(exceptionClass: string, message: string): CoreCause {
  switch (exceptionClass) {
    case OUTCOME_DECODE_CLASS:
      return Object.freeze({ kind: "outcomeDecode", message });
    case HISTORY_CONTRACT_CLASS:
      return Object.freeze({ kind: "historyContract", message });
    default:
      throw new ClientDefect({ subject: "exception slot", rule: `an exception class that this build does not know: ${exceptionClass}` });
  }
}

/** The public core failure of a cause record. Pure. */
export function coreFailureOf(cause: CauseRecord): CoreFailure {
  switch (cause.kind) {
    case "server":
      return Object.freeze({
        kind: "coreFailure",
        failure: "server",
        subject: cause.subject,
        message: cause.message,
        retryable: cause.retryable,
        sqlstate: cause.sqlstate,
      });
    case "connection":
    case "preconditionFailed":
      return Object.freeze({
        kind: "coreFailure",
        failure: cause.kind,
        subject: cause.subject,
        message: cause.message,
        retryable: cause.retryable,
      });
    case "contractViolation":
      return Object.freeze({ kind: "coreFailure", failure: "contractViolation", subject: cause.subject, message: cause.message });
    default: {
      const unexpected: never = cause;

      throw new ClientDefect({ subject: "core failure", rule: `a cause outside its type: ${String(unexpected)}` });
    }
  }
}
