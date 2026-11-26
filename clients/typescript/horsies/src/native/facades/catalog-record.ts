// The raw record of a core catalog error, in the form that the generated converters take.
//
// The bridge gives a catalog error as `{ class, slots }` with one tagged value per slot (`RawCatalogError`). The
// generated converter of a catalog (`monitoringQueryCoreErrorFromNative` and the others) takes the slots by their core
// names, with plain values and the `exception` slot as the cause union of `error_detail.ts`. This module builds that
// form. The slots of the facade catalogs (monitoring, task action, broker) hold text, flags, integers, UUIDs, code
// members and exceptions; a send payload slot is a defect here: the send path converts its own catalog.

import type { CoreCause, CoreFailure, NoCause } from "../../_catalog/error_detail.ts";
import { ClientDefect } from "../../thrown-errors.ts";
import { raiseDefect } from "../call/defect.ts";
import { unreachable } from "../call/never.ts";
import type {
  CauseRecord,
  ClientClassName,
  ExceptionRecord,
  RawCatalogError,
  RawSlotValue,
} from "../values/records.ts";

/** The value of one slot, by the type of the slot. */
export type NativeSlotValue = string | boolean | bigint | CoreCause | NoCause;

/** A catalog error with its slots by core name: the input of a generated converter. */
export interface NativeCatalogRecord {
  readonly class: string;
  readonly slots: Readonly<Record<string, NativeSlotValue>>;
}

/** The record of `raw` with its slots by core name. A send payload slot is a defect. Pure but for the defect. */
export function nativeCatalogRecord(raw: RawCatalogError): NativeCatalogRecord {
  const slots: Record<string, NativeSlotValue> = {};

  for (const slot of raw.slots) {
    Object.defineProperty(slots, slot.name, {
      value: slotValue(raw.class, slot.name, slot.value),
      enumerable: true,
      writable: false,
    });
  }

  return Object.freeze({ class: raw.class, slots: Object.freeze(slots) });
}

/** The plain value of one slot. */
function slotValue(errorClass: string, name: string, value: RawSlotValue): NativeSlotValue {
  switch (value.kind) {
    case "text":
    case "uuid":
      return value.text;
    case "flag":
    case "integer":
    case "codeMember":
      return value.value;
    case "exception":
      return exceptionCause(value.exception);
    case "sendPayload":
      return raiseDefect(
        new ClientDefect({
          subject: `${errorClass}.${name}`,
          rule: "a facade catalog error holds no send payload",
        }),
      );
    default:
      return unreachable(value);
  }
}

/** The cause union of an `exception` slot. */
export function exceptionCause(record: ExceptionRecord): CoreCause | NoCause {
  switch (record.kind) {
    case "coreFailure":
      return Object.freeze(coreFailure(record.cause));
    case "generated":
      return Object.freeze(generatedCause(record.class, record.text));
    case "clientClass":
      return Object.freeze(clientCause(record.class, record.text));
    case "noCause":
      return Object.freeze({ kind: "noCause" });
    default:
      return unreachable(record);
  }
}

/** The cause of an exception class that the client defines. Pure. */
function clientCause(className: ClientClassName, text: string): CoreCause {
  switch (className) {
    case "valueError":
      return { kind: "ruleRefused", message: text };
    case "serializationError":
      return { kind: "jsonUnreadable", message: text };
    default:
      return unreachable(className);
  }
}

/** The core failure of a cause record. Pure. */
function coreFailure(cause: CauseRecord): CoreFailure {
  switch (cause.kind) {
    case "server":
      return {
        kind: "coreFailure",
        failure: "server",
        subject: cause.subject,
        message: cause.message,
        retryable: cause.retryable,
        sqlstate: cause.sqlstate,
      };
    case "connection":
      return {
        kind: "coreFailure",
        failure: "connection",
        subject: cause.subject,
        message: cause.message,
        retryable: cause.retryable,
      };
    case "contractViolation":
      return { kind: "coreFailure", failure: "contractViolation", subject: cause.subject, message: cause.message };
    case "preconditionFailed":
      return {
        kind: "coreFailure",
        failure: "preconditionFailed",
        subject: cause.subject,
        message: cause.message,
        retryable: cause.retryable,
      };
    default:
      return unreachable(cause);
  }
}

/** The cause of an exception class that the core defines. A class name that this build does not know is a defect. */
function generatedCause(className: string, text: string): CoreCause {
  switch (className) {
    case "OutcomeDecodeError":
      return { kind: "outcomeDecode", message: text };
    case "HistoryContractError":
      return { kind: "historyContract", message: text };
    default:
      return raiseDefect(
        new ClientDefect({
          subject: "exception",
          rule: `an exception class that this build does not know: ${className}`,
        }),
      );
  }
}
