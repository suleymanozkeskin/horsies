// The `data` of a task error that a body returns: never refused.
//
// - Data that is strict JSON is stored as its text. That text may hold a key that only the engine writes, or more
//   levels than error data may have: the core replaces such data itself when it stores the error
//   (`TaskError`'s `Serialize`, `storage_refusal`), so that rule has one owner.
// - Data that cannot be strict JSON (a value that is not JSON-native, NaN or an infinity, a lone surrogate, more than
//   300 levels, an integer of more than 4,300 digits) is replaced here by `{"data_refused": "<cause>"}`: the core
//   cannot read it. The cause is the one that the Python client stores, which checks the data in this order
//   (`_report_refusal`, `clients/python/horsies/core/codec/typed.py`):
//   1. the JSON-native fence: not JSON-native, NaN, more than 300 levels: `StrictJsonError`;
//   2. Pydantic's dump: more than `PYDANTIC_DUMP_DEPTH` levels: `ValueValidationError`;
//   3. the scans of the dump: a lone surrogate, a key that only the engine writes: `StrictJsonError`;
//   4. the JSON text: an integer of more than 4,300 digits: `SerializationError`.
//   So with two faults the first step that sees one names the cause.

import { unreachable } from "../wire/unreachable.ts";
import { DATA_REFUSED_KEY, MAX_INT_DIGITS, MAX_JSON_DEPTH } from "./bounds.ts";
import { isEngineKey } from "./engine-keys.ts";
import { type JsonText, writtenJsonText } from "./json-text.ts";
import { engineKeysKeptText } from "./json-value.ts";
import { ROOT_PATH } from "./refusal.ts";
import { isArray, isPlainObject } from "./values.ts";

/**
 * The most levels of data that Pydantic's dump writes (pydantic 2.13.4 refuses data of 255 levels in a model
 * field). It decides the cause of data that has more levels and another fault.
 */
const PYDANTIC_DUMP_DEPTH = 254;

/** The cause of a replaced `data`, as Python and the core store it. */
export type DataRefusedCause = "StrictJsonError" | "ValueValidationError" | "SerializationError";

/** The stored `data` of a task error: its own text, or the replacement and its cause. */
export type StoredErrorData =
  | { readonly state: "stored"; readonly text: JsonText }
  | { readonly state: "replaced"; readonly cause: DataRefusedCause; readonly text: JsonText };

/** The faults of a data value that one walk finds. Each is a fact: found or not. */
type DataFaults = {
  notJsonNative: boolean;
  nonFinite: boolean;
  textTooDeep: boolean;
  loneSurrogate: boolean;
  engineKey: boolean;
  integerTooLong: boolean;
  deepest: number;
};

/** Whether the data has a text that the core can read, or the cause of its replacement. */
type DataVerdict = { readonly state: "storable" } | { readonly state: "replaced"; readonly cause: DataRefusedCause };

/** The stored `data` of a task error. `null` is stored as `null`. Never refused. Pure. */
export function errorDataText(data: unknown): StoredErrorData {
  const verdict = verdictOf(faultsOf(data));

  switch (verdict.state) {
    case "storable":
      return storedText(data);
    case "replaced":
      return replaced(verdict.cause);
    default:
      return unreachable(verdict, "errorDataText");
  }
}

function storedText(data: unknown): StoredErrorData {
  const written = engineKeysKeptText(data, ROOT_PATH, MAX_JSON_DEPTH);

  if (!written.ok) {
    throw new TypeError(`errorDataText: the writer refused data that the fault walk took (${written.error.refusal})`);
  }

  return Object.freeze({ state: "stored", text: writtenJsonText(written.value) });
}

function replaced(cause: DataRefusedCause): StoredErrorData {
  const text = writtenJsonText(`{${JSON.stringify(DATA_REFUSED_KEY)}:${JSON.stringify(cause)}}`);

  return Object.freeze({ state: "replaced", cause, text });
}

/** The verdict of the faults, in the order of the Python client's checks (see the file comment). */
function verdictOf(faults: DataFaults): DataVerdict {
  const unreadable = faults.notJsonNative || faults.nonFinite || faults.textTooDeep || faults.loneSurrogate;

  if (!unreadable && !faults.integerTooLong) {
    return { state: "storable" };
  }

  if (faults.notJsonNative || faults.nonFinite || faults.textTooDeep) {
    return { state: "replaced", cause: "StrictJsonError" };
  }

  if (faults.deepest > PYDANTIC_DUMP_DEPTH) {
    return { state: "replaced", cause: "ValueValidationError" };
  }

  const scanned = faults.loneSurrogate || faults.engineKey;

  return { state: "replaced", cause: scanned ? "StrictJsonError" : "SerializationError" };
}

function faultsOf(data: unknown): DataFaults {
  const faults: DataFaults = {
    notJsonNative: false,
    nonFinite: false,
    textTooDeep: false,
    loneSurrogate: false,
    engineKey: false,
    integerTooLong: false,
    deepest: 0,
  };

  visit(data, 0, faults);

  return faults;
}

/**
 * Records the faults of a value at `depth` (the arrays and objects around it). The walk stops at the text bound. The
 * `switch` lists each of the eight results of `typeof`.
 */
function visit(value: unknown, depth: number, faults: DataFaults): void {
  switch (typeof value) {
    case "boolean":
      return;
    case "string":
      faults.loneSurrogate ||= !value.isWellFormed();

      return;
    case "number":
      faults.nonFinite ||= !Number.isFinite(value);

      return;
    case "bigint":
      faults.integerTooLong ||= (value < 0n ? -value : value).toString().length > MAX_INT_DIGITS;

      return;
    case "object":
      return value === null ? undefined : visitContainer(value, depth + 1, faults);
    case "undefined":
    case "function":
    case "symbol":
      faults.notJsonNative = true;
  }
}

function visitContainer(value: object, level: number, faults: DataFaults): void {
  if (!isArray(value) && !isPlainObject(value)) {
    faults.notJsonNative = true;

    return;
  }

  if (level > MAX_JSON_DEPTH) {
    faults.textTooDeep = true;

    return;
  }

  faults.deepest = Math.max(faults.deepest, level);

  if (isArray(value)) {
    for (const item of value) {
      visit(item, level, faults);
    }

    return;
  }

  for (const [key, item] of Object.entries(value)) {
    faults.engineKey ||= isEngineKey(key);
    faults.loneSurrogate ||= !key.isWellFormed();
    visit(item, level, faults);
  }
}
