// The `data` of a task error that a body returns: never refused. Data that cannot be stored is replaced by
// `{"data_refused": "<cause>"}` with the cause that Python and the core store; the code and the message stay.
//
// Causes (the core's `UserValueRefused::stored_cause` and the Python client's value fence):
// - `StrictJsonError`: a value that is not JSON-native (a `Date`, a `Map`, `undefined`, NaN, a lone surrogate), a key
//   that only the engine writes, or more levels than any stored text may have;
// - `ValueValidationError`: more levels than the `data` of an error may have;
// - `SerializationError`: an integer of more than 4,300 digits.

import { unreachable } from "../wire/unreachable.ts";
import { DATA_REFUSED_KEY, ERROR_DATA_DEPTH_LIMIT, MAX_JSON_DEPTH } from "./bounds.ts";
import { type JsonText, writtenJsonText } from "./json-text.ts";
import { recordedJsonText } from "./json-value.ts";
import { type EncodeRefusalKind, ROOT_PATH } from "./refusal.ts";

/** The cause of a replaced `data`, as Python and the core store it. */
export type DataRefusedCause = "StrictJsonError" | "ValueValidationError" | "SerializationError";

/** The stored `data` of a task error: its own text, or the replacement and its cause. */
export type StoredErrorData =
  | { readonly state: "stored"; readonly text: JsonText }
  | { readonly state: "replaced"; readonly cause: DataRefusedCause; readonly text: JsonText };

/** The stored `data` of a task error. `null` is stored as `null`. Never refused. Pure. */
export function errorDataText(data: unknown): StoredErrorData {
  const written = recordedJsonText(data, ROOT_PATH, MAX_JSON_DEPTH);

  if (!written.ok) {
    return replaced(causeOf(written.error.kind));
  }

  if (written.value.engineKeyFound) {
    return replaced("StrictJsonError");
  }

  return written.value.deepest > ERROR_DATA_DEPTH_LIMIT
    ? replaced("ValueValidationError")
    : Object.freeze({ state: "stored", text: writtenJsonText(written.value.text) });
}

function replaced(cause: DataRefusedCause): StoredErrorData {
  const text = writtenJsonText(`{${JSON.stringify(DATA_REFUSED_KEY)}:${JSON.stringify(cause)}}`);

  return Object.freeze({ state: "replaced", cause, text });
}

/** The cause of a refusal of the JSON writer. A kind that the JSON writer never gives is a defect. */
function causeOf(kind: EncodeRefusalKind): DataRefusedCause {
  switch (kind) {
    case "ExplicitUndefined":
    case "NotJsonNative":
    case "NonFiniteFloat":
    case "LoneSurrogate":
    case "EngineKey":
    case "TooDeep":
      return "StrictJsonError";
    case "IntegerTooLong":
      return "SerializationError";
    case "WrongType":
    case "NotSafeInteger":
    case "UnknownMember":
    case "MissingMember":
    case "YearOutOfRange":
    case "NamedTimeZone":
    case "NotIsoCalendar":
    case "NotCanonicalUuid":
    case "NotAMember":
    case "CheckFailed":
      throw new TypeError(`errorDataText: the JSON writer gave the refusal ${kind}, which it never gives`);
    default:
      return unreachable(kind, "causeOf");
  }
}
