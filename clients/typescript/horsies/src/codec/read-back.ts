// The read-back at a boundary (a send, a body's output, a static node input, a workflow start): the worker can read
// what the sender wrote, and the value that it reads writes the same text again.
//
// Steps: encode the value to text 1; decode text 1 in the worker form (defaults filled, checks run); decode text 1 in
// the stored form (defaults left out) and encode that value to text 2; text 2 must equal text 1. The stored form is
// needed because the sender leaves defaults out, and a worker-form value would write them. Cost: one encode, two
// decodes, one encode.

import type { Kind, ObjectKind } from "../wire/kinds.ts";
import { err, type Result } from "../wire/result.ts";
import type { JsonText } from "./json-text.ts";
import { type DecodeForm, decodeParameters, decodeValue } from "./reader.ts";
import {
  type DecodeRefused,
  type EncodeRefused,
  type ReadBackChanged,
  type ReadBackRefused,
  refusalText,
  ROOT_PATH,
} from "./refusal.ts";
import { encodeParameters, encodeValue } from "./writer.ts";

/** The text of a value that reads back, or why not. */
export type ReadBackOutcome = Result<JsonText, EncodeRefused | ReadBackRefused | ReadBackChanged>;

/** The stored text of `value` under `kind`, after a read-back. Pure. */
export function encodeWithReadBack(kind: Kind, value: unknown): ReadBackOutcome {
  return readBack(
    encodeValue(kind, value),
    (text, form) => decodeValue(kind, text, form),
    (read) => encodeValue(kind, read),
  );
}

/** The stored text of a task input, after a read-back. Pure. */
export function encodeParametersWithReadBack(parameters: ObjectKind, value: unknown): ReadBackOutcome {
  return readBack(
    encodeParameters(parameters, value),
    (text, form) => decodeParameters(parameters, text, form),
    (read) => encodeParameters(parameters, read),
  );
}

function readBack(
  written: Result<JsonText, EncodeRefused>,
  decode: (text: JsonText, form: DecodeForm) => Result<unknown, DecodeRefused>,
  encodeAgain: (read: unknown) => Result<JsonText, EncodeRefused>,
): ReadBackOutcome {
  if (!written.ok) {
    return written;
  }

  const worker = decode(written.value, "worker");

  if (!worker.ok) {
    return err(refused(worker.error));
  }

  const stored = decode(written.value, "stored");

  if (!stored.ok) {
    return err(refused(stored.error));
  }

  const again = encodeAgain(stored.value);

  if (!again.ok) {
    return err(changed(`the read value does not write again: ${refusalText(again.error)}`));
  }

  if (again.value === written.value) {
    return written;
  }

  const offset = firstDifference(written.value, again.value);

  return err(changed(`the read value writes another text; first difference at offset ${offset}`));
}

function refused(cause: DecodeRefused): ReadBackRefused {
  return Object.freeze({
    kind: "readBackRefused",
    path: ROOT_PATH,
    detail: `the worker cannot read what the sender wrote: ${refusalText(cause)}`,
    cause,
  });
}

function changed(detail: string): ReadBackChanged {
  return Object.freeze({ kind: "readBackChanged", path: ROOT_PATH, detail });
}

/** The offset of the first character that differs between two texts. Pure. */
function firstDifference(first: string, second: string): number {
  const shorter = Math.min(first.length, second.length);

  for (let index = 0; index < shorter; index += 1) {
    if (first.charCodeAt(index) !== second.charCodeAt(index)) {
      return index;
    }
  }

  return shorter;
}
