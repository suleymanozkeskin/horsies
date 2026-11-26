// The input type of the codec reader: a text that passed the core's strict read rule, or that the codec wrote.
//
// The reader does not check the strict rule again (repeated keys, lone surrogate escapes, non-standard constants,
// integers of more than 4,300 digits, more than 300 levels): the core checks it where the text enters the system.
// Two functions make a `JsonText`, one per kind of path. The test `tests/unit/codec/reader-paths.test.ts` lists
// every module that calls them, so a new path to the reader names the check that runs in front of it.

declare const checkedText: unique symbol;

/** A text that passed the core's strict read rule, or that the codec writer built. Only the reader takes it. */
export type JsonText = string & { readonly [checkedText]: "JsonText" };

/**
 * A text that the core's strict read rule accepted on this path (`check_json_text`, or a core read that runs it:
 * the child input of `build_task_input`, `StoredEnvelopeText::parse`). Call it only after that check. Pure.
 */
export function jsonTextCheckedByCore(text: string): JsonText {
  // SAFETY: the caller's path ran the core's strict read rule on `text`; the type records that fact.
  return text as JsonText;
}

/** A text that the codec writer built: strict JSON by construction. Only the writer calls it. Pure. */
export function writtenJsonText(text: string): JsonText {
  // SAFETY: the writer builds strict JSON only (finite floats, well-formed strings, bounded depth).
  return text as JsonText;
}
