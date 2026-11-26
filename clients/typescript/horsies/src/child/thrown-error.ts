// The exception record of a value that a task body threw: the one rule for a thrown value, in the main thread of a
// child and in a seat isolate.
//
// From a thrown `Error`:
// - `type` is `error.name`; `message` is `error.message`; `repr` is the same text as `message`;
// - `traceback` is `error.stack`, then the stack of each `cause`, at most `MAX_CAUSE_LINKS` links (the Python
//   client's `MAX_EXCEPTION_CHAIN_LINKS`);
// - a `ZodError` stores a summary of its issues (path and code, never a value) in `message`, `repr` and `traceback`,
//   as the Python client stores a validation error.
// From a thrown value that is not an `Error`:
// - `type`: `null`, `undefined`, the constructor name of an object (`Object` when it has none), or the type text of
//   another value (`string`, `number`, `bigint`, `boolean`, `symbol`);
// - `message` and `repr`: `String(value)`, or `value of type <type>` when that conversion throws; `traceback` is
//   empty.
// Each text is cut at a character boundary: `message` and `repr` at 16 KiB of UTF-8, `traceback` at 64 KiB. A read
// that throws (a getter, a revoked `Proxy`) gives a fixed text. No function here throws.

import { isArray, isBigint, isBoolean, isNumber, isObjectLike, isString } from "../codec/values.ts";

/** The longest `message` and `repr`, in bytes of UTF-8 (the core's panic text bound). */
export const MESSAGE_CUT_BYTES: number = 16 * 1024;

/** The longest `traceback`, in bytes of UTF-8 (the core's backtrace bound). */
export const TRACEBACK_CUT_BYTES: number = 64 * 1024;

/** The most links of a `cause` chain in a traceback, the thrown error included. */
export const MAX_CAUSE_LINKS = 64;

/** The most issues that the summary of a `ZodError` writes; the text then adds `(+N more)`. */
export const MAX_SUMMARY_ISSUES = 10;

/** Written between the stack of an error and the stack of its cause. */
export const CAUSE_SEPARATOR = "\n\nCaused by: ";

/** The text of a fact that cannot be read. */
const NO_TEXT = "a thrown value with no readable text";

/** The record of a thrown value. `module` is always empty: JavaScript has no module of a class. */
export type ThrownRecord = {
  readonly type: string;
  readonly message: string;
  readonly repr: string;
  readonly traceback: string;
};

/** The record of the thrown value `cause`. Pure; never throws. */
export function thrownRecord(cause: unknown): ThrownRecord {
  const error = errorOf(cause);

  if (error === undefined) {
    const message = cutAtCharacter(valueText(cause), MESSAGE_CUT_BYTES);

    return Object.freeze({ type: typeNameOf(cause), message, repr: message, traceback: "" });
  }

  const type = cutAtCharacter(readText(() => error.name), MESSAGE_CUT_BYTES);
  const summary = zodSummaryOf(error);

  if (summary !== undefined) {
    const text = cutAtCharacter(summary, MESSAGE_CUT_BYTES);

    return Object.freeze({ type, message: text, repr: text, traceback: text });
  }

  const message = cutAtCharacter(readText(() => error.message), MESSAGE_CUT_BYTES);

  return Object.freeze({ type, message, repr: message, traceback: cutAtCharacter(chainText(error), TRACEBACK_CUT_BYTES) });
}

/** `cause` as an `Error`, or `undefined` for any other value (and for a value whose prototype check throws). */
function errorOf(cause: unknown): Error | undefined {
  try {
    return cause instanceof Error ? cause : undefined;
  } catch {
    return undefined;
  }
}

/** The type of a thrown value that is not an `Error`. Pure. */
export function typeNameOf(cause: unknown): string {
  if (cause === null) {
    return "null";
  }

  if (cause === undefined) {
    return "undefined";
  }

  if (isObjectLike(cause)) {
    return constructorNameOf(cause);
  }

  return primitiveTypeName(cause);
}

/** The type text of a primitive that is not `null` or `undefined`: the only one left after these four is a symbol. */
function primitiveTypeName(cause: unknown): string {
  if (isString(cause)) {
    return "string";
  }

  if (isNumber(cause)) {
    return "number";
  }

  if (isBigint(cause)) {
    return "bigint";
  }

  return isBoolean(cause) ? "boolean" : "symbol";
}

/** The constructor name of an object, or `Object` when it has no constructor with a name. */
function constructorNameOf(value: object): string {
  try {
    const prototype: unknown = Object.getPrototypeOf(value);

    if (!isObjectLike(prototype)) {
      return "Object";
    }

    const constructor: unknown = Reflect.get(prototype, "constructor");
    const name: unknown = isObjectLike(constructor) ? Reflect.get(constructor, "name") : undefined;

    return isString(name) && name.length > 0 ? name : "Object";
  } catch {
    return "Object";
  }
}

/** `String(cause)`, or `value of type <type>` when that conversion throws. */
function valueText(cause: unknown): string {
  try {
    return String(cause);
  } catch {
    return `value of type ${typeNameOf(cause)}`;
  }
}

/** The text of one read; the fixed text when the read or its conversion throws. */
function readText(read: () => unknown): string {
  try {
    return String(read());
  } catch {
    return NO_TEXT;
  }
}

/** The traceback of an error: its stack, then the stack of each cause, at most `MAX_CAUSE_LINKS` links. */
function chainText(error: Error): string {
  const blocks: string[] = [];
  const seen = new Set<unknown>();
  let link: unknown = error;

  for (let index = 0; index < MAX_CAUSE_LINKS; index += 1) {
    blocks.push(linkText(link));
    seen.add(link);

    const next = causeOf(link);

    if (next.kind === "noCause" || seen.has(next.cause)) {
      return blocks.join(CAUSE_SEPARATOR);
    }

    link = next.cause;
  }

  return `${blocks.join(CAUSE_SEPARATOR)}\n\n(older causes not shown: the chain has more than ${String(MAX_CAUSE_LINKS)} links)`;
}

/** The cause of a link of a chain: an `Error` with an own or inherited `cause` member has one. */
type CauseRead = { readonly kind: "noCause" } | { readonly kind: "cause"; readonly cause: unknown };

function causeOf(cause: unknown): CauseRead {
  const error = errorOf(cause);

  if (error === undefined) {
    return { kind: "noCause" };
  }

  try {
    return "cause" in error ? { kind: "cause", cause: error.cause } : { kind: "noCause" };
  } catch {
    return { kind: "noCause" };
  }
}

/** The text of one link: the stack of an error (its summary for a `ZodError`), else the value's text. */
function linkText(cause: unknown): string {
  const error = errorOf(cause);

  if (error === undefined) {
    return valueText(cause);
  }

  const summary = zodSummaryOf(error);

  if (summary !== undefined) {
    return `${readText(() => error.name)}: ${summary}`;
  }

  const stack: unknown = readValue(() => error.stack);

  return isString(stack) ? stack : `${readText(() => error.name)}: ${readText(() => error.message)}`;
}

/** The value of one read; `undefined` when the read throws. */
function readValue(read: () => unknown): unknown {
  try {
    return read();
  } catch {
    return undefined;
  }
}

/**
 * The summary of a `ZodError`: each issue as `<path>: [<code>]` (`[<code>]` for an issue at the top), joined by
 * `; `, at most `MAX_SUMMARY_ISSUES` issues and `(+N more)`. `undefined` for any other error. It holds no value and no
 * message of an issue.
 */
export function zodSummaryOf(error: Error): string | undefined {
  if (readText(() => error.name) !== "ZodError") {
    return undefined;
  }

  const issues: unknown = readValue(() => Reflect.get(error, "issues"));

  if (!isArray(issues)) {
    return undefined;
  }

  const shown = issues.slice(0, MAX_SUMMARY_ISSUES).map((issue) => issueText(issue));
  const hidden = issues.length - shown.length;

  return [...shown, ...(hidden > 0 ? [`(+${String(hidden)} more)`] : [])].join("; ");
}

/** One issue as `<path>: [<code>]`. A part that is not a text or a number is written as `?`. */
function issueText(cause: unknown): string {
  const code: unknown = isObjectLike(cause) ? readValue(() => Reflect.get(cause, "code")) : undefined;
  const path: unknown = isObjectLike(cause) ? readValue(() => Reflect.get(cause, "path")) : undefined;
  const codeText = isString(code) ? code : "unknown";
  const parts = isArray(path) ? path.map((part) => (isString(part) || isNumber(part) ? String(part) : "?")) : [];

  return parts.length === 0 ? `[${codeText}]` : `${parts.join(".")}: [${codeText}]`;
}

/** `text` cut to at most `bytes` bytes of UTF-8, at a character boundary. Pure. */
export function cutAtCharacter(text: string, bytes: number): string {
  let used = 0;
  let end = 0;

  for (const character of text) {
    const size = utf8Size(character);

    if (used + size > bytes) {
      return text.slice(0, end);
    }

    used += size;
    end += character.length;
  }

  return text;
}

/** The size in bytes of the UTF-8 form of one character (a lone surrogate is written as U+FFFD: 3 bytes). */
function utf8Size(character: string): number {
  const point = character.codePointAt(0) ?? 0;

  if (point < 0x80) {
    return 1;
  }

  if (point < 0x800) {
    return 2;
  }

  return point < 0x1_00_00 ? 3 : 4;
}
