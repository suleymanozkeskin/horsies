// The reader of a JSON text that keeps what the declared type needs from each number text.
//
// `JSON.parse` gives one `number` for `7` and `7.0`, and rounds an integer outside the safe range. The typed decode
// must refuse `7.0` under `h.int()` and keep every digit under `h.bigint()`, so this reader builds a tree in which:
// - an integer text in the safe range is a `number` (`-0` becomes `0`, as Python's `int`);
// - an integer text outside the safe range is a `bigint`, with every digit;
// - a float text (`.`, `e` or `E`) with a non-integral value is a `number`;
// - a float text with an integral value (`7.0`, `1e+16`, `-0.0`) is an `IntegralFloat`.
//
// Precondition: the text is a `JsonText` (see `json-text.ts`). A text that is not JSON, or deeper than the core's
// bound, is a defect of the path that made the `JsonText`: the reader throws a `TypeError`. The reader does not look
// for repeated keys (the last one wins) or lone surrogate escapes: the core refuses them first.

import { MAX_JSON_DEPTH } from "./bounds.ts";
import type { JsonText } from "./json-text.ts";

/** A float text whose value is an integer: `h.float()` takes `value`, `h.int()` refuses it. */
export class IntegralFloat {
  readonly value: number;

  constructor(value: number) {
    this.value = value;
    Object.freeze(this);
  }
}

/** A JSON object of the tree: no prototype, so a key `__proto__` is an own member like any other. */
export type TreeObject = { readonly [key: string]: TreeValue };

/** A JSON value with the number facts of its text. */
export type TreeValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | IntegralFloat
  | readonly TreeValue[]
  | TreeObject;

const NUMBER_TEXT = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
const HEX_DIGITS = /^[0-9a-fA-F]{4}$/;

const SPACE = 32;
const LINE_FEED = 10;
const CARRIAGE_RETURN = 13;
const TAB = 9;
const QUOTE = 34;
const BACKSLASH = 92;
const SLASH = 47;
const COMMA = 44;
const COLON = 58;
const OPEN_BRACE = 123;
const CLOSE_BRACE = 125;
const OPEN_BRACKET = 91;
const CLOSE_BRACKET = 93;
const LOWER_B = 98;
const LOWER_F = 102;
const LOWER_N = 110;
const LOWER_R = 114;
const LOWER_T = 116;
const LOWER_U = 117;
const DOT = 46;
const LOWER_E = 101;
const UPPER_E = 69;
const UNICODE_ESCAPE_LENGTH = 6;
const SHORT_ESCAPE_LENGTH = 2;

/** The tree of `text`. Throws a `TypeError` when the text is not JSON (a defect; see the file comment). Pure. */
export function readTree(text: JsonText): TreeValue {
  const reader = new TreeReader(text);
  const value = reader.value(0);

  reader.skipSpace();

  if (reader.at !== text.length) {
    reader.fail("data after the value");
  }

  return value;
}

/** Whether a tree value is an array. Pure. */
export function isTreeArray(node: TreeValue): node is readonly TreeValue[] {
  return Array.isArray(node);
}

/** Whether a tree value is an object. Pure. */
export function isTreeObject(node: TreeValue): node is TreeObject {
  return typeof node === "object" && node !== null && !Array.isArray(node) && !(node instanceof IntegralFloat);
}

/** Whether a number text is an integer text: no `.`, `e` or `E`. Pure. */
export function isIntegerText(text: string): boolean {
  for (let index = 0; index < text.length; index += 1) {
    const code = text.charCodeAt(index);

    if (code === DOT || code === LOWER_E || code === UPPER_E) {
      return false;
    }
  }

  return true;
}

/** The tree value of one number text. Pure. */
export function numberNode(text: string): number | bigint | IntegralFloat {
  const parsed = Number(text);

  if (isIntegerText(text)) {
    if (Number.isSafeInteger(parsed)) {
      return parsed === 0 ? 0 : parsed;
    }

    return BigInt(text);
  }

  return Number.isInteger(parsed) ? new IntegralFloat(parsed) : parsed;
}

function newTreeObject(): Record<string, TreeValue> {
  // SAFETY: `Object.create(null)` returns an empty object with no prototype; it has no member until one is set.
  return Object.create(null) as Record<string, TreeValue>;
}

/** One pass over one text. `at` is the offset of the next character. */
class TreeReader {
  readonly text: string;
  at = 0;

  constructor(text: string) {
    this.text = text;
  }

  fail(cause: string): never {
    throw new TypeError(
      `readTree: the JsonText is not JSON at offset ${this.at} (${cause}); ` +
        "the path that made it skipped the core's strict read rule",
    );
  }

  skipSpace(): void {
    let code = this.text.charCodeAt(this.at);

    while (code === SPACE || code === LINE_FEED || code === CARRIAGE_RETURN || code === TAB) {
      this.at += 1;
      code = this.text.charCodeAt(this.at);
    }
  }

  value(depth: number): TreeValue {
    this.skipSpace();

    switch (this.text.charCodeAt(this.at)) {
      case OPEN_BRACE:
        return this.object(this.enter(depth));
      case OPEN_BRACKET:
        return this.array(this.enter(depth));
      case QUOTE:
        return this.string();
      case LOWER_T:
        return this.word("true", true);
      case LOWER_F:
        return this.word("false", false);
      case LOWER_N:
        return this.word("null", null);
      default:
        return this.number();
    }
  }

  enter(depth: number): number {
    if (depth + 1 > MAX_JSON_DEPTH) {
      this.fail(`more than ${MAX_JSON_DEPTH} levels`);
    }

    return depth + 1;
  }

  word(word: string, value: boolean | null): boolean | null {
    if (!this.text.startsWith(word, this.at)) {
      this.fail("an unknown word");
    }

    this.at += word.length;

    return value;
  }

  number(): number | bigint | IntegralFloat {
    NUMBER_TEXT.lastIndex = this.at;
    const match = NUMBER_TEXT.exec(this.text);

    if (match === null) {
      this.fail("not a value");
    }

    this.at += match[0].length;

    return numberNode(match[0]);
  }

  string(): string {
    const text = this.text;
    let at = this.at + 1;
    let start = at;
    let built = "";

    for (;;) {
      const code = text.charCodeAt(at);

      if (code === QUOTE) {
        this.at = at + 1;

        return built + text.slice(start, at);
      }

      if (code === BACKSLASH) {
        built += text.slice(start, at) + this.escape(at);
        at += text.charCodeAt(at + 1) === LOWER_U ? UNICODE_ESCAPE_LENGTH : SHORT_ESCAPE_LENGTH;
        start = at;
        continue;
      }

      // A control character, or the end of the text (`NaN`).
      if (!(code >= SPACE)) {
        this.at = at;
        this.fail("an unterminated string or a control character");
      }

      at += 1;
    }
  }

  escape(at: number): string {
    switch (this.text.charCodeAt(at + 1)) {
      case QUOTE:
        return '"';
      case BACKSLASH:
        return "\\";
      case SLASH:
        return "/";
      case LOWER_B:
        return "\b";
      case LOWER_F:
        return "\f";
      case LOWER_N:
        return "\n";
      case LOWER_R:
        return "\r";
      case LOWER_T:
        return "\t";
      case LOWER_U:
        return this.unicodeEscape(at);
      default:
        this.at = at;

        return this.fail("an unknown escape");
    }
  }

  unicodeEscape(at: number): string {
    const hex = this.text.slice(at + SHORT_ESCAPE_LENGTH, at + UNICODE_ESCAPE_LENGTH);

    if (!HEX_DIGITS.test(hex)) {
      this.at = at;
      this.fail("a \\u escape without four hex digits");
    }

    return String.fromCharCode(Number.parseInt(hex, 16));
  }

  array(depth: number): TreeValue[] {
    const items: TreeValue[] = [];

    this.at += 1;
    this.skipSpace();

    if (this.text.charCodeAt(this.at) === CLOSE_BRACKET) {
      this.at += 1;

      return items;
    }

    for (;;) {
      items.push(this.value(depth));
      this.skipSpace();
      const code = this.text.charCodeAt(this.at);

      this.at += 1;

      if (code === CLOSE_BRACKET) {
        return items;
      }

      if (code !== COMMA) {
        this.fail("an array without a comma or a closing bracket");
      }
    }
  }

  object(depth: number): TreeObject {
    const object = newTreeObject();

    this.at += 1;
    this.skipSpace();

    if (this.text.charCodeAt(this.at) === CLOSE_BRACE) {
      this.at += 1;

      return object;
    }

    for (;;) {
      const key = this.key();

      object[key] = this.value(depth);
      this.skipSpace();
      const code = this.text.charCodeAt(this.at);

      this.at += 1;

      if (code === CLOSE_BRACE) {
        return object;
      }

      if (code !== COMMA) {
        this.fail("an object without a comma or a closing brace");
      }
    }
  }

  key(): string {
    this.skipSpace();

    if (this.text.charCodeAt(this.at) !== QUOTE) {
      this.fail("an object key that is not a string");
    }

    const key = this.string();

    this.skipSpace();

    if (this.text.charCodeAt(this.at) !== COLON) {
      this.fail("an object key without a colon");
    }

    this.at += 1;

    return key;
  }
}
