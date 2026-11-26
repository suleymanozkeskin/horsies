// `Decimal`: an exact decimal value, held as Python's `str(Decimal)` text. It has no arithmetic.

import { canonicalDecimalText, type DecimalRefused } from "../codec/decimal-text.ts";
import { ok, type Result } from "./result.ts";

/** An exact decimal value of `h.decimal()`, stored as a JSON string: `"19.90"`, `"1E+3"`. */
export class Decimal {
  readonly #text: string;

  private constructor(text: string) {
    this.#text = text;
    Object.freeze(this);
  }

  /**
   * The decimal of a text that the Python client takes as a decimal, normalised to Python's `str(Decimal(text))`:
   * `1e3` is `1E+3`, `0.0000001` is `1E-7`, `1.10` stays `1.10`, `1_000` is `1000`. As Python's `Decimal`, it takes
   * whitespace at both ends, `_` at any place and any Unicode decimal digit, and it refuses an exponent outside
   * Python's range. Refuses an infinity and a NaN (`NotFinite`) and any other text (`NotADecimal`). Pure.
   */
  static parse(text: string): Result<Decimal, DecimalRefused> {
    const canonical = canonicalDecimalText(text);

    return canonical.ok ? ok(new Decimal(canonical.value)) : canonical;
  }

  /** Whether an object is a `Decimal` of this package (it has the private text). Pure. */
  static isDecimal(value: object): value is Decimal {
    return #text in value;
  }

  /** Python's `str(Decimal)` text: the stored text, without the JSON quotes. */
  toString(): string {
    return this.#text;
  }

  /** The same text: `1.10` and `1.1` are different decimals, as in the stored text. */
  equals(other: Decimal): boolean {
    return this.#text === other.#text;
  }
}
