// Python's `repr` text of a finite float: a port of `python_float_text` (`core/src/core/codec/strict.rs`).

/** The decimal exponents that Python writes in fixed notation (`PYTHON_FIXED_EXPONENTS` of the core). */
const FIXED_EXPONENT_MIN = -4;
const FIXED_EXPONENT_MAX = 15;

/**
 * Python's text of a finite float: `7.0`, `0.1`, `1e-05`, `1e+16`, `-0.0`.
 *
 * Input: a finite number. A caller refuses `NaN` and the infinities first; a non-finite input is a defect and
 * throws a `TypeError`. `toExponential()` with no argument gives the shortest digits that read back, as
 * `d.ddde±X`. `-0` is its own case: `(-0).toExponential()` loses the sign. Pure.
 */
export function pythonFloatText(value: number): string {
  if (!Number.isFinite(value)) {
    throw new TypeError(`pythonFloatText: ${String(value)} is not a finite float`);
  }

  if (Object.is(value, -0)) {
    return "-0.0";
  }

  const negative = value < 0;
  const scientific = Math.abs(value).toExponential();
  const exponentAt = scientific.indexOf("e");
  const mantissa = scientific.slice(0, exponentAt);
  const exponent = Number(scientific.slice(exponentAt + 1));
  const digits = mantissa.length > 1 ? mantissa.slice(0, 1) + mantissa.slice(2) : mantissa;

  const body =
    exponent >= FIXED_EXPONENT_MIN && exponent <= FIXED_EXPONENT_MAX
      ? fixedText(digits, exponent)
      : scientificText(digits, exponent);

  return negative ? `-${body}` : body;
}

/** `digits` times ten to `exponent` (counted from the first digit), in fixed notation. Pure. */
function fixedText(digits: string, exponent: number): string {
  if (exponent < 0) {
    return `0.${"0".repeat(-exponent - 1)}${digits}`;
  }

  const integerDigits = exponent + 1;

  return digits.length <= integerDigits
    ? `${digits}${"0".repeat(integerDigits - digits.length)}.0`
    : `${digits.slice(0, integerDigits)}.${digits.slice(integerDigits)}`;
}

/** `digits` times ten to `exponent`, as `d.ddde±XX`. Pure. */
function scientificText(digits: string, exponent: number): string {
  const head = digits.length > 1 ? `${digits.slice(0, 1)}.${digits.slice(1)}` : digits;
  const exponentSign = exponent < 0 ? "-" : "+";

  return `${head}e${exponentSign}${String(Math.abs(exponent)).padStart(2, "0")}`;
}
