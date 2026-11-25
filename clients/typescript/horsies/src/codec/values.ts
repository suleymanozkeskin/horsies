// Guards over the JavaScript values that cross the codec boundary. The codec takes `unknown` (a sender's value, a
// body's return) and narrows it here.

/** Whether `value` is a string. Pure. */
export function isString(value: unknown): value is string {
  return typeof value === "string";
}

/** Whether `value` is a `number`. Pure. */
export function isNumber(value: unknown): value is number {
  return typeof value === "number";
}

/** Whether `value` is a `bigint`. Pure. */
export function isBigint(value: unknown): value is bigint {
  return typeof value === "bigint";
}

/** Whether `value` is `true` or `false`. Pure. */
export function isBoolean(value: unknown): value is boolean {
  return typeof value === "boolean";
}
