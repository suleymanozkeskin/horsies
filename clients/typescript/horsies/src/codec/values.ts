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

/** Whether `value` is a function, of this realm or of another one (a `vm` context). Pure. */
export function isFunction(value: unknown): value is (...args: never[]) => unknown {
  return typeof value === "function";
}

/** Whether `value` is `true` or `false`. Pure. */
export function isBoolean(value: unknown): value is boolean {
  return typeof value === "boolean";
}

/** A UUID in the lower-case 8-4-4-4-12 form, as Python's `str(UUID)` and `crypto.randomUUID()` write it. */
const UUID_TEXT = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** Whether a text is a UUID in the lower-case 8-4-4-4-12 form. Pure. */
export function isUuidText(text: string): boolean {
  return UUID_TEXT.test(text);
}

/** Whether `value` is an array. Pure. */
export function isArray(value: unknown): value is readonly unknown[] {
  return Array.isArray(value);
}

/** Whether `value` is an object or a function, as opposed to a primitive and `null`. Pure. */
export function isObjectLike(value: unknown): value is object {
  return (typeof value === "object" && value !== null) || typeof value === "function";
}

/**
 * Whether `value` holds data only: an object that is not an array, whose prototype is `Object.prototype` or `null`,
 * and that has no symbol key. A `Date`, a `Map`, a class instance or a function is not. Pure.
 */
export function isPlainObject(value: unknown): value is Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const prototype: unknown = Object.getPrototypeOf(value);

  return (prototype === Object.prototype || prototype === null) && Object.getOwnPropertySymbols(value).length === 0;
}

/** Sets an own data property, so that a key `__proto__` is a member and never a prototype. */
export function setOwn(target: Record<string, unknown>, key: string, value: unknown): void {
  if (key === PROTO_KEY) {
    Object.defineProperty(target, key, { value, writable: true, enumerable: true, configurable: true });
  } else {
    target[key] = value;
  }
}

const PROTO_KEY = "__proto__";
