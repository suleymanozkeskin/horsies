// Literals and unions: the member texts, and the refusal of a value that no union member takes.

import type { LiteralMember } from "../wire/kinds.ts";
import { isBoolean, isNumber, isString } from "./values.ts";

/** Whether `value` has the JavaScript type of a literal member. Pure. */
export function isLiteralType(value: unknown): value is LiteralMember {
  return value === null || isString(value) || isNumber(value) || isBoolean(value);
}

/** The stored text of a literal member: a JSON string, decimal digits, `true`, `false` or `null`. Pure. */
export function literalText(member: LiteralMember): string {
  return isString(member) ? JSON.stringify(member) : String(member);
}

/**
 * The refusal of a value that no member of a union takes: the first member refusal that is not `WrongType` (a member
 * of the value's type refused it under its own rule), else `noMember` (no member has the value's type). Pure.
 */
export function unionRefusal<R extends { readonly kind: string }>(refusals: readonly R[], noMember: R): R {
  return refusals.find((refusal) => refusal.kind !== "WrongType") ?? noMember;
}
