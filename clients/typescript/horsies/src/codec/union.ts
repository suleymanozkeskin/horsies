// Literals and unions: the member texts, and the refusal of a value that no union member takes.

import type { LiteralMember } from "../wire/kinds.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { DecodeRefused, EncodeRefused } from "./refusal.ts";
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
export function unionRefusal<R extends EncodeRefused | DecodeRefused>(refusals: readonly R[], noMember: R): R {
  return refusals.find((refusal) => !isWrongType(refusal)) ?? noMember;
}

/** Whether a member refused the value as `WrongType`: the member does not take the value's JavaScript type. Pure. */
function isWrongType(refusal: EncodeRefused | DecodeRefused): boolean {
  switch (refusal.kind) {
    case "encodeRefused":
    case "decodeRefused":
      return refusal.refusal === "WrongType";
    case "envelopeRefused":
      return false;
    default:
      return unreachable(refusal, "isWrongType");
  }
}
