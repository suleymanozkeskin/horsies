// expect: TS2367 at 8, TS2367 at 10
// A read error that does not decode is `resultDeserializationError` (its code), and a stored code that the contract
// does not list is `unknownCustomCode`; `storedResultUnreadable` and `unknownUserCode` are no read error kinds.

import type { ReadError } from "../../../horsies/src/index.ts";

export function isUnreadable(error: ReadError): boolean {
  return error.kind === "storedResultUnreadable"
    || error.kind === "resultDeserializationError"
    || error.kind === "unknownUserCode";
}
