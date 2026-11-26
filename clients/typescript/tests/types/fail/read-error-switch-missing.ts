// expect: TS2345 at 21
// A switch over the read error kinds that misses one fails at the `assertNever` line.

import type { ReadError } from "../../../horsies/src/index.ts";

function assertNever(value: never): never {
  throw new TypeError(String(value));
}

export function describeReadError(error: ReadError): string {
  switch (error.kind) {
    case "waitTimedOut":
    case "taskNotFound":
    case "resultNotAvailable":
    case "storedResultUnreadable":
    case "unknownUserCode":
    case "waitInsideTask":
    case "database":
      return error.kind;
    default:
      return assertNever(error);
  }
}
