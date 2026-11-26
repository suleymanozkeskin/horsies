// expect: TS2678 at 10, TS2678 at 12
// The read error kinds are named after their catalog codes: `waitTimedOut` and `invalidWait` are no kinds.

import type { ReadError } from "../../../horsies/src/index.ts";

export function retryableOf(error: ReadError): boolean {
  switch (error.kind) {
    case "database":
      return error.retryable;
    case "waitTimedOut":
      return true;
    case "invalidWait":
      return false;
    default:
      return false;
  }
}
