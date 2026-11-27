// expect: TS2678 at 10, TS2678 at 12, TS2678 at 14, TS2678 at 16
// Read error kinds are named after their catalog codes: `waitTimedOut`, `invalidWait`, `database`, `unknownUserCode`.

import type { ReadError } from "../../../horsies/src/index.ts";

export function retryableOf(error: ReadError): boolean {
  switch (error.kind) {
    case "databaseError":
      return error.retryable;
    case "waitTimedOut":
      return true;
    case "invalidWait":
      return false;
    case "database":
      return true;
    case "unknownUserCode":
      return false;
    default:
      return false;
  }
}
