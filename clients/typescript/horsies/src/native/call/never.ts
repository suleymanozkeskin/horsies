// The end of an exhaustive `switch`.

import { raiseDefect } from "./defect.ts";
import { impossibleValueDefect } from "./impossible-value.ts";

/**
 * Raise the defect of a value that the types make impossible: the native addon gave a value that its
 * declarations do not name. It follows the defect rule (a worker process aborts).
 */
export function unreachable(value: never): never {
  return raiseDefect(impossibleValueDefect(value));
}
