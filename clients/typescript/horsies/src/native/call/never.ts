// The end of an exhaustive `switch`.

import { ClientDefect } from "../../thrown-errors.ts";
import { raiseDefect } from "./defect.ts";
import { thrownFacts } from "./thrown-facts.ts";

/**
 * Raise the defect of a value that the types make impossible: the native addon gave a value that its
 * declarations do not name. It follows the defect rule (a worker process aborts).
 */
export function unreachable(value: never): never {
  return raiseDefect(
    new ClientDefect({
      subject: "exhaustive switch",
      rule: `a value outside its declared type reached it: ${describeValue(value)}`,
    }),
  );
}

/** The text of a value; its JSON when it has one. Never throws. */
function describeValue(value: never): string {
  try {
    return JSON.stringify(value);
  } catch (cause) {
    return `${thrownFacts(value).message} (no JSON: ${thrownFacts(cause).message})`;
  }
}
