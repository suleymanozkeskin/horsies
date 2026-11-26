// The defect of a value that the types make impossible. It imports nothing of the addon, so the
// loader uses it too.

import { ClientDefect } from "../../thrown-errors.ts";
import { thrownFacts } from "./thrown-facts.ts";

/** The `ClientDefect` of a value outside its declared type at the end of an exhaustive `switch`. */
export function impossibleValueDefect(value: never): ClientDefect {
  return new ClientDefect({
    subject: "exhaustive switch",
    rule: `a value outside its declared type reached it: ${describeValue(value)}`,
  });
}

/** The text of a value; its JSON when it has one. Never throws. */
function describeValue(value: never): string {
  try {
    return JSON.stringify(value);
  } catch (cause) {
    return `${thrownFacts(value).message} (no JSON: ${thrownFacts(cause).message})`;
  }
}
