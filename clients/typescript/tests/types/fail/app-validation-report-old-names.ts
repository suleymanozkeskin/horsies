// expect: TS2305 at 6, TS2339 at 9
// `Horsies.create` refuses with a `ValidationReport` that holds `HorsiesError` values; `CreateRefused` is no public
// name. A refusal holds its fix lines in `helpText`; `help` is no member.

import type { HorsiesError } from "../../../horsies/src/index.ts";
import type { CreateRefused } from "../../../horsies/src/index.ts";

export function fixLines(error: HorsiesError): readonly string[] {
  return error.help;
}

export type Refused = CreateRefused;
