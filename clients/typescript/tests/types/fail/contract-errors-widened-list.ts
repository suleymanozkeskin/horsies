// expect: TS2322 at 8
// An `errorCodes` list of the type `string[]` fails typecheck: its codes would not be known to the types.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

const codes = ["EMPTY_ORDER"];
export const widened = defineTask({ name: "widened", input: {}, output: h.null(), errorCodes: codes });
