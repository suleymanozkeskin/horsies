// expect: TS2322 at 7
// An empty `errorCodes` list fails typecheck: a task that declares no code leaves `errorCodes` out.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

export const empty = defineTask({ name: "empty", input: {}, output: h.null(), errorCodes: [] });
