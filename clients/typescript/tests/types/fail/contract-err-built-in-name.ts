// expect: TS2322 at 9
// Without `errors`, a built-in name in `err` fails typecheck.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.null() });

export const failure = ping.err({ code: "TASK_TIMEOUT" });
