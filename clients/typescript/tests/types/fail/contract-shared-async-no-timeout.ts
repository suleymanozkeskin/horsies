// expect: TS2345 at 7
// A `SHARED_ASYNC` task needs `timeoutMs`.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

export const notify = defineTask({ name: "notify", execution: "SHARED_ASYNC", input: {}, output: h.null() });
