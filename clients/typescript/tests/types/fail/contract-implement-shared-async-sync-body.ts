// expect: TS2739 at 11
// A `SHARED_ASYNC` task takes an async body only.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const notify = defineTask({ name: "notify", execution: "SHARED_ASYNC", timeoutMs: 10_000, input: {}, output: h.null() });

export const implementation = app.implement(notify, (_input, { ok }) => ok(null));
