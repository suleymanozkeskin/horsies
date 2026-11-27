// expect: TS2322 at 8
// `retriesLeft` is a `RetriesLeft`, not a `MaxRetries`: the two facts are different types.

import type { MaxRetries, TaskRuntime } from "../../../horsies/src/child/public.ts";

declare const runtime: TaskRuntime;

export const budget: MaxRetries = runtime.retriesLeft;
