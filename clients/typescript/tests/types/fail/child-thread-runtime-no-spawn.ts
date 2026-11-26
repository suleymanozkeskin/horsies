// expect: TS2339 at 16
// The runtime of a `SHARED_THREAD` body has no `spawn`.

import type { ThreadTaskRuntime } from "../../../horsies/src/child/public.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

export const resize = defineTask({
  name: "resize",
  execution: "SHARED_THREAD",
  timeoutMs: 1000,
  input: {},
  output: h.null(),
});

export const spawned = (runtime: ThreadTaskRuntime): unknown => runtime.spawn("work", async () => 1);
