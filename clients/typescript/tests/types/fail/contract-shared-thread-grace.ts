// expect: TS2322 at 11
// A `SHARED_THREAD` task takes no `cancelGraceMs`.

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";

export const resize = defineTask({
  name: "resize",
  execution: "SHARED_THREAD",
  timeoutMs: 60_000,
  cancelGraceMs: 1_000,
  input: {},
  output: h.null(),
});
