// expect: TS2322 at 16
// Without `errors`, a built-in name fails at the line of the `err` call of the body's second argument.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const ping = defineTask({ name: "ping", input: {}, output: h.string() });

export const implementation = app.implement(ping, (_input, runtime) => {
  if (runtime.signal.aborted) {
    return runtime.err({ code: "STOPPED" });
  }

  return runtime.err({ code: "TASK_TIMEOUT" });
});
