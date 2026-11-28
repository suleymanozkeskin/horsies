// expect: TS2345 at 16
// A value that is not of the contract's output type fails at the line of the `ok` call of the body's second argument.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const count = defineTask({ name: "count", input: { text: h.string() }, output: h.int() });

export const implementation = app.implement(count, ({ text }, { ok }) => {
  if (text === "") {
    return ok(0);
  }

  return ok(text);
});
