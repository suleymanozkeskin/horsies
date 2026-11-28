// expect: TS2741 at 12
// A send input without a required parameter (`SendableInput` is the input type of `app.send`). A compiler reports the
// same mistake in the argument of `app.send` with another code on TypeScript 6.0 and 7, so the case uses the type.

import { defineTask, type SendableInput } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

const parameters = { n: h.int(), label: h.string() };

export const report = defineTask({ name: "report", input: parameters, output: h.null() });

export const input: SendableInput<typeof parameters> = { n: 1 };
