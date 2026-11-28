// expect: TS2345 at 11
// A task handle takes a `TaskId` (from `TaskId.parse`), not a plain string.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const report = defineTask({ name: "report", input: {}, output: h.null() });

export const handle = app.taskHandle(report, "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b");
