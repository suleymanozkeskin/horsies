// expect: TS2305 at 4
// The execution class of a task has one public name, `Execution`; `ExecutionClass` is not exported.

import type { ExecutionClass } from "../../../horsies/src/index.ts";

export const execution: ExecutionClass = "ISOLATED";
