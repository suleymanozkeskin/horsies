// expect: TS2678 at 18
// A `case` of a custom code that the contract does not declare fails at that line.

import { defineTask, h, type Horsies, type TaskId } from "../../../horsies/src/index.ts";

const validateOrder = defineTask({ name: "v", input: {}, output: h.null(), errors: ["EMPTY_ORDER"] });

export async function codeOf(app: Horsies, taskId: TaskId): Promise<string> {
  const read = await app.taskHandle(validateOrder, taskId).get();

  if (!read.ok || read.value.kind === "succeeded" || read.value.error.kind === "builtIn") {
    return "";
  }

  switch (read.value.error.code) {
    case "EMPTY_ORDER":
      return "empty";
    case "CARD_DECLINED":
      return "declined";
    default:
      return "";
  }
}
