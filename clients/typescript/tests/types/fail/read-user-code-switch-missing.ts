// expect: TS2345 at 25
// A switch over the declared user codes that misses one fails at the `assertNever` line.

import { defineTask, h, type Horsies, type TaskId } from "../../../horsies/src/index.ts";

function assertNever(value: never): never {
  throw new TypeError(String(value));
}

const validateOrder = defineTask({ name: "v", input: {}, output: h.null(), errors: ["EMPTY_ORDER", "INVALID_EMAIL"] });

export async function codeOf(app: Horsies, taskId: TaskId): Promise<string> {
  const read = await app.taskHandle(validateOrder, taskId).get();

  if (!read.ok || read.value.kind === "succeeded" || read.value.error.code.kind === "library") {
    return "";
  }

  const code = read.value.error.code.code;

  switch (code) {
    case "EMPTY_ORDER":
      return "empty";
    default:
      return assertNever(code);
  }
}
