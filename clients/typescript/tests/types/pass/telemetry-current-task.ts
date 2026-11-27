// Compile-pass case of the log context: the task id of `currentTask()` is a `TaskId`, so a body can read its own task
// through `app.taskHandle`.

import { currentTask, defineTask, h, Horsies, type TaskHandle } from "../../../horsies/src/index.ts";

declare const app: Horsies;

const report = defineTask({ name: "report", input: {}, output: h.int() });

export function ownHandle(): TaskHandle<number, string> | undefined {
  const task = currentTask();

  switch (task.kind) {
    case "running":
      return app.taskHandle(report, task.taskId);
    case "noTask":
      return undefined;
    default: {
      const unexpected: never = task;

      return unexpected;
    }
  }
}
