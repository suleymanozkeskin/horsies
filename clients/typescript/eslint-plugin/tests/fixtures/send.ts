// A user's module that sends a task and reads its result. Each line that ends with an `expect` comment gets the
// report of that rule and message id; no other line gets a report (tests/typescript-7-project.test.ts).

import { defineTask, h, type Horsies, type TaskHandle } from "horsies";

const report = defineTask({ name: "report", input: { n: h.int() }, output: h.int() });

export async function sendReports(app: Horsies): Promise<number> {
  await app.send(report, { n: 1 }); // expect: horsies/no-dropped-result droppedResult

  app.send(report, { n: 2 }); // expect: horsies/no-dropped-result droppedPromisedResult

  void (await app.send(report, { n: 3 }));

  const sent = await app.send(report, { n: 4 });

  if (!sent.ok) {
    return 0;
  }

  return readCount(sent.value);
}

async function readCount(handle: TaskHandle<number, string>): Promise<number> {
  await handle.get(); // expect: horsies/no-dropped-result droppedResult

  const outcome = await handle.get();

  return outcome.ok ? 1 : 0;
}
