// `createMonitoringHandler` of an app off the main thread: the addon runs on the main thread of a process only. The
// handler of an app on the main thread needs the addon; the native tests cover it (`tests/runtime/native/`).

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { Worker } from "node:worker_threads";

describe("createMonitoringHandler of an app", () => {
  test("off the main thread it throws AddonOutsideMainThread", async () => {
    const worker = new Worker(new URL("handler-in-worker.ts", import.meta.url));

    const answer = new Promise<string>((resolve, reject) => {
      worker.once("message", (name: string) => {
        resolve(name);
      });
      worker.once("error", reject);
    });

    const exited = new Promise<number>((resolve) => {
      worker.once("exit", resolve);
    });

    assert.equal(await answer, "AddonOutsideMainThread");
    assert.equal(await exited, 0);
  });
});
