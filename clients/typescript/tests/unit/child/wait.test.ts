// A result wait in a task child: `handle.get` from a body answers `waitInsideTask` before any statement.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { implementationOf, implementationParts } from "../../../horsies/src/contract/implementation.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { taskIdFromCore } from "../../../horsies/src/contract/ids.ts";
import { readFormOf, taskHandleOf } from "../../../horsies/src/read/handle.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { fakeCoreHandle, fakeProducer, OWNER_TASK_ID } from "../app/support.ts";
import { adapterRun } from "./support.ts";

const other = defineTask({ name: "other", input: {}, output: h.int() });

const waiter = defineTask({ name: "waiter", input: {}, output: h.string() });

describe("a result wait in a task child", () => {
  test("handle.get in a body answers waitInsideTask and reads nothing", async () => {
    const producer = fakeProducer({ role: "RUNS_TASK_BODIES" });

    const handle = taskHandleOf(
      { kind: "addon", producer, handle: fakeCoreHandle() },
      readFormOf(other),
      taskIdFromCore(OWNER_TASK_ID, "test"),
    );

    const run = await adapterRun(
      implementationParts(
        implementationOf(
          waiter,
          async (_input, { ok }) => {
            const read = await handle.get({ timeoutMs: 1000 });

            return ok(read.ok ? "read" : read.error.kind);
          },
          [],
        ),
      ),
      "{}",
    );

    assert.deepEqual(run.outcome, { kind: "value", json: '"waitInsideTask"' });
    assert.deepEqual(producer.results, []);
  });
});
