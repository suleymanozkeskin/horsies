// The id types: a UUID text in the lower-case 8-4-4-4-12 form, or a closed refusal.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { TaskId, WorkflowId } from "../../../horsies/src/contract/public.ts";
import { taskIdFromCore } from "../../../horsies/src/contract/ids.ts";

const CANONICAL = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b";

describe("TaskId and WorkflowId", () => {
  test("a canonical UUID text parses to the same text", () => {
    assert.deepEqual(TaskId.parse(CANONICAL), { ok: true, value: CANONICAL });
    assert.deepEqual(WorkflowId.parse(CANONICAL), { ok: true, value: CANONICAL });
  });

  test("an upper-case, unhyphenated or empty text is notCanonicalUuid, with no text in the detail", () => {
    for (const text of [CANONICAL.toUpperCase(), CANONICAL.replaceAll("-", ""), ""]) {
      const parsed = TaskId.parse(text);

      assert.equal(parsed.ok, false);
      assert.equal(parsed.ok ? "" : parsed.error.kind, "notCanonicalUuid");
      assert.equal(parsed.ok ? false : parsed.error.detail.includes(text) && text !== "", false);
      assert.equal(WorkflowId.parse(text).ok, false);
    }
  });

  test("an id from the core that is not canonical is a defect: TypeError", () => {
    assert.equal(taskIdFromCore(CANONICAL, "enqueue"), CANONICAL);
    assert.throws(() => taskIdFromCore("not-a-uuid", "enqueue"), TypeError);
  });
});
