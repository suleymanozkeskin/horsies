// The text of the issues of a failure: one variant of `CheckIssue` for each source of issues.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { type CheckFailure, issuesText } from "../../../horsies/src/codec/conversion.ts";

describe("issuesText", () => {
  test("zodIssue, horsiesIssue and notNoWorkflowOutput: each variant gives its path and its code text, joined by `; `", () => {
    const failure: CheckFailure = {
      issues: [
        { kind: "zodIssue", path: "$.a", code: "too_big" },
        { kind: "horsiesIssue", path: "$.b", code: "missing_key" },
        { kind: "notNoWorkflowOutput", path: "kind" },
      ],
    };

    assert.equal(issuesText(failure), '$.a too_big; $.b missing_key; kind expected "noWorkflowOutput"');
  });

  test("a zod code with the name of a horsies code stays a zodIssue", () => {
    const zodIssue: CheckFailure = { issues: [{ kind: "zodIssue", path: "$", code: "async_check" }] };
    const horsiesIssue: CheckFailure = { issues: [{ kind: "horsiesIssue", path: "$", code: "async_check" }] };

    assert.equal(issuesText(zodIssue), issuesText(horsiesIssue));
    assert.notDeepEqual(zodIssue.issues, horsiesIssue.issues);
  });
});
