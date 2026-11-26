// The result envelope and stored task error reader: one test per rule. The vectors of result_envelope.json and
// error_data.json are in tests/stored-forms/.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { MAX_VALUE_JSON_DEPTH } from "../../../horsies/src/codec/bounds.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeEnvelope, decodeParameters } from "../../../horsies/src/codec/reader.ts";
import type { DecodeRefused, EnvelopeRule } from "../../../horsies/src/codec/refusal.ts";
import { encodeParameters } from "../../../horsies/src/codec/writer.ts";
import { defineTask } from "../../../horsies/src/contract/define-task.ts";
import { h, parametersKind } from "../../../horsies/src/wire/builders.ts";
import type { Kind } from "../../../horsies/src/wire/kinds.ts";
import type { TaskOutcome } from "../../../horsies/src/wire/task-error.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";
import { show } from "../../support/values.ts";

const JSON_KIND = kindOf(h.json());

function outcomeOf(okKind: Kind, text: string): TaskOutcome<unknown> {
  const outcome = decodeEnvelope(okKind, jsonTextCheckedByCore(text), "worker");

  assert(outcome.ok, show(outcome));

  return outcome.value;
}

function refusalOf(okKind: Kind, text: string): DecodeRefused {
  const outcome = decodeEnvelope(okKind, jsonTextCheckedByCore(text), "worker");

  assert(!outcome.ok, "read a refused envelope");

  return outcome.error;
}

function ruleOf(text: string): EnvelopeRule {
  const refusal = refusalOf(JSON_KIND, text);

  assert.equal(refusal.kind, "EnvelopeRefused", show(refusal));
  assert(refusal.kind === "EnvelopeRefused");

  return refusal.rule;
}

const ERROR_HEAD = '{"__h_task_result__":true,"ok":null,"err":';

function errorRule(errorText: string): EnvelopeRule {
  return ruleOf(`${ERROR_HEAD}${errorText}}`);
}

describe("stored task error rules", () => {
  test("ErrorNotAnObject", () => {
    assert.equal(errorRule("[1]"), "ErrorNotAnObject");
  });

  test("ErrorFieldInvalid: a code that is null, a number, a built-in or reserved name as a custom code", () => {
    for (const code of ["null", "1", '"BROKER_ERROR"', '"WORKFLOW_STOPPED"', '{"__builtin_task_code__":"X","a":1}']) {
      assert.equal(errorRule(`{"error_code":${code}}`), "ErrorFieldInvalid", code);
    }
  });

  test("ErrorFieldInvalid: an exception record of another form, and a message that is not text", () => {
    assert.equal(errorRule('{"error_code":"X","exception":{"type":"E"}}'), "ErrorFieldInvalid");
    assert.equal(errorRule('{"error_code":"X","message":1}'), "ErrorFieldInvalid");
  });

  test("missing exception, data and message read as null", () => {
    const outcome = outcomeOf(JSON_KIND, `${ERROR_HEAD}{"error_code":"X"}}`);

    assert(outcome.kind === "failed");
    assert.deepEqual(outcome.error, {
      kind: "custom",
      code: "X",
      message: null,
      data: null,
      exception: null,
      subWorkflow: null,
    });
  });

  test("ErrorDataRefused: data with an engine key, and data deeper than 199 levels", () => {
    assert.equal(errorRule('{"error_code":"X","data":{"__h_x":1}}'), "ErrorDataRefused");

    const deep = `${"[".repeat(MAX_VALUE_JSON_DEPTH)}${"]".repeat(MAX_VALUE_JSON_DEPTH)}`;

    assert.equal(errorRule(`{"error_code":"X","data":${deep}}`), "ErrorDataRefused");
  });

  test("a sub-workflow error: the id and the summary are required; an unknown summary key is ignored", () => {
    const id = "0192f3b4-5c6d-7e8f-9a0b-1c2d3e4f5a6b";

    const summary =
      '{"status":"FAILED","total_tasks":1,"completed_tasks":0,"failed_tasks":1,"skipped_tasks":0,"note":1}';

    const floatCount = summary.replace('"total_tasks":1', '"total_tasks":1.0');
    const engineKeyOutput = summary.replace("{", '{"output":{"__h_x":1},');
    const head = `{"error_code":{"__builtin_task_code__":"SUBWORKFLOW_FAILED"},"sub_workflow_id":"${id}"`;
    const outcome = outcomeOf(JSON_KIND, `${ERROR_HEAD}${head},"sub_workflow_summary":${summary}}}`);

    assert(outcome.kind === "failed" && outcome.error.subWorkflow?.summary.output === null);
    assert.equal(errorRule(`${head}}`), "ErrorMissingField");
    assert.equal(errorRule(`${head},"sub_workflow_summary":{"status":"DONE"}}`), "ErrorFieldInvalid");
    assert.equal(errorRule(`${head},"sub_workflow_summary":${floatCount}}`), "ErrorFieldInvalid");
    const badId = '{"error_code":"X","sub_workflow_id":"not-a-uuid","sub_workflow_summary":{}}';

    assert.equal(errorRule(badId), "ErrorFieldInvalid");
    assert.equal(errorRule(`${head},"sub_workflow_summary":${engineKeyOutput}}`), "ErrorDataRefused");
  });

  test("the sub-workflow id: each form that the core reads gives the lower-case hyphenated form", () => {
    const summary = '{"status":"FAILED","total_tasks":1,"completed_tasks":0,"failed_tasks":1,"skipped_tasks":0}';
    const canonical = "0192f3b4-5c6d-7e8f-9a0b-1c2d3e4f5a6b";

    for (const id of [
      canonical,
      canonical.toUpperCase(),
      canonical.replaceAll("-", ""),
      `{${canonical}}`,
      `URN:UUID:${canonical}`,
    ]) {
      const error = `{"error_code":"X","sub_workflow_id":"${id}","sub_workflow_summary":${summary}}`;
      const outcome = outcomeOf(JSON_KIND, `${ERROR_HEAD}${error}}`);

      assert(outcome.kind === "failed", id);
      assert.equal(outcome.error.subWorkflow?.workflowId, canonical, id);
    }

    for (const id of [`{${canonical}`, `${canonical}0`, canonical.replace("-", ""), `urn:${canonical}`]) {
      const error = `{"error_code":"X","sub_workflow_id":"${id}","sub_workflow_summary":${summary}}`;

      assert.equal(errorRule(error), "ErrorFieldInvalid", id);
    }
  });

  test("sub-workflow keys on an error without sub_workflow_id are unknown", () => {
    assert.equal(errorRule('{"error_code":"X","sub_workflow_summary":{}}'), "ErrorUnknownKey");
  });
});

describe("h.taskOutcome parameters", () => {
  const upstream = defineTask({ name: "upstream", input: {}, output: h.object({ id: h.int() }) });
  const parameters = parametersKind({ order: h.taskOutcome(upstream), note: h.string() });

  test("a workflow node's input: the upstream outcome, succeeded or failed", () => {
    const succeeded = decodeParameters(
      parameters,
      jsonTextCheckedByCore('{"order":{"__h_task_result__":true,"ok":{"id":7},"err":null},"note":"x"}'),
      "worker",
    );

    assert.deepEqual(succeeded, { ok: true, value: { order: { kind: "succeeded", value: { id: 7 } }, note: "x" } });

    const failed = decodeParameters(
      parameters,
      jsonTextCheckedByCore('{"order":{"__h_task_result__":true,"ok":null,"err":{"error_code":"X"}},"note":"x"}'),
      "worker",
    );

    const error = { kind: "custom", code: "X", message: null, data: null, exception: null, subWorkflow: null };

    assert.deepEqual(failed, { ok: true, value: { order: { kind: "failed", error }, note: "x" } });
  });

  test("the paths of a refusal inside the upstream outcome", () => {
    const refused = decodeParameters(
      parameters,
      jsonTextCheckedByCore('{"order":{"__h_task_result__":true,"ok":{"id":"7"},"err":null},"note":"x"}'),
      "worker",
    );

    assert(!refused.ok);
    assert.deepEqual([refused.error.kind, refused.error.path], ["WrongType", "$.order.ok.id"]);
  });

  test("a sender never gives an upstream outcome: WrongType", () => {
    const written = encodeParameters(parameters, { order: { kind: "succeeded", value: { id: 1 } }, note: "x" });

    assert(!written.ok);
    assert.deepEqual([written.error.kind, written.error.path], ["WrongType", "$.order"]);
  });
});
