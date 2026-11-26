// The result envelope and stored task error reader: the vectors of result_envelope.json and error_data.json, and
// one test per rule.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { LIBRARY_TASK_CODES, MAX_VALUE_JSON_DEPTH, WORKFLOW_STATUSES } from "../../../horsies/src/codec/bounds.ts";
import { readStoredError } from "../../../horsies/src/codec/envelope.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeEnvelope, decodeParameters } from "../../../horsies/src/codec/reader.ts";
import type { DecodeRefused, EnvelopeRule } from "../../../horsies/src/codec/refusal.ts";
import { isString } from "../../../horsies/src/codec/values.ts";
import { encodeParameters, encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h, parametersKind } from "../../../horsies/src/wire/builders.ts";
import type { Kind } from "../../../horsies/src/wire/kinds.ts";
import type {
  ExceptionRecord,
  LibraryTaskCode,
  TaskError,
  TaskOutcome,
  WorkflowStatus,
} from "../../../horsies/src/wire/task-error.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";
import {
  declaredWires,
  errorDataFile,
  type ErrorNotation,
  type Notation,
  resultEnvelopeFile,
  sameValue,
  show,
  valueOf,
} from "./support/vectors.ts";

const JSON_KIND = kindOf(h.json());
const envelopes = resultEnvelopeFile();

/** The core writes the envelope around the codec's `ok` text (`result_end`); this test stands in for it. */
function okEnvelopeText(okText: string): string {
  return `{"__h_task_result__":true,"ok":${okText},"err":null}`;
}

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

/** The TaskError that a vector's error notation names. */
function expectedError(error: ErrorNotation, storedData?: Notation): TaskError {
  const code = error.error_code;
  const subWorkflow = error.sub_workflow;

  return {
    code:
      "built_in" in code
        ? { kind: "library", code: libraryCode(code.built_in) }
        : { kind: "user", code: code.user },
    message: error.message,
    data: storedData ?? error.data,
    exception: error.exception === null ? null : exceptionOf(error.exception),
    subWorkflow:
      subWorkflow === undefined
        ? null
        : {
            workflowId: subWorkflow.id,
            summary: {
              status: workflowStatus(subWorkflow.summary["status"]),
              output: subWorkflow.summary["output"] ?? null,
              errorSummary: textOrNull(subWorkflow.summary["error_summary"]),
              totalTasks: countOf(subWorkflow.summary["total_tasks"]),
              completedTasks: countOf(subWorkflow.summary["completed_tasks"]),
              failedTasks: countOf(subWorkflow.summary["failed_tasks"]),
              skippedTasks: countOf(subWorkflow.summary["skipped_tasks"]),
            },
          },
  };
}

const LIBRARY_CODES: ReadonlySet<string> = new Set(LIBRARY_TASK_CODES);
const STATUSES: ReadonlySet<string> = new Set(WORKFLOW_STATUSES);

function isLibraryCode(text: string): text is LibraryTaskCode {
  return LIBRARY_CODES.has(text);
}

function isWorkflowStatus(text: string): text is WorkflowStatus {
  return STATUSES.has(text);
}

function libraryCode(text: string): LibraryTaskCode {
  assert(isLibraryCode(text), text);

  return text;
}

function workflowStatus(notation: Notation | undefined): WorkflowStatus {
  assert(isString(notation) && isWorkflowStatus(notation), String(notation));

  return notation;
}

function textOrNull(notation: Notation | undefined): string | null {
  if (notation === null || notation === undefined) {
    return null;
  }

  assert(isString(notation));

  return notation;
}

function countOf(notation: Notation | undefined): number {
  assert(Number.isInteger(notation));

  return Number(notation);
}

function exceptionOf(record: { readonly [key: string]: string }): ExceptionRecord {
  const { type = "", module = "", message = "", repr = "", traceback = "" } = record;

  return { type, module, message, repr, traceback };
}

describe("result_envelope.json vectors", () => {
  for (const vector of envelopes.vectors) {
    if (vector.typescript_cannot_express !== undefined) {
      continue;
    }

    const okSlot = vector.ok;
    const error = vector.err;

    if (okSlot !== undefined) {
      for (const declared of declaredWires(okSlot.type, okSlot.value)) {
        const value = valueOf(okSlot.type, declared, okSlot.value);
        const kind = kindOf(declared.wire);

        test(`${vector.name} [${declared.label}]: the ok text encodes`, () => {
          const written = encodeValue(kind, value);

          assert(written.ok, show(written));
          assert.equal(okEnvelopeText(written.value), vector.text);
        });

        test(`${vector.name} [${declared.label}]: decodes to the ok value`, () => {
          const outcome = outcomeOf(kind, vector.text);

          assert(outcome.kind === "succeeded" && sameValue(outcome.value, value), show(outcome));
        });
      }
    }

    if (error !== undefined) {
      test(`${vector.name}: decodes to the error`, () => {
        const outcome = outcomeOf(JSON_KIND, vector.text);

        assert(outcome.kind === "failed", show(outcome));
        assert.deepEqual(outcome.error, expectedError(error, vector.stored_data));
      });
    }
  }
});

const ENVELOPE_RULE: Readonly<Record<string, EnvelopeRule>> = {
  not_an_object: "NotAnObject",
  marker_missing: "MarkerMissing",
  marker_not_true: "MarkerMissing",
  ok_slot_missing: "MissingSlot",
  extra_key: "ExtraKey",
  both_slots_set: "BothSlotsSet",
  error_without_code: "ErrorMissingField",
  error_with_unknown_key: "ErrorUnknownKey",
  error_with_unknown_built_in_code: "ErrorUnknownBuiltInCode",
};

/** The strict-rule text that the core alone refuses (a repeated key): it never reaches this reader. */
const CORE_ONLY = new Set(["repeated_key"]);

describe("result_envelope.json refused texts", () => {
  for (const row of envelopes.refused_texts) {
    if (CORE_ONLY.has(row.name)) {
      continue;
    }

    switch (row.refusal) {
      case "envelope":
        test(`${row.name}: ${ENVELOPE_RULE[row.name]}`, () => {
          assert.equal(ruleOf(row.text), ENVELOPE_RULE[row.name]);
        });
        break;
      case "ok value":
        for (const [label, wire] of [
          ["json", h.json()],
          ["record(boolean)", h.record(h.boolean())],
        ] as const) {
          test(`${row.name} [${label}]: the ok value is refused`, () => {
            const refusal = refusalOf(kindOf(wire), row.text);

            assert.deepEqual([refusal.kind, refusal.path], ["EngineKey", "$.ok.<input>"]);
          });
        }

        break;
      default:
        test(`${row.name}: a known refusal form`, () => {
          assert.fail(`unknown refusal form ${row.refusal}`);
        });
    }
  }

  test("the rows that the core alone refuses exist", () => {
    assert.deepEqual(
      envelopes.refused_texts.filter((row) => CORE_ONLY.has(row.name)).map((row) => row.name),
      [...CORE_ONLY],
    );
  });
});

describe("error_data.json vectors", () => {
  for (const vector of errorDataFile().vectors) {
    test(`${vector.name} (${vector.stored_as}): decodes to the error`, () => {
      const expected = expectedError(vector.error);

      switch (vector.stored_as) {
        case "envelope": {
          const outcome = outcomeOf(JSON_KIND, vector.text);

          assert(outcome.kind === "failed", show(outcome));
          assert.deepEqual(outcome.error, expected);
          break;
        }

        case "error": {
          const error = readStoredError(jsonTextCheckedByCore(vector.text));

          assert(error.ok, show(error));
          assert.deepEqual(error.value, expected);
          break;
        }
      }
    });
  }
});

const ERROR_HEAD = '{"__h_task_result__":true,"ok":null,"err":';

function errorRule(errorText: string): EnvelopeRule {
  return ruleOf(`${ERROR_HEAD}${errorText}}`);
}

describe("stored task error rules", () => {
  test("ErrorNotAnObject", () => {
    assert.equal(errorRule("[1]"), "ErrorNotAnObject");
  });

  test("ErrorFieldInvalid: a code that is null, a number, a library or reserved name as a user code", () => {
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
      code: { kind: "user", code: "X" },
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

describe("h.resultOf parameters", () => {
  const parameters = parametersKind({ order: h.resultOf(h.object({ id: h.int() })), note: h.string() });

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

    const error = { code: { kind: "user", code: "X" }, message: null, data: null, exception: null, subWorkflow: null };

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
