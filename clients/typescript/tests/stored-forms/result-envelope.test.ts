// result_envelope.json: the text of each `ok` value and of the `data` of each user error that this client writes,
// and the read of every envelope through the codec's envelope reader and through an `h.taskOutcome` parameter (the
// envelope that a workflow node gets from its upstream node). The core writes the envelope around the client's text;
// these tests stand in for it. The named cases are separate tests beside the loops.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { errorDataText } from "../../horsies/src/codec/error-data.ts";
import { jsonTextCheckedByCore } from "../../horsies/src/codec/json-text.ts";
import { decodeEnvelope, decodeParameters } from "../../horsies/src/codec/reader.ts";
import type { DecodeRefused, EnvelopeRule } from "../../horsies/src/codec/refusal.ts";
import { isPlainObject } from "../../horsies/src/codec/values.ts";
import { encodeValue } from "../../horsies/src/codec/writer.ts";
import { defineTask } from "../../horsies/src/contract/define-task.ts";
import { h, parametersKind } from "../../horsies/src/wire/builders.ts";
import type { RequiredWire } from "../../horsies/src/wire/infer.ts";
import type { TaskOutcome } from "../../horsies/src/wire/task-error.ts";
import { kindOf } from "../../horsies/src/wire/wire-type.ts";
import { sameValue, show } from "../support/values.ts";
import {
  declaredWires,
  type ErrorNotation,
  type Notation,
  resultEnvelopeFile,
  rowNamed,
  taskErrorOf,
  valueOf,
} from "./vectors.ts";

const envelopes = resultEnvelopeFile();

/** The core writes the envelope around the codec's `ok` text (`result_end`). */
function okEnvelopeText(okText: string): string {
  return `{"__h_task_result__":true,"ok":${okText},"err":null}`;
}

/** The core writes the error object of a user error around the client's `data` text (`error_envelope`). */
function userErrorEnvelopeText(code: string, message: string | null, dataText: string): string {
  const messageText = message === null ? "null" : JSON.stringify(message);
  const error = `{"exception":null,"error_code":${JSON.stringify(code)},"data":${dataText},"message":${messageText}}`;

  return `{"__h_task_result__":true,"ok":null,"err":${error}}`;
}

/** The outcome that the codec's envelope reader gives for an envelope text. */
function readerOutcome(wire: RequiredWire, text: string): TaskOutcome<unknown> {
  const outcome = decodeEnvelope(kindOf(wire), jsonTextCheckedByCore(text), "worker");

  assert(outcome.ok, show(outcome));

  return outcome.value;
}

/** The outcome that an `h.taskOutcome` parameter gives for the envelope text of its upstream node of output `wire`. */
function taskOutcomeOf(wire: RequiredWire, text: string): unknown {
  const upstream = defineTask({ name: "upstream", input: {}, output: wire });
  const parameters = parametersKind({ upstream: h.taskOutcome(upstream) });
  const decoded = decodeParameters(parameters, jsonTextCheckedByCore(`{"upstream":${text}}`), "worker");

  assert(decoded.ok && isPlainObject(decoded.value), show(decoded));

  return decoded.value["upstream"];
}

function refusalOf(wire: RequiredWire, text: string): DecodeRefused {
  const outcome = decodeEnvelope(kindOf(wire), jsonTextCheckedByCore(text), "worker");

  assert(!outcome.ok, "read a refused envelope");

  return outcome.error;
}

function ruleOf(text: string): EnvelopeRule {
  const refusal = refusalOf(h.json(), text);

  assert(refusal.kind === "EnvelopeRefused", show(refusal));

  return refusal.rule;
}

/** The `ok` value of the vector `name` encodes to its text and reads back through both paths. */
function assertOkVector<V>(name: string, wire: RequiredWire, value: V): void {
  const vector = rowNamed(envelopes.vectors, name);
  const written = encodeValue(kindOf(wire), value);

  assert(written.ok, show(written));
  assert.equal(okEnvelopeText(written.value), vector.text);

  const outcome = readerOutcome(wire, vector.text);

  assert(outcome.kind === "succeeded" && sameValue(outcome.value, value), show(outcome));
  assert(sameValue(taskOutcomeOf(wire, vector.text), { kind: "succeeded", value }));
}

describe("result_envelope.json ok values", () => {
  for (const vector of envelopes.vectors) {
    const okSlot = vector.ok;
    const reason = vector.typescript_cannot_express;

    if (okSlot === undefined) {
      continue;
    }

    if (reason !== undefined) {
      test(`${vector.name}: skipped, ${reason}`, { skip: reason }, () => {});
      continue;
    }

    for (const declared of declaredWires(okSlot.type, okSlot.value)) {
      const value = valueOf(okSlot.type, declared, okSlot.value);

      test(`${vector.name} [${declared.label}]: the ok text encodes`, () => {
        const written = encodeValue(kindOf(declared.wire), value);

        assert(written.ok, show(written));
        assert.equal(okEnvelopeText(written.value), vector.text);
      });

      test(`${vector.name} [${declared.label}]: the envelope reader gives the ok value`, () => {
        const outcome = readerOutcome(declared.wire, vector.text);

        assert(outcome.kind === "succeeded" && sameValue(outcome.value, value), show(outcome));
      });

      test(`${vector.name} [${declared.label}]: an h.taskOutcome parameter gives the ok value`, () => {
        const outcome = taskOutcomeOf(declared.wire, vector.text);

        assert(sameValue(outcome, { kind: "succeeded", value }), show(outcome));
      });
    }
  }
});

/** The data text of a user error, in the error object that the core writes. */
function assertUserErrorData(name: string, code: string, error: ErrorNotation, storedData?: Notation): void {
  const vector = rowNamed(envelopes.vectors, name);
  const data = errorDataText(error.data);

  assert.equal(data.state, "stored");

  if (storedData === undefined) {
    assert.equal(userErrorEnvelopeText(code, error.message, data.text), vector.text);

    return;
  }

  // The data holds what the core refuses: the core stores the replacement (`stored_data`) around this text.
  assert.equal(data.text, JSON.stringify(error.data));
  assert.equal(userErrorEnvelopeText(code, error.message, JSON.stringify(storedData)), vector.text);
}

describe("result_envelope.json errors", () => {
  for (const vector of envelopes.vectors) {
    const error = vector.err;

    if (error === undefined) {
      continue;
    }

    const expected = taskErrorOf(error, vector.stored_data);
    const code = error.error_code;

    if ("user" in code) {
      test(`${vector.name}: the data text of the user error`, () => {
        assertUserErrorData(vector.name, code.user, error, vector.stored_data);
      });
    }

    test(`${vector.name}: the envelope reader gives the error`, () => {
      const outcome = readerOutcome(h.json(), vector.text);

      assert(outcome.kind === "failed", show(outcome));
      assert.deepEqual(outcome.error, expected);
    });

    test(`${vector.name}: an h.taskOutcome parameter gives the error`, () => {
      assert.deepEqual(taskOutcomeOf(h.json(), vector.text), { kind: "failed", error: expected });
    });
  }
});

describe("result_envelope.json named cases", () => {
  test("1e-05: ok_f64_exponent_minus_5", () => {
    assertOkVector("ok_f64_exponent_minus_5", h.float(), 1e-5);
    assert.equal(rowNamed(envelopes.vectors, "ok_f64_exponent_minus_5").text, okEnvelopeText("1e-05"));
  });

  test("1e+16: ok_f64_exponent_plus_16", () => {
    assertOkVector("ok_f64_exponent_plus_16", h.float(), 1e16);
    assert.equal(rowNamed(envelopes.vectors, "ok_f64_exponent_plus_16").text, okEnvelopeText("1e+16"));
  });

  test("123.0: ok_f64_integral", () => {
    assertOkVector("ok_f64_integral", h.float(), 123);
    assert.equal(rowNamed(envelopes.vectors, "ok_f64_integral").text, okEnvelopeText("123.0"));
  });

  test("ok_big_int_above_u128: h.bigint() with 39 digits", () => {
    assertOkVector("ok_big_int_above_u128", h.bigint(), 2n ** 128n);
  });

  test("err_data_with_an_engine_key: the stored data is {\"data_refused\":\"StrictJsonError\"}", () => {
    const vector = rowNamed(envelopes.vectors, "err_data_with_an_engine_key");
    const error = vector.err;

    assert(error !== undefined && "user" in error.error_code);
    assert.deepEqual(vector.stored_data, { data_refused: "StrictJsonError" });
    assertUserErrorData(vector.name, error.error_code.user, error, vector.stored_data);

    const outcome = readerOutcome(h.json(), vector.text);

    assert(outcome.kind === "failed", show(outcome));
    assert.deepEqual(outcome.error.data, { data_refused: "StrictJsonError" });
  });
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

/**
 * The strict-rule text that the core alone refuses (a repeated key): the core's check runs before every path to this
 * reader, so the text never reaches it.
 */
const CORE_ONLY = new Set(["repeated_key"]);

describe("result_envelope.json refused texts", () => {
  for (const row of envelopes.refused_texts) {
    if (CORE_ONLY.has(row.name)) {
      continue;
    }

    switch (row.refusal) {
      case "envelope":
        test(`${row.name}: refused with its named rule`, () => {
          const expected = ENVELOPE_RULE[row.name];

          assert(expected !== undefined, `the row ${row.name} has no expected rule in this test`);
          assert.equal(ruleOf(row.text), expected);
        });
        break;
      case "ok value":
        for (const [label, wire] of [
          ["json", h.json()],
          ["record(boolean)", h.record(h.boolean())],
        ] as const) {
          test(`${row.name} [${label}]: the ok value is refused`, () => {
            const refusal = refusalOf(wire, row.text);

            assert.deepEqual([refusal.kind, refusal.path], ["EngineKey", "$.ok.<input>"]);
          });
        }

        break;
      default:
        test(`${row.name}: a known refusal form`, () => {
          assert.fail(`unknown refusal form ${String(row.refusal)}`);
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
