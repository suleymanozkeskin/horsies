// error_data.json: the core writes these library errors; this client reads each one, in the form that it is stored,
// to the task error with its library code and its `data`.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { readStoredError } from "../../horsies/src/codec/envelope.ts";
import { jsonTextCheckedByCore } from "../../horsies/src/codec/json-text.ts";
import { decodeEnvelope } from "../../horsies/src/codec/reader.ts";
import { h } from "../../horsies/src/wire/builders.ts";
import { kindOf } from "../../horsies/src/wire/wire-type.ts";
import { show } from "../support/values.ts";
import { errorDataFile, taskErrorOf } from "./vectors.ts";

describe("error_data.json vectors", () => {
  for (const vector of errorDataFile().vectors) {
    test(`${vector.name} (${vector.stored_as}): reads to the error`, () => {
      const expected = taskErrorOf(vector.error);

      switch (vector.stored_as) {
        case "envelope": {
          const outcome = decodeEnvelope(kindOf(h.json()), jsonTextCheckedByCore(vector.text), "worker");

          assert(outcome.ok && outcome.value.kind === "failed", show(outcome));
          assert.deepEqual(outcome.value.error, expected);
          break;
        }

        case "error": {
          const error = readStoredError(jsonTextCheckedByCore(vector.text));

          assert(error.ok, show(error));
          assert.deepEqual(error.value, expected);
          break;
        }

        default:
          assert.fail(`unknown stored form ${String(vector.stored_as)}`);
      }
    });
  }
});
