// The core's strict JSON check through the addon. The first path of the addon that runs the check is `enqueue`:
// `enqueue_operation` (`bridge/src/sends/send.rs`) calls `CheckedSend::check` before any connection, and its input
// rule `check_sent_input` (`core/src/broker/postgres.rs`) runs `check_json_text` on `inputJson`. A refused text is the
// send error `TaskSendValidationFailed` with the cause `jsonUnreadable`, whose text is the text of the core's
// `StrictJsonError` variant.
//
// The texts are the 12 `refused_texts` of `task_input.json` and the `repeated_key` text of `result_envelope.json`: the
// core refuses each of them before any path reaches the codec's reader. The handle names a port where no server
// listens, so no test of this file needs a database; the control test shows that a text which passes the check goes on
// to the connection.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { mintTaskId } from "../../../horsies/src/native/process-core/process-core.ts";
import type { EnqueueRequest } from "../../../horsies/src/native/sends/addon.ts";
import { enqueue } from "../../../horsies/src/native/sends/sends.ts";
import type { RawCatalogError } from "../../../horsies/src/native/values/records.ts";
import { epochMicrosTowardPast } from "../../../horsies/src/send/options.ts";
import { show } from "../../support/values.ts";
import { resultEnvelopeFile, rowNamed, taskInputFile } from "../../stored-forms/vectors.ts";
import { manifestJson } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** A database URL where no server listens: a send that passes the check fails to connect. */
const UNREACHABLE_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** The `StrictJsonError` variants that the refused texts name, each with the pattern of the text of that variant. */
const VARIANT_TEXT: Readonly<Record<string, RegExp>> = {
  Empty: /^JSON text is empty$/,
  Syntax: /^JSON parse failed at byte \d+: .+$/,
  NonStandardConstant: /^Non-standard JSON constant is not allowed: (NaN|Infinity|-Infinity)$/,
  IntegerTooLong: /^JSON integer at byte \d+ has more than 4300 digits$/,
  NonFiniteFloat: /^JSON number is outside the finite binary64 range$/,
  LoneSurrogate: /^JSON string holds a lone surrogate$/,
  TooDeep: /^JSON text is nested deeper than \d+ levels$/,
  DuplicateKey: /^JSON object repeats the key at byte \d+$/,
};

/** The send of the input text `inputJson`, with no options, to the unreachable handle. */
function requestOf(inputJson: string): EnqueueRequest {
  const minted = mintTaskId();

  if (minted.kind !== "minted") {
    assert.fail(`the core refused to mint a task id: ${minted.cause}`);
  }

  return {
    taskId: minted.taskId,
    taskName: "strict_check_probe",
    queueName: "default",
    priority: 100,
    inputJson,
    sentAtMicros: epochMicrosTowardPast(Temporal.Now.instant()),
    deadline: { kind: "noDeadline" },
    delay: { kind: "noDelay" },
    taskOptions: { kind: "none" },
    idempotencyKey: { kind: "none" },
    retention: { kind: "classKey", key: "standard_30d" },
  };
}

/** The send error of the input text `inputJson`; a send that succeeds fails the test. */
async function sendError(inputJson: string): Promise<RawCatalogError> {
  const handle = buildCoreHandle(manifestJson(UNREACHABLE_DATABASE_URL, false), "PRODUCER");
  const sent = await enqueue(handle, requestOf(inputJson), "FIRST", { kind: "noCarrier" });

  if (sent.kind !== "failed") {
    assert.fail(`the send of ${JSON.stringify(inputJson)} was stored`);
  }

  return sent.error;
}

/** The text of the `jsonUnreadable` cause of a `TaskSendValidationFailed`, or the failed test. */
function refusalTextOf(error: RawCatalogError): string {
  assert.equal(error.class, "TaskSendValidationFailed", show(error));

  const cause = error.slots.find((slot) => slot.name === "cause")?.value;

  assert(cause?.kind === "exception", show(error));
  assert(
    cause.exception.kind === "clientClass" && cause.exception.class === "serializationError",
    `not a json refusal: ${show(error)}`,
  );

  return cause.exception.text;
}

/** The text of the core's refusal of `text`, and the assert that it is the text of the variant `variant`. */
async function assertRefusedAs(text: string, variant: string, pythonText: string | undefined): Promise<void> {
  const pattern = VARIANT_TEXT[variant];

  assert(pattern !== undefined, `the variant ${variant} has no text pattern in this test`);

  const refusal = refusalTextOf(await sendError(text));

  assert(pattern.test(refusal), `${variant}: the core's text is ${JSON.stringify(refusal)}`);

  if (pythonText !== undefined) {
    assert.equal(refusal, pythonText);
  }
}

describe("task_input.json refused_texts through enqueue", () => {
  for (const row of taskInputFile().refused_texts) {
    test(`${row.name}: the core refuses it as ${row.core_refusal}`, async () => {
      await assertRefusedAs(row.text, row.core_refusal, row.refusal_text);
    });
  }
});

describe("result_envelope.json refused_texts through enqueue", () => {
  test("repeated_key: the core refuses it as DuplicateKey", async () => {
    const row = rowNamed(resultEnvelopeFile().refused_texts, "repeated_key");

    await assertRefusedAs(row.text, "DuplicateKey", undefined);
  });
});

describe("the control of the strict check", () => {
  test("a text that passes the check reaches the connection and fails to connect", async () => {
    const error = await sendError("{}");

    assert.equal(error.class, "TaskSendEnqueueFailed", show(error));
  });
});
