// The send refuses a stored `timeout_ms` that each worker refuses. The addon's `enqueue` passes the options text of
// the request to `CheckedSend::check` (`bridge/src/sends/send.rs`), which applies the core's `check_stored_timeout`
// before any connection. A refused text is the send error `TaskSendValidationFailed`.
//
// The options text of a send that the package prepares comes from a checked contract, whose `timeoutMs` is a whole
// number from 1000 to 4294967295 (`HRS-102`); none of the refused cases reaches the send that way. These tests send
// the text directly, as a resend of a stored request does. The handle names a port where no server listens, so no
// test of this file needs a database; the control tests show that an accepted text goes on to the connection.
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
import { manifestJson } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** A database URL where no server listens: a send that passes the check fails to connect. */
const UNREACHABLE_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** The name of the task of the probe sends; the refusal text names it. */
const TASK_NAME = "stored_timeout_probe";

/** The smallest stored `timeout_ms` that a worker runs (core `worker::retry::MIN_TASK_TIMEOUT_MS`). */
const MINIMUM_TIMEOUT_MS = 1000;

/** The largest stored `timeout_ms` that a worker runs (core `worker::retry::MAX_TASK_TIMEOUT_MS`). */
const MAXIMUM_TIMEOUT_MS = 4_294_967_295;

/** The options text of an `ISOLATED` send whose `timeout_ms` is written as the JSON text `timeoutText`. */
function optionsWithTimeout(timeoutText: string): string {
  return `{"retry_policy":null,"good_until":null,"timeout_ms":${timeoutText},"execution":"ISOLATED","cancel_grace_ms":null}`;
}

/** The send of the options text `options` to the unreachable handle. */
function requestOf(options: string): EnqueueRequest {
  const minted = mintTaskId();

  if (minted.kind !== "minted") {
    assert.fail(`the core refused to mint a task id: ${minted.cause}`);
  }

  return {
    taskId: minted.taskId,
    taskName: TASK_NAME,
    queueName: "default",
    priority: 100,
    inputJson: "{}",
    sentAtMicros: epochMicrosTowardPast(Temporal.Now.instant()),
    deadline: { kind: "noDeadline" },
    delay: { kind: "noDelay" },
    taskOptions: { kind: "text", text: options },
    idempotencyKey: { kind: "none" },
    retention: { kind: "classKey", key: "standard_30d" },
  };
}

/** The send error of the options text `options`; a send that is stored fails the test. */
async function sendError(options: string): Promise<RawCatalogError> {
  const handle = buildCoreHandle(manifestJson(UNREACHABLE_DATABASE_URL, false), "PRODUCER");
  const sent = await enqueue(handle, requestOf(options), "FIRST", { kind: "noCarrier" });

  if (sent.kind !== "failed") {
    assert.fail(`the send of ${options} was stored`);
  }

  return sent.error;
}

/** Check that the send of `options` is `TaskSendValidationFailed` and that its error holds `refusal`, the core's text. */
async function assertRefusedWith(options: string, refusal: string): Promise<void> {
  const error = await sendError(options);

  assert.equal(error.class, "TaskSendValidationFailed", show(error));
  assert(
    show(error).includes(`task_options refused for ${TASK_NAME}: ${refusal}`),
    `${options}: the error is ${show(error)}`,
  );
}

describe("a stored timeout_ms that the send refuses", () => {
  test("a value that is not an int: its type in the text", async () => {
    await assertRefusedWith(optionsWithTimeout('"30s"'), "stored timeout_ms is a str, not an int");
    await assertRefusedWith(optionsWithTimeout("1500.5"), "stored timeout_ms is a float, not an int");
    await assertRefusedWith(optionsWithTimeout("true"), "stored timeout_ms is a bool, not an int");
    await assertRefusedWith(optionsWithTimeout("[1000]"), "stored timeout_ms is a list, not an int");
    await assertRefusedWith(optionsWithTimeout('{"a":1}'), "stored timeout_ms is a dict, not an int");
  });

  test("an int below 1000 ms", async () => {
    await assertRefusedWith(
      optionsWithTimeout(String(MINIMUM_TIMEOUT_MS - 1)),
      `stored timeout_ms is below ${String(MINIMUM_TIMEOUT_MS)} ms`,
    );
  });

  test("an int above 4294967295 ms", async () => {
    await assertRefusedWith(
      optionsWithTimeout(String(MAXIMUM_TIMEOUT_MS + 1)),
      `stored timeout_ms is above the limit of ${String(MAXIMUM_TIMEOUT_MS)} ms`,
    );
  });
});

describe("a stored timeout_ms that the send accepts reaches the connection", () => {
  const accepted: readonly [string, string][] = [
    ["the smallest value", String(MINIMUM_TIMEOUT_MS)],
    ["the largest value", String(MAXIMUM_TIMEOUT_MS)],
    ["null", "null"],
  ];

  for (const [label, timeoutText] of accepted) {
    test(`${label} fails to connect, not to validate`, async () => {
      const error = await sendError(optionsWithTimeout(timeoutText));

      assert.equal(error.class, "TaskSendEnqueueFailed", show(error));
    });
  }
});
