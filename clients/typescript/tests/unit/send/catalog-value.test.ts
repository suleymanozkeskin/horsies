// The value of a core catalog error that the generated converters take, from the raw record of the bridge: each slot
// kind, each field kind of a send payload, and each exception record in its public form.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { ExceptionRecord } from "../../../horsies/src/native/values/records.ts";
import { nativeCatalogValue, publicExceptionCause } from "../../../horsies/src/send/catalog-value.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { OWNER_TASK_ID, payloadFields, SENT_AT_MICROS, TASK_ID } from "../app/support.ts";

describe("nativeCatalogValue", () => {
  test("each slot kind gives its native form, by slot name", () => {
    const value = nativeCatalogValue({
      class: "AnyClass",
      slots: [
        { name: "text", value: { kind: "text", text: "a message" } },
        { name: "flag", value: { kind: "flag", value: true } },
        { name: "integer", value: { kind: "integer", value: 18_446_744_073_709_551_615n } },
        { name: "uuid", value: { kind: "uuid", text: TASK_ID } },
        { name: "member", value: { kind: "codeMember", catalog: "TaskStatus", value: "PENDING" } },
        { name: "cause", value: { kind: "exception", exception: { kind: "noCause" } } },
      ],
    });

    assert.deepEqual(value, {
      class: "AnyClass",
      slots: {
        text: "a message",
        flag: true,
        integer: 18_446_744_073_709_551_615n,
        uuid: TASK_ID,
        member: "PENDING",
        cause: { kind: "noCause" },
      },
    });
    assert.equal(Object.isFrozen(value), true);
    assert.equal(Object.isFrozen(value.slots), true);
  });

  test("a send payload gives its fields: an instant as microseconds, an integer as a number, retain forever as null", () => {
    const value = nativeCatalogValue({
      class: "TaskSendPayloadMismatch",
      slots: [{ name: "payload", value: { kind: "sendPayload", fields: payloadFields("report") } }],
    });

    assert.deepEqual(value.slots["payload"], {
      task_name: "report",
      queue_name: "default",
      priority: 100,
      input_json: '{"n":1}',
      sent_at: SENT_AT_MICROS,
      good_until: SENT_AT_MICROS + 60_000_000n,
      enqueue_delay_seconds: 30,
      task_options: '{"task_name":"fake"}',
      idempotency_key: "key-1",
      retention_class_key: null,
    });
  });

  test("an absent payload field is null", () => {
    const value = nativeCatalogValue({
      class: "C",
      slots: [{ name: "payload", value: { kind: "sendPayload", fields: [{ name: "good_until", value: { kind: "absent" } }] } }],
    });

    assert.deepEqual(value.slots["payload"], { good_until: null });
  });

  test("a slot name given twice is a ClientDefect", () => {
    assert.throws(
      () =>
        nativeCatalogValue({
          class: "C",
          slots: [
            { name: "owner", value: { kind: "uuid", text: OWNER_TASK_ID } },
            { name: "owner", value: { kind: "uuid", text: TASK_ID } },
          ],
        }),
      ClientDefect,
    );
  });
});

describe("publicExceptionCause", () => {
  const cases: readonly (readonly [string, ExceptionRecord, object])[] = [
    ["noCause", { kind: "noCause" }, { kind: "noCause" }],
    [
      "a server failure",
      {
        kind: "coreFailure",
        cause: { kind: "server", subject: "enqueue", sqlstate: "23505", message: "duplicate key", retryable: false },
      },
      {
        kind: "coreFailure",
        failure: "server",
        subject: "enqueue",
        message: "duplicate key",
        retryable: false,
        sqlstate: "23505",
      },
    ],
    [
      "a connection failure",
      { kind: "coreFailure", cause: { kind: "connection", subject: "enqueue", message: "refused", retryable: true } },
      { kind: "coreFailure", failure: "connection", subject: "enqueue", message: "refused", retryable: true },
    ],
    [
      "a contract violation",
      { kind: "coreFailure", cause: { kind: "contractViolation", subject: "enqueue", message: "bad row" } },
      { kind: "coreFailure", failure: "contractViolation", subject: "enqueue", message: "bad row" },
    ],
    [
      "a failed precondition",
      { kind: "coreFailure", cause: { kind: "preconditionFailed", subject: "enqueue", message: "closed", retryable: false } },
      { kind: "coreFailure", failure: "preconditionFailed", subject: "enqueue", message: "closed", retryable: false },
    ],
    [
      "OutcomeDecodeError",
      { kind: "generated", class: "OutcomeDecodeError", text: "bad outcome" },
      { kind: "outcomeDecode", message: "bad outcome" },
    ],
    [
      "HistoryContractError",
      { kind: "generated", class: "HistoryContractError", text: "bad history" },
      { kind: "historyContract", message: "bad history" },
    ],
    [
      "valueError",
      { kind: "clientClass", class: "valueError", text: "priority out of range" },
      { kind: "ruleRefused", message: "priority out of range" },
    ],
    [
      "serializationError",
      { kind: "clientClass", class: "serializationError", text: "not JSON" },
      { kind: "jsonUnreadable", message: "not JSON" },
    ],
  ];

  for (const [name, record, expected] of cases) {
    test(`${name} gives its public form`, () => {
      const cause = publicExceptionCause(record);

      assert.deepEqual(cause, expected);
      assert.equal(Object.isFrozen(cause), true);
    });
  }

  test("a generated exception class that this build does not know is a ClientDefect", () => {
    assert.throws(() => publicExceptionCause({ kind: "generated", class: "NewCoreError", text: "x" }), ClientDefect);
  });
});
