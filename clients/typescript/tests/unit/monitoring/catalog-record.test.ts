// The raw record of a core catalog error, in the form of the generated converters: one case per slot kind and per
// exception kind, and the defects.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { exceptionCause, nativeCatalogRecord } from "../../../horsies/src/native/facades/catalog-record.ts";
import type { CauseRecord } from "../../../horsies/src/native/values/records.ts";
import { codeSlot, flagSlot, textSlot, uuidSlot } from "./support/fakes.ts";

const TASK_ID = "01890a5d-ac96-774b-bcce-b302099a8057";

describe("nativeCatalogRecord", () => {
  test("gives each slot by its core name with its plain value, frozen", () => {
    const record = nativeCatalogRecord({
      class: "TaskActionNotCancellable",
      slots: [
        textSlot("message", "cannot cancel"),
        uuidSlot("task_id", TASK_ID),
        codeSlot("current_status", "TaskStatus", "COMPLETED"),
        flagSlot("retryable", false),
        { name: "size_bytes", value: { kind: "integer", value: 12n } },
      ],
    });

    assert.deepEqual(record, {
      class: "TaskActionNotCancellable",
      slots: { message: "cannot cancel", task_id: TASK_ID, current_status: "COMPLETED", retryable: false, size_bytes: 12n },
    });
    assert.ok(Object.isFrozen(record) && Object.isFrozen(record.slots));
  });

  test("a slot named __proto__ is an own slot, not a prototype", () => {
    const record = nativeCatalogRecord({ class: "X", slots: [textSlot("__proto__", "kept")] });

    assert.deepEqual(Object.getOwnPropertyNames(record.slots), ["__proto__"]);
    assert.equal(Object.getPrototypeOf(record.slots), Object.prototype);
  });

  test("a send payload slot is a defect of a facade catalog", () => {
    assert.throws(
      () => nativeCatalogRecord({ class: "TaskSendEnqueueFailed", slots: [{ name: "payload", value: { kind: "sendPayload", fields: [] } }] }),
      { name: "ClientDefect", subject: "TaskSendEnqueueFailed.payload" },
    );
  });
});

describe("exceptionCause", () => {
  const causes: ReadonlyArray<readonly [CauseRecord, object]> = [
    [
      { kind: "server", subject: "s", sqlstate: "40001", message: "m", retryable: true },
      { kind: "coreFailure", failure: "server", subject: "s", message: "m", retryable: true, sqlstate: "40001" },
    ],
    [
      { kind: "connection", subject: "s", message: "m", retryable: true },
      { kind: "coreFailure", failure: "connection", subject: "s", message: "m", retryable: true },
    ],
    [
      { kind: "contractViolation", subject: "s", message: "m" },
      { kind: "coreFailure", failure: "contractViolation", subject: "s", message: "m" },
    ],
    [
      { kind: "preconditionFailed", subject: "s", message: "m", retryable: false },
      { kind: "coreFailure", failure: "preconditionFailed", subject: "s", message: "m", retryable: false },
    ],
  ];

  for (const [cause, expected] of causes) {
    test(`a ${cause.kind} cause is the core failure ${cause.kind}`, () => {
      assert.deepEqual(exceptionCause({ kind: "coreFailure", cause }), expected);
    });
  }

  test("the two exception classes of the core have their kinds", () => {
    assert.deepEqual(exceptionCause({ kind: "generated", class: "OutcomeDecodeError", text: "t" }), {
      kind: "outcomeDecode",
      message: "t",
    });
    assert.deepEqual(exceptionCause({ kind: "generated", class: "HistoryContractError", text: "t" }), {
      kind: "historyContract",
      message: "t",
    });
  });

  test("an exception class that this build does not know is a defect", () => {
    assert.throws(() => exceptionCause({ kind: "generated", class: "NewError", text: "t" }), {
      name: "ClientDefect",
      subject: "exception",
    });
  });

  test("the client classes are a refused rule and an unreadable JSON text", () => {
    assert.deepEqual(exceptionCause({ kind: "clientClass", class: "valueError", text: "t" }), {
      kind: "ruleRefused",
      message: "t",
    });
    assert.deepEqual(exceptionCause({ kind: "clientClass", class: "serializationError", text: "t" }), {
      kind: "jsonUnreadable",
      message: "t",
    });
  });

  test("no cause is noCause", () => {
    assert.deepEqual(exceptionCause({ kind: "noCause" }), { kind: "noCause" });
  });
});
