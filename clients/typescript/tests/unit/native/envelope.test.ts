// The envelope reader: one case per thrown code, and the value case.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { envelopeValue, thrownError } from "../../../horsies/src/native/call/envelope.ts";
import type { NativeThrown } from "../../../horsies/src/native/call/envelope.ts";
import { CoreError } from "../../../horsies/src/thrown-errors.ts";

/** The facts of the error of a thrown code: its own fields, its message, and its base class. */
interface ErrorFacts {
  readonly own: object;
  readonly message: string;
  readonly base: "CoreError" | "CoreDefect";
}

function errorFacts(thrown: NativeThrown): ErrorFacts {
  const error = thrownError(thrown);

  return {
    own: { ...error },
    message: error.message,
    base: error instanceof CoreError ? "CoreError" : "CoreDefect",
  };
}

describe("envelopeValue", () => {
  test("gives the value of a value envelope", () => {
    assert.equal(envelopeValue({ kind: "value", value: 42n }), 42n);
  });

  test("throws the class of the code of a thrown envelope", () => {
    assert.throws(
      () => envelopeValue({ kind: "thrown", thrown: { code: "CORE_START_FAILED", cause: "no threads" } }),
      { name: "CoreStartFailed", code: "CORE_START_FAILED", cause: "no threads" },
    );
  });
});

describe("thrownError", () => {
  test("CORE_STOPPED is CoreStopped with the panic and the state after", () => {
    assert.deepEqual(
      errorFacts({ code: "CORE_STOPPED", subject: "log forwarder", cause: "boom", stateAfter: "outcomeUnknown" }),
      {
        own: {
          name: "CoreStopped",
          code: "CORE_STOPPED",
          subject: "log forwarder",
          cause: "boom",
          stateAfter: "outcomeUnknown",
        },
        message: "the core stopped in this process: panic in log forwarder: boom",
        base: "CoreError",
      },
    );
  });

  test("CORE_START_FAILED is CoreStartFailed with the cause", () => {
    assert.deepEqual(errorFacts({ code: "CORE_START_FAILED", cause: "no threads" }), {
      own: {
        name: "CoreStartFailed",
        code: "CORE_START_FAILED",
        cause: "no threads",
      },
      message: "the core could not start: no threads",
      base: "CoreError",
    });
  });

  test("TOO_MANY_PENDING_CALLS is TooManyPendingCalls with the bound", () => {
    assert.deepEqual(errorFacts({ code: "TOO_MANY_PENDING_CALLS", subject: "ping_database", bound: 65_536 }), {
      own: {
        name: "TooManyPendingCalls",
        code: "TOO_MANY_PENDING_CALLS",
        subject: "ping_database",
        bound: 65_536,
      },
      message: "ping_database: the process has 65536 core calls in flight, the most it may have",
      base: "CoreError",
    });
  });

  test("FORKED_AFTER_CORE_START is ForkedAfterCoreStart with both process ids", () => {
    assert.deepEqual(errorFacts({ code: "FORKED_AFTER_CORE_START", corePid: 10, thisPid: 11 }), {
      own: {
        name: "ForkedAfterCoreStart",
        code: "FORKED_AFTER_CORE_START",
        corePid: 10,
        thisPid: 11,
      },
      message: "the core started in process 10, and this process (11) is a fork of it without exec",
      base: "CoreError",
    });
  });

  test("MANIFEST_REFUSED is ManifestRefused with the field path", () => {
    assert.deepEqual(errorFacts({ code: "MANIFEST_REFUSED", fieldPath: "queues[0].name", cause: "empty" }), {
      own: {
        name: "ManifestRefused",
        code: "MANIFEST_REFUSED",
        fieldPath: "queues[0].name",
        cause: "empty",
      },
      message: "the core refused the manifest at queues[0].name: empty",
      base: "CoreDefect",
    });
  });

  test("PANIC_RULE_FIXED is PanicRuleFixed with the rule in force", () => {
    assert.deepEqual(errorFacts({ code: "PANIC_RULE_FIXED", current: "ABORT_PROCESS" }), {
      own: {
        name: "PanicRuleFixed",
        code: "PANIC_RULE_FIXED",
        current: "ABORT_PROCESS",
      },
      message: "the panic rule of this process is fixed at ABORT_PROCESS",
      base: "CoreDefect",
    });
  });

  test("CORE_PANIC is CorePanic with the subject and the cause", () => {
    assert.deepEqual(errorFacts({ code: "CORE_PANIC", subject: "core_state", cause: "boom (at lib.rs:1)" }), {
      own: {
        name: "CorePanic",
        code: "CORE_PANIC",
        subject: "core_state",
        cause: "boom (at lib.rs:1)",
      },
      message: "panic in core operation core_state: boom (at lib.rs:1)",
      base: "CoreDefect",
    });
  });

  test("CLIENT_DEFECT is ClientDefect with the subject and the rule", () => {
    assert.deepEqual(errorFacts({ code: "CLIENT_DEFECT", subject: "taskId", rule: "a UUID" }), {
      own: {
        name: "ClientDefect",
        code: "CLIENT_DEFECT",
        subject: "taskId",
        rule: "a UUID",
      },
      message: "a defect of the horsies package: taskId: a UUID",
      base: "CoreDefect",
    });
  });
});
