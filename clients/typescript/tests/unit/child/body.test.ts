// The body adapter: the input decode, the read-back of the value, the stored task error, the exception map, an
// unmapped throw, a return that is not a Result, sync and async bodies, the loop marks, and the body observer.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { refusedCodeParts } from "../../../horsies/src/child/body.ts";
import { implementationOf, implementationParts, type ImplementationParts } from "../../../horsies/src/contract/implementation.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { adapterRun, errorParts, RUN_TASK_ID } from "./support.ts";

const add = defineTask({ name: "add", input: { a: h.int(), b: h.int() }, output: h.int(), errors: ["TOO_BIG"] });

const greet = defineTask({ name: "greet", input: { name: h.string() }, output: h.object({ text: h.string() }) });

class CardDeclined extends Error {
  override readonly name = "CardDeclined";
}

class SoftDecline extends CardDeclined {}

function parts(
  contract: object,
  body: (input: never, runtime: never) => unknown,
  exceptions: ImplementationParts["exceptions"] = [],
): ImplementationParts {
  return { contract, body, exceptions };
}

describe("the input of a body", () => {
  test("the input decodes with the contract's parameters", async () => {
    const run = await adapterRun(implementationParts(implementationOf(add, ({ a, b }) => add.ok(a + b), [])), '{"a":2,"b":3}');

    assert.deepEqual(run.outcome, { kind: "value", json: "5" });
  });

  test("an input that does not decode is WORKER_SERIALIZATION_ERROR with the path and the kind, never the value", async () => {
    const run = await adapterRun(parts(add, () => add.ok(0)), '{"a":"secret-text","b":3}');
    const error = errorParts(run.outcome);

    assert.deepEqual(error.code, { kind: "library", code: "WORKER_SERIALIZATION_ERROR" });
    assert.equal(error.message.kind === "text" ? error.message.text : "", "task input does not decode as add.input: $.a: expected an integer [WrongType]");
    assert.doesNotMatch(error.dataJson, /secret-text/u);
    assert.match(error.dataJson, /"expected_type":"add\.input"/u);
    assert.deepEqual(run.observation.calls, ["end"]);
  });
});

describe("the value of a body", () => {
  test("a value that reads back is stored as its text", async () => {
    const run = await adapterRun(parts(greet, () => greet.ok({ text: "hi ✓" })), '{"name":"x"}');

    assert.deepEqual(run.outcome, { kind: "value", json: '{"text":"hi ✓"}' });
  });

  test("a value that does not read back is WORKER_SERIALIZATION_ERROR", async () => {
    // SAFETY: the test returns a value outside the output type on purpose: the read-back refuses it.
    const run = await adapterRun(parts(greet, () => greet.ok({ text: 7 } as never)), '{"name":"x"}');
    const error = errorParts(run.outcome);

    assert.deepEqual(error.code, { kind: "library", code: "WORKER_SERIALIZATION_ERROR" });
    assert.match(error.message.kind === "text" ? error.message.text : "", /^task returned a value of type greet\.output: \$\.text/u);
  });

  test("a return that is not a Result is RETURN_TYPE_MISMATCH", async () => {
    const run = await adapterRun(parts(add, () => 5), '{"a":1,"b":1}');
    const error = errorParts(run.outcome);

    assert.deepEqual(error.code, { kind: "library", code: "RETURN_TYPE_MISMATCH" });
    assert.equal(error.message.kind === "text" ? error.message.text : "", "Task add returned number but must return ok(value) or err({ code })");
  });
});

describe("the errors of a body", () => {
  test("a declared task error keeps its code, message and data", async () => {
    const run = await adapterRun(
      parts(add, () => add.err({ code: "TOO_BIG", message: "over the cap", data: { cap: 10 } })),
      '{"a":1,"b":1}',
    );

    assert.deepEqual(run.outcome, {
      kind: "error",
      parts: {
        code: { kind: "user", code: "TOO_BIG" },
        message: { kind: "text", text: "over the cap" },
        dataJson: '{"cap":10}',
        exception: { kind: "noException" },
      },
    });
  });

  test("a thrown instance of a mapped class is stored with the mapped code, the message and the record", async () => {
    const run = await adapterRun(
      parts(
        add,
        () => {
          throw new SoftDecline("the card was declined");
        },
        [[CardDeclined, "TOO_BIG"]],
      ),
      '{"a":1,"b":1}',
    );

    const error = errorParts(run.outcome);

    assert.deepEqual(error.code, { kind: "user", code: "TOO_BIG" });
    assert.equal(error.message.kind === "text" ? error.message.text : "", "Unhandled exception in task add: CardDeclined: the card was declined");
    assert.equal(error.dataJson, '{"exception_type":"CardDeclined"}');
    assert.equal(error.exception.kind === "record" ? error.exception.className : "", "CardDeclined");
  });

  test("an unmapped Error is UNHANDLED_EXCEPTION, with a repr equal to the message and an empty module", async () => {
    const run = await adapterRun(
      parts(add, async () => {
        await Promise.resolve();

        throw new RangeError("out of range");
      }),
      '{"a":1,"b":1}',
    );

    const error = errorParts(run.outcome);

    assert.deepEqual(error.code, { kind: "library", code: "UNHANDLED_EXCEPTION" });
    assert.equal(error.message.kind === "text" ? error.message.text : "", "Unhandled exception in task add: RangeError: out of range");

    if (error.exception.kind !== "record") {
      return assert.fail("the error has a record");
    }

    assert.equal(error.exception.module, "");
    assert.equal(error.exception.repr, error.exception.message);
  });

  test("a thrown value that is not an Error is UNHANDLED_EXCEPTION with its type rule", async () => {
    const run = await adapterRun(
      parts(add, () => {
        // oxlint-disable-next-line no-throw-literal -- the rule of a thrown value that is not an Error is the subject
        throw null;
      }),
      '{"a":1,"b":1}',
    );

    assert.equal(errorParts(run.outcome).dataJson, '{"exception_type":"null"}');
  });

  test("a user code that the core refuses is RETURN_TYPE_MISMATCH that names the task and the rule", () => {
    const refused = refusedCodeParts("add", "bad code", "a code has no space");

    assert.deepEqual(refused.code, { kind: "library", code: "RETURN_TYPE_MISMATCH" });
    assert.match(refused.message.kind === "text" ? refused.message.text : "", /^Task add returned err with the error code "bad code"/u);
  });
});

describe("sync and async bodies, the loop marks, the observer", () => {
  test("a sync body and an async body give the same outcome; the run is held for the synchronous part only", async () => {
    const sync = await adapterRun(parts(add, () => add.ok(3)), '{"a":1,"b":2}');

    const async_ = await adapterRun(
      parts(add, async () => {
        await Promise.resolve();

        return add.ok(3);
      }),
      '{"a":1,"b":2}',
    );

    assert.deepEqual(sync.outcome, async_.outcome);
    assert.deepEqual(sync.marks.marks, [`hold ${RUN_TASK_ID}`, "release"]);
    assert.deepEqual(async_.marks.marks, [`hold ${RUN_TASK_ID}`, "release"]);
  });

  test("the observer sees run, then thrown before the mapping, then end", async () => {
    const thrown = new Error("boom");

    const run = await adapterRun(
      parts(add, () => {
        throw thrown;
      }),
      '{"a":1,"b":1}',
    );

    assert.deepEqual(run.observation.calls, ["run", "thrown", "end"]);
    assert.equal(run.observation.thrownValues[0], thrown);
    assert.deepEqual(run.observation.ends, [{ kind: "failed", code: "UNHANDLED_EXCEPTION" }]);
  });

  test("the end is the run's cancel cause when its signal fired", async () => {
    const timedOut = await adapterRun(parts(add, () => add.ok(2)), '{"a":1,"b":1}', "DEADLINE");
    const cancelled = await adapterRun(parts(add, () => add.ok(2)), '{"a":1,"b":1}', "OPERATOR");
    const returned = await adapterRun(parts(add, () => add.ok(2)), '{"a":1,"b":1}');

    assert.deepEqual(timedOut.observation.ends, [{ kind: "timedOut" }]);
    assert.deepEqual(cancelled.observation.ends, [{ kind: "cancelled" }]);
    assert.deepEqual(returned.observation.ends, [{ kind: "returned" }]);
  });
});
