// The post dispatcher: a body settles with its value or its error; a refused user code; a cancel reaches the body's
// signal; a stop request reaches spawned work; a defect goes to the defect rule and never throws into the caller.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { type BodyObserver, NO_BODY_OBSERVER } from "../../../horsies/src/child/body-observer.ts";
import { RunTable } from "../../../horsies/src/child/run-table.ts";
import { postDispatcher } from "../../../horsies/src/child/serve.ts";
import { StateValues } from "../../../horsies/src/child/state.ts";
import { implementationOf, implementationParts, type ImplementationParts } from "../../../horsies/src/contract/implementation.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { ChildPost } from "../../../horsies/src/native/child/addon.ts";
import type { CoreDefect } from "../../../horsies/src/thrown-errors.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import type { Ok } from "../../../horsies/src/wire/result.ts";
import { FakeChildNative, settled } from "./child-native.ts";
import { RecordingMarks, RecordingObservation, RUN_TASK_ID } from "./support.ts";

const double = defineTask({ name: "double", input: { n: h.int() }, output: h.int() });

const wait = defineTask({
  name: "wait",
  input: {},
  output: h.string(),
  execution: "SHARED_ASYNC",
  timeoutMs: 5000,
});

function served(
  implementations: readonly ImplementationParts[],
  native: FakeChildNative = new FakeChildNative(),
  observer: BodyObserver = NO_BODY_OBSERVER,
): { readonly post: (post: ChildPost) => void; readonly native: FakeChildNative; readonly defects: CoreDefect[]; readonly table: RunTable } {
  const defects: CoreDefect[] = [];
  const marks = new RecordingMarks();
  const table = new RunTable(native, marks, new StateValues([]));

  const post = postDispatcher({
    implementations: new Map(implementations.map((parts) => [String(Reflect.get(parts.contract, "name")), parts])),
    table,
    observer,
    native,
    marks,
    reportDefect: (defect) => defects.push(defect),
  });

  return { post, native, defects, table };
}

function runBody(runKey: bigint, taskName: string, inputJson: string): ChildPost {
  return { kind: "runBody", runKey, taskName, inputJson, taskId: RUN_TASK_ID, attempt: 1, facts: { kind: "notRecorded" } };
}

describe("the cancel post and the body observation", () => {
  test("a cancelRun reaches the observation of its run; after the body settled it reaches none", async () => {
    const observation = new RecordingObservation();
    const observer: BodyObserver = { enter: () => observation, flush: async () => undefined };
    let release: () => void = () => undefined;

    const held = new Promise<void>((resolve) => {
      release = resolve;
    });

    const child = served(
      [
        implementationParts(
          implementationOf(
            wait,
            async (_input, { ok }) => {
              await held;

              return ok("done");
            },
            [],
          ),
        ),
      ],
      new FakeChildNative(),
      observer,
    );

    child.post(runBody(7n, "wait", "{}"));
    await settled();
    child.post({ kind: "cancelRun", runKey: 7n, cause: "DEADLINE" });
    release();
    await settled();
    child.post({ kind: "cancelRun", runKey: 7n, cause: "OPERATOR" });
    assert.deepEqual(
      observation.calls.filter((call) => call.startsWith("cancelled")),
      ["cancelled DEADLINE"],
    );
  });
});

describe("the post dispatcher", () => {
  test("a body that returns settles with the stored text of its value, and the run leaves the table", async () => {
    const child = served([implementationParts(implementationOf(double, ({ n }, { ok }) => ok(n * 2), []))]);

    child.post(runBody(5n, "double", '{"n":21}'));
    await settled();
    assert.deepEqual(child.native.values, [[5n, "42"]]);
    assert.deepEqual(child.table.size(), { runs: 0, works: 0 });
  });

  test("the reader of a child gets only the input text that the core posts after its strict check", async () => {
    // The core's child host refuses a run input that breaks its strict read rule (`build_task_input`) and ends that
    // run itself, so the post holds checked text only; the dispatcher gives that text to the reader as it came.
    const seen: unknown[] = [];
    const echo = defineTask({ name: "echo", input: { n: h.bigint(), s: h.string() }, output: h.null() });

    const child = served([
      implementationParts(
        implementationOf(
          echo,
          (input, { ok }) => {
            seen.push(input);

            return ok(null);
          },
          [],
        ),
      ),
    ]);

    child.post(runBody(4n, "echo", '{"n":123456789012345678901234567890,"s":"ğ✓"}'));
    await settled();
    assert.deepEqual(seen, [{ n: 123_456_789_012_345_678_901_234_567_890n, s: "ğ✓" }]);
    assert.deepEqual(child.native.values, [[4n, "null"]]);
  });

    test("a body that throws settles with the core's task error of its parts", async () => {
    const child = served([
      implementationParts(
        implementationOf(
          double,
          () => {
            throw new Error("boom");
          },
          [],
        ),
      ),
    ]);

    child.post(runBody(6n, "double", '{"n":1}'));
    await settled();
    assert.equal(child.native.errors[0]?.[0], 6n);
    assert.deepEqual(child.native.errors[0]?.[1].code, { kind: "library", code: "UNHANDLED_EXCEPTION" });
  });

  test("an err code that the core refuses settles as RETURN_TYPE_MISMATCH", async () => {
    const native = new FakeChildNative();

    native.refusedCodes = new Set(["bad code"]);

    const child = served([implementationParts(implementationOf(double, (_input, { err }) => err({ code: "bad code" }), []))], native);

    child.post(runBody(7n, "double", '{"n":1}'));
    await settled();
    assert.equal(child.native.builds.length, 2);
    assert.deepEqual(child.native.errors[0]?.[1].code, { kind: "library", code: "RETURN_TYPE_MISMATCH" });
  });

  test("a cancel post aborts the body's signal with its cause", async () => {
    const reasons: unknown[] = [];

    const implementation = implementationOf(
      wait,
      async (_input, runtime) =>
        new Promise<Ok<string>>((resolve) => {
          runtime.signal.addEventListener("abort", () => {
            reasons.push(runtime.signal.reason);
            resolve(runtime.ok("stopped"));
          });
        }),
      [],
    );

    const child = served([implementationParts(implementation)]);

    child.post(runBody(8n, "wait", "{}"));
    child.post({ kind: "cancelRun", runKey: 8n, cause: "DEADLINE" });
    await settled();
    assert.deepEqual(reasons, [{ cause: "DEADLINE" }]);
    assert.deepEqual(child.native.values, [[8n, '"stopped"']]);
  });

  test("a stop post aborts the signal of the spawned work with LEFT_WORK", async () => {
    const reasons: unknown[] = [];

    const implementation = implementationOf(
      wait,
      async (_input, runtime) => {
        void runtime.spawn("flush", async (signal) => {
          await new Promise<void>((resolve) => {
            signal.addEventListener("abort", () => {
              reasons.push(signal.reason);
              resolve();
            });
          });
        });

        return runtime.ok("left work");
      },
      [],
    );

    const child = served([implementationParts(implementation)]);

    child.post(runBody(9n, "wait", "{}"));
    await settled();
    child.post({ kind: "stopWork", workKey: 90n, cause: "LEFT_WORK" });
    await settled();
    assert.deepEqual(reasons, [{ cause: "LEFT_WORK" }]);
    assert.deepEqual(child.table.size(), { runs: 0, works: 0 });
  });

  test("a task with no implementation and a second settle are defects of the rule; the dispatcher does not throw", async () => {
    const native = new FakeChildNative();

    native.settle = "unknownRun";

    const child = served([implementationParts(implementationOf(double, ({ n }, { ok }) => ok(n), []))], native);

    assert.doesNotThrow(() => {
      child.post(runBody(10n, "absent", "{}"));
    });
    child.post(runBody(11n, "double", '{"n":1}'));
    await settled();
    assert.equal(child.defects.length, 2);
    assert.match(child.defects[0]?.message ?? "", /absent, which has no implementation/u);
    assert.match(child.defects[1]?.message ?? "", /settled twice/u);
  });
});
