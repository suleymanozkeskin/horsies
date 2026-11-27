// The child mode rules of the review fixes: a send from a body is not suppressed in a served child; a failure of
// child mode ends the child with 1 and never rejects; a package defect in a body goes to the defect rule; spawned work
// may spawn after the body settled; a sync throw of `forTest` work is a rejected Promise.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { createHorsies, type Horsies } from "../../../horsies/src/app.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import { onChildStartFor, provideFor, servedMembers } from "../../../horsies/src/child/app-members.ts";
import { AttemptNumber } from "../../../horsies/src/child/attempt.ts";
import { CHILD_MODE_FAILED_EXIT_CODE, type ChildModeDeps, runChildMode } from "../../../horsies/src/child/host.ts";
import { TaskRuntime } from "../../../horsies/src/child/runtime.ts";
import { stateKeyNamed, StateValues } from "../../../horsies/src/child/state.ts";
import { taskIdFromCore } from "../../../horsies/src/contract/ids.ts";
import type { ChildPost } from "../../../horsies/src/native/child/addon.ts";
import { ClientDefect, type CoreDefect } from "../../../horsies/src/thrown-errors.ts";
import { DATABASE_URL, fakeEnvironment } from "../app/support.ts";
import { ping } from "../worker/support.ts";
import { FakeChildNative, settled } from "./child-native.ts";
import { FakeRunNative, RecordingMarks, RUN_TASK_ID, tableRun } from "./support.ts";
import { unusedSeatHost } from "../seats/support.ts";

const CHILD_ARGUMENTS = ["--horsies-child", "3", "4242"] as const;

class Ended extends Error {
  readonly code: number;

  constructor(code: number) {
    super(`ended with ${String(code)}`);
    this.code = code;
  }
}

/** A defect rule that keeps each defect and throws it, as the rule does outside a worker process. */
class KeptDefects {
  readonly defects: CoreDefect[] = [];

  readonly raise = (defect: CoreDefect): never => {
    this.defects.push(defect);

    throw defect;
  };
}

function deps(native: FakeChildNative, lines: string[], defects: CoreDefect[]): ChildModeDeps {
  return {
    native,
    events: { onSignal: () => undefined, onUncaught: () => undefined, onRejection: () => undefined },
    timer: () => () => undefined,
    culprit: { kind: "none" },
    settlement: { returned: () => undefined, threw: () => undefined },
    hostname: "host-1",
    seats: unusedSeatHost(),
    writeLine: (line) => lines.push(line),
    wait: async () => undefined,
    exit: (code) => {
      throw new Ended(code);
    },
    reportDefect: (defect) => defects.push(defect),
  };
}

async function endedCode(run: Promise<never>): Promise<number> {
  return run.then(
    () => assert.fail("the process did not end"),
    (cause: unknown) => (cause instanceof Ended ? cause.code : assert.fail(`not an end: ${String(cause)}`)),
  );
}

function runBodyPost(runKey: bigint): ChildPost {
  return { kind: "runBody", runKey, taskName: "ping", inputJson: '{"n":1}', taskId: RUN_TASK_ID, attempt: 1, maxRetries: 0, facts: { kind: "notRecorded" } };
}

describe("a served child", () => {
  test("a send from a body is not suppressed once the child serves", async () => {
    const environment = fakeEnvironment({ start: { argumentAfterScript: "--horsies-child", seatIsolate: false } });
    const created = createHorsies({ database: { databaseUrl: DATABASE_URL }, tasks: [ping] }, environment);
    const app = created.ok ? created.value : assert.fail("the test app was refused");
    const sends: string[] = [];

    assert.equal(appInternals(app).suppression.current().kind, "suppressed");
    app.implement(ping, async ({ n }, { ok }) => {
      const sent = await app.send(ping, { n: n + 1 });

      sends.push(sent.ok ? "sent" : sent.error.code);

      return ok(n);
    });

    const native = new FakeChildNative();
    const running = runChildMode(app, CHILD_ARGUMENTS, deps(native, [], []));

    await settled();
    // The core runs the start hooks before it posts a run: hook 0 gives the start facts.
    native.serves[0]?.hook(1n, { index: 0, facts: { kind: "isolated" } });
    await settled();
    native.serves[0]?.post(runBodyPost(1n));
    await settled();
    assert.deepEqual(sends, ["sent"]);
    assert.equal(environment.fakeProducer.enqueues.length, 1);
    native.endServe(0);
    assert.equal(await endedCode(running), 0);
  });

  test("a failure of child mode ends the child with 1: a defect through the rule, another value as a line", async () => {
    const native = new FakeChildNative();
    const lines: string[] = [];
    const defects: CoreDefect[] = [];

    native.checkAppConfig = () => ({ kind: "refused", refusals: [] });
    assert.equal(await endedCode(runChildMode(createdApp(), CHILD_ARGUMENTS, deps(native, lines, defects))), CHILD_MODE_FAILED_EXIT_CODE);
    assert.equal(CHILD_MODE_FAILED_EXIT_CODE, 1);
    assert.equal(defects[0] instanceof ClientDefect, true);

    const failing = new FakeChildNative();

    failing.serveChild = async () => Promise.reject(new RangeError("the host thread did not start"));
    assert.equal(await endedCode(runChildMode(createdApp(), CHILD_ARGUMENTS, deps(failing, lines, []))), 1);
    assert.match(lines.at(-1) ?? "", /^horsies child: child mode failed: RangeError: the host thread did not start/u);
  });
});

function createdApp(): Horsies {
  const created = createHorsies({ database: { databaseUrl: DATABASE_URL }, tasks: [ping] }, fakeEnvironment());

  return created.ok ? created.value : assert.fail("the test app was refused");
}

describe("package defects in a child go to the defect rule", () => {
  test("runtime.state with a key of another app", () => {
    const rule = new KeptDefects();
    const { runtime } = tableRun(new FakeRunNative(), new RecordingMarks(), new StateValues([], rule.raise));

    assert.throws(() => runtime.state(stateKeyNamed<string>("provide #1 other")), ClientDefect);
    assert.match(rule.defects[0]?.message ?? "", /belongs to another app/u);
  });

  test("a provider and a start hook added after the serve", () => {
    const rule = new KeptDefects();
    const context = appInternals(createdApp());

    servedMembers(context);
    assert.throws(() => provideFor(context, () => 1, rule.raise), ClientDefect);
    assert.throws(() => {
      onChildStartFor(context, () => undefined, rule.raise);
    }, ClientDefect);
    assert.equal(rule.defects.length, 2);
  });
});

describe("spawned work", () => {
  test("work that a body spawned may spawn more work after the body settled; the core decides the end", async () => {
    const native = new FakeRunNative();
    const { runtime, table, runKey } = tableRun(native);
    let inner = "not started";
    let release = (): void => undefined;

    const outerDone = new Promise<void>((resolve) => {
      release = resolve;
    });

    native.registration = { kind: "registered", workKey: 31n };
    void runtime.spawn("outer", async () => {
      await outerDone;
      native.registration = { kind: "registered", workKey: 32n };

      const started = runtime.spawn("inner", async () => {
        inner = "ran";
      });

      inner = started.ok ? inner : started.error.kind;
    });
    table.leave(runKey);
    release();
    await settled();
    assert.equal(inner, "ran");
    assert.deepEqual(native.registered, ["outer", "inner"]);
  });

  test("a sync throw of forTest work is a rejected Promise, as in a child", async () => {
    const attempt = AttemptNumber.parse(1);
    const runtime = TaskRuntime.forTest({ taskId: taskIdFromCore(RUN_TASK_ID, "test"), attempt: attempt.ok ? attempt.value : assert.fail("attempt") });

    const started = runtime.spawn("work", () => {
      throw new Error("sync throw");
    });

    assert.equal(started.ok, true);
    await assert.rejects(started.ok ? started.value : Promise.resolve(), /sync throw/u);
  });
});
