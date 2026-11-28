// Child mode: bad arguments, the panic rule, the log format, the schema mode of the task sends, the plan, the start hooks (the child's own, the providers before the
// `onChildStart` hooks, a throw), the culprit method of a shared async child, the turns, and the exit rule.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { onChildStartFor, provideFor } from "../../../horsies/src/child/app-members.ts";
import { CHILD_EXIT_FLUSH_BOUND_MS } from "../../../horsies/src/child/exit.ts";
import { bodyFactsMode, type ChildModeDeps, runChildMode } from "../../../horsies/src/child/host.ts";
import type { CulpritMethod } from "../../../horsies/src/child/loop-marks.ts";
import type { ProcessEventHooks } from "../../../horsies/src/child/process-events.ts";
import { CHILD_KIND_HOOK_NAME } from "../../../horsies/src/child/serve.ts";
import type { CallbackSettlement } from "../../../horsies/src/native/call/callback.ts";
import { contextOf, implementedApp } from "../worker/support.ts";
import { FakeChildNative, settled } from "./child-native.ts";
import { unusedSeatHost } from "../seats/support.ts";

const CHILD_ARGUMENTS = ["--horsies-child", "3", "4242"] as const;

/** The end of a fake process: a throw that carries the exit code. */
class Ended extends Error {
  readonly code: number;

  constructor(code: number) {
    super(`ended with ${String(code)}`);
    this.code = code;
  }
}

type Recorded = {
  readonly lines: string[];
  readonly settlements: string[];
  readonly flushes: string[];
  readonly culprits: number[];
  readonly ticks: (() => void)[];
};

function fakeDeps(native: FakeChildNative): ChildModeDeps & Recorded {
  const lines: string[] = [];
  const settlements: string[] = [];
  const flushes: string[] = [];
  const culprits: number[] = [];
  const ticks: (() => void)[] = [];
  const events: ProcessEventHooks = { onSignal: () => undefined, onUncaught: () => undefined, onRejection: () => undefined };

  const settlement: CallbackSettlement = {
    returned: (callId, value) => {
      settlements.push(`${String(callId)} returned ${String(value)}`);
    },
    threw: (callId, cause) => {
      settlements.push(`${String(callId)} threw ${cause instanceof Error ? cause.message : String(cause)}`);
    },
  };

  const culprit: CulpritMethod = {
    kind: "promiseHooks",
    createHook: () => {
      culprits.push(1);

      return () => undefined;
    },
  };

  return {
    lines,
    settlements,
    flushes,
    culprits,
    ticks,
    native,
    events,
    timer: (_milliseconds, tick) => {
      ticks.push(tick);

      return () => undefined;
    },
    culprit,
    settlement,
    hostname: "host-1",
    seats: unusedSeatHost(),
    writeLine: (line) => lines.push(line),
    wait: async (milliseconds) => {
      flushes.push(`wait ${String(milliseconds)}`);
    },
    exit: (code) => {
      throw new Ended(code);
    },
    reportDefect: (defect) => {
      throw defect;
    },
  };
}

async function endedCode(run: Promise<never>): Promise<number> {
  try {
    await run;
  } catch (cause) {
    if (cause instanceof Ended) {
      return cause.code;
    }

    throw cause;
  }

  return assert.fail("the process did not end");
}

describe("child mode", () => {
  test("bad arguments print the cause and end the process with their code; nothing serves", async () => {
    const native = new FakeChildNative();

    native.arguments = { kind: "badArguments", cause: "child mode needs the socket descriptor and the worker pid", code: 73 };

    const deps = fakeDeps(native);

    assert.equal(await endedCode(runChildMode(implementedApp(), ["--horsies-child"], deps)), 73);
    assert.deepEqual(deps.lines, ["horsies child: child mode needs the socket descriptor and the worker pid"]);
    assert.deepEqual(native.calls, ["childArguments"]);
  });

  test("the panic rule of a worker process is set before the serve, and the plan holds the app's parts", async () => {
    const native = new FakeChildNative();
    const app = implementedApp();

    provideFor(contextOf(app), function openPool() {
      return "pool";
    });
    onChildStartFor(contextOf(app), function warmCache() {
      return undefined;
    });

    const running = runChildMode(app, CHILD_ARGUMENTS, fakeDeps(native));

    await settled();
    assert.deepEqual(native.calls.slice(0, 4), ["childArguments", "setPanicRule ABORT_PROCESS", "applyLogFormat textLogs", "serveChild"]);
    assert.deepEqual(native.schemaModes, ["MIGRATE_AND_VALIDATE"]);
    assert.deepEqual(native.serves[0]?.plan, {
      tasks: [
        { name: "ping", form: "ASYNC", context: "NO_CONTEXT", errorCodes: [] },
        { name: "charge", form: "ASYNC", context: "NO_CONTEXT", errorCodes: ["CARD_DECLINED", "BANK_DOWN"] },
      ],
      startHooks: [CHILD_KIND_HOOK_NAME, "provide #1 openPool", "onChildStart #1 warmCache"],
      producerManifestJson: '{"manifest":"of the app"}',
      hostname: "host-1",
      bodyFacts: "NO_SCOPE",
      exitFlushBoundMs: CHILD_EXIT_FLUSH_BOUND_MS,
      processArguments: [...CHILD_ARGUMENTS],
    });
    native.endServe(0);
    assert.equal(await endedCode(running), 0);
  });

  test("the task sends take the app's schema mode, and the stderr lines take the app's log format", async () => {
    const native = new FakeChildNative();
    const running = runChildMode(implementedApp({ schemaMode: "OBSERVE_ONLY", logFormat: { kind: "jsonLogs" } }), CHILD_ARGUMENTS, fakeDeps(native));

    await settled();
    assert.deepEqual(native.calls.slice(0, 4), ["childArguments", "setPanicRule ABORT_PROCESS", "applyLogFormat jsonLogs", "serveChild"]);
    assert.deepEqual(native.schemaModes, ["OBSERVE_ONLY"]);
    native.endServe(0);
    assert.equal(await endedCode(running), 0);
  });

  test("the providers run before the onChildStart hooks; a hook that throws is the hook's failure", async () => {
    const native = new FakeChildNative();
    const app = implementedApp();
    const order: string[] = [];

    provideFor(contextOf(app), () => {
      order.push("provider");

      return "pool";
    });
    onChildStartFor(contextOf(app), () => {
      order.push("hook");

      throw new Error("the cache did not warm");
    });

    const deps = fakeDeps(native);
    const running = runChildMode(app, CHILD_ARGUMENTS, deps);

    await settled();

    const hook = native.serves[0]?.hook;

    // The core runs the hooks one after the other.
    for (const index of [0, 1, 2]) {
      hook?.(BigInt(index + 1), { index, facts: { kind: "isolated" } });
      await settled();
    }

    assert.deepEqual(order, ["provider", "hook"]);
    assert.deepEqual(deps.settlements, ["1 returned ended", "2 returned ended", "3 threw the cache did not warm"]);
    native.endServe(173);
    assert.equal(await endedCode(running), 173);
  });

  test("the culprit method starts in a shared async child only; the turns start at the serve", async () => {
    for (const [facts, starts] of [
      [{ kind: "sharedAsync", seats: 4 }, 1],
      [{ kind: "isolated" }, 0],
    ] as const) {
      const native = new FakeChildNative();
      const deps = fakeDeps(native);
      const running = runChildMode(implementedApp(), CHILD_ARGUMENTS, deps);

      await settled();
      native.serves[0]?.hook(1n, { index: 0, facts });
      await settled();
      assert.equal(deps.culprits.length, starts);
      assert.equal(native.calls.filter((call) => call === "loopTurned").length, 1);
      deps.ticks[0]?.();
      assert.equal(native.calls.filter((call) => call === "loopTurned").length, 2);
      native.endServe(0);
      await endedCode(running);
    }
  });

  test("the exit waits for the flush, at most the flush bound, then ends with the host's code", async () => {
    const native = new FakeChildNative();
    const deps = fakeDeps(native);
    const running = runChildMode(implementedApp(), CHILD_ARGUMENTS, deps);

    await settled();
    native.endServe(0);
    assert.equal(await endedCode(running), 0);
    assert.deepEqual(deps.flushes, [`wait ${String(CHILD_EXIT_FLUSH_BOUND_MS)}`]);
  });
});

describe("the body facts of a child", () => {
  test("OpenTelemetry tracing records the facts of each run; no tracing records none", () => {
    assert.equal(bodyFactsMode({ kind: "otelTracing", parentage: "linkedToSend", workerEndedSpans: "noSpans" }), "SPANS");
    assert.equal(bodyFactsMode({ kind: "noTracing" }), "NO_SCOPE");
  });
});
