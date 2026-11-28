// The worker run with fakes at the addon boundary: the order of the first steps, each run error, the stop handlers
// around the run, the text of each error, and the facts of the app in the start members (the event handlers and the
// worker-ended spans) and in the run (the core handle that holds the child spec source).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { appCoreHandle } from "../../../horsies/src/app/core-access.ts";

import type { WorkerLogLevel } from "../../../horsies/src/_catalog/worker_manifest.ts";
import type { AppContext } from "../../../horsies/src/app/context.ts";
import { checkReport } from "../../../horsies/src/check/report.ts";
import { refusal } from "../../../horsies/src/app/refusal.ts";
import type {
  NativePreparedWorker,
  SeatIsolateMemory,
  WorkerPreparation,
  WorkerRunEnded,
} from "../../../horsies/src/native/worker/addon.ts";
import { ClientDefect, RuntimeStartFailed, CoreStopped } from "../../../horsies/src/thrown-errors.ts";
import { createHorsies } from "../../../horsies/src/app.ts";
import type { CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { LaunchFacts } from "../../../horsies/src/worker/child-command.ts";
import {
  processWorkerRunDeps,
  runtimeNameOf,
  seatIsolateMemoryOf,
  runWorkerOf,
  type WorkerRunDeps,
  type WorkerRunError,
  workerRunErrorText,
  type WorkerStart,
} from "../../../horsies/src/worker/run.ts";
import { catalogRefusal, DATABASE_URL, fakeChecks, fakeCoreHandle, fakeEnvironment } from "../app/support.ts";
import { API } from "../telemetry/support.ts";
import { contextOf, implementedApp, testApp } from "./support.ts";

const START: WorkerStart = {
  profile: { kind: "NAMED", name: "io" },
  logLevel: "INFO",
  metricsEndpoint: { kind: "NO_METRICS_ENDPOINT" },
};

const LAUNCH: LaunchFacts = {
  execPath: "/usr/local/bin/node",
  execArgv: ["--inspect"],
  argv: ["/usr/local/bin/node", "/srv/app/main.js", "worker"],
  packaging: "script",
};

/** The calls of a fake worker run, in order. */
type Step =
  | { readonly kind: "panicRule" }
  | { readonly kind: "logLevel"; readonly level: WorkerLogLevel }
  | { readonly kind: "line"; readonly line: string }
  | { readonly kind: "prepare"; readonly startJson: string }
  | { readonly kind: "handlers" }
  | { readonly kind: "run" }
  | { readonly kind: "stop" };

type Script = {
  readonly launch?: LaunchFacts;
  readonly prepare?: () => WorkerPreparation;
  readonly run?: (stopHandler: () => void) => Promise<WorkerRunEnded>;
};

const WORKER: NativePreparedWorker = { requestStop: () => ({ kind: "value", value: undefined }) };

/** Fake deps that record each call in `steps`. */
function fakeDeps(script: Script = {}): { readonly deps: WorkerRunDeps; readonly steps: Step[] } {
  const steps: Step[] = [];

  let stopHandler = (): void => {
    throw new Error("no stop handler is installed");
  };

  const deps: WorkerRunDeps = {
    setWorkerPanicRule: () => {
      steps.push({ kind: "panicRule" });
    },
    setLogLevel: (level) => {
      steps.push({ kind: "logLevel", level });
    },
    coreChecks: fakeChecks(),
    launchFacts: () => script.launch ?? LAUNCH,
    writeLine: (line) => {
      steps.push({ kind: "line", line });
    },
    prepare: (_config, startJson) => {
      steps.push({ kind: "prepare", startJson });

      return (script.prepare ?? (() => ({ kind: "prepared", worker: WORKER })))();
    },
    run: async (worker) => {
      assert.equal(worker, WORKER);
      steps.push({ kind: "run" });

      return (script.run ?? (async () => ({ kind: "stopped" })))(stopHandler);
    },
    requestStop: (worker) => {
      assert.equal(worker, WORKER);
      steps.push({ kind: "stop" });
    },
    stopOnSignals: (requestStop) => {
      steps.push({ kind: "handlers" });
      stopHandler = requestStop;
    },
  };

  return { deps, steps };
}

/** The member `name` of the start members of the one preparation in `steps`. */
function startMember(steps: readonly Step[], name: string): unknown {
  const prepares = steps.flatMap((step) => (step.kind === "prepare" ? [step.startJson] : []));

  assert.equal(prepares.length, 1);

  const members: unknown = JSON.parse(prepares[0] ?? "null");

  return Reflect.get(Object(members), name);
}

async function runError(context: AppContext, script: Script): Promise<WorkerRunError> {
  const outcome = await runWorkerOf(context, START, fakeDeps(script).deps);

  if (outcome.ok) {
    assert.fail("the run stopped");
  }

  return outcome.error;
}

describe("the worker run", () => {
  test("the panic rule and the log level come first; the run stops at a stop request", async () => {
    const { deps, steps } = fakeDeps({
      run: async (stopHandler) => {
        stopHandler();

        return { kind: "stopped" };
      },
    });

    assert.deepEqual(await runWorkerOf(contextOf(implementedApp()), START, deps), { ok: true, value: { kind: "stopped" } });
    assert.deepEqual(
      steps.map((step) => step.kind),
      ["panicRule", "logLevel", "line", "prepare", "handlers", "run", "stop"],
    );
    assert.deepEqual(steps[1], { kind: "logLevel", level: "INFO" });
    assert.deepEqual(steps[2], { kind: "line", line: "horsies worker: the children start without the runtime flag --inspect" });
  });

  test("the start members name the profile, each task and the child command", async () => {
    const { deps, steps } = fakeDeps();

    void (await runWorkerOf(contextOf(implementedApp()), START, deps));

    const prepared = steps.find((step) => step.kind === "prepare");
    const members: unknown = JSON.parse(prepared?.kind === "prepare" ? prepared.startJson : "null");

    assert.deepEqual(members, {
      profile: { kind: "NAMED", name: "io" },
      metrics_endpoint: { kind: "NO_METRICS_ENDPOINT" },
      tasks: {
        ping: { execution: "ISOLATED", error_codes: [] },
        charge: { execution: "SHARED_ASYNC", error_codes: ["CARD_DECLINED", "BANK_DOWN"] },
      },
      handlers: { kind: "NO_HANDLERS" },
      worker_ended_spans: "NO_SPANS",
      child_launch: {
        command: { program: "/usr/local/bin/node", args: ["/srv/app/main.js", "--horsies-child"] },
        preload: { kind: "NO_PRELOAD_PROGRAM" },
        program: { kind: "HOST" },
        log_level: "INFO",
      },
    });
  });

  test("a check refusal ends the run before the preparation", async () => {
    const { deps, steps } = fakeDeps();
    const outcome = await runWorkerOf(contextOf(testApp()), START, deps);

    assert.equal(outcome.ok ? "stopped" : outcome.error.kind, "checkRefused");
    assert.equal(
      steps.some((step) => step.kind === "prepare"),
      false,
    );
  });

  test("a child command that cannot be built ends the run before the preparation", async () => {
    const error = await runError(contextOf(implementedApp()), { launch: { ...LAUNCH, argv: ["/usr/local/bin/node"] } });

    assert.deepEqual(error, { kind: "childCommandRefused", refused: { kind: "noEntryScript" } });
  });

  test("each refused preparation is its error", async () => {
    const context = contextOf(implementedApp());

    assert.deepEqual(
      await runError(context, { prepare: () => ({ kind: "manifestBound", path: "claim.prefetchBuffer", min: 0n, max: 2_147_483_647n }) }),
      { kind: "manifestBound", path: "claim.prefetchBuffer", min: 0n, max: 2_147_483_647n },
    );
    assert.deepEqual(await runError(context, { prepare: () => ({ kind: "connectionSettingsRefused", cause: "no session URL" }) }), {
      kind: "connectionSettingsRefused",
      cause: "no session URL",
    });

    const profile = await runError(context, {
      prepare: () => ({ kind: "profileRefused", refusals: [catalogRefusal("HRS-218", "no profile named 'io'")] }),
    });

    assert.equal(profile.kind, "profileRefused");
    assert.deepEqual(
      profile.kind === "profileRefused" ? profile.report.refusals.map((refused) => [refused.code, refused.message]) : [],
      [["HRS-218", "workerProfiles: no profile named 'io'"]],
    );
  });

  test("a core refusal of the preparation or of the run is coreRefused", async () => {
    const context = contextOf(implementedApp());
    const stopped = new CoreStopped({ subject: "forwarder", cause: "panic", stateAfter: "noChange" });
    const failed = new RuntimeStartFailed({ cause: "no threads" });

    assert.deepEqual(
      await runError(context, {
        prepare: () => {
          throw stopped;
        },
      }),
      { kind: "coreRefused", error: stopped },
    );
    assert.deepEqual(
      await runError(context, {
        run: async () => {
          throw failed;
        },
      }),
      { kind: "coreRefused", error: failed },
    );
  });

  test("a failed start and a failed worker are their errors; the stop handlers stay after the run", async () => {
    const context = contextOf(implementedApp());
    const { deps, steps } = fakeDeps({ run: async () => ({ kind: "bootFailed", cause: "SCHEMA_REFUSED", message: "schema" }) });

    assert.deepEqual(await runWorkerOf(context, START, deps), {
      ok: false,
      error: { kind: "bootFailed", cause: "SCHEMA_REFUSED", message: "schema" },
    });
    assert.deepEqual(
      steps.slice(-2).map((step) => step.kind),
      ["handlers", "run"],
    );
    assert.deepEqual(await runError(context, { run: async () => ({ kind: "failed", cause: "CLAIM_FAILED", message: "claim" }) }), {
      kind: "failed",
      cause: "CLAIM_FAILED",
      message: "claim",
    });
  });

  test("each run error has its text", () => {
    const report = checkReport("worker-startup", refusal("HRS-300", "tasks[0] ping", "no implementation", []), []);

    const texts = [
      workerRunErrorText({ kind: "checkRefused", report }),
      workerRunErrorText({ kind: "profileRefused", report }),
      workerRunErrorText({ kind: "childCommandRefused", refused: { kind: "stdinScript" } }),
      workerRunErrorText({ kind: "manifestBound", path: "claim.prefetchBuffer", min: 0n, max: 2_147_483_647n }),
      workerRunErrorText({ kind: "connectionSettingsRefused", cause: "no session URL" }),
      workerRunErrorText({ kind: "sharedThreadUnavailable", profile: "cpu" }),
      workerRunErrorText({ kind: "coreRefused", error: new RuntimeStartFailed({ cause: "no threads" }) }),
      workerRunErrorText({ kind: "bootFailed", cause: "SCHEMA_REFUSED", message: "schema" }),
      workerRunErrorText({ kind: "failed", cause: "CLAIM_FAILED", message: "claim" }),
    ];

    assert.equal(texts[3], "claim.prefetchBuffer: outside 0 to 2147483647, the bound of its worker manifest field");
    assert.ok(texts[5]?.startsWith('workerProfiles["cpu"].sharedThread: SHARED_THREAD children run on Node only'));
    assert.equal(texts[7], "worker start failed (SCHEMA_REFUSED): schema");
    assert.equal(texts[8], "worker failed (CLAIM_FAILED): claim");
    assert.ok(texts[0]?.startsWith("error[HRS-300]: tasks[0] ping: no implementation"));
  });
  test("a profile with SHARED_THREAD children is refused with its name where worker threads have no memory cap", async () => {
    assert.deepEqual(
      await runError(contextOf(implementedApp()), { prepare: () => ({ kind: "sharedThreadUnavailable", profile: "cpu" }) }),
      { kind: "sharedThreadUnavailable", profile: "cpu" },
    );
  });

  test("a profile refusal with no refusal is a defect of the package", async () => {
    await assert.rejects(runWorkerOf(contextOf(implementedApp()), START, fakeDeps({ prepare: () => ({ kind: "profileRefused", refusals: [] }) }).deps), ClientDefect);
  });

  test("the handlers of the start members are the handlers that app.onEvent registered", async () => {
    const { deps, steps } = fakeDeps();
    const app = implementedApp();

    app.onEvent("AttemptEnded", { name: "alerts" }, () => undefined);
    app.onEvent("TaskClaimed", { name: "claims" }, () => undefined);

    assert.deepEqual(await runWorkerOf(contextOf(app), START, deps), { ok: true, value: { kind: "stopped" } });
    assert.deepEqual(startMember(steps, "handlers"), {
      kind: "HANDLERS",
      handlers: [
        { name: "alerts", event_type: "AttemptEnded" },
        { name: "claims", event_type: "TaskClaimed" },
      ],
    });
  });

  test("the worker-ended spans of the start members follow the tracing of Horsies.create", async () => {
    const { deps, steps } = fakeDeps();

    const created = createHorsies(
      { database: { databaseUrl: DATABASE_URL }, tracing: { kind: "otelTracing" }, tasks: [] },
      fakeEnvironment({ otelApi: () => ({ kind: "loaded", api: API }) }),
    );

    if (!created.ok) {
      assert.fail("the app is created");
    }

    assert.deepEqual(await runWorkerOf(contextOf(created.value), START, deps), { ok: true, value: { kind: "stopped" } });
    assert.equal(startMember(steps, "worker_ended_spans"), "MAKE_SPANS");
  });
});

describe("the worker run of this process", () => {
  test("the run gets the core handle that Horsies.create built for the app", async () => {
    const handle = fakeCoreHandle();
    const created = createHorsies({ database: { databaseUrl: DATABASE_URL }, tasks: [] }, fakeEnvironment({ handle }));

    if (!created.ok) {
      assert.fail("the app is created");
    }

    const handles: CoreHandle[] = [];

    const deps = processWorkerRunDeps(contextOf(created.value), () => undefined, {
      prepareWorker: () => ({ kind: "prepared", worker: WORKER }),
      runWorker: async (_worker, given) => {
        handles.push(given);

        return { kind: "stopped" };
      },
      requestWorkerStop: () => undefined,
    });

    assert.deepEqual(await deps.run(WORKER), { kind: "stopped" });
    assert.equal(handles.length, 1);
    assert.equal(handles[0], handle);
    assert.equal(appCoreHandle(contextOf(created.value).core), handle);
  });

  test("the preparation gets the heap cap of the worker threads of the runtime of this process", () => {
    const created = createHorsies({ database: { databaseUrl: DATABASE_URL }, tasks: [] }, fakeEnvironment());

    if (!created.ok) {
      assert.fail("the app is created");
    }

    const given: SeatIsolateMemory[] = [];

    const deps = processWorkerRunDeps(contextOf(created.value), () => undefined, {
      prepareWorker: (_config, _start, seatIsolates) => {
        given.push(seatIsolates);

        return { kind: "prepared", worker: WORKER };
      },
      runWorker: async () => ({ kind: "stopped" }),
      requestWorkerStop: () => undefined,
    });

    assert.deepEqual(deps.prepare("{}", "{}"), { kind: "prepared", worker: WORKER });
    assert.deepEqual(given, [seatIsolateMemoryOf(runtimeNameOf(process.versions))]);
  });

  test("each runtime by its version facts: Node by its own version only; Bun and Deno by theirs; else unknown", () => {
    assert.equal(runtimeNameOf({ node: "26.13.1" }), "node");
    assert.equal(runtimeNameOf({ node: "26.3.0", bun: "1.4.4" }), "bun");
    assert.equal(runtimeNameOf({ node: "24.0.0", deno: "2.5.0" }), "deno");
    assert.equal(runtimeNameOf({}), "unknown");
  });

  test("the worker threads of Node have a heap cap; those of every other runtime do not", () => {
    assert.equal(seatIsolateMemoryOf("node"), "CAPPED");

    for (const runtime of ["bun", "deno", "unknown"] as const) {
      assert.equal(seatIsolateMemoryOf(runtime), "NOT_CAPPED");
    }
  });

  test("the handle of an app without the addon is a defect", () => {
    const created = createHorsies(
      { database: { databaseUrl: DATABASE_URL }, tasks: [] },
      fakeEnvironment({ mainThread: false }),
    );

    if (!created.ok) {
      assert.fail("the app is created");
    }

    assert.throws(() => appCoreHandle(contextOf(created.value).core), ClientDefect);
  });
});
