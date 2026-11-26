// The worker run with fakes at the addon boundary: the order of the first steps, each run error, the stop handlers
// around the run, and the text of each error.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { WorkerLogLevel } from "../../../horsies/src/_catalog/worker_manifest.ts";
import type { AppContext } from "../../../horsies/src/app/context.ts";
import { checkReport } from "../../../horsies/src/check/report.ts";
import { refusal } from "../../../horsies/src/app/refusal.ts";
import type { NativePreparedWorker, WorkerPreparation, WorkerRunEnded } from "../../../horsies/src/native/worker/addon.ts";
import { CoreStartFailed, CoreStopped } from "../../../horsies/src/thrown-errors.ts";
import type { LaunchFacts } from "../../../horsies/src/worker/child-command.ts";
import {
  runWorkerOf,
  type StopHandlers,
  type WorkerRunDeps,
  type WorkerRunError,
  workerRunErrorText,
  type WorkerStart,
} from "../../../horsies/src/worker/run.ts";
import { catalogRefusal, fakeChecks } from "../app/support.ts";
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
  | { readonly kind: "stop" }
  | { readonly kind: "removed" };

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
    stopOnSignals: (requestStop): StopHandlers => {
      steps.push({ kind: "handlers" });
      stopHandler = requestStop;

      return {
        remove: () => {
          steps.push({ kind: "removed" });
        },
      };
    },
  };

  return { deps, steps };
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
      ["panicRule", "logLevel", "line", "prepare", "handlers", "run", "stop", "removed"],
    );
    assert.deepEqual(steps[1], { kind: "logLevel", level: "INFO" });
    assert.deepEqual(steps[2], { kind: "line", line: "horsies worker: the children start without the runtime flag --inspect" });
  });

  test("the start members name the profile, each task and the child command", async () => {
    const { deps, steps } = fakeDeps();

    await runWorkerOf(contextOf(implementedApp()), START, deps);

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
        program: { kind: "RUST" },
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
      await runError(context, { prepare: () => ({ kind: "manifestBound", path: "prefetchBuffer", min: 0n, max: 2_147_483_647n }) }),
      { kind: "manifestBound", path: "prefetchBuffer", min: 0n, max: 2_147_483_647n },
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
    const failed = new CoreStartFailed({ cause: "no threads" });

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

  test("a failed start and a failed worker are their errors; the stop handlers are removed", async () => {
    const context = contextOf(implementedApp());
    const { deps, steps } = fakeDeps({ run: async () => ({ kind: "bootFailed", cause: "SCHEMA_REFUSED", message: "schema" }) });

    assert.deepEqual(await runWorkerOf(context, START, deps), {
      ok: false,
      error: { kind: "bootFailed", cause: "SCHEMA_REFUSED", message: "schema" },
    });
    assert.equal(steps.at(-1)?.kind, "removed");
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
      workerRunErrorText({ kind: "manifestBound", path: "prefetchBuffer", min: 0n, max: 2_147_483_647n }),
      workerRunErrorText({ kind: "connectionSettingsRefused", cause: "no session URL" }),
      workerRunErrorText({ kind: "coreRefused", error: new CoreStartFailed({ cause: "no threads" }) }),
      workerRunErrorText({ kind: "bootFailed", cause: "SCHEMA_REFUSED", message: "schema" }),
      workerRunErrorText({ kind: "failed", cause: "CLAIM_FAILED", message: "claim" }),
    ];

    assert.equal(texts[3], "prefetchBuffer: outside 0 to 2147483647, the bound of its worker manifest field");
    assert.equal(texts[6], "worker start failed (SCHEMA_REFUSED): schema");
    assert.equal(texts[7], "worker failed (CLAIM_FAILED): claim");
    assert.ok(texts[0]?.startsWith("error[HRS-300]: tasks[0] ping: no implementation"));
  });
});
