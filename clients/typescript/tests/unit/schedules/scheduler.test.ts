// The scheduler run and the `scheduler` command with fakes: each outcome of the run and its exit code (a stop gives
// 0, each error gives 1, arguments that do not parse give 2), the order of the steps, the stop on a signal, and the
// sends of the process, which open when the scheduler starts to serve.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { createHorsies, type Horsies } from "../../../horsies/src/app.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import { checkReport } from "../../../horsies/src/check/report.ts";
import { staticCheck } from "../../../horsies/src/check/static-check.ts";
import { COMMAND_NAMES } from "../../../horsies/src/cli/run.ts";
import { refusal } from "../../../horsies/src/app/refusal.ts";
import { parseSchedulerStart, runSchedulerCommand, type SchedulerCommandDeps } from "../../../horsies/src/cli/scheduler-command.ts";
import type { CommandOutput } from "../../../horsies/src/cli/usage.ts";
import type { NativePreparedScheduler, SchedulerRunEnded } from "../../../horsies/src/native/scheduler/addon.ts";
import {
  runSchedulerOf,
  type SchedulerRunDeps,
  type SchedulerRunError,
  schedulerRunErrorText,
  type SchedulerStart,
  type SchedulerStopped,
} from "../../../horsies/src/scheduler/run.ts";
import { CoreStartFailed } from "../../../horsies/src/thrown-errors.ts";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";
import { DATABASE_URL, fakeChecks, fakeEnvironment } from "../app/support.ts";
import { declare, FAKE_SCHEDULER, fakeCore, HOURLY, ping, unlisted } from "./support.ts";

/** An app of `ping`, with no implementation, in a process started as `<entry> scheduler`: its sends start suppressed. */
function unimplementedSchedulerApp(): Horsies {
  const created = createHorsies(
    { broker: { databaseUrl: DATABASE_URL }, tasks: [ping] },
    fakeEnvironment({ start: { argumentAfterScript: "scheduler", seatIsolate: false } }),
  );

  assert.ok(created.ok);

  return created.value;
}

/** The app of `unimplementedSchedulerApp` with `ping` implemented. */
function schedulerApp(): Horsies {
  const app = unimplementedSchedulerApp();

  app.implement(ping, (_input, { ok }) => ok(null));

  return app;
}

/** A schedule of `ping` every hour. */
const PING_SCHEDULE = [{ name: "ping", task: ping, input: {}, pattern: HOURLY }] as const;

/** The fake run deps of a run that ends with `ended`, or throws `ended`; each step is recorded in order. */
function runDeps(ended: SchedulerRunEnded | Error): SchedulerRunDeps & { readonly steps: string[] } {
  const steps: string[] = [];

  return {
    steps,
    setLogLevel: (level) => steps.push(`level ${level}`),
    coreChecks: fakeChecks(),
    run: async (scheduler: NativePreparedScheduler) => {
      steps.push(scheduler === FAKE_SCHEDULER ? "run the prepared scheduler" : "run another scheduler");

      if (ended instanceof Error) {
        throw ended;
      }

      return ended;
    },
    requestStop: (scheduler) => steps.push(scheduler === FAKE_SCHEDULER ? "stop the prepared scheduler" : "stop another"),
    stopOnSignals: (requestStop) => {
      steps.push("stop handlers");
      requestStop();
    },
  };
}

describe("the scheduler run", () => {
  test("a stop: the level first, then the stop handlers, the sends open, then the run of the prepared scheduler", async () => {
    const context = declare(schedulerApp(), PING_SCHEDULE, { kind: "core", calls: fakeCore() });
    const deps = runDeps({ kind: "stopped" });

    assert.equal(context.suppression.current().kind, "suppressed");
    assert.deepEqual(await runSchedulerOf(context, { logLevel: "DEBUG" }, deps), ok({ kind: "stopped" }));
    assert.deepEqual(deps.steps, ["level DEBUG", "stop handlers", "stop the prepared scheduler", "run the prepared scheduler"]);
    assert.equal(context.suppression.current().kind, "open");
  });

  test("an entry with no implementation passes the start check: the scheduler sends by contract", async () => {
    const context = declare(unimplementedSchedulerApp(), PING_SCHEDULE, { kind: "core", calls: fakeCore() });
    const deps = runDeps({ kind: "stopped" });

    assert.equal(context.implementations.size, 0);
    assert.deepEqual(await runSchedulerOf(context, { logLevel: "INFO" }, deps), ok({ kind: "stopped" }));
    assert.equal(staticCheck(context, fakeChecks(), "withSchedules", "check").kind, "refused");
  });

  test("no app.schedules call: notConfigured, and nothing starts", async () => {
    const deps = runDeps({ kind: "stopped" });

    assert.deepEqual(await runSchedulerOf(appInternals(schedulerApp()), { logLevel: "INFO" }, deps), err({ kind: "notConfigured" }));
    assert.deepEqual(deps.steps, ["level INFO"]);
  });

  test("the master switch off: disabled, and nothing starts", async () => {
    const context = declare(schedulerApp(), PING_SCHEDULE, { kind: "core", calls: fakeCore() }, { enabled: false });
    const deps = runDeps({ kind: "stopped" });

    assert.deepEqual(await runSchedulerOf(context, { logLevel: "INFO" }, deps), err({ kind: "disabled" }));
    assert.deepEqual(deps.steps, ["level INFO"]);
    assert.equal(context.suppression.current().kind, "suppressed");
  });

  test("a schedule refusal: the scheduler-startup report, and nothing starts", async () => {
    const context = declare(schedulerApp(), [{ name: "ghost", task: unlisted, input: {}, pattern: HOURLY }], {
      kind: "core",
      calls: fakeCore(),
    });

    const deps = runDeps({ kind: "stopped" });
    const ended = await runSchedulerOf(context, { logLevel: "INFO" }, deps);

    assert.ok(!ended.ok && ended.error.kind === "checkRefused");
    assert.equal(ended.error.report.phase, "scheduler-startup");
    assert.deepEqual(
      ended.error.report.refusals.map((refused) => refused.path),
      ["schedules[0] ghost"],
    );
    assert.deepEqual(deps.steps, ["level INFO"]);
  });

  test("a start that the core refuses; a start that fails", async () => {
    for (const [ended, error] of [
      [
        { kind: "startRefused", schedule: "ping", reason: "schedule 'ping' has the timezone 'X'" },
        { kind: "startRefused", schedule: "ping", reason: "schedule 'ping' has the timezone 'X'" },
      ],
      [
        { kind: "startFailed", message: "connection refused", retryable: true },
        { kind: "startFailed", message: "connection refused", retryable: true },
      ],
    ] as const) {
      const context = declare(schedulerApp(), PING_SCHEDULE, { kind: "core", calls: fakeCore() });

      assert.deepEqual(await runSchedulerOf(context, { logLevel: "INFO" }, runDeps(ended)), err(error));
    }
  });

  test("a core that refuses the call is coreRefused; a defect passes on", async () => {
    const refused = new CoreStartFailed({ cause: "no threads" });
    const context = declare(schedulerApp(), PING_SCHEDULE, { kind: "core", calls: fakeCore() });

    assert.deepEqual(await runSchedulerOf(context, { logLevel: "INFO" }, runDeps(refused)), err({ kind: "coreRefused", error: refused }));

    const defect = new TypeError("a defect");
    const other = declare(schedulerApp(), PING_SCHEDULE, { kind: "core", calls: fakeCore() });

    await assert.rejects(runSchedulerOf(other, { logLevel: "INFO" }, runDeps(defect)), defect);
  });
});

function recordingOutput(): CommandOutput & { readonly out: string[]; readonly errors: string[] } {
  const out: string[] = [];
  const errors: string[] = [];

  return { out, errors, writeOut: (line) => out.push(line), writeError: (line) => errors.push(line) };
}

/** The deps of a command whose run ends with `ended`, and the starts that it was given. */
function commandDeps(ended: Result<SchedulerStopped, SchedulerRunError>): SchedulerCommandDeps & {
  readonly output: ReturnType<typeof recordingOutput>;
  readonly starts: SchedulerStart[];
} {
  const starts: SchedulerStart[] = [];

  return {
    output: recordingOutput(),
    starts,
    runScheduler: async (start) => {
      starts.push(start);

      return ended;
    },
  };
}

const ERRORS: readonly SchedulerRunError[] = [
  { kind: "notConfigured" },
  { kind: "disabled" },
  { kind: "checkRefused", report: checkReport("scheduler-startup", refusal("HRS-205", "schedules[0] a", "a rule", []), []) },
  { kind: "startRefused", schedule: "a", reason: "schedule 'a' cannot be sent" },
  { kind: "startFailed", message: "connection refused", retryable: true },
  { kind: "coreRefused", error: new CoreStartFailed({ cause: "no threads" }) },
];

describe("the scheduler command", () => {
  test("the doc of cli.run names each command of the command table, in its order", () => {
    const source = readFileSync(new URL("../../../horsies/src/cli/public.ts", import.meta.url), "utf8");
    const listed = /names for `app` \(([^)]+)\)/.exec(source.replaceAll(/\n\s*\*\s*/g, " "))?.[1];

    assert.deepEqual(
      listed?.split(", ").map((name) => name.replaceAll("`", "")),
      [...COMMAND_NAMES],
    );
  });

  test("no flag: INFO; --log-level in any case", () => {
    assert.deepEqual(parseSchedulerStart([]), ok({ logLevel: "INFO" }));
    assert.deepEqual(parseSchedulerStart(["--log-level=warning"]), ok({ logLevel: "WARNING" }));
  });

  test("a stop gives 0 and writes nothing", async () => {
    const deps = commandDeps(ok({ kind: "stopped" }));

    assert.equal(await runSchedulerCommand(["--log-level", "DEBUG"], deps), 0);
    assert.deepEqual(deps.starts, [{ logLevel: "DEBUG" }]);
    assert.deepEqual(deps.output.errors, []);
  });

  for (const error of ERRORS) {
    test(`${error.kind} gives 1 and writes its text to stderr`, async () => {
      const deps = commandDeps(err(error));

      assert.equal(await runSchedulerCommand([], deps), 1);
      assert.deepEqual(deps.output.errors, [schedulerRunErrorText(error)]);
    });
  }

  test("an unknown flag and a level that does not parse give 2 with the usage, and nothing runs", async () => {
    for (const args of [["--profile", "io"], ["--log-level", "LOUD"], ["extra"]]) {
      const deps = commandDeps(ok({ kind: "stopped" }));

      assert.equal(await runSchedulerCommand(args, deps), 2);
      assert.equal(deps.output.errors[0], "usage: <entry> scheduler [--log-level LEVEL]");
      assert.match(deps.output.errors[1] ?? "", /^horsies scheduler: error: /);
      assert.deepEqual(deps.starts, []);
    }
  });

  test("the text of each error", () => {
    assert.deepEqual(
      ERRORS.map((error) => schedulerRunErrorText(error)),
      [
        "no schedules: the app makes no app.schedules call before cli.run",
        "the scheduler is disabled: the app.schedules section has enabled: false",
        "error[HRS-205]: schedules[0] a: a rule\nerror: aborting due to 1 previous errors",
        "schedule 'a' cannot be sent",
        "the scheduler start failed: connection refused",
        "the core refused the scheduler: CoreStartFailed: the core could not start: no threads",
      ],
    );
  });
});
