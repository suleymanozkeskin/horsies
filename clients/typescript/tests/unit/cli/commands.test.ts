// The `worker` and `check` commands with fakes: each flag and its default, each usage error, and the exit code of
// each outcome.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { refusal } from "../../../horsies/src/app/refusal.ts";
import {
  type CheckCommandDeps,
  liveSteps,
  type LiveStepsOutcome,
  parseCheckFlags,
  runCheckCommand,
} from "../../../horsies/src/cli/check-command.ts";
import type { CommandOutput } from "../../../horsies/src/cli/usage.ts";
import { parseWorkerStart, runWorkerCommand, type WorkerCommandDeps } from "../../../horsies/src/cli/worker-command.ts";
import { type CheckOutcome, checkReport } from "../../../horsies/src/check/report.ts";
import type { WorkerLogLevel } from "../../../horsies/src/_catalog/worker_manifest.ts";
import type { ListenAddressCheck } from "../../../horsies/src/native/worker/addon.ts";
import { CoreStartFailed } from "../../../horsies/src/thrown-errors.ts";
import { err, ok } from "../../../horsies/src/wire/result.ts";
import type { WorkerRunError, WorkerStart } from "../../../horsies/src/worker/run.ts";

/** The listen address rule of the core, for the fakes: `HOST:PORT` with a port from 1 to 65535. */
function checkAddress(text: string): ListenAddressCheck {
  const [host, port] = text.split(":");
  const number = Number(port);

  return host !== undefined && host !== "" && Number.isInteger(number) && number >= 1 && number <= 65_535
    ? { kind: "address", host, port: number }
    : { kind: "refused", cause: `'${text}' is not HOST:PORT` };
}

function recordingOutput(): CommandOutput & { readonly out: string[]; readonly errors: string[] } {
  const out: string[] = [];
  const errors: string[] = [];

  return { out, errors, writeOut: (line) => out.push(line), writeError: (line) => errors.push(line) };
}

const REPORT = checkReport("check", refusal("HRS-300", "tasks[0] ping", "no implementation", []), []);
const PASSED: CheckOutcome = { kind: "passed", passed: { taskCount: 1, declaredErrorCodes: new Map() } };

describe("the worker command", () => {
  test("no flag: no profile, INFO, no metrics endpoint", () => {
    assert.deepEqual(parseWorkerStart([], checkAddress), {
      ok: true,
      value: { profile: { kind: "NOT_GIVEN" }, logLevel: "INFO", metricsEndpoint: { kind: "NO_METRICS_ENDPOINT" } },
    });
  });

  test("each flag, in both forms", () => {
    assert.deepEqual(parseWorkerStart(["--profile", "io", "--loglevel=debug", "--metrics-listen", "0.0.0.0:9464"], checkAddress), {
      ok: true,
      value: {
        profile: { kind: "NAMED", name: "io" },
        logLevel: "DEBUG",
        metricsEndpoint: { kind: "LISTEN_ADDRESS", host: "0.0.0.0", port: 9464 },
      },
    });
  });

  test("a refused --metrics-listen value, a bad level, an unknown flag and a positional argument give 2", async () => {
    for (const args of [["--metrics-listen", "localhost"], ["--loglevel", "loud"], ["--queues", "a"], ["io"]]) {
      const output = recordingOutput();

      const deps: WorkerCommandDeps = {
        output,
        parseListenAddress: checkAddress,
        runWorker: async () => assert.fail("no run after a usage error"),
      };

      assert.equal(await runWorkerCommand(args, deps), 2, args.join(" "));
      assert.ok(output.errors[0]?.startsWith("usage: "));
      assert.ok(output.errors[1]?.startsWith("horsies worker: error: "));
    }
  });

  test("the refusal text of --metrics-listen is the core's", () => {
    const parsed = parseWorkerStart(["--metrics-listen", "localhost"], checkAddress);

    assert.deepEqual(parsed.ok ? undefined : parsed.error.message, "--metrics-listen: 'localhost' is not HOST:PORT");
  });

  test("a stop gives 0; each worker run error gives 1 with its text", async () => {
    const errors: readonly WorkerRunError[] = [
      { kind: "checkRefused", report: REPORT },
      { kind: "childCommandRefused", refused: { kind: "noEntryScript" } },
      { kind: "manifestBound", path: "claim.prefetchBuffer", min: 0n, max: 2_147_483_647n },
      { kind: "profileRefused", report: REPORT },
      { kind: "connectionSettingsRefused", cause: "no session URL" },
      { kind: "sharedThreadUnavailable", profile: "cpu" },
      { kind: "bootFailed", cause: "SCHEMA_REFUSED", message: "schema" },
      { kind: "failed", cause: "CLAIM_FAILED", message: "claim" },
    ];

    const starts: WorkerStart[] = [];

    const stopped: WorkerCommandDeps = {
      output: recordingOutput(),
      parseListenAddress: checkAddress,
      runWorker: async (start) => {
        starts.push(start);

        return ok({ kind: "stopped" });
      },
    };

    assert.equal(await runWorkerCommand(["--profile", "io"], stopped), 0);
    assert.equal(starts[0]?.logLevel, "INFO");

    for (const error of errors) {
      const output = recordingOutput();
      const deps: WorkerCommandDeps = { output, parseListenAddress: checkAddress, runWorker: async () => err(error) };

      assert.equal(await runWorkerCommand(["--profile", "io"], deps), 1, error.kind);
      assert.equal(output.errors.length, 1);
    }
  });
});

/** Fake deps of `check` with the outcomes of the static and the live check. */
function checkDeps(staticOutcome: CheckOutcome, liveOutcome: LiveStepsOutcome): CheckCommandDeps & {
  readonly output: ReturnType<typeof recordingOutput>;
  readonly levels: WorkerLogLevel[];
  readonly lives: number[];
} {
  const output = recordingOutput();
  const levels: WorkerLogLevel[] = [];
  const lives: number[] = [];

  return {
    output,
    levels,
    lives,
    setLogLevel: (level) => {
      levels.push(level);
    },
    staticCheck: () => staticOutcome,
    liveCheck: async () => {
      lives.push(1);

      return liveOutcome;
    },
  };
}

describe("the check command", () => {
  test("no flag: the static check only, at WARNING", () => {
    assert.deepEqual(parseCheckFlags([]), { ok: true, value: { steps: "staticOnly", logLevel: "WARNING" } });
    assert.deepEqual(parseCheckFlags(["--live", "--loglevel=info"]), { ok: true, value: { steps: "staticAndLive", logLevel: "INFO" } });
  });

  test("a passing check gives 0 with its summary on stdout, and no live step", async () => {
    const deps = checkDeps(PASSED, { kind: "refused", report: REPORT });

    assert.equal(await runCheckCommand([], deps), 0);
    assert.deepEqual(deps.levels, ["WARNING"]);
    assert.equal(deps.lives.length, 0);
    assert.ok(deps.output.out[0]?.startsWith("ok: all validations passed"));
  });

  test("a check report gives 1 with the report on stderr", async () => {
    const deps = checkDeps({ kind: "refused", report: REPORT }, PASSED);

    assert.equal(await runCheckCommand(["--live"], deps), 1);
    assert.equal(deps.lives.length, 0);
    assert.ok(deps.output.errors[0]?.startsWith("error[HRS-300]"));
  });

  test("--live runs the live steps after a passing static check: passed gives 0, refused gives 1", async () => {
    assert.equal(await runCheckCommand(["--live"], checkDeps(PASSED, PASSED)), 0);
    assert.equal(await runCheckCommand(["--live"], checkDeps(PASSED, { kind: "refused", report: REPORT })), 1);
  });

  test("an unknown flag gives 2", async () => {
    const deps = checkDeps(PASSED, PASSED);

    assert.equal(await runCheckCommand(["--profile", "io"], deps), 2);
    assert.deepEqual(deps.levels, []);
  });
  test("a core that refuses the live steps gives 1 with its name and message", async () => {
    const deps = checkDeps(PASSED, { kind: "coreRefused", error: new CoreStartFailed({ cause: "no threads" }) });

    assert.equal(await runCheckCommand(["--live"], deps), 1);
    assert.deepEqual(deps.output.errors, [
      "horsies check: the core refused the live check: CoreStartFailed: the core could not start: no threads",
    ]);
  });

  test("the live steps map a core refusal; a passing live check keeps the summary", async () => {
    const passed = { taskCount: 1, declaredErrorCodes: new Map() };
    const refusedByCore = new CoreStartFailed({ cause: "no threads" });

    assert.deepEqual(
      await liveSteps(passed, async () => {
        throw refusedByCore;
      }),
      { kind: "coreRefused", error: refusedByCore },
    );
    assert.deepEqual(await liveSteps(passed, async () => ({ kind: "passed" })), { kind: "passed", passed });
  });

  test("another throw of the live steps passes on", async () => {
    await assert.rejects(
      liveSteps({ taskCount: 0, declaredErrorCodes: new Map() }, async () => {
        throw new RangeError("not a core refusal");
      }),
      RangeError,
    );
  });
});
