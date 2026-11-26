// The `metrics` command with fake I/O: the flags, the core's check of `--listen`, the run and the
// stop.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { EXIT_FAILED, EXIT_STOPPED, EXIT_USAGE } from "../../../../horsies/src/cli/command-parts.ts";
import { parseMetricsFlags, runMetricsCommand } from "../../../../horsies/src/cli/metrics-command.ts";
import type { FleetMetricsRun, MetricsCommandDeps } from "../../../../horsies/src/cli/metrics-command.ts";
import type { FleetMetricsRunEnd, MetricsListenAddressCheck } from "../../../../horsies/src/native/facades/fleet-metrics.ts";
import { FakeStopSignals, recordedOutput } from "../web-command/command-fakes.ts";

/** The record of one run of `metrics` with fake I/O. */
interface MetricsRun {
  readonly deps: MetricsCommandDeps;
  readonly errors: string[];
  readonly events: string[];
  readonly signals: FakeStopSignals;
  /** End the fake server run with `end`. */
  readonly end: (end: FleetMetricsRunEnd) => void;
}

/** The core's answer for the texts of these tests. */
function coreCheck(text: string): MetricsListenAddressCheck {
  return text === "127.0.0.1"
    ? { kind: "refused", message: "\"127.0.0.1\" is not HOST:PORT" }
    : { kind: "address", host: "127.0.0.1", port: 9100 };
}

function fakeMetrics(): MetricsRun {
  const { output, errors } = recordedOutput();
  const events: string[] = [];
  const signals = new FakeStopSignals();
  const ended = Promise.withResolvers<FleetMetricsRunEnd>();

  const run: FleetMetricsRun = {
    ended: ended.promise,
    requestStop: () => {
      events.push("stop requested");
      ended.resolve({ kind: "stopped" });

      return undefined;
    },
  };

  const deps: MetricsCommandDeps = {
    output,
    setLogLevel: (level) => {
      events.push(`log level ${level}`);

      return undefined;
    },
    checkListenAddress: coreCheck,
    startFleetMetrics: (listen) => {
      events.push(`started on ${listen}`);

      return run;
    },
    stopSignals: signals.listen,
  };

  return { deps, errors, events, signals, end: (end) => ended.resolve(end) };
}

describe("the flags of metrics", () => {
  test("--listen and the default log level", () => {
    assert.deepEqual(parseMetricsFlags(["--listen", "127.0.0.1:9100"]), {
      ok: true,
      value: { listen: "127.0.0.1:9100", logLevel: "INFO" },
    });
  });

  for (const [name, args, text] of [
    ["no --listen", [], "--listen HOST:PORT is required"],
    ["an unknown flag", ["--listen", "127.0.0.1:9100", "--port", "1"], "--port"],
    ["a positional argument", ["127.0.0.1:9100"], "127.0.0.1:9100"],
    ["an unknown log level", ["--listen", "127.0.0.1:9100", "--loglevel", "loud"], "--loglevel must be one of"],
    ["a --listen that the core refuses", ["--listen", "127.0.0.1"], "invalid --listen: \"127.0.0.1\" is not HOST:PORT"],
  ] as const) {
    test(`${name} exits 2 and names the cause; nothing starts`, async () => {
      const run = fakeMetrics();
      const code = await runMetricsCommand([...args], run.deps);

      assert.equal(code, EXIT_USAGE);
      assert.ok(run.errors[0]?.startsWith("horsies metrics: error: "), run.errors.join("\n"));
      assert.ok(run.errors[0]?.includes(text), run.errors.join("\n"));
      assert.deepEqual(run.events, []);
      assert.equal(run.signals.installed, 0);
    });
  }
});

describe("the run of metrics", () => {
  test("a stop signal asks the run to stop; the stopped run exits 0", async () => {
    const run = fakeMetrics();
    const running = runMetricsCommand(["--listen", "127.0.0.1:9100", "--loglevel", "error"], run.deps);

    run.signals.send("SIGINT");

    assert.equal(await running, EXIT_STOPPED);
    assert.deepEqual(run.events, ["log level ERROR", "started on 127.0.0.1:9100", "stop requested"]);
    assert.equal(run.signals.removed, 1);
  });

  test("an address that does not bind exits 1 with the cause", async () => {
    const run = fakeMetrics();
    const running = runMetricsCommand(["--listen", "127.0.0.1:9100"], run.deps);

    run.end({ kind: "failed", message: "could not bind the metrics endpoint to 127.0.0.1:9100: address in use" });

    assert.equal(await running, EXIT_FAILED);
    assert.deepEqual(run.errors, ["could not bind the metrics endpoint to 127.0.0.1:9100: address in use"]);
    assert.ok(!run.events.includes("stop requested"));
    assert.equal(run.signals.removed, 1);
  });
});
