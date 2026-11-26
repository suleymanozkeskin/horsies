// The `web` command with fake I/O: each flag rule, the check step, the serve and the stop.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { EXIT_FAILED, EXIT_STOPPED, EXIT_USAGE } from "../../../../horsies/src/cli/command-parts.ts";
import type { CliLogLevel } from "../../../../horsies/src/cli/command-parts.ts";
import {
  isLoopbackHost,
  parseWebFlags,
  runWebCommand,
  trustedHeaderWarning,
  WEB_DEFAULT_HOST,
  WEB_DEFAULT_PORT,
} from "../../../../horsies/src/cli/web-command.ts";
import type { CheckStepOutcome, WebCommandDeps } from "../../../../horsies/src/cli/web-command.ts";
import type { MonitoringHandler, MonitoringHandlerOptions } from "../../../../horsies/src/web/handler.ts";
import type { ServeAddress, ServeOutcome } from "../../../../horsies/src/web/server.ts";
import { traceLinkOf } from "../../../../horsies/src/web/trace-link.ts";
import { FakeStopSignals, recordedOutput } from "./command-fakes.ts";

/** The record of one run of `web` with fake I/O. */
interface WebRun {
  readonly deps: WebCommandDeps;
  readonly errors: string[];
  readonly events: string[];
  readonly options: MonitoringHandlerOptions[];
  readonly served: ServeAddress[];
  readonly signals: FakeStopSignals;
}

/** Whether the fake serve binds its address. */
type FakeBind = "binds" | "addressInUse";

/** Fake dependencies: a check with `check`, a serve that `bind` says, a trace link check that refuses `bad`. */
function fakeWeb(check: CheckStepOutcome = { kind: "passed" }, bind: FakeBind = "binds"): WebRun {
  const { output, errors } = recordedOutput();
  const events: string[] = [];
  const options: MonitoringHandlerOptions[] = [];
  const served: ServeAddress[] = [];
  const signals = new FakeStopSignals();

  const recordClose = (): undefined => {
    events.push("server closed");

    return undefined;
  };

  const handlerOf = (handlerOptions: MonitoringHandlerOptions): MonitoringHandler => {
    options.push(handlerOptions);

    return Object.assign(async () => new Response("unused"), {
      close: async () => {
        events.push("handler closed");
      },
    });
  };

  const deps: WebCommandDeps = {
    output,
    setLogLevel: (level: CliLogLevel) => {
      events.push(`log level ${level}`);

      return undefined;
    },
    checkStatic: async () => {
      events.push("check");

      return check;
    },
    parseTraceLink: (text) =>
      traceLinkOf(text, text === "bad" ? { kind: "refused", rule: "the template has no host after its scheme" } : { kind: "accepted" }),
    createHandler: handlerOf,
    serve: async (_handler, address) => {
      served.push(address);

      return serveOutcome(address, bind, recordClose);
    },
    stopSignals: signals.listen,
  };

  return { deps, errors, events, options, served, signals };
}

/** The answer of the fake serve. */
function serveOutcome(address: ServeAddress, bind: FakeBind, recordClose: () => undefined): ServeOutcome {
  return bind === "binds"
    ? { kind: "serving", server: { address: { host: address.host, port: 8600 }, close: async () => recordClose() } }
    : { kind: "bindFailed", address, cause: "address already in use" };
}

/** Run `web` with `args`; send SIGTERM once it serves. */
async function runToStop(run: WebRun, args: string[]): Promise<number> {
  const running = runWebCommand(args, run.deps);

  await Promise.resolve();
  run.signals.send("SIGTERM");

  return running;
}

describe("the flags of web", () => {
  test("the defaults", () => {
    assert.deepEqual(parseWebFlags([]), {
      ok: true,
      value: {
        address: { host: WEB_DEFAULT_HOST, port: WEB_DEFAULT_PORT },
        auth: "none",
        trustedHeaderName: "X-Forwarded-User",
        actions: "disabled",
        traceLink: { kind: "noTraceLink" },
        logLevel: "INFO",
      },
    });
  });

  test("every flag", () => {
    const parsed = parseWebFlags([
      "--host",
      "0.0.0.0",
      "--port",
      "9000",
      "--auth",
      "trusted-header",
      "--trusted-header",
      "X-User",
      "--enable-actions",
      "--trace-link",
      "https://grafana.test/{trace_id}",
      "--loglevel",
      "debug",
    ]);

    assert.deepEqual(parsed, {
      ok: true,
      value: {
        address: { host: "0.0.0.0", port: 9000 },
        auth: "trustedHeader",
        trustedHeaderName: "X-User",
        actions: "enabled",
        traceLink: { kind: "template", text: "https://grafana.test/{trace_id}" },
        logLevel: "DEBUG",
      },
    });
  });

  test("port 0 asks for a free port", () => {
    const parsed = parseWebFlags(["--port", "0"]);

    assert.ok(parsed.ok && parsed.value.address.port === 0);
  });

  for (const [name, args, text] of [
    ["an unknown flag", ["--database-url", "postgresql://x"], "database-url"],
    ["a positional argument", ["myapp"], "myapp"],
    ["a flag with no value", ["--port"], "--port"],
    ["a port past 65535", ["--port", "65536"], "--port must be a whole number from 0 to 65535"],
    ["a port that is not a number", ["--port", "80a"], "--port must be"],
    ["an unknown auth mode", ["--auth", "basic"], "--auth must be none or trusted-header"],
    ["an unknown log level", ["--loglevel", "verbose"], "--loglevel must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL"],
  ] as const) {
    test(`${name} exits 2 and names the cause`, async () => {
      const run = fakeWeb();
      const code = await runWebCommand([...args], run.deps);

      assert.equal(code, EXIT_USAGE);
      assert.equal(run.errors.length, 1);
      assert.ok(run.errors[0]?.startsWith("horsies web: error: "), run.errors.join("\n"));
      assert.ok(run.errors[0]?.includes(text), run.errors.join("\n"));
      assert.deepEqual(run.events, []);
    });
  }
});

describe("the access rules", () => {
  test("--auth none on 0.0.0.0 exits 2 with the cause; nothing is checked or served", async () => {
    const run = fakeWeb();
    const code = await runWebCommand(["--auth", "none", "--host", "0.0.0.0"], run.deps);

    assert.equal(code, EXIT_USAGE);
    assert.match(run.errors[0] ?? "", /--auth none is only allowed on a loopback host; "0\.0\.0\.0" is reachable/u);
    assert.deepEqual(run.events, []);
    assert.deepEqual(run.served, []);
  });

  test("--auth none serves viewOnly, and allowAll with --enable-actions", async () => {
    const viewing = fakeWeb();

    assert.equal(await runToStop(viewing, []), EXIT_STOPPED);

    const request = new Request("http://127.0.0.1/");

    assert.equal(await viewing.options[0]?.auth.canAct(request), false);
    assert.equal(viewing.options[0]?.actions, "disabled");

    const acting = fakeWeb();

    assert.equal(await runToStop(acting, ["--enable-actions"]), EXIT_STOPPED);
    assert.equal(await acting.options[0]?.auth.canAct(request), true);
    assert.equal(acting.options[0]?.actions, "enabled");
  });

  test("--auth trusted-header prints the security warning and trusts the header", async () => {
    const run = fakeWeb();
    const code = await runToStop(run, ["--host", "0.0.0.0", "--auth", "trusted-header", "--trusted-header", "X-User"]);

    assert.equal(code, EXIT_STOPPED);
    assert.equal(run.errors[0], trustedHeaderWarning("X-User"));
    assert.match(run.errors[0] ?? "", /^SECURITY: the reverse proxy in front of this server MUST strip or overwrite the X-User header/u);

    const policy = run.options[0]?.auth;

    assert.equal(await policy?.canView(new Request("http://x/", { headers: { "X-User": "ops" } })), true);
    assert.equal(await policy?.canView(new Request("http://x/")), false);
  });

  test("a trusted header name that is not valid exits 2", async () => {
    const run = fakeWeb();
    const code = await runWebCommand(["--auth", "trusted-header", "--trusted-header", "X User"], run.deps);

    assert.equal(code, EXIT_USAGE);
    assert.match(run.errors[0] ?? "", /--trusted-header: the trusted header name "X User" is not valid/u);
  });

  test("a blank trusted header name exits 2", async () => {
    const run = fakeWeb();
    const code = await runWebCommand(["--auth", "trusted-header", "--trusted-header", " "], run.deps);

    assert.equal(code, EXIT_USAGE);
    assert.match(run.errors[0] ?? "", /--trusted-header: the trusted header needs a header name/u);
  });

  test("loopback hosts", () => {
    for (const host of ["localhost", "127.0.0.1", "127.9.9.9", "::1", "0:0:0:0:0:0:0:1", "0000::0001"]) {
      assert.equal(isLoopbackHost(host), true, host);
    }

    for (const host of ["0.0.0.0", "192.168.1.10", "128.0.0.1", "::", "[::1]", "::ffff:127.0.0.1", "::1%lo0", "example.test", ""]) {
      assert.equal(isLoopbackHost(host), false, host);
    }
  });
});

describe("the trace link", () => {
  test("a refused --trace-link exits 2 and names the rule", async () => {
    const run = fakeWeb();
    const code = await runWebCommand(["--trace-link", "bad"], run.deps);

    assert.equal(code, EXIT_USAGE);
    assert.equal(run.errors[0], "horsies web: error: invalid --trace-link: the template has no host after its scheme");
    assert.deepEqual(run.events, []);
  });

  test("an accepted --trace-link reaches the handler", async () => {
    const run = fakeWeb();

    assert.equal(await runToStop(run, ["--trace-link", "https://grafana.test/{trace_id}"]), EXIT_STOPPED);
    assert.equal(run.options[0]?.ui?.traceLink?.text, "https://grafana.test/{trace_id}");
  });
});

describe("the check, the serve and the stop", () => {
  test("a check refusal exits 1 with the report; nothing is served", async () => {
    const run = fakeWeb({ kind: "refused", report: "error[HRS-001]: the contract has no implementation" });
    const code = await runWebCommand([], run.deps);

    assert.equal(code, EXIT_FAILED);
    assert.deepEqual(run.errors, ["error[HRS-001]: the contract has no implementation"]);
    assert.deepEqual(run.served, []);
  });

  test("the log level is set before the check", async () => {
    const run = fakeWeb({ kind: "refused", report: "refused" });

    await runWebCommand(["--loglevel", "warning"], run.deps);

    assert.deepEqual(run.events, ["log level WARNING", "check"]);
  });

  test("it serves on the address, and a stop signal closes the handler, then the server", async () => {
    const run = fakeWeb();
    const code = await runToStop(run, ["--port", "9100"]);

    assert.equal(code, EXIT_STOPPED);
    assert.deepEqual(run.served, [{ host: "127.0.0.1", port: 9100 }]);
    assert.deepEqual(run.events, ["log level INFO", "check", "handler closed", "server closed"]);
    assert.ok(run.errors.includes("Serving horsies monitoring UI on http://127.0.0.1:8600"), run.errors.join("\n"));
    assert.equal(run.signals.removed, 1);
  });

  test("an address that does not bind exits 1 and closes the handler", async () => {
    const run = fakeWeb({ kind: "passed" }, "addressInUse");
    const code = await runWebCommand([], run.deps);

    assert.equal(code, EXIT_FAILED);
    assert.ok(
      run.errors.includes("could not bind the monitoring server to 127.0.0.1:8600: address already in use"),
      run.errors.join("\n"),
    );
    assert.deepEqual(run.events, ["log level INFO", "check", "handler closed"]);
    assert.equal(run.signals.removed, 1);
  });
});
