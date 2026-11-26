import { describe, expect, test } from "bun:test";
import { tmpdir } from "node:os";

import { runTool, TOOL_LIMITS, toolFailedText } from "./tools.ts";

const BUN = process.execPath;

describe("runTool", () => {
  test("a tool that ends gives its exit code and its output", () => {
    const run = runTool(BUN, ["-e", "console.log('out'); console.error('err'); process.exit(3)"], tmpdir());

    expect(run).toEqual({ ok: true, value: { code: 3, output: "out\nerr\n" } });
  });

  test("NotStarted: a command that does not exist", () => {
    const run = runTool("horsies-no-such-command", [], tmpdir());

    expect(!run.ok && run.error.kind).toBe("NotStarted");
    expect(!run.ok && toolFailedText(run.error)).toContain("did not start");
  });

  test("TimedOut: a tool that runs past the time bound is stopped", () => {
    const limits = { ...TOOL_LIMITS, time: Temporal.Duration.from({ milliseconds: 200 }) };
    const run = runTool(BUN, ["-e", "setTimeout(() => {}, 10_000)"], tmpdir(), limits);

    expect(!run.ok && run.error.kind).toBe("TimedOut");
  });

  test("OutputTooLarge: a tool that writes more than the output bound is stopped", () => {
    const limits = { ...TOOL_LIMITS, outputBytes: 1024 };
    const run = runTool(BUN, ["-e", "process.stdout.write('x'.repeat(65536))"], tmpdir(), limits);

    expect(!run.ok && run.error.kind).toBe("OutputTooLarge");
  });

  test("Signalled: a tool that a signal ends", () => {
    const run = runTool(BUN, ["-e", "process.kill(process.pid, 'SIGKILL')"], tmpdir());

    expect(run).toMatchObject({ ok: false, error: { kind: "Signalled", signal: "SIGKILL" } });
  });
});
