// One bounded run of a command: one case per outcome.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { commandFailedText, runCommand } from "./command.ts";

const SHORT_LIMITS = { time: Temporal.Duration.from({ milliseconds: 500 }), outputBytes: 1024 };

describe("runCommand", () => {
  test("a command that ends gives its exit code, stdout and stderr apart", () => {
    const run = runCommand("sh", ["-c", "printf out; printf err >&2; exit 3"], process.cwd());

    assert.deepEqual(run, { ok: true, value: { code: 3, stdout: "out", stderr: "err" } });
  });

  test("the environment of the call reaches the command", () => {
    const run = runCommand("sh", ["-c", "printf %s \"$CHECK_VALUE\""], process.cwd(), { CHECK_VALUE: "given" });

    assert.ok(run.ok && run.value.stdout === "given");
  });

  test("NotStarted: a command that does not exist", () => {
    const run = runCommand("horsies-no-such-command", [], process.cwd());

    assert.equal(run.ok ? "ran" : run.error.kind, "NotStarted");
  });

  test("TimedOut: a command that runs past the time bound", () => {
    const run = runCommand("sleep", ["5"], process.cwd(), {}, SHORT_LIMITS);

    assert.equal(run.ok ? "ran" : run.error.kind, "TimedOut");
  });

  test("OutputTooLarge: a command that writes past the output bound", () => {
    const run = runCommand("sh", ["-c", "head -c 4096 /dev/zero"], process.cwd(), {}, SHORT_LIMITS);

    assert.equal(run.ok ? "ran" : run.error.kind, "OutputTooLarge");
  });

  test("Signalled: a command that a signal ends", () => {
    const run = runCommand("sh", ["-c", "kill -9 $$"], process.cwd());

    assert.equal(run.ok ? "ran" : run.error.kind, "Signalled");
  });
});

describe("commandFailedText", () => {
  test("names the exit code and the output, or the failure", () => {
    assert.equal(commandFailedText("tar", { ok: true, value: { code: 2, stdout: "a", stderr: "b" } }), "tar: exit 2\nab");
    assert.equal(commandFailedText("tar", { ok: false, error: { kind: "NotStarted", tool: "tar", cause: "x" } }), "tar: NotStarted");
  });
});
