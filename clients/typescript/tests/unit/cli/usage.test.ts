// The shared parts of the commands: the `--log-level` values and the flag parse.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { LOG_LEVELS, parseFlags, parseLogLevel, switchFlag, textFlag, usageErrorLines } from "../../../horsies/src/cli/usage.ts";

const FLAGS = { profile: { type: "string" }, live: { type: "boolean" } } as const;

describe("the --log-level values", () => {
  test("each level of the core, in any case", () => {
    assert.deepEqual(LOG_LEVELS, ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]);

    for (const level of LOG_LEVELS) {
      assert.deepEqual(parseLogLevel(level.toLowerCase()), { ok: true, value: level });
    }
  });

  test("another text is a usage error that names the levels", () => {
    const parsed = parseLogLevel("verbose");

    assert.equal(parsed.ok, false);
    assert.ok(!parsed.ok && parsed.error.message.includes("DEBUG, INFO, WARNING, ERROR, CRITICAL"));
  });
});

describe("the flag parse", () => {
  test("a text flag takes its value as the next argument or after =", () => {
    for (const args of [["--profile", "io"], ["--profile=io"]]) {
      const parsed = parseFlags(args, FLAGS);

      assert.equal(parsed.ok && textFlag(parsed.value, "profile"), "io", args.join(" "));
    }
  });

  test("a switch is given or not", () => {
    const given = parseFlags(["--live"], FLAGS);
    const absent = parseFlags([], FLAGS);

    assert.equal(given.ok && switchFlag(given.value, "live"), true);
    assert.equal(absent.ok && switchFlag(absent.value, "live"), false);
    assert.equal(absent.ok && textFlag(absent.value, "profile"), undefined);
  });

  test("an unknown flag, a missing value, a value of a switch and a positional argument are usage errors", () => {
    for (const args of [["--unknown"], ["--profile"], ["--live=yes"], ["extra"]]) {
      assert.equal(parseFlags(args, FLAGS).ok, false, args.join(" "));
    }
  });

  test("a usage error is the usage line and the error line", () => {
    assert.deepEqual(usageErrorLines("worker", "<entry> worker", { kind: "usageError", message: "bad" }), [
      "usage: <entry> worker",
      "horsies worker: error: bad",
    ]);
  });

  test("a valid parse gives the values of the flags", () => {
    const parsed = parseFlags(["--profile", "a", "--live"], FLAGS);

    assert.deepEqual(parsed.ok && { ...parsed.value }, { profile: "a", live: true });
  });

  test("each refusal is a usage error with the message of the rule", () => {
    const refusals: readonly (readonly [readonly string[], string])[] = [
      [["--unknown"], "Unknown option '--unknown'"],
      [["--profile"], "Option '--profile <value>' argument missing"],
      [["--live=yes"], "Option '--live' does not take an argument"],
      [["extra"], "Unexpected argument 'extra'. This command does not take positional arguments"],
    ];

    for (const [args, message] of refusals) {
      assert.deepEqual(parseFlags(args, FLAGS), { ok: false, error: { kind: "usageError", message } }, args.join(" "));
    }
  });
});
