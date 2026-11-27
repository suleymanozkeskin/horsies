// The CLI report: it lists each command of `cli.run` once, and it is the committed report.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { COMMAND_NAMES } from "../../horsies/src/cli/run.ts";
import { CLI_REPORT_FILE, cliReportText } from "./cli-report.ts";

describe("the CLI report", () => {
  test("it has one block per command, with the flags of the parser table", () => {
    const report = cliReportText();

    for (const name of COMMAND_NAMES) {
      assert.equal(report.split(`command ${name}\n`).length, 2, `one block for ${name}`);
    }

    assert.match(report, /command worker\n {2}usage: .*\n {2}--log-level \(string\)\n {2}--metrics-listen \(string\)\n {2}--profile \(string\)/u);
    assert.match(report, /--enable-actions \(boolean\)/u);
  });

  test("it is the committed report", () => {
    assert.equal(cliReportText(), readFileSync(CLI_REPORT_FILE, "utf8"));
  });
});
