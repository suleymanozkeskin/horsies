import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";

// The RuleTester of `oxlint/plugins-dev` refuses to parse on Bun. Each vendored
// rule test file runs in its own Node process; a failed case exits non-zero.
const ruleFolder = fileURLToPath(new URL("../../lint/anti-slop/rules/", import.meta.url));
const ruleTestSuffix = ".test.ts";
const nodeRunTimeoutMs = 60_000;
const ruleTestFiles = readdirSync(ruleFolder).filter((name) => name.endsWith(ruleTestSuffix));

describe("vendored anti-slop rules", () => {
  test("the rule folder holds rule test files", () => {
    expect(ruleTestFiles.length).toBeGreaterThan(0);
  });

  for (const fileName of ruleTestFiles) {
    test(`${fileName} passes on Node`, () => {
      const run = spawnSync("node", [fileName], {
        cwd: ruleFolder,
        encoding: "utf8",
        timeout: nodeRunTimeoutMs,
      });

      expect(run.error).toBeUndefined();
      expect(run.status, run.stderr).toBe(0);
    });
  }
});
