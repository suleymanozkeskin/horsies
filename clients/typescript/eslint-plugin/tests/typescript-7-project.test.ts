// The plugin in a user's TypeScript 7 project (tests/fixtures/): tsc of TypeScript 7 compiles the project, and
// ESLint with the flat config of README.md (`lint-config.ts`) lints it through the TypeScript 6.0 API that the `typescript` alias of
// package.json gives (`npm:@typescript/typescript6`). Runs on Node (`node --test`).

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";

import { ESLint, Linter } from "eslint";
import { defineConfig } from "eslint/config";
import ts from "typescript";
import tseslint from "typescript-eslint";

import { lintConfig } from "../lint-config.ts";
import horsies from "../src/index.ts";

const packageFolder = fileURLToPath(new URL("../", import.meta.url));
const fixtureFolder = fileURLToPath(new URL("fixtures/", import.meta.url));

/** tsc of TypeScript 7: the `@typescript/native` alias of package.json. */
const typescript7Compiler = `${packageFolder}node_modules/.bin/tsc`;
const compilerTimeoutMs = 120_000;

/** The fixture files that the ESLint case lints, each with its `expect` comments. */
const LINTED_FIXTURES: readonly string[] = ["send.ts", "switch.ts"];

/** A report of a rule: the file, the line, the rule id and the message id. */
type Report = { readonly file: string; readonly line: number; readonly ruleId: string; readonly messageId: string };

const expectMarker = /\/\/ expect: (\S+) (\w+)$/;

/** The reports that the `expect` comments of the fixture `file` name, in line order. Effects: reads the file. */
function expectedReports(file: string): readonly Report[] {
  return readFileSync(`${fixtureFolder}${file}`, "utf8")
    .split("\n")
    .flatMap((line, index) => {
      const marker = expectMarker.exec(line);

      return marker?.[1] === undefined || marker[2] === undefined
        ? []
        : [{ file, line: index + 1, ruleId: marker[1], messageId: marker[2] }];
    });
}

describe("a TypeScript 7 project", () => {
  it("compiles with tsc of TypeScript 7", () => {
    const run = spawnSync(typescript7Compiler, ["--project", `${fixtureFolder}tsconfig.json`, "--pretty", "false"], {
      encoding: "utf8",
      timeout: compilerTimeoutMs,
    });

    assert.equal(run.error, undefined);
    assert.match(spawnSync(typescript7Compiler, ["--version"], { encoding: "utf8" }).stdout, /^Version 7\./);
    assert.equal(run.status, 0, run.stdout);
  });

  it("gives the rule the TypeScript 6.0 API of the alias", () => {
    assert.match(ts.version, /^6\.0\./);
  });

  it("gets a report on each line that drops a Result or misses a case, from ESLint with the flat config of README.md", async () => {
    const config = lintConfig({ kind: "projectService", rootDir: fixtureFolder });
    const eslint = new ESLint({ cwd: fixtureFolder, overrideConfigFile: true, overrideConfig: config });
    const results = await eslint.lintFiles([...LINTED_FIXTURES]);

    const reports = results.flatMap((result) =>
      result.messages.map((message): Report => {
        assert.ok(message.ruleId !== null && message.messageId !== undefined, message.message);

        return { file: result.filePath.slice(fixtureFolder.length), line: message.line, ruleId: message.ruleId, messageId: message.messageId };
      }),
    );

    const expected = LINTED_FIXTURES.flatMap(expectedReports);

    assert.equal(expected.length, 4);
    assert.deepEqual(reports, expected);
  });
});

describe("a project without type information", () => {
  it("stops the lint with the error that names the rule and the parser setting", () => {
    const linter = new Linter({ configType: "flat" });

    const config = defineConfig({
      files: ["**/*.ts"],
      languageOptions: { parser: tseslint.parser },
      plugins: { horsies },
      rules: { "horsies/no-dropped-result": "error" },
    });

    assert.throws(
      () => linter.verify("export const count = 1;\n", config, "file.ts"),
      { message: /^Error while loading rule 'horsies\/no-dropped-result': You have used a rule which requires type information/ },
    );
  });
});
