// The plugin in a user's TypeScript 7 project (tests/fixtures/): tsc of TypeScript 7 compiles the project, and
// ESLint with the flat config of README.md lints it through the TypeScript 6.0 API that the `typescript` alias of
// package.json gives (`npm:@typescript/typescript6`). Runs on Node (`node --test`).

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";

import parser from "@typescript-eslint/parser";
import { ESLint, Linter } from "eslint";
import { defineConfig } from "eslint/config";
import ts from "typescript";

import horsies from "../src/index.ts";

const packageFolder = fileURLToPath(new URL("../", import.meta.url));
const fixtureFolder = fileURLToPath(new URL("fixtures/", import.meta.url));

/** tsc of TypeScript 7: the `@typescript/native` alias of package.json. */
const typescript7Compiler = `${packageFolder}node_modules/.bin/tsc`;
const compilerTimeoutMs = 120_000;

/** The flat config of README.md, with the folder of the fixture project. */
const readmeConfig = defineConfig({
  files: ["**/*.ts"],
  languageOptions: { parser, parserOptions: { projectService: true, tsconfigRootDir: fixtureFolder } },
  plugins: { horsies },
  rules: { "horsies/no-dropped-result": "error" },
});

/** A report of the rule: the line and the message id. */
type Report = { readonly line: number; readonly messageId: string };

const expectMarker = /\/\/ expect: (\w+)$/;

/** The reports that the `expect` comments of `text` name, in line order. Pure. */
function expectedReports(text: string): readonly Report[] {
  return text.split("\n").flatMap((line, index) => {
    const marker = expectMarker.exec(line);

    return marker?.[1] === undefined ? [] : [{ line: index + 1, messageId: marker[1] }];
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

  it("gets a report on each line that drops a Result, from ESLint with the flat config of README.md", async () => {
    const eslint = new ESLint({ cwd: fixtureFolder, overrideConfigFile: true, overrideConfig: readmeConfig });
    const [result] = await eslint.lintFiles(["send.ts"]);

    assert.ok(result !== undefined);

    const reports = result.messages.map((message): Report => {
      assert.equal(message.ruleId, "horsies/no-dropped-result", message.message);
      assert.ok(message.messageId !== undefined, message.message);

      return { line: message.line, messageId: message.messageId };
    });

    const expected = expectedReports(readFileSync(`${fixtureFolder}send.ts`, "utf8"));

    assert.equal(expected.length, 3);
    assert.deepEqual(reports, expected);
  });
});

describe("a project without type information", () => {
  it("stops the lint with the error that names the rule and the parser setting", () => {
    const linter = new Linter({ configType: "flat" });

    const config = defineConfig({
      files: ["**/*.ts"],
      languageOptions: { parser },
      plugins: { horsies },
      rules: { "horsies/no-dropped-result": "error" },
    });

    assert.throws(
      () => linter.verify("export const count = 1;\n", config, "file.ts"),
      { message: /^Error while loading rule 'horsies\/no-dropped-result': You have used a rule which requires type information/ },
    );
  });
});
