// The docs check end to end on the fixture page (`fixture/page.md`): the package is built, and each compiler of the
// two lanes compiles the blocks of the page. The page shows a passing block, a must-fail block, a fragment and a
// failing block; the check reports the failing block only, at its page line, in each compiler.

import { describe, expect, test } from "bun:test";
import { fileURLToPath } from "node:url";

import { checkPages, extractPages } from "./check.ts";
import { readRoot } from "./pages.ts";

const REPOSITORY_DIR: string = fileURLToPath(new URL("../../../../", import.meta.url));

const FIXTURE_ROOT = "clients/typescript/tests/docs-check/fixture";

/** The bound of a test that builds the package and runs the three compilers. */
const COMPILE_TEST_TIMEOUT_MS = 120_000;

describe("checkPages", () => {
  test(
    "the fixture page: the failing block is reported with its page and line in each compiler; the others pass",
    () => {
      const root = readRoot(REPOSITORY_DIR, FIXTURE_ROOT);
      const pages = root.ok && root.value.state === "read" ? root.value.pages : [];
      const report = checkPages(pages);

      expect(pages.map((page) => page.path)).toEqual([`${FIXTURE_ROOT}/page.md`]);
      expect(report.ok && report.value.state === "compiled" && report.value.extracted.skipped).toEqual([
        { page: `${FIXTURE_ROOT}/page.md`, fenceLine: 34, skip: "partial", reason: "`app` is defined on the app config page" },
      ]);
      expect(
        report.ok &&
          report.value.state === "compiled" &&
          report.value.runs.map((run) => [run.compiler.name, run.state === "compiled" ? run.problems : run.failure]),
      ).toEqual([
        ["tsc 7", [{ kind: "Diagnostic", page: `${FIXTURE_ROOT}/page.md`, line: 44, code: "TS2322" }]],
        ["bun check", [{ kind: "Diagnostic", page: `${FIXTURE_ROOT}/page.md`, line: 44, code: "TS2322" }]],
        ["tsc 6.0", [{ kind: "Diagnostic", page: `${FIXTURE_ROOT}/page.md`, line: 44, code: "TS2322" }]],
      ]);
    },
    COMPILE_TEST_TIMEOUT_MS,
  );

  test("nothingToCompile: pages whose blocks are all skipped compile nothing", () => {
    const report = checkPages([{ path: "skipped.md", kind: "md", text: "<!-- ts-check: skip elided -- a signature -->\n```ts\nf();\n```\n" }]);

    expect(report.ok && report.value.state).toBe("nothingToCompile");
  });

  test("PagesRefused: the errors of every page, not only of the first", () => {
    const report = checkPages([
      { path: "a.md", kind: "md", text: "```ts\n" },
      { path: "b.md", kind: "md", text: "Text.\n\n```ts\nconst a = 1;\n```\n" },
      { path: "c.mdx", kind: "mdx", text: "{/* ts-check: skip */}\n```ts\n```\n" },
    ]);

    expect(report).toEqual({
      ok: false,
      error: {
        kind: "PagesRefused",
        errors: [
          { page: "a.md", error: { kind: "UnclosedFence", line: 1 } },
          { page: "c.mdx", error: { kind: "SkipWithoutKind", line: 1 } },
        ],
      },
    });
  });
});

describe("extractPages", () => {
  test("the counts of compiled blocks, and each skipped block with its page", () => {
    const extracted = extractPages([
      {
        path: "a.md",
        kind: "md",
        text:
          "```ts\nconst a = 1;\n```\n\n<!-- ts-check: alone -->\n```ts\nconst b = 2;\n```\n\n" +
          "<!-- ts-check: expect TS2322 at 1 -->\n```ts\nconst c: number = '';\n```\n\n" +
          "<!-- ts-check: skip defect -- the option was renamed -->\n```ts\nold();\n```\n",
      },
    ]);

    expect(extracted.ok && [extracted.value.checkedBlocks, extracted.value.mustFailBlocks, extracted.value.files.length]).toEqual([2, 1, 3]);
    expect(extracted.ok && extracted.value.skipped).toEqual([{ page: "a.md", fenceLine: 16, skip: "defect", reason: "the option was renamed" }]);
  });
});
