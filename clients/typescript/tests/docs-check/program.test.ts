import { describe, expect, test } from "bun:test";

import type { Block } from "./extract.ts";
import { blockProblemText, fileProblems, pageLineOf, type ProgramFile, programFiles } from "./program.ts";

const PAGE = "website/src/content/docs/typescript/page.md";

const MUST_FAIL: Block["mode"] = {
  kind: "mustFail",
  expectation: { current: [{ code: "TS2322", line: 1 }], floor: [{ code: "TS2345", line: 1 }] },
};

const SKIPPED: Block = { fenceLine: 14, mode: { kind: "skipped", skip: "partial", reason: "a fragment" }, code: ["app.send();"] };

const BLOCKS: readonly Block[] = [
  { fenceLine: 3, mode: { kind: "chained" }, code: ['import { h } from "horsies";', "const a = h.int();"] },
  { fenceLine: 9, mode: { kind: "alone" }, code: ["const b = 2;"] },
  SKIPPED,
  { fenceLine: 19, mode: MUST_FAIL, code: ["const c: number = a;"] },
  { fenceLine: 24, mode: { kind: "chained" }, code: ['import { h } from "horsies";', "const d = h.string();"] },
];

function fileNamed(name: string): ProgramFile {
  const file = programFiles(7, PAGE, BLOCKS).find((candidate) => candidate.name === name);

  if (file === undefined) {
    throw new Error(`no program file ${name}`);
  }

  return file;
}

describe("programFiles", () => {
  test("one file for the chain, one per alone block and one per must-fail block; a skipped block has none", () => {
    expect(programFiles(7, PAGE, BLOCKS).map((file) => [file.kind, file.name])).toEqual([
      ["clean", "0007-website-src-content-docs-typescript-page-chain.ts"],
      ["clean", "0007-website-src-content-docs-typescript-page-line-9.ts"],
      ["mustFail", "0007-website-src-content-docs-typescript-page-line-19.ts"],
    ]);
  });

  test("the chain file: each block after a comment that names the page and the offset, a repeated import dropped", () => {
    const chain = fileNamed("0007-website-src-content-docs-typescript-page-chain.ts");

    expect(chain.text.split("\n")).toEqual([
      `// ${PAGE}, the TypeScript block at line 3: line L of this file is line L + 2 of the page.`,
      'import { h } from "horsies";',
      "const a = h.int();",
      `// ${PAGE}, the TypeScript block at line 24: line L of this file is line L + 20 of the page.`,
      "",
      "const d = h.string();",
      "export {};",
      "",
    ]);
    expect(chain.segments).toEqual([
      { fenceLine: 3, firstFileLine: 2 },
      { fenceLine: 24, firstFileLine: 5 },
    ]);
  });

  test("a must-fail file holds the chained blocks above it, then the block", () => {
    const mustFail = fileNamed("0007-website-src-content-docs-typescript-page-line-19.ts");

    expect(mustFail.text.split("\n")).toEqual([
      `// ${PAGE}, the TypeScript block at line 3: line L of this file is line L + 2 of the page.`,
      'import { h } from "horsies";',
      "const a = h.int();",
      `// ${PAGE}, the TypeScript block at line 19: line L of this file is line L + 15 of the page.`,
      "const c: number = a;",
      "export {};",
      "",
    ]);
    expect(mustFail.segments.map((segment) => segment.fenceLine)).toEqual([3, 19]);
  });

  test("a page with no compiled block has no file", () => {
    expect(programFiles(0, PAGE, [SKIPPED])).toEqual([]);
  });
});

describe("pageLineOf", () => {
  const segments = [
    { fenceLine: 3, firstFileLine: 2 },
    { fenceLine: 24, firstFileLine: 5 },
  ];

  test("a code line maps to its page line; a comment line maps to its block's fence", () => {
    expect([pageLineOf(segments, 2), pageLineOf(segments, 3), pageLineOf(segments, 6)]).toEqual([4, 5, 26]);
    expect([pageLineOf(segments, 1), pageLineOf(segments, 4)]).toEqual([3, 24]);
  });

  test("a line after the code of the last block maps to its closing fence and below", () => {
    expect(pageLineOf(segments, 7)).toBe(27);
  });
});

describe("fileProblems", () => {
  test("a clean file: each diagnostic is a problem at its page line", () => {
    const chain = fileNamed("0007-website-src-content-docs-typescript-page-chain.ts");

    expect(fileProblems(chain, "current", [{ code: "TS2322", line: 6 }])).toEqual([
      { kind: "Diagnostic", page: PAGE, line: 26, code: "TS2322" },
    ]);
    expect(fileProblems(chain, "floor", [])).toEqual([]);
  });

  test("a must-fail file passes on exactly the expected diagnostics of the lane", () => {
    const mustFail = fileNamed("0007-website-src-content-docs-typescript-page-line-19.ts");

    expect(fileProblems(mustFail, "current", [{ code: "TS2322", line: 5 }])).toEqual([]);
    expect(fileProblems(mustFail, "floor", [{ code: "TS2345", line: 5 }])).toEqual([]);
  });

  test("ExpectationMissed: a must-fail file with other diagnostics, or with none", () => {
    const mustFail = fileNamed("0007-website-src-content-docs-typescript-page-line-19.ts");

    expect(fileProblems(mustFail, "current", [])).toEqual([
      { kind: "ExpectationMissed", page: PAGE, fenceLine: 19, expected: ["TS2322 at 20"], actual: [] },
    ]);
    expect(fileProblems(mustFail, "floor", [{ code: "TS2322", line: 5 }, { code: "TS2345", line: 5 }])).toEqual([
      { kind: "ExpectationMissed", page: PAGE, fenceLine: 19, expected: ["TS2345 at 20"], actual: ["TS2322 at 20", "TS2345 at 20"] },
    ]);
  });

  test("a must-fail file: a diagnostic in the chained blocks above the block is the chain file's problem", () => {
    const mustFail = fileNamed("0007-website-src-content-docs-typescript-page-line-19.ts");

    expect(fileProblems(mustFail, "current", [{ code: "TS2304", line: 3 }, { code: "TS2322", line: 5 }])).toEqual([]);
  });
});

describe("blockProblemText", () => {
  test("a problem names its page and line", () => {
    expect(blockProblemText({ kind: "Diagnostic", page: PAGE, line: 26, code: "TS2322" })).toBe(`${PAGE}:26: TS2322`);
    expect(blockProblemText({ kind: "ExpectationMissed", page: PAGE, fenceLine: 19, expected: ["TS2322 at 20"], actual: [] })).toBe(
      `${PAGE}:19: the must-fail block expects [TS2322 at 20] (page lines) and got []`,
    );
    expect(blockProblemText({ kind: "OutsideBlocks", file: "/work/package/dist/index.d.ts", line: 4, code: "TS2307" })).toBe(
      "TS2307 outside the blocks: /work/package/dist/index.d.ts:4",
    );
  });
});
