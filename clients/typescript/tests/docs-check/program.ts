// The program files of the blocks of a page, and the problems of a compile of them. Pure.
//
// A page gives one file for its chained blocks, one file per `alone` block, and one file per must-fail block (the
// chained blocks above it, then the block). Each block in a file starts with a comment that names the page and the
// line offset of the block; the file ends with `export {};`, so the file is a module and its names stay in it.

import { type CaseExpectation, caseVerdict, type ExpectedDiagnostic, type Lane } from "../types/cases.ts";
import type { Block } from "./extract.ts";
import { mergeImports } from "./imports.ts";

/**
 * The file lines of one block: its comment line, then its code from `firstFileLine` on. Line `firstFileLine` of the
 * file is line `fenceLine + 1` of the page.
 */
export type Segment = { readonly fenceLine: number; readonly firstFileLine: number };

/** A program file: its name in the work folder, its page, its text and its segments. */
export type ProgramFile =
  | { readonly kind: "clean"; readonly name: string; readonly page: string; readonly text: string; readonly segments: readonly Segment[] }
  | {
      readonly kind: "mustFail";
      readonly name: string;
      readonly page: string;
      readonly text: string;
      readonly segments: readonly Segment[];
      readonly fenceLine: number;
      readonly expectation: CaseExpectation;
    };

/** A finding of one compiler run. `line` is a page line. */
export type BlockProblem =
  | { readonly kind: "Diagnostic"; readonly page: string; readonly line: number; readonly code: string }
  | {
      readonly kind: "ExpectationMissed";
      readonly page: string;
      readonly fenceLine: number;
      readonly expected: readonly string[];
      readonly actual: readonly string[];
    }
  | { readonly kind: "OutsideBlocks"; readonly file: string; readonly line: number; readonly code: string };

/** The text of a file of `blocks` (one block or more) of `page`, and its segments. */
function fileText(page: string, blocks: readonly Block[]): { readonly text: string; readonly segments: readonly Segment[] } {
  const lines: string[] = [];
  let bound: ReadonlySet<string> = new Set();

  const segments = blocks.map((block): Segment => {
    const firstFileLine = lines.length + 2;
    const merged = mergeImports(block.code, bound);

    lines.push(
      `// ${page}, the TypeScript block at line ${block.fenceLine}: ` +
        `line L of this file is line L + ${block.fenceLine + 1 - firstFileLine} of the page.`,
      ...merged.code,
    );
    bound = new Set([...bound, ...merged.bindings]);

    return { fenceLine: block.fenceLine, firstFileLine };
  });

  lines.push("export {};", "");

  return { text: lines.join("\n"), segments };
}

/** The file name stem of a page: its index (for a unique name) and its path in letters, digits and dashes. */
function stemOf(pageIndex: number, page: string): string {
  const path = page.toLowerCase().replace(/\.[a-z]+$/, "").replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");

  return `${String(pageIndex).padStart(4, "0")}-${path}`;
}

/** The program files of the blocks of `page` (the page at `pageIndex` of the run). A skipped block has none. Pure. */
export function programFiles(pageIndex: number, page: string, blocks: readonly Block[]): readonly ProgramFile[] {
  const stem = stemOf(pageIndex, page);
  const chain: Block[] = [];
  const files: ProgramFile[] = [];

  for (const block of blocks) {
    const name = `${stem}-line-${block.fenceLine}.ts`;

    switch (block.mode.kind) {
      case "chained":
        chain.push(block);
        break;
      case "alone":
        files.push({ kind: "clean", name, page, ...fileText(page, [block]) });
        break;
      case "mustFail":
        files.push({
          kind: "mustFail",
          name,
          page,
          ...fileText(page, [...chain, block]),
          fenceLine: block.fenceLine,
          expectation: block.mode.expectation,
        });
        break;
      case "skipped":
        break;
    }
  }

  return chain.length === 0 ? files : [{ kind: "clean", name: `${stem}-chain.ts`, page, ...fileText(page, chain) }, ...files];
}

/**
 * The page line of `fileLine` (from 1): the line of the block whose comment line or code holds it, or a line below the
 * block above it (a line after the code of a block maps to its closing fence and below). A block's comment line maps
 * to its fence. Pure.
 */
export function pageLineOf(segments: readonly Segment[], fileLine: number): number {
  const segment = segments.findLast((candidate) => candidate.firstFileLine - 1 <= fileLine);

  if (segment === undefined) {
    throw new Error(`the file line ${fileLine} is above the first block of its file`);
  }

  return segment.fenceLine + 1 + fileLine - segment.firstFileLine;
}

/**
 * The problem of a must-fail file: none when the diagnostics at the lines of the block are exactly the expected ones.
 * A diagnostic in the chained blocks above it is a problem of the chain file, which holds the same blocks.
 */
function mustFailProblems(
  file: Extract<ProgramFile, { kind: "mustFail" }>,
  lane: Lane,
  located: readonly ExpectedDiagnostic[],
): readonly BlockProblem[] {
  const expected = file.expectation[lane].map((item) => ({ code: item.code, line: file.fenceLine + item.line }));
  const ownLines = located.filter((diagnostic) => diagnostic.line >= file.fenceLine);
  const verdict = caseVerdict(expected, ownLines);

  switch (verdict.state) {
    case "passed":
      return [];
    case "failed":
      return [
        { kind: "ExpectationMissed", page: file.page, fenceLine: file.fenceLine, expected: verdict.expected, actual: verdict.actual },
      ];
  }
}

/** The problems of `file` in a compiler run of `lane`, from the diagnostics of the file (file lines). Pure. */
export function fileProblems(file: ProgramFile, lane: Lane, diagnostics: readonly ExpectedDiagnostic[]): readonly BlockProblem[] {
  const located = diagnostics.map((diagnostic) => ({ code: diagnostic.code, line: pageLineOf(file.segments, diagnostic.line) }));

  switch (file.kind) {
    case "clean":
      return located.map((diagnostic) => ({ kind: "Diagnostic", page: file.page, ...diagnostic }));
    case "mustFail":
      return mustFailProblems(file, lane, located);
  }
}

/** The text of a problem, for a message: `page:line: what`. */
export function blockProblemText(problem: BlockProblem): string {
  switch (problem.kind) {
    case "Diagnostic":
      return `${problem.page}:${problem.line}: ${problem.code}`;
    case "ExpectationMissed":
      return (
        `${problem.page}:${problem.fenceLine}: the must-fail block expects [${problem.expected.join(", ")}] ` +
        `(page lines) and got [${problem.actual.join(", ")}]`
      );
    case "OutsideBlocks":
      return `${problem.code} outside the blocks: ${problem.file}:${problem.line}`;
  }
}
