// The docs check of TypeScript code blocks: every `ts` block of the TypeScript docs pages and of the skill files
// compiles against the built package, with each compiler of the two lanes (tsc of TypeScript 7, bun check, tsc of
// TypeScript 6.0). The rules of a block and of its markers are in `extract.ts`. Run from `clients/typescript/`:
//
//   bun tests/docs-check/run.ts
//
// A docs root that has no folder is printed and passes: the docs pages are held on the docs branch until the release,
// so on the other branches the check reads the skill files only. Exit status: 0 when every block passes; 1 when a root
// or a page does not read or a block fails; 2 when a tool fails. Needs `bun`, and the floor compiler installed in
// `ci/typescript-floor/`.

import { fileURLToPath } from "node:url";

import { compileFailedText } from "../types/lanes.ts";
import { type CheckReport, checkPages, type CompilerRun, compileSetupFailedText, type ExtractedPages } from "./check.ts";
import { extractErrorText, SKIP_KINDS } from "./extract.ts";
import { type Page, readRoot, walkFailedText } from "./pages.ts";
import { blockProblemText } from "./program.ts";

/** The repository folder. */
const REPOSITORY_DIR: string = fileURLToPath(new URL("../../../../", import.meta.url));

/** The folders whose pages hold the checked blocks, from the repository folder. */
const DOC_ROOTS: readonly string[] = ["website/src/content/docs/typescript", "clients/typescript/horsies/.agents/skills/horsies"];

/** The exit status of a run. */
type Verdict = "passed" | "refused" | "toolFailed";

const EXIT_STATUS: Readonly<Record<Verdict, number>> = { passed: 0, refused: 1, toolFailed: 2 };

/** `count word`, with the plural of `word` for a count other than 1. */
function counted(count: number, word: string): string {
  return `${count} ${word}${count === 1 ? "" : "s"}`;
}

function worse(first: Verdict, second: Verdict): Verdict {
  return EXIT_STATUS[first] >= EXIT_STATUS[second] ? first : second;
}

/** The pages of every root, each root printed; `refused` when a root does not read. */
function rootPages(): readonly Page[] | "refused" {
  const pages: Page[] = [];

  for (const root of DOC_ROOTS) {
    const read = readRoot(REPOSITORY_DIR, root);

    if (!read.ok) {
      console.error(walkFailedText(read.error));

      return "refused";
    }

    switch (read.value.state) {
      case "absent":
        console.log(`${root}: no folder`);
        break;
      case "read":
        console.log(`${root}: ${counted(read.value.pages.length, "page")}`);
        pages.push(...read.value.pages);
        break;
    }
  }

  return pages;
}

function printBlocks(extracted: ExtractedPages): void {
  const skipCounts = SKIP_KINDS.map((kind) => `${extracted.skipped.filter((block) => block.skip === kind).length} ${kind}`);

  console.log(
    `${counted(extracted.checkedBlocks, "checked block")}, ${counted(extracted.mustFailBlocks, "must-fail block")}, ` +
      `${counted(extracted.skipped.length, "skipped block")} (${skipCounts.join(", ")})`,
  );

  for (const block of extracted.skipped.filter((skipped) => skipped.skip === "defect")) {
    console.log(`${block.page}:${block.fenceLine}: skipped as a docs defect: ${block.reason}`);
  }
}

/** Prints the problems of a compiler run; its verdict. */
function runVerdict(run: CompilerRun, fileCount: number): Verdict {
  switch (run.state) {
    case "notCompiled":
      console.error(compileFailedText(run.compiler.name, run.failure));

      return "toolFailed";
    case "compiled":
      for (const problem of run.problems) {
        console.error(`${run.compiler.name}: ${blockProblemText(problem)}`);
      }

      console.log(
        `${run.compiler.name} (${run.compiler.lane} lane): ${counted(fileCount, "file")}: ${run.problems.length === 0 ? "passed" : "failed"}`,
      );

      return run.problems.length === 0 ? "passed" : "refused";
  }
}

/** Prints the report; its verdict. */
function reportVerdict(report: CheckReport): Verdict {
  printBlocks(report.extracted);

  switch (report.state) {
    case "nothingToCompile":
      console.log("no block to compile");

      return "passed";
    case "compiled":
      return report.runs.map((run) => runVerdict(run, report.extracted.files.length)).reduce(worse, "passed");
  }
}

function main(): Verdict {
  const pages = rootPages();

  if (pages === "refused") {
    return "refused";
  }

  const report = checkPages(pages);

  if (report.ok) {
    return reportVerdict(report.value);
  }

  switch (report.error.kind) {
    case "PagesRefused":
      for (const { page, error } of report.error.errors) {
        console.error(extractErrorText(page, error));
      }

      return "refused";
    case "PackageNotBuilt":
    case "WorkFolderNotWritten":
      console.error(compileSetupFailedText(report.error));

      return "toolFailed";
  }
}

if (import.meta.main) {
  process.exitCode = EXIT_STATUS[main()];
}
