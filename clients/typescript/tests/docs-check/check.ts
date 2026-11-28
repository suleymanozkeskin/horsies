// The docs check of a set of pages: the blocks of each page, their program files, and one compile of all the files with
// each compiler of the two lanes (`../types/lanes.ts`). The files compile in the consumer project of the public API
// gate (`../types/consumer/tsconfig.json`), against the built package, as the code of a user's project.

import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import type { Result } from "../../horsies/src/wire/result.ts";
import { type BuildFailed, buildPackage, type BuiltPackage, manifestRefusedText } from "../public-api/package.ts";
import { TESTS_DIR, toolFailedText, WORKSPACE_DIR } from "../public-api/tools.ts";
import type { Diagnostic, Lane } from "../types/cases.ts";
import { compile, type CompileFailed, COMPILERS, type LaneCompiler } from "../types/lanes.ts";
import { type ExtractError, readBlocks, type SkipKind } from "./extract.ts";
import type { Page } from "./pages.ts";
import { type BlockProblem, fileProblems, type ProgramFile, programFiles } from "./program.ts";

/** The project whose compiler options the blocks compile with: the consumer project of the public API gate. */
const CONSUMER_PROJECT = `${TESTS_DIR}types/consumer/tsconfig.json`;

/**
 * The types that the blocks see besides the package's own: Node's and Bun's. A block is the code of a user's project on
 * Node 26 or Bun 1.4 (a block can call `Bun.serve`), and the package itself uses the Node API only
 * (`horsies/tsconfig.json`).
 */
const BLOCK_TYPES: readonly string[] = ["node", "bun"];

/**
 * The packages that a block can import besides `horsies`: the optional peers of the package (the versions of the
 * tests workspace), the OpenTelemetry SDK packages that a tracing setup block imports, and the types of `pg`, of Node
 * and of Bun. Each one is its folder name in `node_modules` and its folder.
 */
const BLOCK_PACKAGES: readonly (readonly [string, string])[] = [
  ["zod", `${TESTS_DIR}node_modules/zod`],
  ["pg", `${TESTS_DIR}node_modules/pg`],
  ["postgres", `${TESTS_DIR}node_modules/postgres`],
  ["@opentelemetry/api", `${TESTS_DIR}node_modules/@opentelemetry/api`],
  ["@opentelemetry/context-async-hooks", `${TESTS_DIR}node_modules/@opentelemetry/context-async-hooks`],
  ["@opentelemetry/core", `${TESTS_DIR}node_modules/@opentelemetry/core`],
  ["@opentelemetry/sdk-trace", `${TESTS_DIR}node_modules/@opentelemetry/sdk-trace`],
  ["@types/pg", `${TESTS_DIR}node_modules/@types/pg`],
  ["@types/node", `${WORKSPACE_DIR}node_modules/@types/node`],
  ["@types/bun", `${WORKSPACE_DIR}node_modules/@types/bun`],
];

/** A block that does not compile: its page, its fence line, its kind and its reason. */
export type SkippedBlock = { readonly page: string; readonly fenceLine: number; readonly skip: SkipKind; readonly reason: string };

/** The blocks of the pages: the program files, the counts of compiled blocks, and the skipped blocks. */
export type ExtractedPages = {
  readonly files: readonly ProgramFile[];
  readonly checkedBlocks: number;
  readonly mustFailBlocks: number;
  readonly skipped: readonly SkippedBlock[];
};

/** A page whose reading stopped. */
export type PageError = { readonly page: string; readonly error: ExtractError };

/** The blocks of `pages`. Every page is read before the errors are returned, so one run lists the errors of every page. Pure. */
export function extractPages(pages: readonly Page[]): Result<ExtractedPages, readonly PageError[]> {
  const errors: PageError[] = [];
  const files: ProgramFile[] = [];
  const skipped: SkippedBlock[] = [];
  let checkedBlocks = 0;
  let mustFailBlocks = 0;

  for (const [pageIndex, page] of pages.entries()) {
    const blocks = readBlocks(page.kind, page.text);

    if (!blocks.ok) {
      errors.push({ page: page.path, error: blocks.error });
      continue;
    }

    files.push(...programFiles(pageIndex, page.path, blocks.value));

    for (const block of blocks.value) {
      switch (block.mode.kind) {
        case "chained":
        case "alone":
          checkedBlocks += 1;
          break;
        case "mustFail":
          mustFailBlocks += 1;
          break;
        case "skipped":
          skipped.push({ page: page.path, fenceLine: block.fenceLine, skip: block.mode.skip, reason: block.mode.reason });
          break;
      }
    }
  }

  return errors.length === 0 ? { ok: true, value: { files, checkedBlocks, mustFailBlocks, skipped } } : { ok: false, error: errors };
}

/** The outcome of one compiler: the problems of the files, or the failure of the compile. */
export type CompilerRun =
  | { readonly state: "compiled"; readonly compiler: LaneCompiler; readonly problems: readonly BlockProblem[] }
  | { readonly state: "notCompiled"; readonly compiler: LaneCompiler; readonly failure: CompileFailed };

/** Why the files were not compiled. */
export type CompileSetupFailed =
  | { readonly kind: "PackageNotBuilt"; readonly failure: BuildFailed }
  | { readonly kind: "WorkFolderNotWritten"; readonly cause: string };

/** Writes the project of `files` into `workFolder`; returns the path of its `tsconfig.json`. */
function writeProject(workFolder: string, builtPackage: BuiltPackage, files: readonly ProgramFile[]): Result<string, CompileSetupFailed> {
  const blocksFolder = join(workFolder, "blocks");
  const project = join(workFolder, "tsconfig.json");

  const config = {
    extends: CONSUMER_PROJECT,
    compilerOptions: { types: BLOCK_TYPES },
    files: files.map((file) => join(blocksFolder, file.name)),
    include: [],
  };

  try {
    writeFileSync(join(workFolder, "package.json"), JSON.stringify({ private: true, type: "module" }));
    mkdirSync(join(workFolder, "node_modules"));
    symlinkSync(builtPackage.folder, join(workFolder, "node_modules", "horsies"), "dir");

    for (const [name, folder] of BLOCK_PACKAGES) {
      const link = join(workFolder, "node_modules", name);

      mkdirSync(dirname(link), { recursive: true });
      symlinkSync(folder, link, "dir");
    }

    mkdirSync(blocksFolder);

    for (const file of files) {
      writeFileSync(join(blocksFolder, file.name), file.text);
    }

    writeFileSync(project, JSON.stringify(config, null, 2));
  } catch (error) {
    return { ok: false, error: { kind: "WorkFolderNotWritten", cause: String(error) } };
  }

  return { ok: true, value: project };
}

/** The problems of one compile: those of each file, and each diagnostic in no file of the blocks. */
function runProblems(files: readonly ProgramFile[], blocksFolder: string, lane: Lane, diagnostics: readonly Diagnostic[]): readonly BlockProblem[] {
  const filePaths = new Set(files.map((file) => join(blocksFolder, file.name)));

  const outside = diagnostics
    .filter((diagnostic) => !filePaths.has(diagnostic.file))
    .map((diagnostic): BlockProblem => ({ kind: "OutsideBlocks", file: diagnostic.file, line: diagnostic.line, code: diagnostic.code }));

  const inFiles = files.flatMap((file) => {
    const path = join(blocksFolder, file.name);

    return fileProblems(file, lane, diagnostics.filter((diagnostic) => diagnostic.file === path));
  });

  return [...outside, ...inFiles];
}

/**
 * Builds the package into `workFolder` (an empty folder that the caller owns and removes), writes the project of
 * `files` there, and compiles it with each compiler of the two lanes.
 */
export function compileFiles(files: readonly ProgramFile[], workFolder: string): Result<readonly CompilerRun[], CompileSetupFailed> {
  const packageFolder = join(workFolder, "package");

  try {
    mkdirSync(packageFolder);
  } catch (error) {
    return { ok: false, error: { kind: "WorkFolderNotWritten", cause: String(error) } };
  }

  const builtPackage = buildPackage(packageFolder);

  if (!builtPackage.ok) {
    return { ok: false, error: { kind: "PackageNotBuilt", failure: builtPackage.error } };
  }

  const project = writeProject(workFolder, builtPackage.value, files);

  if (!project.ok) {
    return project;
  }

  const runs = COMPILERS.map((compiler): CompilerRun => {
    const compiled = compile(compiler.name, project.value, WORKSPACE_DIR);

    return compiled.ok
      ? { state: "compiled", compiler, problems: runProblems(files, join(workFolder, "blocks"), compiler.lane, compiled.value) }
      : { state: "notCompiled", compiler, failure: compiled.error };
  });

  return { ok: true, value: runs };
}

/** The report of a check: no program file (every block skipped, or no block), or the run of each compiler. */
export type CheckReport =
  | { readonly state: "nothingToCompile"; readonly extracted: ExtractedPages }
  | { readonly state: "compiled"; readonly extracted: ExtractedPages; readonly runs: readonly CompilerRun[] };

/** Why a check stopped before a compile. */
export type CheckStopped = { readonly kind: "PagesRefused"; readonly errors: readonly PageError[] } | CompileSetupFailed;

/**
 * The docs check of `pages`. The work files are in a new folder of the system's temporary folder, removed at the end.
 * Effects: the compilers' runs and the work folder only.
 */
export function checkPages(pages: readonly Page[]): Result<CheckReport, CheckStopped> {
  const extracted = extractPages(pages);

  if (!extracted.ok) {
    return { ok: false, error: { kind: "PagesRefused", errors: extracted.error } };
  }

  if (extracted.value.files.length === 0) {
    return { ok: true, value: { state: "nothingToCompile", extracted: extracted.value } };
  }

  let workFolder: string;

  try {
    workFolder = realpathSync(mkdtempSync(join(tmpdir(), "horsies-docs-check-")));
  } catch (error) {
    return { ok: false, error: { kind: "WorkFolderNotWritten", cause: String(error) } };
  }

  try {
    const runs = compileFiles(extracted.value.files, workFolder);

    return runs.ok ? { ok: true, value: { state: "compiled", extracted: extracted.value, runs: runs.value } } : runs;
  } finally {
    rmSync(workFolder, { recursive: true, force: true });
  }
}

/** The text of a failed build of the package, for a message. */
function buildFailedText(failure: BuildFailed): string {
  switch (failure.kind) {
    case "CompilerNotRun":
      return toolFailedText(failure.failure);
    case "CompilerErrors":
      return `the TypeScript 7 compiler refused the package source:\n${failure.output}`;
    case "ManifestRefused":
      return manifestRefusedText(failure.refusal);
  }
}

/** The text of a failed compile setup, for a message. */
export function compileSetupFailedText(failure: CompileSetupFailed): string {
  switch (failure.kind) {
    case "PackageNotBuilt":
      return `the package declarations were not built: ${buildFailedText(failure.failure)}`;
    case "WorkFolderNotWritten":
      return `the work folder was not written: ${failure.cause}`;
  }
}
