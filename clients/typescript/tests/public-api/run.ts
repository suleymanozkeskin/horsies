// The public API gate of the published entry points (`horsies`, `horsies/zod`). Run from `clients/typescript/`:
//
//   bun tests/public-api/run.ts check   compares the API report of each entry point with the committed report,
//                                       refuses each type that a public declaration names and the entry point does
//                                       not export, and runs the no-`any` check with the `unknown` allow list
//   bun tests/public-api/run.ts write   writes the API report files of `report/`
//
// Exit status: 0 when the check passes or the files are written; 1 when a report differs, a type is not exported or
// a top type is refused; 2 when a tool fails or the command is unknown. The work files are in a new folder of the
// system's temporary folder, removed at the end.

import { mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { allowListOf, allowListRefusedText, judgeTopTypes, topTypesPass, type TopTypeVerdict } from "./allow-list.ts";
import { type BuiltPackage, buildPackage, manifestRefusedText } from "./package.ts";
import { apiReport, compareReport, type EntryReport, REPORT_DIR, reportFailedText, reportFileName } from "./report.ts";
import { anyPlaces, declarationProgram, type SurfaceWalkFailed, type TopTypePlace, unknownPlaces } from "./surface.ts";
import { toolFailedText } from "./tools.ts";

/** The command that writes the report files again. */
export const REGENERATE_COMMAND = "(cd clients/typescript && bun tests/public-api/run.ts write)";

const ALLOW_LIST_FILE: string = fileURLToPath(new URL("unknown-allowed.json", import.meta.url));

type Command = "check" | "write";

/** The exit status of a run. */
type Verdict = "passed" | "refused" | "toolFailed";

const EXIT_STATUS: Readonly<Record<Verdict, number>> = { passed: 0, refused: 1, toolFailed: 2 };

/** The built package, or the verdict of a build that failed (its message is printed). */
function built(workFolder: string): BuiltPackage | "toolFailed" {
  const packageFolder = join(workFolder, "package");

  mkdirSync(packageFolder);

  const result = buildPackage(packageFolder);

  if (result.ok) {
    return result.value;
  }

  switch (result.error.kind) {
    case "CompilerNotRun":
      console.error(toolFailedText(result.error.failure));
      break;
    case "CompilerErrors":
      console.error(`The TypeScript 7 compiler refused the package:\n${result.error.output}`);
      break;
    case "ManifestRefused":
      console.error(manifestRefusedText(result.error.refusal));
      break;
  }

  return "toolFailed";
}

/** The report of each entry point, by file name, or `toolFailed` (its message is printed). */
function reports(builtPackage: BuiltPackage, workFolder: string): ReadonlyMap<string, EntryReport> | "toolFailed" {
  const texts = new Map<string, EntryReport>();
  const reportsFolder = join(workFolder, "reports");

  mkdirSync(reportsFolder);

  for (const entryPoint of builtPackage.entryPoints) {
    const report = apiReport(builtPackage, entryPoint, reportsFolder);

    if (!report.ok) {
      console.error(reportFailedText(entryPoint, report.error));

      return "toolFailed";
    }

    texts.set(reportFileName(entryPoint), report.value);
  }

  return texts;
}

function committedReportFiles(): readonly string[] {
  return readdirSync(REPORT_DIR).filter((name) => name.endsWith(".api.md"));
}

/** Writes each report file and removes each committed report file that no entry point has. */
function writeReports(texts: ReadonlyMap<string, EntryReport>): Verdict {
  for (const [fileName, report] of texts) {
    writeFileSync(join(REPORT_DIR, fileName), report.text);
    console.log(`wrote tests/public-api/report/${fileName}`);
  }

  for (const stale of committedReportFiles().filter((name) => !texts.has(name))) {
    rmSync(join(REPORT_DIR, stale));
    console.log(`removed tests/public-api/report/${stale}: no entry point has this report`);
  }

  return "passed";
}

/** The bound of the changed lines that a check prints per report and side. */
const SHOWN_LINES_LIMIT = 60;

/** The changed lines of one side, each with its mark, within the bound. */
function shownLines(mark: "-" | "+", lines: readonly string[]): readonly string[] {
  const shown = lines.slice(0, SHOWN_LINES_LIMIT).map((line) => `  ${mark} ${line}`);
  const hidden = lines.length - shown.length;

  return hidden > 0 ? [...shown, `  ${mark} (${hidden} more lines)`] : shown;
}

/** Compares each report with its committed file; prints each difference and the command that writes them. */
function compareReports(texts: ReadonlyMap<string, EntryReport>): Verdict {
  const committed = new Set(committedReportFiles());
  const problems: string[] = [];

  for (const [fileName, report] of texts) {
    const committedText = committed.has(fileName) ? readFileSync(join(REPORT_DIR, fileName), "utf8") : undefined;
    const comparison = compareReport(committedText, report.text);

    switch (comparison.state) {
      case "same":
        console.log(`report/${fileName}: same as the committed report`);
        break;
      case "notCommitted":
        problems.push(`report/${fileName}: no committed report`);
        break;
      case "changed":
        problems.push(
          [
            `report/${fileName}: differs from the committed report`,
            ...shownLines("-", comparison.removed),
            ...shownLines("+", comparison.added),
          ].join("\n"),
        );
        break;
    }
  }

  for (const stale of [...committed].filter((name) => !texts.has(name))) {
    problems.push(`report/${stale}: no entry point has this report`);
  }

  if (problems.length === 0) {
    return "passed";
  }

  console.error(`The public API differs from the committed API report:\n${problems.join("\n")}`);
  console.error(`A change of the public API needs the owner's word. To write the reports of this tree: ${REGENERATE_COMMAND}`);

  return "refused";
}

/** Each type that a public declaration names and its entry point does not export: each one is refused. */
function checkExported(texts: ReadonlyMap<string, EntryReport>): Verdict {
  const forgotten = [...texts].flatMap(([fileName, report]) =>
    report.forgotten.map((item) => `report/${fileName}: ${item.file}:${item.line} ${item.text}`),
  );

  for (const line of forgotten) {
    console.error(`not exported (export it from the entry point, or name an exported type): ${line}`);
  }

  return forgotten.length === 0 ? "passed" : "refused";
}

function surfaceWalkFailedText(failure: SurfaceWalkFailed): string {
  switch (failure.kind) {
    case "EntryNotFound":
      return `the declaration program has no module for ${failure.file}`;
    case "TooManyDeclarations":
      return `the public surface has more than ${failure.bound} declarations`;
    case "TooDeep":
      return `${failure.file}: a syntax tree is deeper than ${failure.bound} levels`;
    case "TooManyNodes":
      return `${failure.file}: a syntax tree has more than ${failure.bound} nodes`;
  }
}

function placeText(place: TopTypePlace): string {
  return `${place.file}:${place.line} ${place.path}: ${place.form}`;
}

function printTopTypes(verdict: TopTypeVerdict): void {
  for (const place of verdict.any) {
    console.error(`any is refused: ${placeText(place)}`);
  }

  for (const place of verdict.unallowed) {
    console.error(`unknown outside the allow list (needs the owner's sign-off): ${placeText(place)}`);
  }

  console.log(`unknown on allowed places: ${verdict.used.map((place) => place.path).join(", ") || "none"}`);
  console.log(`allowed places not in the built surface: ${verdict.unused.map((place) => place.path).join(", ") || "none"}`);
}

/** The no-`any` check and the `unknown` allow list over the built declarations. */
function checkTopTypes(builtPackage: BuiltPackage): Verdict {
  const allowList = allowListOf(JSON.parse(readFileSync(ALLOW_LIST_FILE, "utf8")));

  if (!allowList.ok) {
    console.error(allowListRefusedText(allowList.error));

    return "refused";
  }

  const program = declarationProgram(builtPackage);
  const anys = anyPlaces(program, builtPackage);

  if (!anys.ok) {
    console.error(surfaceWalkFailedText(anys.error));

    return "toolFailed";
  }

  const unknowns = unknownPlaces(program, builtPackage);

  if (!unknowns.ok) {
    console.error(surfaceWalkFailedText(unknowns.error));

    return "toolFailed";
  }

  const verdict = judgeTopTypes(anys.value, unknowns.value, allowList.value);

  printTopTypes(verdict);

  return topTypesPass(verdict) ? "passed" : "refused";
}

/** The worst of two verdicts. */
function worse(first: Verdict, second: Verdict): Verdict {
  return EXIT_STATUS[first] >= EXIT_STATUS[second] ? first : second;
}

function runCommand(command: Command, workFolder: string): Verdict {
  const builtPackage = built(workFolder);

  if (builtPackage === "toolFailed") {
    return "toolFailed";
  }

  const texts = reports(builtPackage, workFolder);

  if (texts === "toolFailed") {
    return "toolFailed";
  }

  switch (command) {
    case "write":
      return writeReports(texts);
    case "check":
      return worse(worse(compareReports(texts), checkExported(texts)), checkTopTypes(builtPackage));
  }
}

function commandOf(argument: string | undefined): Command | "unknown" {
  switch (argument) {
    case "check":
    case "write":
      return argument;
    case undefined:
    default:
      return "unknown";
  }
}

function main(): Verdict {
  const command = commandOf(process.argv[2]);

  if (command === "unknown") {
    console.error("usage: bun tests/public-api/run.ts check|write");

    return "toolFailed";
  }

  const workFolder = realpathSync(mkdtempSync(join(tmpdir(), "horsies-public-api-")));

  try {
    return runCommand(command, workFolder);
  } finally {
    rmSync(workFolder, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  process.exitCode = EXIT_STATUS[main()];
}
