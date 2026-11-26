// The API report of each published entry point: API Extractor reads the entry point's declarations and writes one
// `.api.md` file of the exported surface. A type that the surface uses but does not export is a
// `ae-forgotten-export` line in the report.

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import type { Result } from "../../horsies/src/wire/result.ts";
import type { BuiltPackage, EntryPoint, ParsedJson } from "./package.ts";
import { API_EXTRACTOR, runTool, type ToolFailed } from "./tools.ts";

/** The committed report files. */
export const REPORT_DIR: string = fileURLToPath(new URL("report/", import.meta.url));

/** The file name of an entry point's report. */
export function reportFileName(entryPoint: EntryPoint): string {
  return `${entryPoint.reportName}.api.md`;
}

/**
 * The API Extractor config of one entry point. The package has no release tags (`@public`), and its doc comments are
 * not TSDoc, so those messages are off. Any other message of the compiler or of API Extractor is an error. Pure.
 */
export function extractorConfig(built: BuiltPackage, entryPoint: EntryPoint, reportFolder: string): ParsedJson {
  return {
    projectFolder: built.folder,
    mainEntryPointFilePath: `<projectFolder>/${entryPoint.typesFile}`,
    compiler: {
      overrideTsconfig: {
        compilerOptions: {
          strict: true,
          module: "nodenext",
          moduleResolution: "nodenext",
          lib: ["es2025", "esnext.temporal"],
          types: [],
        },
        files: [entryPoint.typesFile],
      },
    },
    apiReport: {
      enabled: true,
      reportFileName: entryPoint.reportName,
      reportFolder,
      reportTempFolder: join(reportFolder, "temp"),
    },
    docModel: { enabled: false },
    dtsRollup: { enabled: false },
    tsdocMetadata: { enabled: false },
    newlineKind: "lf",
    messages: {
      compilerMessageReporting: { default: { logLevel: "error" } },
      extractorMessageReporting: {
        default: { logLevel: "error" },
        "ae-missing-release-tag": { logLevel: "none", addToApiReportFile: false },
        "ae-forgotten-export": { logLevel: "none", addToApiReportFile: true },
      },
      tsdocMessageReporting: { default: { logLevel: "none", addToApiReportFile: false } },
    },
  };
}

export type ReportFailed =
  | { readonly kind: "ExtractorNotRun"; readonly failure: ToolFailed }
  | { readonly kind: "ExtractorErrors"; readonly output: string }
  | { readonly kind: "NoReportWritten"; readonly output: string };

/**
 * The API report text of one entry point. Writes its report into `workFolder` and its config into the package folder
 * (API Extractor finds the `package.json` from the config's folder); the caller owns and removes both folders. Runs
 * API Extractor on Node.
 */
export function apiReport(built: BuiltPackage, entryPoint: EntryPoint, workFolder: string): Result<string, ReportFailed> {
  const reportFolder = join(workFolder, entryPoint.reportName);
  const configFile = join(built.folder, `${entryPoint.reportName}.api-extractor.json`);

  mkdirSync(reportFolder);
  writeFileSync(configFile, JSON.stringify(extractorConfig(built, entryPoint, reportFolder), null, 2));

  const run = runTool("node", [API_EXTRACTOR, "run", "--local", "--config", configFile], workFolder);

  if (!run.ok) {
    return { ok: false, error: { kind: "ExtractorNotRun", failure: run.error } };
  }

  if (run.value.code !== 0) {
    return { ok: false, error: { kind: "ExtractorErrors", output: run.value.output } };
  }

  const reportFile = join(reportFolder, reportFileName(entryPoint));

  if (!existsSync(reportFile)) {
    return { ok: false, error: { kind: "NoReportWritten", output: run.value.output } };
  }

  return { ok: true, value: readFileSync(reportFile, "utf8") };
}

/** The text of a failed report run, for a message. */
export function reportFailedText(entryPoint: EntryPoint, failure: ReportFailed): string {
  switch (failure.kind) {
    case "ExtractorNotRun":
      return `${entryPoint.specifier}: API Extractor did not run (${failure.failure.kind}):\n${failure.failure.tool}`;
    case "ExtractorErrors":
      return `${entryPoint.specifier}: API Extractor reported errors:\n${failure.output}`;
    case "NoReportWritten":
      return `${entryPoint.specifier}: API Extractor wrote no report:\n${failure.output}`;
  }
}

/**
 * How a generated report compares with the committed one. `changed` holds the lines that only the committed report
 * has (`removed`) and the lines that only the generated report has (`added`), each in file order; both are empty when
 * only the order of lines differs.
 */
export type ReportComparison =
  | { readonly state: "same" }
  | { readonly state: "notCommitted" }
  | { readonly state: "changed"; readonly removed: readonly string[]; readonly added: readonly string[] };

/** The lines of `lines` that `other` does not hold, each counted as often as it occurs. Pure. */
function linesNotIn(lines: readonly string[], other: readonly string[]): readonly string[] {
  const counts = new Map<string, number>();

  for (const line of other) {
    counts.set(line, (counts.get(line) ?? 0) + 1);
  }

  return lines.filter((line) => {
    const count = counts.get(line) ?? 0;

    counts.set(line, count - 1);

    return count <= 0;
  });
}

/** The comparison of a generated report text with the committed text (`undefined`: no committed file). Pure. */
export function compareReport(committed: string | undefined, generated: string): ReportComparison {
  if (committed === undefined) {
    return { state: "notCommitted" };
  }

  if (committed === generated) {
    return { state: "same" };
  }

  const committedLines = committed.split("\n");
  const generatedLines = generated.split("\n");

  return {
    state: "changed",
    removed: linesNotIn(committedLines, generatedLines),
    added: linesNotIn(generatedLines, committedLines),
  };
}
