// The API report of each published entry point: API Extractor reads the entry point's declarations and writes one
// `.api.md` file of the exported surface. It also names each type that a public declaration names but the entry point
// does not export (`ae-forgotten-export`): the gate refuses each one, because a user cannot import it.

import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  Extractor,
  ExtractorConfig,
  ExtractorLogLevel,
  type ExtractorMessage,
  ExtractorMessageId,
  type IConfigFile,
} from "@microsoft/api-extractor";

import type { Result } from "../../horsies/src/wire/result.ts";
import type { BuiltPackage, EntryPoint } from "./package.ts";

/** The committed report files. */
export const REPORT_DIR: string = fileURLToPath(new URL("report/", import.meta.url));

/** The file name of an entry point's report. */
export function reportFileName(entryPoint: EntryPoint): string {
  return `${entryPoint.reportName}.api.md`;
}

/**
 * The API Extractor config of one entry point. The package has no release tags (`@public`), and its doc comments are
 * not TSDoc, so those messages are off. A forgotten export is a warning that the gate reads; any other message of the
 * compiler or of API Extractor is an error. Pure.
 */
export function extractorConfig(built: BuiltPackage, entryPoint: EntryPoint, reportFolder: string): IConfigFile {
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
      compilerMessageReporting: { default: { logLevel: ExtractorLogLevel.Error } },
      extractorMessageReporting: {
        default: { logLevel: ExtractorLogLevel.Error },
        [ExtractorMessageId.MissingReleaseTag]: { logLevel: ExtractorLogLevel.None, addToApiReportFile: false },
        [ExtractorMessageId.Undocumented]: { logLevel: ExtractorLogLevel.None, addToApiReportFile: false },
        [ExtractorMessageId.ForgottenExport]: { logLevel: ExtractorLogLevel.Warning, addToApiReportFile: false },
      },
      tsdocMessageReporting: { default: { logLevel: ExtractorLogLevel.None, addToApiReportFile: false } },
    },
  };
}

/** A type that a public declaration names and the entry point does not export: API Extractor's text and place. */
export type ForgottenExport = { readonly text: string; readonly file: string; readonly line: number };

/** The report of one entry point, and the forgotten exports of its surface (each one fails the gate). */
export type EntryReport = { readonly text: string; readonly forgotten: readonly ForgottenExport[] };

export type ReportFailed =
  | { readonly kind: "ExtractorThrew"; readonly cause: string }
  | { readonly kind: "ExtractorErrors"; readonly messages: readonly string[] }
  | { readonly kind: "NoReportWritten" };

/** Where a message points, relative to the package folder. */
function placeOf(message: ExtractorMessage, built: BuiltPackage): { readonly file: string; readonly line: number } {
  const file = message.sourceFilePath ?? "(no file)";

  return {
    file: file.startsWith(`${built.folder}/`) ? file.slice(built.folder.length + 1) : file,
    line: message.sourceFileLine ?? 0,
  };
}

/**
 * The API report of one entry point. Runs API Extractor in this process; writes the report into `workFolder` (a folder
 * that the caller owns and removes). Every message of API Extractor goes to this function, none to the console.
 */
export function apiReport(built: BuiltPackage, entryPoint: EntryPoint, workFolder: string): Result<EntryReport, ReportFailed> {
  const reportFolder = join(workFolder, entryPoint.reportName);
  const forgotten: ForgottenExport[] = [];
  const errors: string[] = [];

  mkdirSync(reportFolder);

  const collect = (message: ExtractorMessage): void => {
    message.handled = true;

    if (message.messageId === ExtractorMessageId.ForgottenExport) {
      forgotten.push({ text: message.text, ...placeOf(message, built) });
    } else if (message.logLevel === ExtractorLogLevel.Error) {
      const { file, line } = placeOf(message, built);

      errors.push(`${file}:${line} ${message.messageId}: ${message.text}`);
    }
  };

  try {
    const config = ExtractorConfig.prepare({
      configObject: extractorConfig(built, entryPoint, reportFolder),
      configObjectFullPath: undefined,
      packageJsonFullPath: join(built.folder, "package.json"),
    });

    Extractor.invoke(config, { localBuild: true, messageCallback: collect });
  } catch (cause) {
    return { ok: false, error: { kind: "ExtractorThrew", cause: String(cause) } };
  }

  if (errors.length > 0) {
    return { ok: false, error: { kind: "ExtractorErrors", messages: errors } };
  }

  const reportFile = join(reportFolder, reportFileName(entryPoint));

  if (!existsSync(reportFile)) {
    return { ok: false, error: { kind: "NoReportWritten" } };
  }

  return { ok: true, value: { text: readFileSync(reportFile, "utf8"), forgotten } };
}

/** The text of a failed report run, for a message. */
export function reportFailedText(entryPoint: EntryPoint, failure: ReportFailed): string {
  switch (failure.kind) {
    case "ExtractorThrew":
      return `${entryPoint.specifier}: API Extractor stopped: ${failure.cause}`;
    case "ExtractorErrors":
      return `${entryPoint.specifier}: API Extractor reported errors:\n${failure.messages.join("\n")}`;
    case "NoReportWritten":
      return `${entryPoint.specifier}: API Extractor wrote no report`;
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
