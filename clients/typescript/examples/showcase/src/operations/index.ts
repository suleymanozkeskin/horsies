// The operator commands of the showcase: `report` (monitoring and health reads), `rerun` (a rerun with the store's
// audit row in one transaction) and `cancel` (the operator cancel of one task).

import type { Result } from "horsies";

import { appRefusedText, buildShowcase, type ShowcaseApp } from "../app.ts";
import { processDatabaseSettings, type DatabaseSettings, settingsRefusedText } from "../settings.ts";
import { closeStore, openStore } from "../store/connection.ts";
import { err, ok } from "../result.ts";
import { cancelLine, cancelTask, parseCancelArguments } from "./cancel.ts";
import { parseReportArguments, reportLines } from "./report.ts";
import { parseRerunArguments, rerunFailureLine, rerunOutcomeLine, rerunWithAudit } from "./rerun.ts";

export type OperationName = "report" | "rerun" | "cancel";

/** Each operation name, by itself. */
const OPERATION_NAME_SET: { readonly [Name in OperationName]: Name } = { report: "report", rerun: "rerun", cancel: "cancel" };

export const OPERATION_NAMES: readonly OperationName[] = Object.values(OPERATION_NAME_SET);

/** The exit code of a command: 0 done, 1 failed, 2 for arguments that do not parse. */
export type ExitCode = 0 | 1 | 2;

/** The operation that `text` names, or none. Pure. */
export function operationNameOf(text: string): { readonly kind: "operation"; readonly name: OperationName } | { readonly kind: "notOperation" } {
  const name = OPERATION_NAMES.find((known) => known === text);

  return name === undefined ? { kind: "notOperation" } : { kind: "operation", name };
}

/** The app and the settings of the process environment, or the line that says why there is none. */
function processShowcase(): Result<{ readonly showcase: ShowcaseApp; readonly settings: DatabaseSettings }, string> {
  const settings = processDatabaseSettings();

  if (!settings.ok) {
    return err(settingsRefusedText(settings.error));
  }

  const built = buildShowcase(settings.value);

  return built.ok ? ok({ showcase: built.value, settings: settings.value }) : err(appRefusedText(built.error));
}

function printLines(lines: readonly string[]): void {
  for (const line of lines) {
    process.stdout.write(`${line}\n`);
  }
}

/** Run the operation `name` with `args`: its exit code. Effects: the reads and writes of the operation. */
export async function runOperation(name: OperationName, args: readonly string[]): Promise<ExitCode> {
  const built = processShowcase();

  if (!built.ok) {
    process.stderr.write(`acme ${name}: ${built.error}\n`);

    return 1;
  }

  switch (name) {
    case "report":
      return runReport(built.value.showcase, args);
    case "rerun":
      return runRerun(built.value.showcase, built.value.settings, args);
    case "cancel":
      return runCancel(built.value.showcase, args);
    default: {
      const unexpected: never = name;

      throw new TypeError(`runOperation: an operation outside its type: ${String(unexpected)}`);
    }
  }
}

function usageFailed(name: OperationName, message: string): ExitCode {
  process.stderr.write(`acme ${name}: ${message}\n`);

  return 2;
}

async function runReport(showcase: ShowcaseApp, args: readonly string[]): Promise<ExitCode> {
  const parsed = parseReportArguments(args);

  if (!parsed.ok) {
    return usageFailed("report", parsed.error);
  }

  printLines(await reportLines(showcase.app, parsed.value.failedRows));

  return 0;
}

async function runRerun(showcase: ShowcaseApp, settings: DatabaseSettings, args: readonly string[]): Promise<ExitCode> {
  const parsed = parseRerunArguments(args);

  if (!parsed.ok) {
    return usageFailed("rerun", parsed.error);
  }

  const store = openStore(settings.url);
  const rerun = await rerunWithAudit(showcase, store, parsed.value.sourceTaskId, parsed.value.reason);

  await closeStore(store);
  printLines([rerun.ok ? rerunOutcomeLine(rerun.value) : rerunFailureLine(rerun.error)]);

  return rerun.ok ? 0 : 1;
}

async function runCancel(showcase: ShowcaseApp, args: readonly string[]): Promise<ExitCode> {
  const parsed = parseCancelArguments(args);

  if (!parsed.ok) {
    return usageFailed("cancel", parsed.error);
  }

  const cancelled = await cancelTask(showcase.app, parsed.value);

  printLines([cancelLine(cancelled)]);

  return cancelled.ok ? 0 : 1;
}
