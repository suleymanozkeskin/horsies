// The outcomes of a check and their text: the refusals of the first phase that refused, or the passed summary. The
// text is the form of the Python and Rust clients (`ValidationReport`, `CheckPassed`).

import type { Refusal } from "../app/refusal.ts";
import type { NonEmpty } from "../wire/builders.ts";

/** The name of the check in a report: `check`, or the check before a worker starts. */
export type CheckPhaseName = "check" | "worker-startup";

/** The refusals of the first phase of a check that refused, in the order the phase found them. */
export type CheckReport = { readonly phase: CheckPhaseName; readonly refusals: NonEmpty<Refusal> };

/** A passing check: the count of listed tasks, and the user error codes of each task that declares some. */
export type CheckPassed = {
  readonly taskCount: number;
  /** By task name, in name order. */
  readonly declaredErrorCodes: ReadonlyMap<string, readonly string[]>;
};

/** How a check ended. */
export type CheckOutcome =
  | { readonly kind: "passed"; readonly passed: CheckPassed }
  | { readonly kind: "refused"; readonly report: CheckReport };

/** The frozen report of `first` and `rest` in `phase`. Pure. */
export function checkReport(phase: CheckPhaseName, first: Refusal, rest: readonly Refusal[]): CheckReport {
  const refusals: NonEmpty<Refusal> = Object.freeze([first, ...rest]);

  return Object.freeze({ phase, refusals });
}

/**
 * Each refusal, then the count: `error[HRS-300]: <message>`, one `  = help: <line>` per help line, and
 * `error: aborting due to <n> previous errors`. A refusal with no catalog code is `error: <message>`. Pure.
 */
export function reportText(report: CheckReport): string {
  const lines = report.refusals.flatMap((refused) => [
    refused.code === "FIELD_BOUND" ? `error: ${refused.message}` : `error[${refused.code}]: ${refused.message}`,
    ...refused.help.map((line) => `  = help: ${line}`),
  ]);

  return [...lines, `error: aborting due to ${String(report.refusals.length)} previous errors`].join("\n");
}

/** The success text of `check`: the task count, then the declared error codes of each task, or `none`. Pure. */
export function passedText(passed: CheckPassed): string {
  const head = ["ok: all validations passed", `  ${String(passed.taskCount)} task(s) registered`];

  if (passed.declaredErrorCodes.size === 0) {
    return [...head, "  declared error codes: none"].join("\n");
  }

  const tasks = Array.from(passed.declaredErrorCodes, ([name, codes]) => `    ${name}: ${codes.join(", ")}`);

  return [...head, "  declared error codes:", ...tasks].join("\n");
}
