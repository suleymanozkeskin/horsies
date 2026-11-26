// The static check of an app (the `check` command, the worker start and the scheduler start), in phases; it stops at
// the first phase that refuses, as the Python and Rust clients do. No I/O; on a main thread the core's pure code check
// runs through the addon, which starts no core.
//
// 1. Registrations: every refusal of the refusal sink (`app.implement` and the other registrations after
//    `Horsies.create`) whose area is in the scope of the check. An implementation of a contract that the app does
//    not list is such a refusal.
// 2. Implementations: each listed contract has an implementation in this entry, so a passing check means that a
//    worker start from the same entry passes the same rule.
// 3. Exception codes: the codes of each implementation's exception list pass the core's code rules.
//
// The scheduler start runs phase 1 only: the scheduler sends by contract and runs no body, so its entry needs no
// implementation, as the Python scheduler needs no task bodies.

import { ErrorCode } from "../_catalog/error_codes.ts";
import type { AppContext } from "../app/context.ts";
import type { CoreChecks } from "../app/core-checks.ts";
import { exceptionCodeRefusals } from "../app/implement.ts";
import { refusal, type Refusal } from "../app/refusal.ts";
import type { RefusalArea } from "../app/refusals.ts";
import { unreachable } from "../native/call/never.ts";
import { type CheckOutcome, type CheckPassed, type CheckPhaseName, checkReport } from "./report.ts";

/** Which refusals a check reads: every area, or every area but those that only a scheduler reads. */
export type CheckScope = "withSchedules" | "withoutSchedules";

/** The scopes in which the refusals of `area` count. A new area is a type change here. Pure. */
export function areaScopes(area: RefusalArea): readonly CheckScope[] {
  switch (area) {
    case "contracts":
      return ["withSchedules", "withoutSchedules"];
    case "schedules":
      return ["withSchedules"];
    default:
      return unreachable(area);
  }
}

/**
 * The static check of `context` in `scope`; a report names `phase`. `checks` gives the core's code rules (the
 * addon on a main thread, a fake in a test).
 */
export function staticCheck(
  context: AppContext,
  checks: CoreChecks,
  scope: CheckScope,
  phase: CheckPhaseName,
): CheckOutcome {
  const phases: readonly (() => readonly Refusal[])[] = [
    () => registrationRefusals(context, scope),
    ...implementationPhases(context, checks, phase),
  ];

  for (const run of phases) {
    const [first, ...rest] = run();

    if (first !== undefined) {
      return { kind: "refused", report: checkReport(phase, first, rest) };
    }
  }

  return { kind: "passed", passed: passedOf(context) };
}

/**
 * The phases of the implementations in a check of `phase`: `check` and the worker start check them (a worker runs the
 * bodies); the scheduler start checks none (it sends by contract). Pure.
 */
function implementationPhases(
  context: AppContext,
  checks: CoreChecks,
  phase: CheckPhaseName,
): readonly (() => readonly Refusal[])[] {
  switch (phase) {
    case "check":
    case "worker-startup":
      return [() => implementationRefusals(context), () => exceptionCodeRefusals(context, checks)];
    case "scheduler-startup":
      return [];
    default:
      return unreachable(phase);
  }
}

/** The refusals of the sink whose area counts in `scope`, in registration order. Pure. */
export function registrationRefusals(context: AppContext, scope: CheckScope): readonly Refusal[] {
  return context.refusals
    .recorded()
    .filter((recorded) => areaScopes(recorded.area).includes(scope))
    .map((recorded) => recorded.refusal);
}

/** One refusal per listed contract with no implementation in this entry, in list order. Pure. */
export function implementationRefusals(context: AppContext): readonly Refusal[] {
  return context.listed
    .filter((entry) => !context.implementations.has(entry.task.compiled.record.name))
    .map((entry) =>
      refusal(ErrorCode.TASK_NOT_REGISTERED, entry.path, "the contract has no implementation in this entry", [
        "call app.implement(contract, body) in a module that the entry imports before cli.run",
      ]),
    );
}

/** The summary of a passing check: the listed tasks, and the declared codes of each task by name. Pure. */
function passedOf(context: AppContext): CheckPassed {
  const declared = context.listed
    .map((entry) => entry.task.compiled.record)
    .flatMap((record) => (record.codes.kind === "declared" && record.codes.codes.length > 0 ? [[record.name, record.codes.codes] as const] : []))
    .toSorted(([left], [right]) => codeUnitOrder(left, right));

  return { taskCount: context.listed.length, declaredErrorCodes: new Map(declared) };
}

/** The order of two texts by UTF-16 code units, as the Rust client orders the names in a sorted map. Pure. */
function codeUnitOrder(left: string, right: string): number {
  if (left === right) {
    return 0;
  }

  return left < right ? -1 : 1;
}
