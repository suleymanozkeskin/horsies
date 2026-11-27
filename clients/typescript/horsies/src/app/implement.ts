// `app.implement`: one implementation per listed contract. It never throws and calls no addon function: a refusal
// goes to the refusal sink, which the `check` command and the worker start read.

import { HorsiesErrorCode } from "../_catalog/error_codes.ts";
import { contractRecord } from "../contract/definition.ts";
import { type ImplementationParts, implementationParts } from "../contract/implementation.ts";
import type { AppContext } from "./context.ts";
import { type CoreChecks, refusalOfCore } from "./core-checks.ts";
import { refusal, type HorsiesError } from "./refusal.ts";

/** Register the parts of an implementation; record a refusal for a contract that the app does not list, or a second implementation. */
export function registerImplementation(context: AppContext, implementation: object): void {
  const parts: ImplementationParts = implementationParts(implementation);
  const name = contractRecord(parts.contract).name;
  const path = `implement ${name}`;
  const listed = context.listed.some((entry) => entry.contract === parts.contract);

  if (!listed) {
    context.refusals.record(
      "contracts",
      refusal(HorsiesErrorCode.TASK_NOT_REGISTERED, path, "the contract is not in the tasks list of this app", [
        "add the contract to Horsies.create({ tasks })",
      ]),
    );

    return;
  }

  if (context.implementations.has(name)) {
    context.refusals.record(
      "contracts",
      refusal(HorsiesErrorCode.TASK_DUPLICATE_NAME, path, "the contract has a second implementation", ["implement each contract once"]),
    );

    return;
  }

  context.implementations.set(name, parts);
}

/**
 * The refusals of the exception codes of each implementation whose contract declares no `errorCodes`: each code must
 * pass the core's code rules (`parseDeclaredErrorCodes`). With `errorCodes`, the static type allows only a declared
 * code.
 * `app.implement` calls no addon function, so the `check` command and the worker start call this, on a main thread,
 * with the core's checks. Refusals in registration order, with the path `implement <task name>`.
 */
export function exceptionCodeRefusals(context: AppContext, checks: CoreChecks): readonly HorsiesError[] {
  const refusals: HorsiesError[] = [];

  for (const [name, parts] of context.implementations) {
    const record = contractRecord(parts.contract);
    const codes = [...new Set(parts.exceptionMapper.map(([, code]) => code))];

    if (record.codes.kind === "open" && codes.length > 0) {
      const checked = checks.parseDeclaredErrorCodes(codes);

      if (checked.kind === "refused") {
        refusals.push(...checked.refusals.map((refused) => refusalOfCore(`implement ${name}`, refused)));
      }
    }
  }

  return refusals;
}
