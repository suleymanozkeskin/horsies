// The declared custom codes of the tasks that an app lists, as a rerun passes them to the core.

import type { ListedEntry } from "../app/contract-list.ts";
import type { RegisteredTaskArg, RegisteredTaskCodesArg } from "../native/facades/rerun.ts";
import { unreachable } from "../wire/unreachable.ts";

/**
 * The tasks of `listed`, each with the custom codes that its contract declares in `errorCodes`. A contract with no
 * `errorCodes` list declares none: the core takes a closed set, so such a task passes no custom code of a stored retry
 * list. Pure.
 */
export function registeredCodesOf(listed: readonly ListedEntry[]): RegisteredTaskCodesArg {
  return { tasks: listed.map(registeredTaskOf) };
}

function registeredTaskOf(entry: ListedEntry): RegisteredTaskArg {
  const record = entry.task.compiled.record;

  switch (record.codes.kind) {
    case "open":
      return { taskName: record.name, codes: [] };
    case "declared":
      return { taskName: record.name, codes: [...record.codes.codes] };
    default:
      return unreachable(record.codes, "registeredTaskOf");
  }
}
