import { describe, expect, test } from "bun:test";

import {
  BUILT_IN_TASK_CODES,
  RESERVED_NON_TASK_CODES,
  UNDECLARED_LABEL,
} from "../../../horsies/src/_catalog/task_code.ts";
import type { BuiltInTaskCode } from "../../../horsies/src/_catalog/task_code.ts";
import {
  ContractCode,
  OperationalCode,
  OutcomeCode,
  RetrievalCode,
} from "../../../horsies/src/_catalog/task_codes.ts";

describe("task codes", () => {
  test("the built-in codes are the members of the four catalogs, in order", () => {
    expect([...BUILT_IN_TASK_CODES]).toEqual([
      ...Object.values(OperationalCode),
      ...Object.values(ContractCode),
      ...Object.values(RetrievalCode),
      ...Object.values(OutcomeCode),
    ]);
    expect(BUILT_IN_TASK_CODES).toHaveLength(38);
    expect(new Set(BUILT_IN_TASK_CODES).size).toBe(38);
    expect(Object.isFrozen(BUILT_IN_TASK_CODES)).toBe(true);
  });

  test("a code that only a blocking form gives is a built-in code", () => {
    const asyncContext: BuiltInTaskCode = RetrievalCode.ASYNC_CONTEXT;

    expect(BUILT_IN_TASK_CODES).toContain(asyncContext);
  });

  test("the contract codes name a node with no id and a node that is not in the context apart", () => {
    expect(Object.values(ContractCode)).toEqual([
      "RETURN_TYPE_MISMATCH",
      "WORKFLOW_CONTEXT_MISSING_ID",
      "NODE_NOT_IN_CONTEXT",
      "NO_TYPE_AVAILABLE",
    ]);
  });

  test("the reserved names are not built-in codes", () => {
    expect(RESERVED_NON_TASK_CODES).toEqual(["WORKFLOW_STOPPED"]);
    expect(Object.isFrozen(RESERVED_NON_TASK_CODES)).toBe(true);

    const builtIn: ReadonlySet<string> = new Set(BUILT_IN_TASK_CODES);

    expect(RESERVED_NON_TASK_CODES.filter((name) => builtIn.has(name))).toEqual([]);
  });

  test("the label of an undeclared code", () => {
    expect(UNDECLARED_LABEL).toBe("undeclared");
  });
});
