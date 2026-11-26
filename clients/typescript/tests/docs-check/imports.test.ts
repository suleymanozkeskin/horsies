import { describe, expect, test } from "bun:test";

import { mergeImports } from "./imports.ts";

/** The code of `later` after a block of `earlier` in the same file. */
function after(earlier: string, later: string): readonly string[] {
  const above = mergeImports(earlier.split("\n"), new Set());

  return mergeImports(later.split("\n"), above.bindings).code;
}

describe("mergeImports", () => {
  test("an import that a block above makes already is dropped; its lines stay as blank lines", () => {
    const code = after('import { h } from "horsies";', 'import {\n  h,\n} from "horsies";\nconst a = h.int();');

    expect(code).toEqual(["", "", "", "const a = h.int();"]);
  });

  test("a declaration that keeps some of its names is written as one-name imports on its first line", () => {
    const code = after(
      'import { h } from "horsies";',
      'import {\n  defineTask,\n  h,\n  type Result as Outcome,\n} from "horsies";\nconst a = 1;',
    );

    expect(code).toEqual([
      'import { defineTask } from "horsies"; import { type Result as Outcome } from "horsies";',
      "",
      "",
      "",
      "",
      "const a = 1;",
    ]);
  });

  test("default, namespace and type-only imports are dropped when a block above makes the same binding", () => {
    const earlier = 'import pg from "pg";\nimport * as z from "zod";\nimport type { Pool } from "pg";';

    expect(after(earlier, earlier)).toEqual(["", "", ""]);
  });

  test("a binding of another module, another imported name or another type form stays", () => {
    const later = 'import { h } from "./h.ts";\nimport { int as h2 } from "horsies";\nimport type { h as h3 } from "horsies";';

    expect(after('import { h, h as h2 } from "horsies";\nimport { h as h3 } from "horsies";', later)).toEqual(later.split("\n"));
  });

  test("a repeat inside one block stays, so the compiler reports it", () => {
    const code = 'import { h } from "horsies";\nimport { h } from "horsies";';

    expect(mergeImports(code.split("\n"), new Set()).code).toEqual(code.split("\n"));
  });

  test("side-effect, deferred and attributed imports stay as they are", () => {
    const code = 'import "./setup.ts";\nimport defer * as heavy from "./heavy.ts";\nimport data from "./data.json" with { type: "json" };';

    expect(after(code, code)).toEqual(code.split("\n"));
  });

  test("a block with no code has no line and no binding", () => {
    const merged = mergeImports([], new Set());

    expect([merged.code, [...merged.bindings]]).toEqual([[], []]);
  });
});
