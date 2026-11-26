// zod is an optional peer dependency: the main entry point (`src/index.ts`) must load, bundle and type check without
// it. This test walks the module imports of each entry point (`import`, `export ... from`, `import(...)`,
// `require(...)`, with either quote). No module that `src/index.ts` reaches imports zod, also as a type. The main entry
// point imports the zod adapter statically (the contract compile), so it reaches the zod-free modules of `src/zod/`
// below and no other module there; the `horsies/zod` entry point (`src/zod.ts`, the `hz.*` builders) reaches zod.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

const SOURCE_DIR = fileURLToPath(new URL("../../../horsies/src/", import.meta.url));

/**
 * The module specifiers of the statements `import ... from`, `export ... from` and `import "..."` (at the start of a
 * line, so a text in a string or a comment that reads `from "zod"` is not one), and of the calls `import(...)` and
 * `require(...)`. The specifier is the first group that matched (`specifierOf`).
 */
const SPECIFIER =
  /^\s*(?:import|export)\b[^;"'`]*?\bfrom\s+(["'])([^"']+)\1|^\s*import\s+(["'])([^"']+)\3|\bimport\s*\(\s*(["'])([^"']+)\5|\brequire\s*\(\s*(["'])([^"']+)\7/gm;

/** The specifier of one match of `SPECIFIER`. */
function specifierOf(match: RegExpMatchArray): string {
  return match[2] ?? match[4] ?? match[6] ?? match[8] ?? "";
}

/**
 * The modules of `src/zod/` that import no zod: the adapter (the release check, the definition reader, the walk, the
 * zod runs, the `hz.*` brand reader) and the static types. The main entry point may reach them.
 */
const ZOD_FREE_MODULES: ReadonlySet<string> = new Set([
  "zod/adapter.ts",
  "zod/brand.ts",
  "zod/definition.ts",
  "zod/infer.ts",
  "zod/internals.ts",
  "zod/kind.ts",
  "zod/known-fields.ts",
  "zod/rules.ts",
  "zod/runs.ts",
  "zod/version.ts",
]);

/** The most modules one walk visits: far more than the package holds, so a walk always ends. */
const MAX_MODULES = 2000;

type Reach = { readonly modules: ReadonlySet<string>; readonly packages: ReadonlySet<string> };

function specifiersOf(file: string): readonly string[] {
  return [...readFileSync(file, "utf8").matchAll(SPECIFIER)].map(specifierOf);
}

/** The source modules (relative to `src/`) and the package specifiers that an entry point reaches. */
function reachOf(entry: string): Reach {
  const modules = new Set<string>();
  const packages = new Set<string>();
  const pending = [join(SOURCE_DIR, entry)];

  while (pending.length > 0) {
    assert.ok(modules.size < MAX_MODULES, `more than ${MAX_MODULES} modules from ${entry}`);

    const file = pending.pop() ?? "";
    const module = relative(SOURCE_DIR, file);

    if (modules.has(module)) {
      continue;
    }

    modules.add(module);

    for (const specifier of specifiersOf(file)) {
      if (specifier.startsWith(".")) {
        pending.push(join(dirname(file), specifier));
      } else {
        packages.add(specifier);
      }
    }
  }

  return { modules, packages };
}

describe("entry points", () => {
  test("src/index.ts reaches no zod import, and of src/zod/ only the zod-free modules", () => {
    const reach = reachOf("index.ts");

    assert.deepEqual(
      [...reach.packages].filter((name) => name === "zod" || name.startsWith("zod/")),
      [],
    );
    assert.deepEqual(
      [...reach.modules].filter((module) => module.startsWith("zod") && !ZOD_FREE_MODULES.has(module)),
      [],
    );
    assert.ok(reach.modules.has("wire/public.ts"));
  });

  test("src/index.ts reaches the zod adapter statically: the contract compile imports it", () => {
    const reach = reachOf("index.ts");

    assert.ok(reach.modules.has("contract/compile.ts"));
    assert.ok(reach.modules.has("zod/kind.ts"));
    assert.ok(reach.modules.has("zod/brand.ts"));
  });

  test("each zod-free module of the adapter reaches no zod import", () => {
    for (const module of ZOD_FREE_MODULES) {
      const reach = reachOf(module);

      assert.deepEqual(
        [...reach.packages].filter((name) => name === "zod" || name.startsWith("zod/")),
        [],
      );
      assert.deepEqual(
        [...reach.modules].filter((reached) => reached.startsWith("zod") && !ZOD_FREE_MODULES.has(reached)),
        [],
      );
    }
  });

  test("the specifier pattern finds both quotes, require, a dynamic import and an import over lines", () => {
    const text = [
      "import a from 'zod';",
      'const b = require("zod/v4/core");',
      "await import('zod/mini');",
      'export * from "./x.ts";',
      "import {",
      "  y,",
      '} from "./y.ts";',
      'import "./z.ts";',
      'import type { W } from "zod/v4";',
    ].join("\n");

    assert.deepEqual(
      [...text.matchAll(SPECIFIER)].map(specifierOf),
      ["zod", "zod/v4/core", "zod/mini", "./x.ts", "./y.ts", "./z.ts", "zod/v4"],
    );
  });

  test("the specifier pattern does not read a text in a string or a comment as an import", () => {
    const text = [
      "const fix = 'import { z } from \"zod\", not from \"zod/v3\"';",
      '// a schema built with import { z } from "zod"',
    ].join("\n");

    assert.deepEqual([...text.matchAll(SPECIFIER)].map(specifierOf), []);
  });

  test("src/zod.ts reaches zod and the hz.* builders", () => {
    const reach = reachOf("zod.ts");

    assert.ok(reach.packages.has("zod"));
    assert.ok(reach.modules.has("zod/public.ts"));
    assert.ok(reach.modules.has("zod/codecs.ts"));
  });
});
