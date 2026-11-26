// zod is an optional peer dependency: the main entry point (`src/index.ts`) must load and type check without it.
// This test walks the module imports of each entry point (`import`, `export ... from`, `import(...)`, `require(...)`,
// with either quote). No module that `src/index.ts` reaches imports zod, also as a type. The only modules of the
// adapter (`src/zod/**`) that it may reach are the zod-free ones (the static types of `src/zod/infer.ts`); the
// `horsies/zod` entry point (`src/zod.ts`) reaches zod.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

const SOURCE_DIR = fileURLToPath(new URL("../../../horsies/src/", import.meta.url));

/** The module specifiers of `import ... from`, `export ... from`, `import "..."`, `import(...)` and `require(...)`. */
const SPECIFIER = /(?:\bfrom\s+|\bimport\s+|\bimport\s*\(\s*|\brequire\s*\(\s*)(["'])([^"']+)\1/g;

/** The modules of the adapter that import no zod: the main entry may reach them for their types. */
const ZOD_FREE_MODULES: ReadonlySet<string> = new Set(["zod/infer.ts"]);

/** The most modules one walk visits: far more than the package holds, so a walk always ends. */
const MAX_MODULES = 2000;

type Reach = { readonly modules: ReadonlySet<string>; readonly packages: ReadonlySet<string> };

function specifiersOf(file: string): readonly string[] {
  return [...readFileSync(file, "utf8").matchAll(SPECIFIER)].map((match) => match[2] ?? "");
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
  test("src/index.ts reaches no zod import and no module of the adapter", () => {
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

  test("the specifier pattern finds both quotes, require and a dynamic import", () => {
    const text = [
      "import a from 'zod';",
      'const b = require("zod/v4/core");',
      "await import('zod/mini');",
      'export * from "./x.ts";',
    ].join("\n");

    assert.deepEqual(
      [...text.matchAll(SPECIFIER)].map((match) => match[2]),
      ["zod", "zod/v4/core", "zod/mini", "./x.ts"],
    );
  });

  test("src/zod.ts reaches zod and the adapter", () => {
    const reach = reachOf("zod.ts");

    assert.ok(reach.packages.has("zod"));
    assert.ok(reach.modules.has("zod/public.ts"));
  });
});
