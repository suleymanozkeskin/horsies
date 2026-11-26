// zod is an optional peer dependency: the main entry point (`src/index.ts`) must load and type check without it.
// This test walks the module imports of each entry point. No module that `src/index.ts` reaches imports zod, also
// as a type, or a module of the adapter (`src/zod/**`); the `horsies/zod` entry point (`src/zod.ts`) does.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

const SOURCE_DIR = fileURLToPath(new URL("../../../horsies/src/", import.meta.url));

/** The module specifiers of `import ... from`, `export ... from`, `import "..."` and `import("...")`. */
const SPECIFIER = /(?:\bfrom\s+|\bimport\s+|\bimport\s*\(\s*)"([^"]+)"/g;

/** The most modules one walk visits: far more than the package holds, so a walk always ends. */
const MAX_MODULES = 2000;

type Reach = { readonly modules: ReadonlySet<string>; readonly packages: ReadonlySet<string> };

function specifiersOf(file: string): readonly string[] {
  return [...readFileSync(file, "utf8").matchAll(SPECIFIER)].map((match) => match[1] ?? "");
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
      [...reach.modules].filter((module) => module.startsWith("zod")),
      [],
    );
    assert.ok(reach.modules.has("wire/public.ts"));
  });

  test("src/zod.ts reaches zod and the adapter", () => {
    const reach = reachOf("zod.ts");

    assert.ok(reach.packages.has("zod"));
    assert.ok(reach.modules.has("zod/public.ts"));
  });
});
