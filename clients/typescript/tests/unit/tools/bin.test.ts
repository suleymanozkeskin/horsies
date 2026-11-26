// The `bin` of the package: the manifest names it, it runs as its own process on Bun and on Node, and no module that it
// imports loads the native addon.

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

const PACKAGE_DIR = fileURLToPath(new URL("../../../horsies/", import.meta.url));
const SOURCE_DIR = join(PACKAGE_DIR, "src");
const BIN_SOURCE = join(SOURCE_DIR, "tools", "bin.ts");

/** The most modules that the import walk visits: more is a defect of the walk. */
const MAX_WALKED_MODULES = 64;

/** The relative modules that `file` imports as values: `import ... from "./x.ts"`, not `import type`. Pure but for the read. */
function valueImportsOf(file: string): readonly string[] {
  const text = readFileSync(file, "utf8");
  const imports = [...text.matchAll(/^import\s+(?!type\s)[^;]*?from\s+"(\.[^"]+)";/gmu)];

  return imports.map((match) => resolve(dirname(file), match[1] ?? ""));
}

/** Every source module that the bin loads, by its path under `src/`. */
function binModules(): readonly string[] {
  const seen = new Set<string>();
  const waiting = [BIN_SOURCE];

  while (waiting.length > 0) {
    const file = waiting.pop() ?? "";

    if (seen.has(file)) {
      continue;
    }

    assert.ok(seen.size < MAX_WALKED_MODULES, "the import walk passed its bound");
    seen.add(file);
    waiting.push(...valueImportsOf(file));
  }

  return [...seen].map((file) => relative(SOURCE_DIR, file)).toSorted();
}

describe("the bin of the package", () => {
  test("the manifest names the bin horsies at the emitted file of src/tools/bin.ts", () => {
    // SAFETY: the committed package.json is a JSON object.
    const manifest = JSON.parse(readFileSync(join(PACKAGE_DIR, "package.json"), "utf8")) as { readonly bin?: Readonly<Record<string, string>> };

    assert.deepEqual(manifest.bin, { horsies: "./dist/tools/bin.js" });
    assert.ok(readFileSync(BIN_SOURCE, "utf8").startsWith("#!/usr/bin/env node\n"));
  });

  test("no module that the bin loads is a module of the native addon or of cli.run", () => {
    const modules = binModules();

    assert.ok(modules.includes(join("tools", "run.ts")));
    assert.deepEqual(
      modules.filter((module) => module.startsWith(`native${"/"}`) || module.startsWith(`cli${"/"}`)),
      [],
    );
  });

  for (const runtime of ["bun", "node"] as const) {
    test(`it runs on ${runtime}: the help with exit code 0, a command of the app's entry with exit code 2`, () => {
      const help = spawnSync(runtime, [BIN_SOURCE, "--help"], { encoding: "utf8", timeout: 30_000 });

      assert.equal(help.status, 0, help.stderr);
      assert.match(help.stdout, /^usage: horsies \[-h\] \[-V\] \{get-docs,skills\}/u);

      const refused = spawnSync(runtime, [BIN_SOURCE, "worker"], { encoding: "utf8", timeout: 30_000 });

      assert.equal(refused.status, 2, refused.stderr);
      assert.match(refused.stderr, /`horsies worker` is not a command of this tool/u);
    });
  }
});
