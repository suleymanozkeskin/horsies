import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";

import { entryPointsOf, manifestRefusedText } from "./package.ts";
import { PACKAGE_DIR } from "./tools.ts";

describe("entryPointsOf", () => {
  test("each export with a types file is an entry point; a plain path publishes no declarations", () => {
    const manifest = {
      name: "horsies",
      exports: {
        ".": { types: "./dist/index.d.ts", default: "./dist/index.js" },
        "./zod": { types: "./dist/zod.d.ts", default: "./dist/zod.js" },
        "./package.json": "./package.json",
      },
    };

    expect(entryPointsOf(manifest)).toEqual({
      ok: true,
      value: [
        { specifier: "horsies", typesFile: "dist/index.d.ts", reportName: "horsies" },
        { specifier: "horsies/zod", typesFile: "dist/zod.d.ts", reportName: "horsies-zod" },
      ],
    });
  });

  test("the package's own package.json names its four entry points", () => {
    const entryPoints = entryPointsOf(JSON.parse(readFileSync(`${PACKAGE_DIR}package.json`, "utf8")));

    expect(entryPoints.ok && entryPoints.value.map((entry) => entry.specifier)).toEqual([
      "horsies",
      "horsies/zod",
      "horsies/rerun/pg",
      "horsies/rerun/postgres",
    ]);
  });

  test("NoName: a manifest without a name", () => {
    expect(entryPointsOf({ exports: {} })).toEqual({ ok: false, error: { kind: "NoName" } });
    expect(entryPointsOf([])).toEqual({ ok: false, error: { kind: "NoName" } });
  });

  test("NoExports: a manifest without an exports map", () => {
    expect(entryPointsOf({ name: "horsies", exports: "./dist/index.js" })).toEqual({
      ok: false,
      error: { kind: "NoExports" },
    });
  });

  test("TargetForm: an export without a types file, or a path outside the ./ form", () => {
    for (const exportsMap of [
      { ".": { default: "./dist/index.js" } },
      { ".": { types: "dist/index.d.ts" } },
      { zod: { types: "./dist/zod.d.ts" } },
    ]) {
      const refused = entryPointsOf({ name: "horsies", exports: exportsMap });

      expect(refused.ok).toBe(false);
      expect(!refused.ok && refused.error.kind).toBe("TargetForm");
      expect(!refused.ok && manifestRefusedText(refused.error)).toContain("names no");
    }
  });
});
