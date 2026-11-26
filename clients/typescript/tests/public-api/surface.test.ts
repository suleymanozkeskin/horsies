import { afterAll, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { BuiltPackage } from "./package.ts";
import { anyPlaces, declarationProgram, SURFACE_BOUNDS, surfaceStatements, unknownPlaces } from "./surface.ts";

// A small declaration package: `index.d.ts` re-exports `api.d.ts`, which names `Helper` of `helper.d.ts`. `Unused`
// is in a package file but no entry point reaches it.
const FILES: Readonly<Record<string, string>> = {
  "index.d.ts": 'export * from "./api.ts";\n',
  "api.d.ts": [
    'import type { Helper } from "./helper.ts";',
    "export type AnyThing = Box<unknown>;",
    "export interface Box<T> {",
    "    readonly value: T;",
    "    read(input: Helper): string;",
    "}",
    "",
  ].join("\n"),
  "helper.d.ts": [
    "export type Helper = { readonly raw: unknown };",
    "export type Unused = { readonly raw: unknown };",
    "export declare function loose(): any;",
    "",
  ].join("\n"),
};

const folder = realpathSync(mkdtempSync(join(tmpdir(), "horsies-surface-test-")));

mkdirSync(join(folder, "dist"));

for (const [name, text] of Object.entries(FILES)) {
  writeFileSync(join(folder, "dist", name), text);
}

const built: BuiltPackage = {
  folder,
  entryPoints: [{ specifier: "probe", typesFile: "dist/index.d.ts", reportName: "probe" }],
};

const program = declarationProgram(built);

afterAll(() => {
  rmSync(folder, { recursive: true, force: true });
});

describe("the top types of a declaration package", () => {
  test("unknown: each place of the public surface, with its path and form; an unreached declaration is not public", () => {
    const places = unknownPlaces(program, built);

    expect(places.ok && places.value.map(({ path, form, file, line }) => ({ path, form, file, line }))).toEqual([
      { path: "AnyThing", form: "Box<unknown>", file: "dist/api.d.ts", line: 2 },
      { path: "Helper.raw", form: "unknown", file: "dist/helper.d.ts", line: 1 },
    ]);
  });

  test("any: each place in every declaration file of the package, reached or not", () => {
    const places = anyPlaces(program, built);

    expect(places).toEqual({
      ok: true,
      value: [{ keyword: "any", path: "loose", form: "any", file: "dist/helper.d.ts", line: 3 }],
    });
  });

  test("EntryNotFound: an entry point whose declaration file the program does not hold", () => {
    const missing: BuiltPackage = {
      folder,
      entryPoints: [{ specifier: "probe", typesFile: "dist/missing.d.ts", reportName: "probe" }],
    };

    expect(surfaceStatements(program, missing)).toEqual({
      ok: false,
      error: { kind: "EntryNotFound", file: `${folder}/dist/missing.d.ts` },
    });
  });

  test("TooManyDeclarations: the surface has more declarations than the bound", () => {
    expect(surfaceStatements(program, built, { ...SURFACE_BOUNDS, declarations: 1 })).toEqual({
      ok: false,
      error: { kind: "TooManyDeclarations", bound: 1 },
    });
  });

  test("TooDeep: a syntax tree is deeper than the bound", () => {
    const walked = unknownPlaces(program, built, { ...SURFACE_BOUNDS, depth: 1 });

    expect(!walked.ok && walked.error.kind).toBe("TooDeep");
  });

  test("TooManyNodes: a syntax tree has more nodes than the bound", () => {
    const walked = anyPlaces(program, built, { ...SURFACE_BOUNDS, nodes: 2 });

    expect(!walked.ok && walked.error.kind).toBe("TooManyNodes");
  });
});
