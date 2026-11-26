// The declarations of the package as it is published: written by the TypeScript 7 compiler, laid out as the package
// (`package.json` and `dist/`), with the published entry points of its `exports` map. The exports map allows no deep
// import, so the entry points are the whole public surface.

import { copyFileSync, mkdirSync, readFileSync, symlinkSync } from "node:fs";
import { join } from "node:path";

import { isPlainObject, isString } from "../../horsies/src/codec/values.ts";
import type { Result } from "../../horsies/src/wire/result.ts";
import { CURRENT_COMPILER, PACKAGE_DIR, runTool, type ToolFailed, TESTS_DIR, WORKSPACE_DIR } from "./tools.ts";

/** A parsed JSON document. */
export type ParsedJson =
  | null
  | boolean
  | number
  | string
  | readonly ParsedJson[]
  | { readonly [key: string]: ParsedJson };

/** A published entry point: the import specifier, the declaration file of its `types` condition, its report name. */
export type EntryPoint = {
  readonly specifier: string;
  readonly typesFile: string;
  readonly reportName: string;
};

/** A `package.json` that does not name its entry points in the form that this gate reads. */
export type ManifestRefused =
  | { readonly kind: "NoName" }
  | { readonly kind: "NoExports" }
  | { readonly kind: "TargetForm"; readonly subpath: string };

/**
 * The entry points of a `package.json`: each subpath of `exports` whose target names a `types` file. A subpath whose
 * target is a plain path (`./package.json`) publishes no declarations. Pure.
 */
export function entryPointsOf(manifest: ParsedJson): Result<readonly EntryPoint[], ManifestRefused> {
  if (!isPlainObject(manifest) || !isString(manifest["name"])) {
    return { ok: false, error: { kind: "NoName" } };
  }

  const name = manifest["name"];
  const exportsMap = manifest["exports"];

  if (!isPlainObject(exportsMap)) {
    return { ok: false, error: { kind: "NoExports" } };
  }

  const entryPoints: EntryPoint[] = [];

  for (const [subpath, target] of Object.entries(exportsMap)) {
    if (isString(target)) {
      continue;
    }

    const typesFile = isPlainObject(target) ? target["types"] : undefined;

    if (!isString(typesFile) || !typesFile.startsWith("./") || !(subpath === "." || subpath.startsWith("./"))) {
      return { ok: false, error: { kind: "TargetForm", subpath } };
    }

    const specifier = subpath === "." ? name : `${name}/${subpath.slice(2)}`;

    entryPoints.push({ specifier, typesFile: typesFile.slice(2), reportName: specifier.replaceAll("/", "-") });
  }

  return { ok: true, value: entryPoints };
}

/** The text of a refused `package.json`, for a message. */
export function manifestRefusedText(refusal: ManifestRefused): string {
  switch (refusal.kind) {
    case "NoName":
      return "horsies/package.json has no name";
    case "NoExports":
      return "horsies/package.json has no exports map";
    case "TargetForm":
      return `horsies/package.json: the export ${refusal.subpath} names no "types" file of the form ./<path>`;
  }
}

/** The declarations of the package, laid out as the published package in `folder`. */
export type BuiltPackage = { readonly folder: string; readonly entryPoints: readonly EntryPoint[] };

export type BuildFailed =
  | { readonly kind: "CompilerNotRun"; readonly failure: ToolFailed }
  | { readonly kind: "CompilerErrors"; readonly output: string }
  | { readonly kind: "ManifestRefused"; readonly refusal: ManifestRefused };

/**
 * Writes the declarations of the package into `folder` (an empty folder that the caller owns and removes): the
 * TypeScript 7 compiler emits `dist/` from `horsies/tsconfig.json`, `package.json` is copied, and `node_modules/zod`
 * links the workspace's zod, the optional peer that the `horsies/zod` declarations import. It does not pack a tarball:
 * the npm package checks pack and check it.
 */
export function buildPackage(folder: string): Result<BuiltPackage, BuildFailed> {
  const manifestText = readFileSync(`${PACKAGE_DIR}package.json`, "utf8");
  const entryPoints = entryPointsOf(JSON.parse(manifestText));

  if (!entryPoints.ok) {
    return { ok: false, error: { kind: "ManifestRefused", refusal: entryPoints.error } };
  }

  const emit = runTool(
    CURRENT_COMPILER,
    [
      "--project",
      `${PACKAGE_DIR}tsconfig.json`,
      "--noEmit",
      "false",
      "--emitDeclarationOnly",
      "--rootDir",
      `${PACKAGE_DIR}src`,
      "--outDir",
      join(folder, "dist"),
      "--pretty",
      "false",
    ],
    WORKSPACE_DIR,
  );

  if (!emit.ok) {
    return { ok: false, error: { kind: "CompilerNotRun", failure: emit.error } };
  }

  if (emit.value.code !== 0) {
    return { ok: false, error: { kind: "CompilerErrors", output: emit.value.output } };
  }

  copyFileSync(`${PACKAGE_DIR}package.json`, join(folder, "package.json"));
  mkdirSync(join(folder, "node_modules"));
  symlinkSync(`${TESTS_DIR}node_modules/zod`, join(folder, "node_modules", "zod"), "dir");

  return { ok: true, value: { folder, entryPoints: entryPoints.value } };
}
