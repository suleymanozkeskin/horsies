// The manifest of the `horsies` package (`horsies/package.json`) as the npm package checks read it, and the manifest
// that the packed package carries.

import { readFileSync } from "node:fs";

import { isPlainObject, isString } from "../../horsies/src/codec/values.ts";
import type { Result } from "../../horsies/src/wire/result.ts";
import type { ParsedJson } from "../public-api/package.ts";
import { PACKAGE_DIR } from "../public-api/tools.ts";
import { platformPackageOf } from "./platforms.ts";
import type { ReleasePlatform } from "./platforms.ts";

/** A JSON object. */
export type JsonObject = Readonly<Record<string, ParsedJson>>;

/**
 * The `imports` map of the packed `horsies`: the committed manifest has none, or its map with each source target
 * `./src/<path>.ts` written as the built file `./dist/<path>.js`.
 */
export type PackedImports = { readonly kind: "none" } | { readonly kind: "map"; readonly map: JsonObject };

/** The manifest of `horsies`: its version, the one version of the release, the whole document, and the packed imports. */
export type HorsiesManifest = { readonly version: string; readonly document: JsonObject; readonly packedImports: PackedImports };

export type ManifestRefused =
  | { readonly kind: "NotAnObject" }
  | { readonly kind: "NoVersion" }
  | { readonly kind: "VersionForm"; readonly version: string }
  | { readonly kind: "ImportsTarget"; readonly specifier: string; readonly target: string };

/** A version of the npm form `<major>.<minor>.<patch>`, with an optional pre-release part. */
const VERSION_FORM = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/u;

/** A target of the committed `imports` map: a TypeScript file of `src/`. The match is the path without the extension. */
const SOURCE_TARGET_FORM = /^\.\/src\/(.+)\.ts$/u;

/** The deepest nesting of conditions in the `imports` map that the reader reads. */
const IMPORTS_DEPTH_BOUND = 4;

/** The manifest of `horsies` from its document: an object with a version of the npm form. Pure. */
export function horsiesManifestOf(document: ParsedJson): Result<HorsiesManifest, ManifestRefused> {
  if (!isPlainObject(document)) {
    return { ok: false, error: { kind: "NotAnObject" } };
  }

  const version = document["version"];

  if (!isString(version)) {
    return { ok: false, error: { kind: "NoVersion" } };
  }

  if (!VERSION_FORM.test(version)) {
    return { ok: false, error: { kind: "VersionForm", version } };
  }

  const imports = document["imports"];

  if (imports === undefined) {
    // SAFETY: the members of a parsed JSON object are parsed JSON.
    return { ok: true, value: { version, document: document as JsonObject, packedImports: { kind: "none" } } };
  }

  // SAFETY: a member of a parsed JSON object is parsed JSON.
  const packed = packedImportsOf(imports as ParsedJson);

  if (!packed.ok) {
    return packed;
  }

  // SAFETY: the members of a parsed JSON object are parsed JSON.
  return { ok: true, value: { version, document: document as JsonObject, packedImports: { kind: "map", map: packed.value } } };
}

/**
 * The packed `imports` map: each target `./src/<path>.ts` as `./dist/<path>.js` (the file that `tsconfig.build.json`
 * writes from it), through conditions nested at most `IMPORTS_DEPTH_BOUND` deep. Another target, or a deeper map, is
 * `ImportsTarget`. Pure.
 */
export function packedImportsOf(imports: ParsedJson): Result<JsonObject, ManifestRefused> {
  if (!isPlainObject(imports)) {
    return { ok: false, error: { kind: "ImportsTarget", specifier: "imports", target: JSON.stringify(imports) } };
  }

  const packed: Record<string, ParsedJson> = {};

  for (const [specifier, target] of Object.entries(imports)) {
    // SAFETY: the members of a parsed JSON object are parsed JSON.
    const written = packedTargetOf(specifier, target as ParsedJson, 1);

    if (!written.ok) {
      return written;
    }

    packed[specifier] = written.value;
  }

  return { ok: true, value: packed };
}

/** One target of the `imports` map, packed; `depth` is its nesting. Pure. */
function packedTargetOf(specifier: string, target: ParsedJson, depth: number): Result<ParsedJson, ManifestRefused> {
  const refused: Result<ParsedJson, ManifestRefused> = { ok: false, error: { kind: "ImportsTarget", specifier, target: JSON.stringify(target) } };

  if (isString(target)) {
    const source = SOURCE_TARGET_FORM.exec(target);

    return source === null ? refused : { ok: true, value: `./dist/${source[1] ?? ""}.js` };
  }

  if (!isPlainObject(target) || depth >= IMPORTS_DEPTH_BOUND) {
    return refused;
  }

  const packed: Record<string, ParsedJson> = {};

  for (const [condition, member] of Object.entries(target)) {
    // SAFETY: the members of a parsed JSON object are parsed JSON.
    const written = packedTargetOf(specifier, member as ParsedJson, depth + 1);

    if (!written.ok) {
      return written;
    }

    packed[condition] = written.value;
  }

  return { ok: true, value: packed };
}

/** The text of a refused manifest, for a message. Pure. */
export function manifestRefusedText(refusal: ManifestRefused): string {
  switch (refusal.kind) {
    case "NotAnObject":
      return "horsies/package.json is not a JSON object";
    case "NoVersion":
      return "horsies/package.json has no version";
    case "VersionForm":
      return `horsies/package.json: the version ${JSON.stringify(refusal.version)} is not of the form <major>.<minor>.<patch>`;
    case "ImportsTarget":
      return `horsies/package.json: the import ${refusal.specifier} has the target ${refusal.target}, not ./src/<path>.ts or conditions of such targets`;
  }
}

/** Read `horsies/package.json`. Effects: reads the file; a file that is not JSON throws (a defect of the repository). */
export function readHorsiesManifest(): Result<HorsiesManifest, ManifestRefused> {
  // SAFETY: JSON.parse gives a parsed JSON document.
  return horsiesManifestOf(JSON.parse(readFileSync(`${PACKAGE_DIR}package.json`, "utf8")) as ParsedJson);
}

/**
 * The optional dependencies of the packed `horsies`: the platform package of each row that the release publishes, at
 * the one version. A held row is not listed. Pure.
 */
export function optionalDependenciesOf(rows: readonly ReleasePlatform[], version: string): Readonly<Record<string, string>> {
  return Object.fromEntries(rows.filter((row) => row.release === "publish").map((row) => [platformPackageOf(row), version]));
}

/**
 * The manifest that the packed `horsies` carries: the committed manifest without the field `private`, with the packed
 * `imports` map (built files of `dist/` in place of the source files) and the optional dependencies. The committed file
 * stays private until the release, so no publish from the source folder can happen; the packed tarball is the
 * publishable form that the checks read. Pure.
 */
export function packedManifestOf(manifest: HorsiesManifest, rows: readonly ReleasePlatform[]): JsonObject {
  return {
    ...withoutPrivate(manifest.document),
    ...packedImportsMember(manifest.packedImports),
    optionalDependencies: optionalDependenciesOf(rows, manifest.version),
  };
}

/** The `imports` member of the packed manifest: none, or the packed map. Pure. */
function packedImportsMember(imports: PackedImports): JsonObject {
  switch (imports.kind) {
    case "none":
      return {};
    case "map":
      return { imports: imports.map };
  }
}

/** The manifest without the field `private`: the publishable form of a committed manifest. Pure. */
export function withoutPrivate(document: JsonObject): JsonObject {
  return Object.fromEntries(Object.entries(document).filter(([field]) => field !== "private"));
}

/** The manifest of the lint package: a JSON object, or `undefined`. Pure. */
export function lintManifestOf(document: ParsedJson): JsonObject | undefined {
  // SAFETY: the members of a parsed JSON object are parsed JSON.
  return isPlainObject(document) ? (document as JsonObject) : undefined;
}
