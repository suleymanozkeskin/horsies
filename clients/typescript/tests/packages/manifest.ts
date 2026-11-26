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

/** The manifest of `horsies`: its version, the one version of the release, and the whole document. */
export type HorsiesManifest = { readonly version: string; readonly document: JsonObject };

export type ManifestRefused =
  | { readonly kind: "NotAnObject" }
  | { readonly kind: "NoVersion" }
  | { readonly kind: "VersionForm"; readonly version: string };

/** A version of the npm form `<major>.<minor>.<patch>`, with an optional pre-release part. */
const VERSION_FORM = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/u;

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

  // SAFETY: the members of a parsed JSON object are parsed JSON.
  return { ok: true, value: { version, document: document as JsonObject } };
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

/** The manifest that the packed `horsies` carries: the committed manifest with the optional dependencies. Pure. */
export function packedManifestOf(manifest: HorsiesManifest, rows: readonly ReleasePlatform[]): JsonObject {
  return { ...manifest.document, optionalDependencies: optionalDependenciesOf(rows, manifest.version) };
}
