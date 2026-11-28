// The platform list of the release (`.github/release-platforms.json`) as the npm package checks read it: the rows,
// their npm fields, and the matrices of `typescript-packages.yml`. The wheel fields belong to release.yml; this reader
// checks only that each row has them.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { isArray, isPlainObject, isString } from "../../horsies/src/codec/values.ts";
import { platformPackageName } from "../../horsies/src/native/addon-file.ts";
import type { NativePlatform } from "../../horsies/src/native/addon-file.ts";
import type { Result } from "../../horsies/src/wire/result.ts";
import type { ParsedJson } from "../public-api/package.ts";

/** The committed platform list. */
export const PLATFORM_LIST_FILE: string = fileURLToPath(new URL("../../../../.github/release-platforms.json", import.meta.url));

/** What the release does with a row: see the `$comment` of the platform list. */
export type ReleaseState = "publish" | "hold";

/** The C library of a row's platform. */
export type CLibrary = "glibc" | "musl" | "libsystem";

/** The npm `cpu` value of a platform package. */
export type NpmCpu = "x64" | "arm64";

/**
 * How a row builds its addon. A glibc row builds with the glibc 2.17 toolchain of napi or in a manylinux image (the
 * image of the row's wheel; `glibcFloor` is its glibc); a musl row in an Alpine container; a macOS row on the runner.
 */
export type NpmBuild =
  | { readonly kind: "glibc-2.17-toolchain" }
  | { readonly kind: "manylinux-container"; readonly image: string; readonly glibcFloor: readonly number[] }
  | { readonly kind: "alpine-container"; readonly nodeVariant: string }
  | { readonly kind: "runner" };

/** Where a row's load and install checks run. */
export type NpmInstall =
  | { readonly kind: "container"; readonly nodeVariant: string; readonly bunVariant: string }
  | { readonly kind: "runner" };

/** One row of the platform list, with its npm fields. */
export type ReleasePlatform = {
  readonly name: string;
  readonly release: ReleaseState;
  readonly runner: string;
  readonly rustTarget: string;
  readonly cLibrary: CLibrary;
  readonly platform: NativePlatform;
  readonly cpu: NpmCpu;
  readonly build: NpmBuild;
  readonly install: NpmInstall;
};

/** Why the platform list does not read. */
export type PlatformListRefused =
  | { readonly kind: "NoPlatforms" }
  | { readonly kind: "RowForm"; readonly index: number; readonly field: string }
  | { readonly kind: "UnknownValue"; readonly row: string; readonly field: string; readonly value: string }
  | { readonly kind: "PlatformMismatch"; readonly row: string; readonly platform: string; readonly expected: string }
  | { readonly kind: "BuildMismatch"; readonly row: string; readonly build: NpmBuild["kind"]; readonly cLibrary: CLibrary }
  | { readonly kind: "RepeatedName"; readonly row: string }
  | { readonly kind: "RepeatedPlatform"; readonly platform: string };

const RELEASE_STATES: Readonly<Record<ReleaseState, true>> = { publish: true, hold: true };
const C_LIBRARIES: Readonly<Record<CLibrary, true>> = { glibc: true, musl: true, libsystem: true };
const NPM_CPUS: Readonly<Record<NpmCpu, true>> = { x64: true, arm64: true };

/** Every platform that the loader maps a host to (`nativePlatform` of `load.ts`). */
export const NATIVE_PLATFORMS: Readonly<Record<NativePlatform, true>> = {
  "darwin-arm64": true,
  "linux-arm64-gnu": true,
  "linux-arm64-musl": true,
  "linux-x64-gnu": true,
  "linux-x64-musl": true,
};

/** The fields of a row, in the order of the file. */
const ROW_FIELDS: readonly string[] = ["name", "release", "runner", "rust_target", "c_library", "wheel", "npm"];

/** Whether `value` is one of the keys of the closed map `known`. Pure. */
function isMember<K extends string>(known: Readonly<Record<K, true>>, value: string): value is K {
  return Object.hasOwn(known, value);
}

/** The non-empty text fields `fields` of `object`, or the refusal that names the first one that is not. Pure. */
function textFields<F extends string>(
  object: Readonly<Record<string, ParsedJson>>,
  fields: readonly F[],
  index: number,
  prefix: string,
): Result<Readonly<Record<F, string>>, PlatformListRefused> {
  const texts: Partial<Record<F, string>> = {};

  for (const field of fields) {
    const value = object[field];

    if (!isString(value) || value.length === 0) {
      return { ok: false, error: { kind: "RowForm", index, field: `${prefix}${field}` } };
    }

    texts[field] = value;
  }

  // SAFETY: the loop above set every field of `fields`.
  return { ok: true, value: texts as Readonly<Record<F, string>> };
}

/** The napi platform name of a C library and a CPU. Pure. */
export function expectedPlatform(cLibrary: CLibrary, cpu: NpmCpu): string {
  switch (cLibrary) {
    case "libsystem":
      return `darwin-${cpu}`;
    case "glibc":
      return `linux-${cpu}-gnu`;
    case "musl":
      return `linux-${cpu}-musl`;
  }
}

/** The npm package name of a row's platform package. Pure. */
export function platformPackageOf(row: ReleasePlatform): string {
  return platformPackageName(row.platform);
}

/** The text of a refused platform list, for a message. Pure. */
export function platformListRefusedText(refusal: PlatformListRefused): string {
  const file = ".github/release-platforms.json";

  switch (refusal.kind) {
    case "NoPlatforms":
      return `${file} has no list of platforms`;
    case "RowForm":
      return `${file}: row ${refusal.index} has no valid field ${refusal.field}`;
    case "UnknownValue":
      return `${file}: row ${refusal.row}: ${refusal.field} ${JSON.stringify(refusal.value)} is not a known value`;
    case "PlatformMismatch":
      return `${file}: row ${refusal.row}: npm.platform is ${refusal.platform}; its C library and CPU give ${refusal.expected}`;
    case "BuildMismatch":
      return `${file}: row ${refusal.row}: the build ${refusal.build} does not build for the C library ${refusal.cLibrary}`;
    case "RepeatedName":
      return `${file}: the name ${refusal.row} is on two rows`;
    case "RepeatedPlatform":
      return `${file}: the npm platform ${refusal.platform} is on two rows`;
  }
}

/** Read the committed platform list. Effects: reads the file; a file that is not JSON throws (a defect of the repository). */
export function readPlatformList(): Result<readonly ReleasePlatform[], PlatformListRefused> {
  // SAFETY: JSON.parse gives a parsed JSON document.
  return platformListOf(JSON.parse(readFileSync(PLATFORM_LIST_FILE, "utf8")) as ParsedJson);
}

/** The rows of a platform list document: every row valid, no name and no npm platform twice. Pure. */
export function platformListOf(document: ParsedJson): Result<readonly ReleasePlatform[], PlatformListRefused> {
  const entries = isPlainObject(document) ? document["platforms"] : undefined;

  if (!isArray(entries) || entries.length === 0) {
    return { ok: false, error: { kind: "NoPlatforms" } };
  }

  const rows: ReleasePlatform[] = [];

  for (const [index, entry] of entries.entries()) {
    // SAFETY: an element of a parsed JSON array is parsed JSON.
    const row = platformRowOf(entry as ParsedJson, index);

    if (!row.ok) {
      return row;
    }

    if (rows.some((known) => known.name === row.value.name)) {
      return { ok: false, error: { kind: "RepeatedName", row: row.value.name } };
    }

    if (rows.some((known) => known.platform === row.value.platform)) {
      return { ok: false, error: { kind: "RepeatedPlatform", platform: row.value.platform } };
    }

    rows.push(row.value);
  }

  return { ok: true, value: rows };
}

/** One row of the list. Pure. */
function platformRowOf(entry: ParsedJson, index: number): Result<ReleasePlatform, PlatformListRefused> {
  if (!isPlainObject(entry)) {
    return { ok: false, error: { kind: "RowForm", index, field: "(the row)" } };
  }

  const extra = Object.keys(entry).find((key) => !ROW_FIELDS.includes(key));

  if (extra !== undefined) {
    return { ok: false, error: { kind: "RowForm", index, field: extra } };
  }

  const texts = textFields(entry, ["name", "release", "runner", "rust_target", "c_library"], index, "");

  if (!texts.ok) {
    return texts;
  }

  const { name, release, runner, rust_target: rustTarget, c_library: cLibrary } = texts.value;

  if (!isMember(RELEASE_STATES, release)) {
    return { ok: false, error: { kind: "UnknownValue", row: name, field: "release", value: release } };
  }

  if (!isMember(C_LIBRARIES, cLibrary)) {
    return { ok: false, error: { kind: "UnknownValue", row: name, field: "c_library", value: cLibrary } };
  }

  if (!isPlainObject(entry["wheel"])) {
    return { ok: false, error: { kind: "RowForm", index, field: "wheel" } };
  }

  const npm = npmFieldsOf(entry["npm"], name, index, cLibrary);

  return npm.ok ? { ok: true, value: { name, release, runner, rustTarget, cLibrary, ...npm.value } } : npm;
}

type NpmFields = Pick<ReleasePlatform, "platform" | "cpu" | "build" | "install">;

/** The npm fields of a row: platform and cpu agree with the C library. Pure. */
function npmFieldsOf(
  npm: ParsedJson | undefined,
  row: string,
  index: number,
  cLibrary: CLibrary,
): Result<NpmFields, PlatformListRefused> {
  if (npm === undefined || !isPlainObject(npm)) {
    return { ok: false, error: { kind: "RowForm", index, field: "npm" } };
  }

  const texts = textFields(npm, ["platform", "cpu"], index, "npm.");

  if (!texts.ok) {
    return texts;
  }

  const { platform, cpu } = texts.value;

  if (!isMember(NPM_CPUS, cpu)) {
    return { ok: false, error: { kind: "UnknownValue", row, field: "npm.cpu", value: cpu } };
  }

  if (!isMember(NATIVE_PLATFORMS, platform)) {
    return { ok: false, error: { kind: "UnknownValue", row, field: "npm.platform", value: platform } };
  }

  const expected = expectedPlatform(cLibrary, cpu);

  if (platform !== expected) {
    return { ok: false, error: { kind: "PlatformMismatch", row, platform, expected } };
  }

  const build = buildOf(npm["build"]);
  const install = installOf(npm["install"]);

  if (build === undefined) {
    return { ok: false, error: { kind: "RowForm", index, field: "npm.build" } };
  }

  if (!buildFits(build.kind, cLibrary)) {
    return { ok: false, error: { kind: "BuildMismatch", row, build: build.kind, cLibrary } };
  }

  if (install === undefined) {
    return { ok: false, error: { kind: "RowForm", index, field: "npm.install" } };
  }

  return { ok: true, value: { platform, cpu, build, install } };
}

/** Whether a build form builds for a C library. Pure. */
export function buildFits(build: NpmBuild["kind"], cLibrary: CLibrary): boolean {
  switch (build) {
    case "glibc-2.17-toolchain":
    case "manylinux-container":
      return cLibrary === "glibc";
    case "alpine-container":
      return cLibrary === "musl";
    case "runner":
      return cLibrary === "libsystem";
  }
}

/** A glibc version of the form `<major>.<minor>`. */
const GLIBC_VERSION_FORM = /^\d+\.\d+$/u;

/** The manylinux build form, or `undefined` when its image or glibc floor is not one. Pure. */
function manylinuxBuildOf(value: Readonly<Record<string, ParsedJson>>): NpmBuild | undefined {
  const image = value["image"];
  const floor = value["glibc_floor"];

  if (Object.keys(value).length !== 3 || !isString(image) || image.length === 0 || !isString(floor) || !GLIBC_VERSION_FORM.test(floor)) {
    return undefined;
  }

  return { kind: "manylinux-container", image, glibcFloor: floor.split(".").map(Number) };
}

/** The build form of a row, or `undefined` when it is not one. Pure. */
function buildOf(value: ParsedJson | undefined): NpmBuild | undefined {
  if (value === undefined || !isPlainObject(value)) {
    return undefined;
  }

  const kind = value["kind"];
  const nodeVariant = value["node_variant"];

  switch (kind) {
    case "glibc-2.17-toolchain":
    case "runner":
      return Object.keys(value).length === 1 ? { kind } : undefined;
    case "manylinux-container":
      // SAFETY: the members of a parsed JSON object are parsed JSON.
      return manylinuxBuildOf(value as Readonly<Record<string, ParsedJson>>);
    case "alpine-container":
      return Object.keys(value).length === 2 && isString(nodeVariant) && nodeVariant.length > 0
        ? { kind, nodeVariant }
        : undefined;
    default:
      return undefined;
  }
}

/** The install form of a row, or `undefined` when it is not one. Pure. */
function installOf(value: ParsedJson | undefined): NpmInstall | undefined {
  if (value === undefined || !isPlainObject(value)) {
    return undefined;
  }

  const kind = value["kind"];
  const nodeVariant = value["node_variant"];
  const bunVariant = value["bun_variant"];

  switch (kind) {
    case "runner":
      return Object.keys(value).length === 1 ? { kind } : undefined;
    case "container":
      return Object.keys(value).length === 3 && isString(nodeVariant) && nodeVariant.length > 0 && isString(bunVariant) && bunVariant.length > 0
        ? { kind, nodeVariant, bunVariant }
        : undefined;
    default:
      return undefined;
  }
}
