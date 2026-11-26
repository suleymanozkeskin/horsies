// The content checks of the packed tarballs: the files that each one holds and the manifest that it carries. Each
// check gives the list of its problems; an empty list passes.

import { isArray, isPlainObject, isString } from "../../horsies/src/codec/values.ts";
import { PACKAGE_SKILL_FOLDER, SKILL_FILE_NAMES } from "../../horsies/src/tools/skills.ts";
import type { ParsedJson } from "../public-api/package.ts";
import { NAPI_CROSS_GLIBC_FLOOR } from "./glibc-floor.ts";
import type { JsonObject } from "./manifest.ts";
import { platformPackageOf } from "./platforms.ts";
import type { CLibrary, NpmBuild, ReleasePlatform } from "./platforms.ts";

/** The folder of the skill files in the package: the folder that `horsies skills install` copies from. */
export const SKILL_FOLDER: string = PACKAGE_SKILL_FOLDER;

/** The skill files that the Python and the Rust packages ship, by name: the files that `skills install` copies. */
export const SKILL_FILES: readonly string[] = SKILL_FILE_NAMES;

/** The command that the `bin` field of `horsies` names: the tool with `get-docs` and `skills install`. */
export const BIN_NAME = "horsies";

/** The skill folder of the package source: not written yet, or the names of its files. */
export type SkillFolder = { readonly kind: "notWritten" } | { readonly kind: "written"; readonly files: readonly string[] };

/** The deepest nesting of conditions in an exports map that the check reads. */
const EXPORTS_DEPTH_BOUND = 4;

/** The file paths that an exports map names (each `./` target, without the `./`), in order. Pure. */
export function exportTargets(exportsMap: ParsedJson | undefined): readonly string[] {
  const targets: string[] = [];

  const visit = (value: ParsedJson | undefined, depth: number): void => {
    if (isString(value)) {
      targets.push(value.startsWith("./") ? value.slice(2) : value);
    } else if (isPlainObject(value) && depth < EXPORTS_DEPTH_BOUND) {
      for (const member of Object.values(value)) {
        // SAFETY: the members of a parsed JSON object are parsed JSON.
        visit(member as ParsedJson, depth + 1);
      }
    }
  };

  visit(exportsMap, 0);

  return targets;
}

/** The problems of the paths that the `horsies` tarball holds. Pure. */
export function horsiesFileProblems(files: readonly string[], targets: readonly string[], skills: SkillFolder): readonly string[] {
  const problems = [
    ...targets.filter((target) => !files.includes(target)).map((target) => `the exports map names ${target}, which is not packed`),
    ...files.filter((file) => file.endsWith(".node")).map((file) => `holds the addon file ${file}`),
    ...files.filter((file) => file.startsWith("src/")).map((file) => `holds the source file ${file}`),
    ...files.filter((file) => file.includes("dev-addon")).map((file) => `holds the dev-only entry ${file}`),
    ...files.filter((file) => file.endsWith(".tsbuildinfo")).map((file) => `holds the build info ${file}`),
  ];

  return [...problems, ...skillProblems(files, skills)];
}

/** The problems of the packed skill files against the source folder. Pure. */
function skillProblems(files: readonly string[], skills: SkillFolder): readonly string[] {
  const packed = files.filter((file) => file.startsWith(`${SKILL_FOLDER}/`)).map((file) => file.slice(SKILL_FOLDER.length + 1));

  switch (skills.kind) {
    case "notWritten":
      return packed.map((file) => `holds the skill file ${file}, and the source has no skill folder`);
    case "written": {
      const missing = SKILL_FILES.filter((name) => !skills.files.includes(name));
      const unpacked = skills.files.filter((name) => !packed.includes(name));

      return [
        ...missing.map((name) => `the skill folder of the source has no ${name}`),
        ...unpacked.map((name) => `the skill file ${name} is not packed`),
      ];
    }
  }
}

/**
 * The problems of the `bin` field of the packed manifest: it must name the command `horsies`, and the tarball must hold
 * the file that it names. Pure.
 */
export function binFileProblems(files: readonly string[], binField: ParsedJson | undefined): readonly string[] {
  const target = isPlainObject(binField) ? binField[BIN_NAME] : undefined;

  if (!isString(target)) {
    return [`the manifest has no bin ${BIN_NAME}`];
  }

  const file = target.startsWith("./") ? target.slice(2) : target;

  return files.includes(file) ? [] : [`the bin ${BIN_NAME} names ${file}, which is not packed`];
}

/** Whether two JSON values are equal, with object members in any order. Pure. */
export function sameJson(left: ParsedJson | undefined, right: ParsedJson | undefined): boolean {
  return JSON.stringify(sortedJson(left)) === JSON.stringify(sortedJson(right));
}

function sortedJson(value: ParsedJson | undefined): ParsedJson | undefined {
  if (isArray(value)) {
    // SAFETY: the elements of a parsed JSON array are parsed JSON.
    return value.map((element) => sortedJson(element as ParsedJson) ?? null);
  }

  if (isPlainObject(value)) {
    // SAFETY: the members of a parsed JSON object are parsed JSON.
    const entries = Object.entries(value as JsonObject).toSorted(([left], [right]) => left.localeCompare(right));

    return Object.fromEntries(entries.map(([key, member]) => [key, sortedJson(member) ?? null]));
  }

  return value;
}

/** The problems of the manifest that the `horsies` tarball carries. Pure. */
export function horsiesManifestProblems(
  manifest: JsonObject,
  version: string,
  optionalDependencies: Readonly<Record<string, string>>,
): readonly string[] {
  const problems: string[] = [];

  if (manifest["version"] !== version) {
    problems.push(`the packed version is ${JSON.stringify(manifest["version"])}, not ${version}`);
  }

  if ("private" in manifest) {
    problems.push("the packed manifest has the field private, so npm refuses to publish it");
  }

  if (!sameJson(manifest["optionalDependencies"], optionalDependencies)) {
    problems.push(
      `the optional dependencies are ${JSON.stringify(manifest["optionalDependencies"])}, not ${JSON.stringify(optionalDependencies)}`,
    );
  }

  return [...problems, ...publicAccessProblems(manifest)];
}

function publicAccessProblems(manifest: JsonObject): readonly string[] {
  const publishConfig = manifest["publishConfig"];

  return isPlainObject(publishConfig) && publishConfig["access"] === "public"
    ? []
    : ["publishConfig.access is not public: a scoped package publishes as restricted"];
}

/** The npm `libc` field of a platform package: its one value, or no field (macOS). */
export type LibcField = { readonly kind: "field"; readonly value: "glibc" | "musl" } | { readonly kind: "noField" };

/** The highest glibc version that a platform's addon file may need: the glibc of its build, or none (not glibc). */
export type GlibcFloor = { readonly kind: "floor"; readonly version: readonly number[] } | { readonly kind: "noFloor" };

/** What a platform package must be. */
export type PlatformExpectation = {
  readonly packageName: string;
  readonly version: string;
  readonly os: "linux" | "darwin";
  readonly cpu: string;
  readonly libc: LibcField;
  readonly glibcFloor: GlibcFloor;
  readonly binary: string;
};

/** The glibc floor of a build form: the napi cross toolchain gives 2.17, a manylinux image its glibc. Pure. */
export function glibcFloorOf(build: NpmBuild): GlibcFloor {
  switch (build.kind) {
    case "glibc-2.17-toolchain":
      return { kind: "floor", version: NAPI_CROSS_GLIBC_FLOOR };
    case "manylinux-container":
      return { kind: "floor", version: build.glibcFloor };
    case "alpine-container":
    case "runner":
      return { kind: "noFloor" };
  }
}

/** The binary file name of a platform: `horsies.<platform>.node` (`napi.binaryName` of `bridge/package.json`). Pure. */
export function binaryFileOf(row: ReleasePlatform): string {
  return `horsies.${row.platform}.node`;
}

function osAndLibcOf(cLibrary: CLibrary): Pick<PlatformExpectation, "os" | "libc"> {
  switch (cLibrary) {
    case "glibc":
      return { os: "linux", libc: { kind: "field", value: "glibc" } };
    case "musl":
      return { os: "linux", libc: { kind: "field", value: "musl" } };
    case "libsystem":
      return { os: "darwin", libc: { kind: "noField" } };
  }
}

/** The expectation of a row's platform package at `version`. Pure. */
export function platformExpectationOf(row: ReleasePlatform, version: string): PlatformExpectation {
  return {
    packageName: platformPackageOf(row),
    version,
    cpu: row.cpu,
    binary: binaryFileOf(row),
    glibcFloor: glibcFloorOf(row.build),
    ...osAndLibcOf(row.cLibrary),
  };
}

/** The files that a platform tarball holds: exactly the manifest, the README, the license and the binary. */
export function platformFileProblems(files: readonly string[], expected: PlatformExpectation): readonly string[] {
  const wanted = ["package.json", "README.md", "LICENSE", expected.binary];

  return [
    ...wanted.filter((file) => !files.includes(file)).map((file) => `${expected.packageName} does not hold ${file}`),
    ...files.filter((file) => !wanted.includes(file)).map((file) => `${expected.packageName} holds ${file}, which it must not`),
  ];
}

/** The problems of the manifest that a platform tarball carries. Pure. */
export function platformManifestProblems(manifest: JsonObject, expected: PlatformExpectation): readonly string[] {
  const libc = expected.libc.kind === "field" ? [expected.libc.value] : undefined;

  const fields: readonly (readonly [string, ParsedJson | undefined])[] = [
    ["name", expected.packageName],
    ["version", expected.version],
    ["os", [expected.os]],
    ["cpu", [expected.cpu]],
    ["libc", libc],
    ["main", expected.binary],
    ["files", [expected.binary]],
  ];

  const problems = fields
    .filter(([field, value]) => !sameJson(manifest[field], value))
    .map(([field, value]) => `${expected.packageName}: ${field} is ${JSON.stringify(manifest[field])}, not ${JSON.stringify(value)}`);

  return [...problems, ...publicAccessProblems(manifest).map((problem) => `${expected.packageName}: ${problem}`)];
}
