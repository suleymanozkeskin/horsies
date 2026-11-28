// The checks of the unpacked tarballs that read files: the text of `horsies`, its API report, and the addon file of
// each platform package. Each check gives the list of its problems; an empty list passes.

import { existsSync, mkdirSync, readFileSync, symlinkSync } from "node:fs";
import { join } from "node:path";

import { DEV_ADDON_PATH_VARIABLE } from "../../horsies/src/native/dev-addon.ts";
import { entryPointsOf } from "../public-api/package.ts";
import type { ParsedJson } from "../public-api/package.ts";
import { apiReport, compareReport, REPORT_DIR, reportFailedText, reportFileName } from "../public-api/report.ts";
import { TESTS_DIR, toolFailedText } from "../public-api/tools.ts";
import { testOnlyNamesInBytes } from "./addon-exports.ts";
import {
  binFileProblems,
  exportTargets,
  SKILL_FOLDER,
  horsiesFileProblems,
  horsiesManifestProblems,
  lintFileProblems,
  lintManifestProblems,
  platformFileProblems,
  platformManifestProblems,
} from "./content.ts";
import type { PlatformExpectation, SkillFolder } from "./content.ts";
import { compareVersions, glibcFloorProblems, glibcNeedsOf, readelfVersionInfo } from "./glibc-floor.ts";
import type { JsonObject } from "./manifest.ts";
import type { PackedTarball } from "./npm-pack.ts";

/** The manifest of an unpacked package folder. Effects: reads the file. */
export function unpackedManifest(packageDir: string): JsonObject {
  // SAFETY: npm packs a package.json that is a JSON object.
  return JSON.parse(readFileSync(join(packageDir, "package.json"), "utf8")) as JsonObject;
}

/** The packed files of `horsies` whose text names the variable of the dev-only entry. Effects: reads the files. */
function filesNamingDevVariable(packageDir: string, files: readonly string[]): readonly string[] {
  return files.filter((file) => readFileSync(join(packageDir, file), "latin1").includes(DEV_ADDON_PATH_VARIABLE));
}

/** What the `horsies` tarball must hold and carry. */
export type HorsiesExpectation = {
  readonly version: string;
  readonly optionalDependencies: Readonly<Record<string, string>>;
  readonly skills: SkillFolder;
};

/** The problems of the unpacked `horsies` tarball. Effects: reads the files; runs API Extractor in `workDir`. */
export function horsiesProblems(
  tarball: PackedTarball,
  packageDir: string,
  expected: HorsiesExpectation,
  workDir: string,
): readonly string[] {
  const manifest = unpackedManifest(packageDir);

  return [
    ...horsiesFileProblems(tarball.files, [...exportTargets(manifest["exports"]), ...exportTargets(manifest["imports"])], expected.skills),
    ...binFileProblems(tarball.files, manifest["bin"]),
    ...horsiesManifestProblems(manifest, expected.version, expected.optionalDependencies),
    ...filesNamingDevVariable(packageDir, tarball.files).map((file) => `${file} names ${DEV_ADDON_PATH_VARIABLE}`),
    ...apiReportProblems(packageDir, manifest, workDir),
  ];
}

/**
 * The problems of the API report of each entry point of the unpacked package against the committed report. Effects:
 * links the workspace's zod into the package (the optional peer that the `horsies/zod` declarations import); runs API
 * Extractor in `workDir`.
 */
function apiReportProblems(packageDir: string, manifest: JsonObject, workDir: string): readonly string[] {
  // SAFETY: a JSON object is parsed JSON.
  const entryPoints = entryPointsOf(manifest as ParsedJson);

  if (!entryPoints.ok) {
    return [`the packed manifest names no entry points: ${entryPoints.error.kind}`];
  }

  mkdirSync(join(packageDir, "node_modules"), { recursive: true });
  symlinkSync(`${TESTS_DIR}node_modules/zod`, join(packageDir, "node_modules", "zod"), "dir");
  mkdirSync(workDir, { recursive: true });

  const built = { folder: packageDir, entryPoints: entryPoints.value };

  return entryPoints.value.flatMap((entryPoint) => {
    const report = apiReport(built, entryPoint, workDir);

    if (!report.ok) {
      return [reportFailedText(entryPoint, report.error)];
    }

    const committedFile = join(REPORT_DIR, reportFileName(entryPoint));
    const committed = existsSync(committedFile) ? readFileSync(committedFile, "utf8") : undefined;
    const comparison = compareReport(committed, report.value.text);

    switch (comparison.state) {
      case "same":
        return [];
      case "notCommitted":
        return [`no committed API report for ${entryPoint.specifier}`];
      case "changed":
        return [
          `the API report of the packed ${entryPoint.specifier} differs from tests/public-api/report/${reportFileName(entryPoint)}: ` +
            `removed ${JSON.stringify(comparison.removed)}, added ${JSON.stringify(comparison.added)}`,
        ];
    }
  });
}

/** The problems of an addon file against the glibc floor `floor`. Effects: runs readelf. */
function glibcProblems(binaryFile: string, floor: readonly number[]): readonly string[] {
  const output = readelfVersionInfo(binaryFile);

  if (!output.ok) {
    switch (output.error.kind) {
      case "ToolFailed":
        return [`readelf did not run: ${toolFailedText(output.error.failure)}`];
      case "ReadelfRefused":
        return [`readelf ended with ${output.error.code}: ${output.error.output}`];
    }
  }

  return glibcFloorProblems(glibcNeedsOf(output.value), floor).map((problem) => `${binaryFile}: ${problem}`);
}

/**
 * The glibc floor problems of a platform's addon file: a glibc platform is read against the floor of its build; a
 * glibc platform with no floor is a problem; another platform has none. Effects: runs readelf for a glibc platform.
 */
function floorProblems(binaryFile: string, expected: PlatformExpectation): readonly string[] {
  const glibc = expected.libc.kind === "field" && expected.libc.value === "glibc";

  switch (expected.glibcFloor.kind) {
    case "floor":
      return glibc ? glibcProblems(binaryFile, expected.glibcFloor.version) : [];
    case "noFloor":
      return glibc ? [`${expected.packageName}: a glibc platform whose build has no glibc floor`] : [];
  }
}

/** The problems of one unpacked platform tarball. Effects: reads the addon file; runs readelf for a glibc build. */
export function platformProblems(tarball: PackedTarball, packageDir: string, expected: PlatformExpectation): readonly string[] {
  const binaryFile = join(packageDir, expected.binary);
  const bytes = existsSync(binaryFile) ? readFileSync(binaryFile) : new Uint8Array();

  const testOnly = testOnlyNamesInBytes(bytes).map(
    (name) => `${expected.packageName}: the addon holds the test-only export ${name}; it was built with test-probes`,
  );

  const floor = floorProblems(binaryFile, expected);

  return [
    ...platformFileProblems(tarball.files, expected),
    ...platformManifestProblems(unpackedManifest(packageDir), expected),
    ...testOnly,
    ...floor,
  ];
}

/** The problems of the unpacked lint tarball: its files, and its manifest at the version of the release. Effects: reads the manifest. */
export function lintProblems(tarball: PackedTarball, packageDir: string, version: string): readonly string[] {
  const manifest = unpackedManifest(packageDir);

  return [...lintFileProblems(tarball.files, exportTargets(manifest["exports"])), ...lintManifestProblems(manifest, version)];
}

/** The `horsies` tarball facts that the checks above verified, one log line. Pure. */
export function horsiesFacts(tarball: PackedTarball): string {
  const nodeFiles = tarball.files.filter((file) => file.endsWith(".node"));
  const skillFiles = tarball.files.filter((file) => file.startsWith(`${SKILL_FOLDER}/`));
  const present = (file: string): string => `${file} ${tarball.files.includes(file) ? "present" : "ABSENT"}`;

  return `verified ${tarball.filename}: ${present("dist/index.d.ts")}; ${present("dist/zod.d.ts")}; ${String(nodeFiles.length)} .node files; ${String(skillFiles.length)} skill files`;
}

/** The highest `GLIBC_` version that `binaryFile` needs, as text, or why it is unknown. Effects: runs readelf. */
function highestGlibcText(binaryFile: string): string {
  const output = readelfVersionInfo(binaryFile);

  if (!output.ok) {
    return "unknown (readelf failed)";
  }

  const { numbered } = glibcNeedsOf(output.value);

  if (numbered.length === 0) {
    return "none";
  }

  return `GLIBC_${numbered.reduce((high, version) => (compareVersions(version, high) > 0 ? version : high)).join(".")}`;
}

/**
 * The platform tarball facts that the checks above verified, one log line: the `.node` files, the `os`, `cpu` and
 * `libc` fields, and for a glibc addon the highest `GLIBC_` version against its floor. Effects: reads the manifest;
 * runs readelf for a glibc addon.
 */
export function platformFacts(tarball: PackedTarball, packageDir: string, expected: PlatformExpectation): string {
  const manifest = unpackedManifest(packageDir);
  const nodeFiles = tarball.files.filter((file) => file.endsWith(".node"));
  const glibc = expected.libc.kind === "field" && expected.libc.value === "glibc";
  const floor = expected.glibcFloor.kind === "floor" ? expected.glibcFloor.version.join(".") : "none";
  const glibcText = glibc ? `; highest ${highestGlibcText(join(packageDir, expected.binary))}, floor ${floor}` : "";

  return (
    `verified ${tarball.filename}: .node files [${nodeFiles.join(", ")}]; os ${JSON.stringify(manifest["os"])}, ` +
    `cpu ${JSON.stringify(manifest["cpu"])}, libc ${JSON.stringify(manifest["libc"] ?? null)}${glibcText}`
  );
}
