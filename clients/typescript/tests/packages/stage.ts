// The package folders that `npm pack` packs: `horsies` (built with the TypeScript 7 compiler) and one platform
// package per row of the platform list (written by `napi create-npm-dirs`, with the row's built addon file).

import { copyFileSync, cpSync, existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { isPlainObject } from "../../horsies/src/codec/values.ts";
import type { Result } from "../../horsies/src/wire/result.ts";
import type { ParsedJson } from "../public-api/package.ts";
import { CURRENT_COMPILER, PACKAGE_DIR, WORKSPACE_DIR } from "../public-api/tools.ts";
import { commandFailedText, runCommand } from "./command.ts";
import { binaryFileOf, SKILL_FOLDER } from "./content.ts";
import type { SkillFolder } from "./content.ts";
import { packedManifestOf } from "./manifest.ts";
import type { HorsiesManifest, JsonObject } from "./manifest.ts";
import type { ReleasePlatform } from "./platforms.ts";

/** The license of the repository: each package holds a copy. */
const LICENSE_FILE = `${WORKSPACE_DIR}../../LICENSE`;

/** The bridge folder: its `package.json` holds the napi config. */
const BRIDGE_DIR = `${WORKSPACE_DIR}bridge/`;

/** The napi CLI of the bridge (`@napi-rs/cli` of `bridge/package.json`). */
const NAPI_CLI = `${BRIDGE_DIR}node_modules/.bin/napi`;

/** The package files of `horsies/` that the package holds when they exist (the release adds them). */
const OPTIONAL_PACKAGE_FILES: readonly string[] = ["README.md", "CHANGELOG.md"];

/** A step of the staging that did not complete, and why. */
export type StageFailed =
  | { readonly kind: "BuildFailed"; readonly text: string }
  | { readonly kind: "CreateNpmDirsFailed"; readonly text: string }
  | { readonly kind: "PlatformDirsDiffer"; readonly generated: readonly string[]; readonly listed: readonly string[] }
  | { readonly kind: "AddonMissing"; readonly file: string };

/** The text of a failed staging step, for a message. Pure. */
export function stageFailedText(failure: StageFailed): string {
  switch (failure.kind) {
    case "BuildFailed":
      return `the build of horsies failed: ${failure.text}`;
    case "CreateNpmDirsFailed":
      return `napi create-npm-dirs failed: ${failure.text}`;
    case "PlatformDirsDiffer":
      return `napi.targets of bridge/package.json give the platforms ${failure.generated.join(", ")}; the platform list has ${failure.listed.join(", ")}`;
    case "AddonMissing":
      return `the built addon ${failure.file} is not in the addons folder`;
  }
}

/** The skill folder of the package source. Effects: reads the folder. */
export function sourceSkillFolder(): SkillFolder {
  const folder = `${PACKAGE_DIR}${SKILL_FOLDER}`;

  return existsSync(folder) ? { kind: "written", files: readdirSync(folder).toSorted() } : { kind: "notWritten" };
}

function writeJson(file: string, value: JsonObject): void {
  writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

/**
 * Stage `horsies` in `stageDir/horsies`: `dist/` from `tsconfig.build.json`, the packed manifest, the license, the
 * README and changelog when they exist, and the skill folder when it exists. Effects: writes the folder; runs the
 * compiler. Gives the folder.
 */
export function stageHorsies(stageDir: string, manifest: HorsiesManifest, rows: readonly ReleasePlatform[]): Result<string, StageFailed> {
  const folder = join(stageDir, "horsies");

  mkdirSync(folder, { recursive: true });

  const args = ["--project", `${PACKAGE_DIR}tsconfig.build.json`, "--outDir", join(folder, "dist"), "--pretty", "false"];
  const build = runCommand(CURRENT_COMPILER, args, WORKSPACE_DIR);

  if (!build.ok || build.value.code !== 0) {
    return { ok: false, error: { kind: "BuildFailed", text: commandFailedText("tsc --project horsies/tsconfig.build.json", build) } };
  }

  writeJson(join(folder, "package.json"), packedManifestOf(manifest, rows));
  copyFileSync(LICENSE_FILE, join(folder, "LICENSE"));

  for (const file of OPTIONAL_PACKAGE_FILES.filter((name) => existsSync(`${PACKAGE_DIR}${name}`))) {
    copyFileSync(`${PACKAGE_DIR}${file}`, join(folder, file));
  }

  if (sourceSkillFolder().kind === "written") {
    cpSync(`${PACKAGE_DIR}${SKILL_FOLDER}`, join(folder, SKILL_FOLDER), { recursive: true });
  }

  return { ok: true, value: folder };
}

/**
 * The manifest that `napi create-npm-dirs` reads: the napi config of the bridge, with the version and the package
 * fields of `horsies` (the platform packages copy license, homepage, repository, bugs, engines and publishConfig).
 * Effects: reads `bridge/package.json`.
 */
function napiManifest(manifest: HorsiesManifest): JsonObject {
  // SAFETY: JSON.parse gives a parsed JSON document.
  const bridge = JSON.parse(readFileSync(`${BRIDGE_DIR}package.json`, "utf8")) as ParsedJson;
  const napi = isPlainObject(bridge) ? bridge["napi"] : undefined;
  const repository = manifest.document["repository"];

  return {
    name: "horsies-native",
    version: manifest.version,
    description: "The native addon of the horsies TypeScript client for one platform",
    // SAFETY: a member of a parsed JSON object is parsed JSON.
    napi: (napi ?? null) as ParsedJson,
    ...Object.fromEntries(
      ["license", "homepage", "bugs", "engines", "publishConfig"].flatMap((field) => {
        const value = manifest.document[field];

        return value === undefined ? [] : [[field, value]];
      }),
    ),
    ...(isPlainObject(repository) ? { repository: { ...repository, directory: "clients/typescript/bridge" } } : {}),
  };
}

/**
 * Stage one platform package per row in `stageDir/platforms/npm/<platform>`: `napi create-npm-dirs` writes the
 * manifest and README, then the row's addon file and the license are copied in. The platforms that napi writes must be
 * the platforms of the list. Effects: writes the folders; runs the napi CLI. Gives each row with its folder.
 */
export function stagePlatforms(
  stageDir: string,
  manifest: HorsiesManifest,
  rows: readonly ReleasePlatform[],
  addonsDir: string,
): Result<readonly (readonly [ReleasePlatform, string])[], StageFailed> {
  const folder = join(stageDir, "platforms");

  mkdirSync(folder, { recursive: true });
  writeJson(join(folder, "package.json"), napiManifest(manifest));

  const created = runCommand(NAPI_CLI, ["create-npm-dirs", "--cwd", folder], folder);

  if (!created.ok || created.value.code !== 0) {
    return { ok: false, error: { kind: "CreateNpmDirsFailed", text: commandFailedText("napi create-npm-dirs", created) } };
  }

  const generated = readdirSync(join(folder, "npm")).toSorted();
  const listed = rows.map((row) => row.platform).toSorted();

  if (generated.join(" ") !== listed.join(" ")) {
    return { ok: false, error: { kind: "PlatformDirsDiffer", generated, listed } };
  }

  const folders: (readonly [ReleasePlatform, string])[] = [];

  for (const row of rows) {
    const binary = join(addonsDir, binaryFileOf(row));
    const packageDir = join(folder, "npm", row.platform);

    if (!existsSync(binary)) {
      return { ok: false, error: { kind: "AddonMissing", file: binaryFileOf(row) } };
    }

    copyFileSync(binary, join(packageDir, binaryFileOf(row)));
    copyFileSync(LICENSE_FILE, join(packageDir, "LICENSE"));
    folders.push([row, packageDir]);
  }

  return { ok: true, value: folders };
}
