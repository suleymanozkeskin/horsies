// The npm packages of one run: builds and packs `horsies` and one platform package per row of the platform list, then
// checks each tarball. Run from `clients/typescript/`, after `bun install --frozen-lockfile`:
//
//   bun tests/packages/pack.ts --addons <folder of the built horsies.<platform>.node files> --out <folder>
//
// `--out` receives every tarball and `index.json` (the name, version, file, digests and manifest of each tarball; the
// install check serves them from a local registry). Needs `npm` (Node 26), `tar` and, for the glibc rows, `readelf`.
// Exit status: 0 when every check passes; 1 when a check fails; 2 when a step fails or an input does not read.

import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";

import type { Result } from "../../horsies/src/wire/result.ts";
import { horsiesProblems, platformProblems, unpackedManifest } from "./checks.ts";
import { commandFailedText, runCommand } from "./command.ts";
import { platformExpectationOf } from "./content.ts";
import { manifestRefusedText, optionalDependenciesOf, readHorsiesManifest } from "./manifest.ts";
import type { HorsiesManifest, JsonObject } from "./manifest.ts";
import { npmPack, packFailedText } from "./npm-pack.ts";
import type { PackedTarball, PackFailed } from "./npm-pack.ts";
import { platformListRefusedText, readPlatformList } from "./platforms.ts";
import type { ReleasePlatform } from "./platforms.ts";
import { sourceSkillFolder, stageFailedText, stageHorsies, stagePlatforms } from "./stage.ts";
import type { StageFailed } from "./stage.ts";

/** One entry of `index.json`: a packed tarball and the manifest it carries. */
export type IndexEntry = PackedTarball & { readonly manifest: JsonObject };

/** A packed tarball, unpacked for the checks. */
type Unpacked = { readonly tarball: PackedTarball; readonly packageDir: string };

/** A step of a run that did not complete. */
type StepFailed =
  | { readonly kind: "StageFailed"; readonly failure: StageFailed }
  | { readonly kind: "PackFailed"; readonly failure: PackFailed }
  | { readonly kind: "UnpackFailed"; readonly text: string };

function stepFailedText(failure: StepFailed): string {
  switch (failure.kind) {
    case "StageFailed":
      return stageFailedText(failure.failure);
    case "PackFailed":
      return packFailedText(failure.failure);
    case "UnpackFailed":
      return failure.text;
  }
}

/** Pack `folder` into `outDir` and unpack the tarball into `unpackDir`. Effects: writes both. */
function packAndUnpack(folder: string, outDir: string, unpackDir: string): Result<Unpacked, StepFailed> {
  const packed = npmPack(folder, outDir);

  if (!packed.ok) {
    return { ok: false, error: { kind: "PackFailed", failure: packed.error } };
  }

  mkdirSync(unpackDir, { recursive: true });

  const untar = runCommand("tar", ["-xzf", join(outDir, packed.value.filename), "-C", unpackDir], unpackDir);

  if (!untar.ok || untar.value.code !== 0) {
    return { ok: false, error: { kind: "UnpackFailed", text: commandFailedText(`tar -xzf ${packed.value.filename}`, untar) } };
  }

  return { ok: true, value: { tarball: packed.value, packageDir: join(unpackDir, "package") } };
}

/** The inputs of a run. */
type Inputs = { readonly addonsDir: string; readonly outDir: string; readonly manifest: HorsiesManifest; readonly rows: readonly ReleasePlatform[] };

/** The inputs from the arguments, the platform list and the manifest, or the text of the refusal. */
function inputsOf(args: readonly string[]): Inputs | { readonly refused: string } {
  let values: { readonly addons?: string | undefined; readonly out?: string | undefined };

  try {
    values = parseArgs({ args: [...args], options: { addons: { type: "string" }, out: { type: "string" } }, strict: true }).values;
  } catch (cause) {
    return { refused: cause instanceof Error ? cause.message : String(cause) };
  }

  if (values.addons === undefined || values.out === undefined) {
    return { refused: "give --addons <folder> and --out <folder>" };
  }

  const rows = readPlatformList();

  if (!rows.ok) {
    return { refused: platformListRefusedText(rows.error) };
  }

  const manifest = readHorsiesManifest();

  if (!manifest.ok) {
    return { refused: manifestRefusedText(manifest.error) };
  }

  return { addonsDir: resolve(values.addons), outDir: resolve(values.out), manifest: manifest.value, rows: rows.value };
}

/** Every package of a run, packed and unpacked. */
type PackedPackages = { readonly horsies: Unpacked; readonly platforms: readonly (readonly [ReleasePlatform, Unpacked])[] };

/** Stage, pack and unpack every package. Effects: writes the stage folder and the tarballs. */
function packEvery(inputs: Inputs, stageDir: string): Result<PackedPackages, StepFailed> {
  const horsiesDir = stageHorsies(stageDir, inputs.manifest, inputs.rows);

  if (!horsiesDir.ok) {
    return { ok: false, error: { kind: "StageFailed", failure: horsiesDir.error } };
  }

  const platformDirs = stagePlatforms(stageDir, inputs.manifest, inputs.rows, inputs.addonsDir);

  if (!platformDirs.ok) {
    return { ok: false, error: { kind: "StageFailed", failure: platformDirs.error } };
  }

  const horsies = packAndUnpack(horsiesDir.value, inputs.outDir, join(stageDir, "unpacked", "horsies"));

  if (!horsies.ok) {
    return horsies;
  }

  const platforms: (readonly [ReleasePlatform, Unpacked])[] = [];

  for (const [row, folder] of platformDirs.value) {
    const unpacked = packAndUnpack(folder, inputs.outDir, join(stageDir, "unpacked", row.platform));

    if (!unpacked.ok) {
      return unpacked;
    }

    platforms.push([row, unpacked.value]);
  }

  return { ok: true, value: { horsies: horsies.value, platforms } };
}

/** Check every tarball and write `index.json`; the problems. Effects: reads the unpacked files; writes the index. */
function checkAndIndex(inputs: Inputs, packed: PackedPackages, stageDir: string): readonly string[] {
  const { version } = inputs.manifest;
  const expected = { version, optionalDependencies: optionalDependenciesOf(inputs.rows, version), skills: sourceSkillFolder() };

  const problems = [
    ...horsiesProblems(packed.horsies.tarball, packed.horsies.packageDir, expected, join(stageDir, "api-report")),
    ...packed.platforms.flatMap(([row, unpacked]) =>
      platformProblems(unpacked.tarball, unpacked.packageDir, platformExpectationOf(row, version)),
    ),
  ];

  const entries: IndexEntry[] = [packed.horsies, ...packed.platforms.map(([, unpacked]) => unpacked)].map((unpacked) => ({
    ...unpacked.tarball,
    manifest: unpackedManifest(unpacked.packageDir),
  }));

  writeFileSync(join(inputs.outDir, "index.json"), `${JSON.stringify(entries, null, 2)}\n`);

  for (const entry of entries) {
    console.log(`packed ${entry.filename}: ${entry.name}@${entry.version}, ${entry.files.length} files, ${entry.integrity}`);
  }

  return problems;
}

/**
 * Pack and check; the exit status. Effects: writes the tarballs and `index.json`; writes stdout and stderr; removes
 * its stage folder at the end.
 */
function main(args: readonly string[]): number {
  const inputs = inputsOf(args);

  if ("refused" in inputs) {
    console.error(inputs.refused);

    return 2;
  }

  mkdirSync(inputs.outDir, { recursive: true });

  const stageDir = mkdtempSync(join(tmpdir(), "horsies-npm-packages-"));

  try {
    const packed = packEvery(inputs, stageDir);

    if (!packed.ok) {
      console.error(stepFailedText(packed.error));

      return 2;
    }

    const problems = checkAndIndex(inputs, packed.value, stageDir);

    for (const problem of problems) {
      console.error(`FAIL: ${problem}`);
    }

    return problems.length === 0 ? 0 : 1;
  } finally {
    rmSync(stageDir, { recursive: true, force: true });
  }
}

process.exitCode = main(process.argv.slice(2));
