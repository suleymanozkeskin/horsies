// `npm pack` of one package folder, and the facts of the tarball that it writes.

import { isArray, isPlainObject, isString } from "../../horsies/src/codec/values.ts";
import type { Result } from "../../horsies/src/wire/result.ts";
import type { ParsedJson } from "../public-api/package.ts";
import { commandFailedText, runCommand } from "./command.ts";

/** A packed tarball: its package, its file in the destination folder, its digests and the paths it holds. */
export type PackedTarball = {
  readonly name: string;
  readonly version: string;
  readonly filename: string;
  readonly integrity: string;
  readonly shasum: string;
  readonly files: readonly string[];
};

/** The facts of the one tarball that the JSON output of `npm pack --json` describes, or `undefined`. Pure. */
export function packedTarballOf(output: ParsedJson): PackedTarball | undefined {
  const entry = isArray(output) && output.length === 1 ? output[0] : undefined;

  if (!isPlainObject(entry)) {
    return undefined;
  }

  const { name, version, filename, integrity, shasum, files } = entry;

  if (!isString(name) || !isString(version) || !isString(filename) || !isString(integrity) || !isString(shasum)) {
    return undefined;
  }

  if (!isArray(files)) {
    return undefined;
  }

  const paths = files.map((file) => (isPlainObject(file) && isString(file["path"]) ? file["path"] : undefined));

  return paths.every(isString) ? { name, version, filename, integrity, shasum, files: paths.filter(isString) } : undefined;
}

/** Why `npm pack` gave no tarball. */
export type PackFailed =
  | { readonly kind: "NpmFailed"; readonly folder: string; readonly text: string }
  | { readonly kind: "OutputNotJson"; readonly folder: string; readonly stdout: string }
  | { readonly kind: "NotOneTarball"; readonly folder: string; readonly stdout: string };

/** The text of a failed pack, for a message. Pure. */
export function packFailedText(failure: PackFailed): string {
  switch (failure.kind) {
    case "NpmFailed":
      return failure.text;
    case "OutputNotJson":
      return `npm pack in ${failure.folder}: stdout is not JSON:\n${failure.stdout}`;
    case "NotOneTarball":
      return `npm pack in ${failure.folder}: the JSON does not describe one tarball:\n${failure.stdout}`;
  }
}

/** Pack the package in `folder` into `destination` with npm, with no lifecycle script. Effects: writes the tarball. */
export function npmPack(folder: string, destination: string): Result<PackedTarball, PackFailed> {
  const args = ["pack", "--json", "--ignore-scripts", "--pack-destination", destination];
  const run = runCommand("npm", args, folder);

  if (!run.ok || run.value.code !== 0) {
    return { ok: false, error: { kind: "NpmFailed", folder, text: commandFailedText(`npm pack in ${folder}`, run) } };
  }

  let output: ParsedJson;

  try {
    // SAFETY: JSON.parse gives a parsed JSON document.
    output = JSON.parse(run.value.stdout) as ParsedJson;
  } catch {
    return { ok: false, error: { kind: "OutputNotJson", folder, stdout: run.value.stdout } };
  }

  const tarball = packedTarballOf(output);

  return tarball === undefined
    ? { ok: false, error: { kind: "NotOneTarball", folder, stdout: run.value.stdout } }
    : { ok: true, value: tarball };
}
