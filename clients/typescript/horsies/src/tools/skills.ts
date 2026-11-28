// `horsies skills install --target <dir> [--force]`: copy the agent skill folder of this release to
// `<dir>/horsies/`, as the Python client's `horsies skills install` and the Rust client's shipped binary do.
//
// The skill files are in the package, in `.agents/skills/horsies/`. A copy at the destination that differs from them is
// replaced only with `--force`. The new copy is written beside the destination first and then moved into place, so a
// failure says what is at the destination.

import { mkdirSync, readdirSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { dirname, join } from "node:path";

import { err, ok, type Result } from "../wire/result.ts";
import { attempt, type BoundedReadError, boundedReadText, type IoCause, readBounded } from "./files.ts";

/** The name of the skill folder inside the target. */
export const SKILL_FOLDER_NAME = "horsies";

/** The skill folder of the package, from the package folder. */
export const PACKAGE_SKILL_FOLDER = ".agents/skills/horsies";

/** The skill files of this release, by name: the five files that the Python and the Rust packages ship. */
export const SKILL_FILE_NAMES: readonly string[] = ["SKILL.md", "configs.md", "monitoring.md", "tasks.md", "workflows.md"];

/** The deepest folder nesting that the read of an installed copy walks. */
export const MAX_SKILL_DIRECTORY_DEPTH = 4;

/** The most files that the read of an installed copy reads. */
export const MAX_SKILL_FILE_COUNT = 64;

/** The largest skill file that a read takes, in bytes. */
export const MAX_SKILL_FILE_BYTES: number = 1024 * 1024;

/** Skill files by relative path (`/` between folders), in path order. */
export type SkillFiles = ReadonlyMap<string, Buffer>;

/** How an install ended. */
export type InstallOutcome =
  /** The folder was written: it did not exist, or it differed and `--force` was given. */
  | { readonly kind: "installed"; readonly destination: string; readonly fileCount: number }
  /** The folder holds exactly the skill files. Nothing was written. */
  | { readonly kind: "alreadyCurrent"; readonly destination: string };

/** What is at the destination after a failed write. */
export type DestinationState =
  /** As before the call: the old copy, or no copy. */
  | { readonly kind: "unchanged" }
  /** The new copy is in place; the old copy is still at `backup`. */
  | { readonly kind: "newCopyInPlaceBackupLeft"; readonly backup: string }
  /** No copy; the old copy is at `backup`. */
  | { readonly kind: "oldCopyAtBackup"; readonly backup: string };

/** Whether the staging folder of a failed write is gone. */
export type StagingState = { readonly kind: "removed" } | { readonly kind: "leftAt"; readonly path: string };

/** The step of an install whose system call failed. */
export type InstallOperation =
  | "inspect the target"
  | "create the target"
  | "create the staging folder"
  | "write the new copy"
  | "install"
  | "back up the old copy"
  | "remove the backup";

/** Why an install wrote nothing, or did not finish. */
export type InstallError =
  /** A skill file of the package cannot be read: the package is not complete. Nothing was written. */
  | { readonly kind: "packagedSkillUnreadable"; readonly path: string; readonly cause: BoundedReadError }
  /** The target, or the skill folder inside it, is not a directory. Nothing was written. */
  | { readonly kind: "targetNotDirectory"; readonly path: string }
  /** The destination differs from the skill files and `--force` was not given. Nothing was written. */
  | { readonly kind: "changedCopyExists"; readonly destination: string; readonly differingPaths: readonly string[] }
  /** A system call failed. `destination` and `staging` say what is on disk. */
  | {
      readonly kind: "ioFailed";
      readonly path: string;
      readonly operation: InstallOperation;
      readonly cause: IoCause;
      readonly destination: DestinationState;
      readonly staging: StagingState;
    };

/** What is at a path. */
type PathKind = "missing" | "directory" | "notDirectory";

/** The kind of the entry at `path`, following a symbolic link. Effects: reads the entry. */
function pathKindOf(path: string): Result<PathKind, IoCause> {
  const stats = attempt(() => statSync(path, { throwIfNoEntry: false }));

  if (!stats.ok) {
    return stats;
  }

  if (stats.value === undefined) {
    return ok("missing");
  }

  return ok(stats.value.isDirectory() ? "directory" : "notDirectory");
}

function unchangedFailure(path: string, operation: InstallOperation, cause: IoCause): InstallError {
  return { kind: "ioFailed", path, operation, cause, destination: { kind: "unchanged" }, staging: { kind: "removed" } };
}

/** The five skill files of the package in `packageFolder`. Effects: reads the files. */
export function packagedSkillFiles(packageFolder: string): Result<SkillFiles, InstallError> {
  const files = new Map<string, Buffer>();

  for (const name of SKILL_FILE_NAMES) {
    const path = join(packageFolder, PACKAGE_SKILL_FOLDER, name);
    const bytes = readBounded(path, MAX_SKILL_FILE_BYTES);

    if (!bytes.ok) {
      return err({ kind: "packagedSkillUnreadable", path, cause: bytes.error });
    }

    files.set(name, bytes.value);
  }

  return ok(files);
}

/** The relative paths where two file sets differ (edited, missing and extra files), sorted. Pure. */
export function differingPaths(packaged: SkillFiles, installed: SkillFiles): readonly string[] {
  const names = [...new Set([...packaged.keys(), ...installed.keys()])].toSorted();

  return names.filter((name) => {
    const left = packaged.get(name);
    const right = installed.get(name);

    return left === undefined || right === undefined || !left.equals(right);
  });
}

/** A walk of an installed copy: the files read so far. */
type InstalledWalk = { readonly files: Map<string, Buffer> };

/**
 * The files of an installed copy by relative path, read with the bounds of the skill folder. Effects: reads the
 * folder. Errors: the relative path where the read stopped (a bound passed, or an entry that cannot be read); the copy
 * then counts as different there.
 */
export function readInstalledFiles(destination: string): Result<SkillFiles, string> {
  const walk: InstalledWalk = { files: new Map() };
  const walked = collectFiles(walk, destination, "", 0);

  return walked.ok ? ok(walk.files) : walked;
}

function collectFiles(walk: InstalledWalk, folder: string, prefix: string, depth: number): Result<undefined, string> {
  if (depth > MAX_SKILL_DIRECTORY_DEPTH) {
    return err(prefix);
  }

  const names = attempt(() => readdirSync(folder));

  if (!names.ok) {
    return err(prefix);
  }

  for (const name of names.value.toSorted()) {
    const path = join(folder, name);
    const relative = `${prefix}${name}`;
    const kind = pathKindOf(path);

    if (!kind.ok) {
      return err(relative);
    }

    if (kind.value === "directory") {
      const nested = collectFiles(walk, path, `${relative}/`, depth + 1);

      if (!nested.ok) {
        return nested;
      }

      continue;
    }

    if (walk.files.size >= MAX_SKILL_FILE_COUNT) {
      return err(relative);
    }

    const bytes = readBounded(path, MAX_SKILL_FILE_BYTES);

    if (!bytes.ok) {
      return err(relative);
    }

    walk.files.set(relative, bytes.value);
  }

  return ok(undefined);
}

/** What to do at the destination: write the new copy, or keep the copy there (it holds exactly the skill files). */
type CopyVerdict = { readonly kind: "write" } | { readonly kind: "current" };

/** The verdict on the copy at `destination`. Effects: reads the copy. Errors: `changedCopyExists`. */
function existingCopyVerdict(destination: string, packaged: SkillFiles, overwriteChanged: boolean): Result<CopyVerdict, InstallError> {
  const installed = readInstalledFiles(destination);
  const differing = installed.ok ? differingPaths(packaged, installed.value) : [installed.error];

  if (differing.length === 0) {
    return ok({ kind: "current" });
  }

  return overwriteChanged ? ok({ kind: "write" }) : err({ kind: "changedCopyExists", destination, differingPaths: differing });
}

/** The verdict on the destination of the kind `kind`. Effects: reads a copy that is there. */
function destinationVerdict(
  destination: string,
  kind: PathKind,
  packaged: SkillFiles,
  overwriteChanged: boolean,
): Result<CopyVerdict, InstallError> {
  switch (kind) {
    case "missing":
      return ok({ kind: "write" });
    case "notDirectory":
      return err({ kind: "targetNotDirectory", path: destination });
    case "directory":
      return existingCopyVerdict(destination, packaged, overwriteChanged);
  }
}

/** The kind of the entry at `path`, or the `ioFailed` error of the inspection (the disk is unchanged). */
function inspected(path: string): Result<PathKind, InstallError> {
  const kind = pathKindOf(path);

  return kind.ok ? kind : err(unchangedFailure(path, "inspect the target", kind.error));
}

/**
 * Copy `packaged` to `<target>/horsies/`. `installed`: the destination was absent, or it differed and
 * `overwriteChanged` is set. `alreadyCurrent`: the destination holds exactly `packaged`; nothing is written. Effects:
 * reads and writes the target folder. Errors: every error but `ioFailed` leaves the disk unchanged; `ioFailed` says
 * what is at the destination.
 */
export function installSkill(target: string, packaged: SkillFiles, overwriteChanged: boolean): Result<InstallOutcome, InstallError> {
  const destination = join(target, SKILL_FOLDER_NAME);
  const targetKind = inspected(target);

  if (!targetKind.ok || targetKind.value === "notDirectory") {
    return targetKind.ok ? err({ kind: "targetNotDirectory", path: target }) : targetKind;
  }

  const destinationKind = inspected(destination);

  if (!destinationKind.ok) {
    return destinationKind;
  }

  const verdict = destinationVerdict(destination, destinationKind.value, packaged, overwriteChanged);

  if (!verdict.ok || verdict.value.kind === "current") {
    return verdict.ok ? ok({ kind: "alreadyCurrent", destination }) : verdict;
  }

  const created = attempt(() => mkdirSync(target, { recursive: true }));

  if (!created.ok) {
    return err(unchangedFailure(target, "create the target", created.error));
  }

  const written = writeSwapped(destination, packaged);

  return written.ok ? ok({ kind: "installed", destination, fileCount: packaged.size }) : written;
}

/** A path beside `path` named `<prefix><random hex>`. Pure but for the random name. */
function siblingOf(path: string, prefix: string): string {
  return join(dirname(path), `${prefix}${randomUUID().replaceAll("-", "")}`);
}

/**
 * Write `packaged` into a staging folder beside `destination`, then move it into place (`swapIn`). A failure removes
 * the staging folder, or names it when it cannot be removed. Effects: writes beside the destination.
 */
function writeSwapped(destination: string, packaged: SkillFiles): Result<undefined, InstallError> {
  const staging = siblingOf(destination, ".horsies-skill-");
  const created = attempt(() => mkdirSync(staging));

  if (!created.ok) {
    return err(unchangedFailure(staging, "create the staging folder", created.error));
  }

  const staged = writeStaging(staging, packaged);
  const written = staged.ok ? swapIn(staging, destination) : staged;

  return written.ok ? written : err(withStagingRemoved(written.error, staging));
}

function writeStaging(staging: string, packaged: SkillFiles): Result<undefined, InstallError> {
  for (const [relative, content] of packaged) {
    const path = join(staging, relative);
    const parent = attempt(() => mkdirSync(dirname(path), { recursive: true }));

    if (!parent.ok) {
      return err(unchangedFailure(dirname(path), "write the new copy", parent.error));
    }

    const file = attempt(() => writeFileSync(path, content));

    if (!file.ok) {
      return err(unchangedFailure(path, "write the new copy", file.error));
    }
  }

  return ok(undefined);
}

/**
 * Move `staging` to `destination`. An old copy moves to a backup first; the backup is removed after the new copy is in
 * place. Each failure states what is at the destination. Effects: renames and removes folders.
 */
function swapIn(staging: string, destination: string): Result<undefined, InstallError> {
  const existing = pathKindOf(destination);

  if (existing.ok && existing.value === "missing") {
    const moved = attempt(() => renameSync(staging, destination));

    return moved.ok ? ok(undefined) : err(unchangedFailure(destination, "install", moved.error));
  }

  const backup = siblingOf(destination, ".horsies-skill-old-");
  const backedUp = attempt(() => renameSync(destination, backup));

  if (!backedUp.ok) {
    return err(unchangedFailure(destination, "back up the old copy", backedUp.error));
  }

  const moved = attempt(() => renameSync(staging, destination));

  if (!moved.ok) {
    const restored = attempt(() => renameSync(backup, destination));
    const state: DestinationState = restored.ok ? { kind: "unchanged" } : { kind: "oldCopyAtBackup", backup };

    return err({ kind: "ioFailed", path: destination, operation: "install", cause: moved.error, destination: state, staging: { kind: "removed" } });
  }

  const removed = attempt(() => rmSync(backup, { recursive: true, force: true }));

  return removed.ok
    ? ok(undefined)
    : err({
        kind: "ioFailed",
        path: backup,
        operation: "remove the backup",
        cause: removed.error,
        destination: { kind: "newCopyInPlaceBackupLeft", backup },
        staging: { kind: "removed" },
      });
}

/**
 * `failure` after an attempt to remove `staging`: a staging folder that is still there is named in it. A staging
 * folder that was moved into place is not there to remove. Effects: removes the staging folder.
 */
function withStagingRemoved(failure: InstallError, staging: string): InstallError {
  const present = pathKindOf(staging);

  if (present.ok && present.value === "missing") {
    return failure;
  }

  const removed = attempt(() => rmSync(staging, { recursive: true, force: true }));

  switch (failure.kind) {
    case "ioFailed":
      return removed.ok ? failure : { ...failure, staging: { kind: "leftAt", path: staging } };
    case "packagedSkillUnreadable":
    case "targetNotDirectory":
    case "changedCopyExists":
      return failure;
  }
}

function destinationText(state: DestinationState): string {
  switch (state.kind) {
    case "unchanged":
      return "The destination is unchanged.";
    case "newCopyInPlaceBackupLeft":
      return `The new copy is in place. The old copy is left at ${state.backup}. Remove it by hand.`;
    case "oldCopyAtBackup":
      return `The destination has no copy. The old copy is at ${state.backup}. Move it back by hand.`;
  }
}

function stagingText(state: StagingState): string {
  switch (state.kind) {
    case "removed":
      return "";
    case "leftAt":
      return ` A staging folder is left at ${state.path}. Remove it by hand.`;
  }
}

/** The message of a failed install, as Python's `describe_install_error` and the Rust tool write it. Pure. */
export function installErrorText(error: InstallError): string {
  switch (error.kind) {
    case "packagedSkillUnreadable":
      return `the skill file ${error.path} of the package cannot be read: ${boundedReadText(error.cause)}. Nothing was changed. Install the horsies package again.`;
    case "targetNotDirectory":
      return `${error.path} exists and is not a directory`;
    case "changedCopyExists":
      return `${error.destination} differs from the packaged skill (${error.differingPaths.join(", ")}). Nothing was changed. Run again with --force to replace it.`;
    case "ioFailed":
      return `${error.operation} failed for ${error.path}: ${error.cause.message}. ${destinationText(error.destination)}${stagingText(error.staging)}`;
  }
}

/** The output line of a finished install. Pure. */
export function installOutcomeText(outcome: InstallOutcome): string {
  switch (outcome.kind) {
    case "installed":
      return `installed ${outcome.fileCount} files to ${outcome.destination}`;
    case "alreadyCurrent":
      return `${outcome.destination} is already current`;
  }
}
