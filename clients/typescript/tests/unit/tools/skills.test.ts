// `skills install`: one case per outcome of the install and per error of its catalog.

import assert from "node:assert/strict";
import { chmodSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, describe, test } from "node:test";

import { packageFolder } from "../../../horsies/src/tools/files.ts";
import {
  differingPaths,
  installErrorText,
  installOutcomeText,
  installSkill,
  MAX_SKILL_FILE_COUNT,
  PACKAGE_SKILL_FOLDER,
  packagedSkillFiles,
  readInstalledFiles,
  SKILL_FILE_NAMES,
  type SkillFiles,
} from "../../../horsies/src/tools/skills.ts";

const folders: string[] = [];

function newFolder(): string {
  const folder = mkdtempSync(join(tmpdir(), "horsies-skills-test-"));

  folders.push(folder);

  return folder;
}

after(() => {
  for (const folder of folders) {
    rmSync(folder, { recursive: true, force: true });
  }
});

function files(entries: Readonly<Record<string, string>>): SkillFiles {
  return new Map(Object.entries(entries).map(([name, text]) => [name, Buffer.from(text)]));
}

describe("the packaged skill", () => {
  test("the package's skill folder holds exactly the five skill files, and each one reads", () => {
    const folder = join(packageFolder(), PACKAGE_SKILL_FOLDER);
    const packaged = packagedSkillFiles(packageFolder());

    assert.deepEqual(readdirSync(folder).toSorted(), [...SKILL_FILE_NAMES]);
    assert.ok(packaged.ok);
    assert.deepEqual([...packaged.value.keys()], SKILL_FILE_NAMES);
  });

  test("a package folder with a missing skill file is refused, naming the file", () => {
    const folder = newFolder();

    mkdirSync(join(folder, PACKAGE_SKILL_FOLDER), { recursive: true });
    writeFileSync(join(folder, PACKAGE_SKILL_FOLDER, "SKILL.md"), "skill");

    const packaged = packagedSkillFiles(folder);

    assert.ok(!packaged.ok && packaged.error.kind === "packagedSkillUnreadable");
    assert.equal(packaged.error.path, join(folder, PACKAGE_SKILL_FOLDER, "configs.md"));
    assert.match(installErrorText(packaged.error), /configs\.md of the package cannot be read: ENOENT.*Nothing was changed/u);
  });
});

describe("installSkill", () => {
  test("a new target gets every file", () => {
    const target = join(newFolder(), "skills");
    const skill = files({ "SKILL.md": "skill", "docs/tasks.md": "tasks" });

    const outcome = installSkill(target, skill, false);

    assert.deepEqual(outcome, { ok: true, value: { kind: "installed", destination: join(target, "horsies"), fileCount: 2 } });
    assert.deepEqual(readInstalledFiles(join(target, "horsies")), { ok: true, value: skill });
    assert.ok(outcome.ok);
    assert.equal(installOutcomeText(outcome.value), `installed 2 files to ${join(target, "horsies")}`);
  });

  test("a current copy is left as it is", () => {
    const target = newFolder();
    const skill = files({ "SKILL.md": "skill" });

    assert.ok(installSkill(target, skill, false).ok);

    const outcome = installSkill(target, skill, false);

    assert.deepEqual(outcome, { ok: true, value: { kind: "alreadyCurrent", destination: join(target, "horsies") } });
    assert.ok(outcome.ok);
    assert.equal(installOutcomeText(outcome.value), `${join(target, "horsies")} is already current`);
  });

  test("a changed copy is refused without --force and replaced with it, with no folder left beside it", () => {
    const target = newFolder();
    const destination = join(target, "horsies");
    const skill = files({ "SKILL.md": "skill", "tasks.md": "tasks" });

    assert.ok(installSkill(target, skill, false).ok);
    writeFileSync(join(destination, "tasks.md"), "edited");
    writeFileSync(join(destination, "notes.md"), "mine");

    const refused = installSkill(target, skill, false);

    assert.ok(!refused.ok && refused.error.kind === "changedCopyExists");
    assert.deepEqual(refused.error.differingPaths, ["notes.md", "tasks.md"]);
    assert.match(installErrorText(refused.error), /differs from the packaged skill \(notes\.md, tasks\.md\)\. Nothing was changed\. Run again with --force/u);
    assert.equal(readFileSync(join(destination, "tasks.md"), "utf8"), "edited");

    const replaced = installSkill(target, skill, true);

    assert.ok(replaced.ok && replaced.value.kind === "installed");
    assert.deepEqual(readInstalledFiles(destination), { ok: true, value: skill });
    assert.deepEqual(readdirSync(target), ["horsies"]);
  });

  test("a target that is a file, and a skill folder that is a file, are refused", () => {
    const parent = newFolder();
    const file = join(parent, "skills");

    writeFileSync(file, "not a folder");

    const refused = installSkill(file, files({ "SKILL.md": "s" }), true);

    assert.deepEqual(refused, { ok: false, error: { kind: "targetNotDirectory", path: file } });
    assert.ok(!refused.ok);
    assert.equal(installErrorText(refused.error), `${file} exists and is not a directory`);

    const inner = join(parent, "other");

    mkdirSync(inner);
    writeFileSync(join(inner, "horsies"), "a file");
    assert.deepEqual(installSkill(inner, files({ "SKILL.md": "s" }), true), {
      ok: false,
      error: { kind: "targetNotDirectory", path: join(inner, "horsies") },
    });
  });

  test("a target that cannot be written is ioFailed, and the destination is unchanged", { skip: process.getuid?.() === 0 }, () => {
    const parent = newFolder();
    const locked = join(parent, "locked");

    mkdirSync(locked);
    chmodSync(locked, 0o500);

    try {
      const failed = installSkill(join(locked, "skills"), files({ "SKILL.md": "s" }), false);

      assert.ok(!failed.ok && failed.error.kind === "ioFailed");
      assert.equal(failed.error.operation, "create the target");
      assert.deepEqual(failed.error.destination, { kind: "unchanged" });
      assert.match(installErrorText(failed.error), /^create the target failed for .*locked.skills: EACCES.*The destination is unchanged\.$/u);
    } finally {
      chmodSync(locked, 0o700);
    }
  });
});

describe("the read of an installed copy", () => {
  test("a copy with more files than the bound counts as different at the file past the bound", () => {
    const destination = newFolder();

    for (let index = 0; index <= MAX_SKILL_FILE_COUNT; index += 1) {
      writeFileSync(join(destination, `file-${String(index).padStart(3, "0")}.md`), "x");
    }

    const read = readInstalledFiles(destination);

    assert.deepEqual(read, { ok: false, error: `file-${String(MAX_SKILL_FILE_COUNT).padStart(3, "0")}.md` });
  });

  test("differingPaths names edited, missing and extra files, sorted", () => {
    assert.deepEqual(differingPaths(files({ "a.md": "a", "b.md": "b", "c.md": "c" }), files({ "a.md": "a", "c.md": "edited", "d.md": "d" })), [
      "b.md",
      "c.md",
      "d.md",
    ]);
  });
});

describe("installErrorText", () => {
  test("each failure text says what is at the destination and names a staging folder that is left", () => {
    const text = installErrorText({
      kind: "ioFailed",
      path: "/t/horsies",
      operation: "install",
      cause: { code: "ENOSPC", message: "disk full" },
      destination: { kind: "oldCopyAtBackup", backup: "/t/.old" },
      staging: { kind: "leftAt", path: "/t/.stage" },
    });

    assert.equal(
      text,
      "install failed for /t/horsies: disk full. The destination has no copy. The old copy is at /t/.old. Move it back by hand. " +
        "A staging folder is left at /t/.stage. Remove it by hand.",
    );

    const backupLeft = installErrorText({
      kind: "ioFailed",
      path: "/t/.old",
      operation: "remove the backup",
      cause: { code: "EBUSY", message: "busy" },
      destination: { kind: "newCopyInPlaceBackupLeft", backup: "/t/.old" },
      staging: { kind: "removed" },
    });

    assert.equal(backupLeft, "remove the backup failed for /t/.old: busy. The new copy is in place. The old copy is left at /t/.old. Remove it by hand.");
  });
});
