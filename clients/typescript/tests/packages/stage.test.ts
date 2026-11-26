// The staging of the platform packages with `napi create-npm-dirs`: the folders of the list, a missing addon file,
// and a list that differs from the napi targets. Needs the workspace install (the napi CLI of the bridge).

import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, test } from "node:test";

import { binaryFileOf } from "./content.ts";
import { readHorsiesManifest } from "./manifest.ts";
import { readPlatformList } from "./platforms.ts";
import type { ReleasePlatform } from "./platforms.ts";
import { stageFailedText, stagePlatforms } from "./stage.ts";

/** The committed list and manifest. */
function committed(): { readonly rows: readonly ReleasePlatform[]; readonly manifest: ReturnType<typeof readHorsiesManifest> } {
  const rows = readPlatformList();

  assert.ok(rows.ok);

  return { rows: rows.value, manifest: readHorsiesManifest() };
}

/** Run `check` with a new stage folder and a new addons folder, removed at the end. */
function withFolders(check: (stageDir: string, addonsDir: string) => void): void {
  const root = mkdtempSync(join(tmpdir(), "horsies-stage-test-"));

  try {
    mkdirSync(join(root, "addons"));
    check(join(root, "stage"), join(root, "addons"));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

describe("stagePlatforms", () => {
  test("writes one package folder per row, with its addon file, license and manifest", () => {
    const { rows, manifest } = committed();

    assert.ok(manifest.ok);

    withFolders((stageDir, addonsDir) => {
      for (const row of rows) {
        writeFileSync(join(addonsDir, binaryFileOf(row)), "stand-in");
      }

      const staged = stagePlatforms(stageDir, manifest.value, rows, addonsDir);

      assert.ok(staged.ok, staged.ok ? "" : stageFailedText(staged.error));
      assert.deepEqual(staged.value.map(([row]) => row.platform), rows.map((row) => row.platform));

      for (const [row, folder] of staged.value) {
        assert.ok(existsSync(join(folder, binaryFileOf(row))));
        assert.ok(existsSync(join(folder, "LICENSE")));
        assert.equal(JSON.parse(readFileSync(join(folder, "package.json"), "utf8")).version, manifest.value.version);
      }
    });
  });

  test("AddonMissing: a row whose addon file is not in the addons folder", () => {
    const { rows, manifest } = committed();

    assert.ok(manifest.ok);

    withFolders((stageDir, addonsDir) => {
      const staged = stagePlatforms(stageDir, manifest.value, rows, addonsDir);

      assert.deepEqual(staged.ok ? "staged" : staged.error, { kind: "AddonMissing", file: binaryFileOf(rows[0] ?? assert.fail("no row")) });
    });
  });

  test("PlatformDirsDiffer: a list with fewer platforms than the napi targets", () => {
    const { rows, manifest } = committed();

    assert.ok(manifest.ok);

    withFolders((stageDir, addonsDir) => {
      const staged = stagePlatforms(stageDir, manifest.value, rows.slice(0, 1), addonsDir);

      assert.equal(staged.ok ? "staged" : staged.error.kind, "PlatformDirsDiffer");
      assert.match(staged.ok ? "" : stageFailedText(staged.error), /napi.targets of bridge\/package.json give the platforms/u);
    });
  });
});
