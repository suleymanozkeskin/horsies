// The reader of the JSON output of `npm pack --json`, and one pack of a package folder.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, test } from "node:test";

import { npmPack, packedTarballOf } from "./npm-pack.ts";

const ENTRY = {
  name: "@horsies/native-linux-x64-gnu",
  version: "0.8.1",
  filename: "horsies-native-linux-x64-gnu-0.8.1.tgz",
  integrity: "sha512-abc",
  shasum: "0123",
  files: [{ path: "package.json", size: 1, mode: 420 }, { path: "horsies.linux-x64-gnu.node", size: 2, mode: 420 }],
};

describe("packedTarballOf", () => {
  test("reads the one tarball of the output", () => {
    assert.deepEqual(packedTarballOf([ENTRY]), {
      name: "@horsies/native-linux-x64-gnu",
      version: "0.8.1",
      filename: "horsies-native-linux-x64-gnu-0.8.1.tgz",
      integrity: "sha512-abc",
      shasum: "0123",
      files: ["package.json", "horsies.linux-x64-gnu.node"],
    });
  });

  test("an output that is not one tarball", () => {
    assert.equal(packedTarballOf({}), undefined);
    assert.equal(packedTarballOf([ENTRY, ENTRY]), undefined);
  });

  test("a tarball without a field, or a file without a path", () => {
    assert.equal(packedTarballOf([{ ...ENTRY, integrity: 1 }]), undefined);
    assert.equal(packedTarballOf([{ ...ENTRY, files: [{ size: 1 }] }]), undefined);
  });
});

describe("npmPack", () => {
  test("packs a package folder and gives the tarball", () => {
    const folder = mkdtempSync(join(tmpdir(), "horsies-npm-pack-test-"));

    try {
      writeFileSync(join(folder, "package.json"), JSON.stringify({ name: "pack-check", version: "1.0.0", files: ["a.txt"] }));
      writeFileSync(join(folder, "a.txt"), "a");

      const packed = npmPack(folder, folder);

      assert.ok(packed.ok, packed.ok ? "" : packed.error.kind);
      assert.equal(packed.value.filename, "pack-check-1.0.0.tgz");
      assert.deepEqual(packed.value.files.toSorted(), ["a.txt", "package.json"]);
    } finally {
      rmSync(folder, { recursive: true, force: true });
    }
  });

  test("NpmFailed: a folder without a package.json", () => {
    const folder = mkdtempSync(join(tmpdir(), "horsies-npm-pack-test-"));

    try {
      const packed = npmPack(folder, folder);

      assert.equal(packed.ok ? "packed" : packed.error.kind, "NpmFailed");
    } finally {
      rmSync(folder, { recursive: true, force: true });
    }
  });
});
