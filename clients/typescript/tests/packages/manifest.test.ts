// The manifest of `horsies`: one case per refusal, the committed manifest, and the optional dependencies of the packed
// manifest.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { horsiesManifestOf, manifestRefusedText, optionalDependenciesOf, packedManifestOf, readHorsiesManifest } from "./manifest.ts";
import { readPlatformList } from "./platforms.ts";
import type { ReleasePlatform } from "./platforms.ts";

const PUBLISHED: ReleasePlatform = {
  name: "linux-x86_64",
  release: "publish",
  runner: "ubuntu-latest",
  rustTarget: "x86_64-unknown-linux-gnu",
  cLibrary: "glibc",
  platform: "linux-x64-gnu",
  cpu: "x64",
  build: { kind: "glibc-2.17-toolchain" },
  install: { kind: "container", nodeVariant: "trixie-slim", bunVariant: "debian" },
};

const HELD: ReleasePlatform = { ...PUBLISHED, name: "linux-musl-x86_64", release: "hold", cLibrary: "musl", platform: "linux-x64-musl" };

describe("horsiesManifestOf", () => {
  test("reads the version and keeps the document", () => {
    const document = { name: "horsies", version: "0.8.1" };

    assert.deepEqual(horsiesManifestOf(document), { ok: true, value: { version: "0.8.1", document } });
  });

  test("reads a pre-release version", () => {
    assert.ok(horsiesManifestOf({ version: "0.9.0-rc.1" }).ok);
  });

  test("NotAnObject, NoVersion and VersionForm", () => {
    assert.deepEqual(horsiesManifestOf([]), { ok: false, error: { kind: "NotAnObject" } });
    assert.deepEqual(horsiesManifestOf({ name: "horsies" }), { ok: false, error: { kind: "NoVersion" } });
    assert.deepEqual(horsiesManifestOf({ version: "0.8" }), { ok: false, error: { kind: "VersionForm", version: "0.8" } });
    assert.match(manifestRefusedText({ kind: "VersionForm", version: "0.8" }), /"0.8" is not of the form/u);
  });

  test("the committed manifest reads", () => {
    const read = readHorsiesManifest();

    assert.ok(read.ok, read.ok ? "" : manifestRefusedText(read.error));
  });
});

describe("optionalDependenciesOf and packedManifestOf", () => {
  test("list the platform package of each published row at the version, and no held row", () => {
    assert.deepEqual(optionalDependenciesOf([PUBLISHED, HELD], "0.8.1"), { "@horsies/native-linux-x64-gnu": "0.8.1" });
  });

  test("the packed manifest is the committed one with the optional dependencies", () => {
    const manifest = { version: "0.8.1", document: { name: "horsies", version: "0.8.1" } };

    assert.deepEqual(packedManifestOf(manifest, [PUBLISHED, HELD]), {
      name: "horsies",
      version: "0.8.1",
      optionalDependencies: { "@horsies/native-linux-x64-gnu": "0.8.1" },
    });
  });

  test("the committed manifest has no optional dependencies: the pack writes them", () => {
    const read = readHorsiesManifest();
    const rows = readPlatformList();

    assert.ok(read.ok && rows.ok);
    assert.equal(read.value.document["optionalDependencies"], undefined);
    assert.equal(read.value.document["private"], undefined);
  });
});
