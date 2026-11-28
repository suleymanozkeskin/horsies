// The manifest of `horsies`: one case per refusal, the committed manifest, and the optional dependencies of the packed
// manifest.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { horsiesManifestOf, manifestRefusedText, optionalDependenciesOf, packedImportsOf, packedManifestOf, readHorsiesManifest, lintManifestOf, withoutPrivate } from "./manifest.ts";
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

    assert.deepEqual(horsiesManifestOf(document), { ok: true, value: { version: "0.8.1", document, packedImports: { kind: "none" } } });
  });

  test("reads the imports map as its packed form", () => {
    const document = { version: "0.8.1", imports: { "#a": { bun: "./src/a/bun.ts", default: "./src/a/node.ts" } } };

    assert.deepEqual(horsiesManifestOf(document), {
      ok: true,
      value: { version: "0.8.1", document, packedImports: { kind: "map", map: { "#a": { bun: "./dist/a/bun.js", default: "./dist/a/node.js" } } } },
    });
  });

  test("ImportsTarget: an imports target that is not a source file", () => {
    const refused = horsiesManifestOf({ version: "0.8.1", imports: { "#a": "./dist/a.js" } });

    assert.deepEqual(refused, { ok: false, error: { kind: "ImportsTarget", specifier: "#a", target: '"./dist/a.js"' } });
    assert.match(manifestRefusedText({ kind: "ImportsTarget", specifier: "#a", target: '"./dist/a.js"' }), /not \.\/src\/<path>\.ts/u);
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

describe("packedImportsOf", () => {
  test("a plain target is packed", () => {
    assert.deepEqual(packedImportsOf({ "#a": "./src/a.ts" }), { ok: true, value: { "#a": "./dist/a.js" } });
  });

  test("an imports member that is not an object is refused", () => {
    assert.deepEqual(packedImportsOf([]), { ok: false, error: { kind: "ImportsTarget", specifier: "imports", target: "[]" } });
  });

  test("conditions nested deeper than the bound are refused", () => {
    const deep = { "#a": { x: { y: { z: { w: "./src/a.ts" } } } } };

    assert.deepEqual(packedImportsOf(deep), { ok: false, error: { kind: "ImportsTarget", specifier: "#a", target: '{"w":"./src/a.ts"}' } });
  });

  test("the committed imports map packs to built files that select the Bun open step on Bun", () => {
    const read = readHorsiesManifest();

    assert.ok(read.ok, read.ok ? "" : manifestRefusedText(read.error));
    assert.deepEqual(read.value.packedImports, {
      kind: "map",
      map: { "#native/platform-open": { bun: "./dist/native/platform-open/bun.js", default: "./dist/native/platform-open/node.js" } },
    });
  });
});

describe("optionalDependenciesOf and packedManifestOf", () => {
  test("list the platform package of each published row at the version, and no held row", () => {
    assert.deepEqual(optionalDependenciesOf([PUBLISHED, HELD], "0.8.1"), { "@horsies/native-linux-x64-gnu": "0.8.1" });
  });

  test("the packed manifest is the committed one without private, with the optional dependencies", () => {
    const manifest = { version: "0.8.1", document: { name: "horsies", private: true, version: "0.8.1" }, packedImports: { kind: "none" } } as const;

    assert.deepEqual(packedManifestOf(manifest, [PUBLISHED, HELD]), {
      name: "horsies",
      version: "0.8.1",
      optionalDependencies: { "@horsies/native-linux-x64-gnu": "0.8.1" },
    });
  });

  test("the packed manifest carries the packed imports map in place of the committed one", () => {
    const manifest = {
      version: "0.8.1",
      document: { name: "horsies", version: "0.8.1", imports: { "#a": "./src/a.ts" } },
      packedImports: { kind: "map", map: { "#a": "./dist/a.js" } },
    } as const;

    assert.deepEqual(packedManifestOf(manifest, []), { name: "horsies", version: "0.8.1", imports: { "#a": "./dist/a.js" }, optionalDependencies: {} });
  });

  test("the committed manifest is private and has no optional dependencies: the pack writes the publishable form", () => {
    const read = readHorsiesManifest();

    assert.ok(read.ok);
    assert.equal(read.value.document["private"], true);
    assert.equal(read.value.document["optionalDependencies"], undefined);

    const rows = readPlatformList();

    assert.ok(rows.ok);
    assert.equal("private" in packedManifestOf(read.value, rows.value), false);
  });
});

describe("withoutPrivate and lintManifestOf", () => {
  test("withoutPrivate drops the field private and keeps the others", () => {
    assert.deepEqual(withoutPrivate({ name: "x", private: true, version: "1.0.0" }), { name: "x", version: "1.0.0" });
  });

  test("lintManifestOf: a JSON object is the manifest", () => {
    assert.deepEqual(lintManifestOf({ name: "x" }), { name: "x" });
  });

  test("lintManifestOf: a document that is not an object is no manifest", () => {
    assert.equal(lintManifestOf([]), undefined);
  });
});
