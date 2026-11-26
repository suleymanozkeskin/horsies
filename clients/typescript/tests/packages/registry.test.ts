// The local registry of the install checks: the index, the package documents, the request paths and the answers.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { answerOf, packumentOf, registryEntriesOf, requestTargetOf } from "./registry.ts";

const ENTRY = {
  name: "@horsies/native-linux-x64-gnu",
  version: "0.8.1",
  filename: "horsies-native-linux-x64-gnu-0.8.1.tgz",
  integrity: "sha512-abc",
  shasum: "0123",
  files: ["package.json"],
  manifest: { name: "@horsies/native-linux-x64-gnu", version: "0.8.1", os: ["linux"], cpu: ["x64"], libc: ["glibc"] },
};

describe("registryEntriesOf", () => {
  test("reads the entries of the index", () => {
    const read = registryEntriesOf([ENTRY]);

    assert.ok(read.ok);
    assert.equal(read.value[0]?.filename, ENTRY.filename);
  });

  test("NotAList, EntryForm and RepeatedName", () => {
    assert.deepEqual(registryEntriesOf({}), { ok: false, error: { kind: "NotAList" } });
    assert.deepEqual(registryEntriesOf([{ ...ENTRY, shasum: 1 }]), { ok: false, error: { kind: "EntryForm", index: 0 } });
    assert.deepEqual(registryEntriesOf([ENTRY, ENTRY]), { ok: false, error: { kind: "RepeatedName", name: ENTRY.name } });
  });

  test("TooManyTarballs above the bound", () => {
    const entries = Array.from({ length: 33 }, (_, index) => ({ ...ENTRY, name: `package-${index}` }));

    assert.deepEqual(registryEntriesOf(entries), { ok: false, error: { kind: "TooManyTarballs", count: 33, bound: 32 } });
  });
});

describe("packumentOf", () => {
  test("is the package document of the npm registry, with the manifest and the dist of the tarball", () => {
    const read = registryEntriesOf([ENTRY]);

    assert.ok(read.ok && read.value[0] !== undefined);
    assert.deepEqual(packumentOf(read.value[0], "http://127.0.0.1:4873"), {
      name: ENTRY.name,
      "dist-tags": { latest: "0.8.1" },
      versions: {
        "0.8.1": {
          ...ENTRY.manifest,
          _id: "@horsies/native-linux-x64-gnu@0.8.1",
          dist: {
            tarball: "http://127.0.0.1:4873/-/tarballs/horsies-native-linux-x64-gnu-0.8.1.tgz",
            integrity: "sha512-abc",
            shasum: "0123",
          },
        },
      },
    });
  });
});

describe("requestTargetOf", () => {
  test("a scoped name with an encoded or a plain slash, and a plain name", () => {
    assert.deepEqual(requestTargetOf("/@horsies%2fnative-linux-x64-gnu"), { kind: "packument", name: "@horsies/native-linux-x64-gnu" });
    assert.deepEqual(requestTargetOf("/@horsies/native-linux-x64-gnu"), { kind: "packument", name: "@horsies/native-linux-x64-gnu" });
    assert.deepEqual(requestTargetOf("/horsies?write=true"), { kind: "packument", name: "horsies" });
  });

  test("a tarball path", () => {
    assert.deepEqual(requestTargetOf("/-/tarballs/horsies-0.8.1.tgz"), { kind: "tarball", filename: "horsies-0.8.1.tgz" });
  });

  test("any other path is unknown", () => {
    assert.deepEqual(requestTargetOf("/-/tarballs/../index.json"), { kind: "unknown" });
    assert.deepEqual(requestTargetOf("/-/npm/v1/security/advisories/bulk"), { kind: "unknown" });
    assert.deepEqual(requestTargetOf("/%E0%A4%A"), { kind: "unknown" });
  });
});

describe("answerOf", () => {
  const packuments = new Map([["horsies", '{"name":"horsies"}']]);
  const tarballs = new Map([["horsies-0.8.1.tgz", new Uint8Array([1, 2])]]);

  test("a known package document and a known tarball", () => {
    assert.deepEqual(answerOf({ kind: "packument", name: "horsies" }, packuments, tarballs), {
      status: 200,
      contentType: "application/json",
      body: '{"name":"horsies"}',
    });

    assert.equal(answerOf({ kind: "tarball", filename: "horsies-0.8.1.tgz" }, packuments, tarballs).status, 200);
  });

  test("an unknown name, an unknown tarball and an unknown path are 404", () => {
    assert.equal(answerOf({ kind: "packument", name: "left-pad" }, packuments, tarballs).status, 404);
    assert.equal(answerOf({ kind: "tarball", filename: "other.tgz" }, packuments, tarballs).status, 404);
    assert.equal(answerOf({ kind: "unknown" }, packuments, tarballs).status, 404);
  });
});
