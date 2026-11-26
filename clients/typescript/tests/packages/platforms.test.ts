// The reader of the platform list: one case per refusal, the committed list, and the agreement of the list with the
// platforms of the loader.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { isPlainObject } from "../../horsies/src/codec/values.ts";
import type { ParsedJson } from "../public-api/package.ts";
import { expectedPlatform, NATIVE_PLATFORMS, platformListOf, platformListRefusedText, readPlatformList } from "./platforms.ts";
import type { PlatformListRefused } from "./platforms.ts";

/** A valid row of the list, with `changes` applied to its top-level fields. */
function row(changes: Readonly<Record<string, ParsedJson>> = {}): Readonly<Record<string, ParsedJson>> {
  return {
    name: "linux-x86_64",
    release: "publish",
    runner: "ubuntu-latest",
    rust_target: "x86_64-unknown-linux-gnu",
    c_library: "glibc",
    wheel: { manylinux: "2_28", platform_tag: "manylinux_2_28_x86_64", import_image: "python:3.13-slim-trixie", web_ui_check: true },
    npm: {
      platform: "linux-x64-gnu",
      cpu: "x64",
      build: { kind: "glibc-2.17-toolchain" },
      install: { kind: "container", node_variant: "trixie-slim", bun_variant: "debian" },
    },
    ...changes,
  };
}

/** The npm section of the valid row, with `changes` applied. */
function npm(changes: Readonly<Record<string, ParsedJson>>): ParsedJson {
  const base = row()["npm"];

  // SAFETY: the npm section of `row()` is an object literal of parsed JSON.
  return isPlainObject(base) ? { ...(base as Readonly<Record<string, ParsedJson>>), ...changes } : null;
}

/** The refusal of a list of `rows`. */
function refusalOf(...rows: readonly ParsedJson[]): PlatformListRefused | "read" {
  const read = platformListOf({ platforms: rows });

  return read.ok ? "read" : read.error;
}

describe("platformListOf", () => {
  test("reads a valid row with its npm fields", () => {
    const read = platformListOf({ platforms: [row()] });

    assert.ok(read.ok);
    assert.deepEqual(read.value, [
      {
        name: "linux-x86_64",
        release: "publish",
        runner: "ubuntu-latest",
        rustTarget: "x86_64-unknown-linux-gnu",
        cLibrary: "glibc",
        platform: "linux-x64-gnu",
        cpu: "x64",
        build: { kind: "glibc-2.17-toolchain" },
        install: { kind: "container", nodeVariant: "trixie-slim", bunVariant: "debian" },
      },
    ]);
  });

  test("NoPlatforms: no list, or an empty list", () => {
    assert.deepEqual(platformListOf({}), { ok: false, error: { kind: "NoPlatforms" } });
    assert.deepEqual(platformListOf({ platforms: [] }), { ok: false, error: { kind: "NoPlatforms" } });
  });

  test("RowForm: a row that is not an object, an extra field, a missing text, a missing wheel", () => {
    assert.deepEqual(refusalOf("linux"), { kind: "RowForm", index: 0, field: "(the row)" });
    assert.deepEqual(refusalOf(row({ extra: 1 })), { kind: "RowForm", index: 0, field: "extra" });
    assert.deepEqual(refusalOf(row({ runner: "" })), { kind: "RowForm", index: 0, field: "runner" });
    assert.deepEqual(refusalOf(row({ wheel: "none" })), { kind: "RowForm", index: 0, field: "wheel" });
    assert.deepEqual(refusalOf(row({ npm: npm({ cpu: 64 }) })), { kind: "RowForm", index: 0, field: "npm.cpu" });
  });

  test("RowForm: a build or an install form that is not one of the list", () => {
    assert.deepEqual(refusalOf(row({ npm: npm({ build: { kind: "cross" } }) })), { kind: "RowForm", index: 0, field: "npm.build" });

    assert.deepEqual(refusalOf(row({ npm: npm({ build: { kind: "alpine-container" } }) })), {
      kind: "RowForm",
      index: 0,
      field: "npm.build",
    });

    assert.deepEqual(refusalOf(row({ npm: npm({ install: { kind: "container", node_variant: "slim" } }) })), {
      kind: "RowForm",
      index: 0,
      field: "npm.install",
    });
  });

  test("reads a manylinux build with its image and glibc floor", () => {
    const build = { kind: "manylinux-container", image: "quay.io/pypa/manylinux_2_28_x86_64:2026.11.21-1", glibc_floor: "2.28" };
    const read = platformListOf({ platforms: [row({ npm: npm({ build }) })] });

    assert.ok(read.ok);
    assert.deepEqual(read.value[0]?.build, {
      kind: "manylinux-container",
      image: "quay.io/pypa/manylinux_2_28_x86_64:2026.11.21-1",
      glibcFloor: [2, 28],
    });
  });

  test("RowForm: a manylinux build without an image or with a glibc floor that is not <major>.<minor>", () => {
    for (const build of [
      { kind: "manylinux-container", glibc_floor: "2.28" },
      { kind: "manylinux-container", image: "quay.io/pypa/manylinux_2_28_x86_64", glibc_floor: "2_28" },
    ]) {
      assert.deepEqual(refusalOf(row({ npm: npm({ build }) })), { kind: "RowForm", index: 0, field: "npm.build" });
    }
  });

  test("BuildMismatch: a build form that does not build for the row's C library", () => {
    assert.deepEqual(refusalOf(row({ npm: npm({ build: { kind: "alpine-container", node_variant: "alpine3.23" } }) })), {
      kind: "BuildMismatch",
      row: "linux-x86_64",
      build: "alpine-container",
      cLibrary: "glibc",
    });

    assert.deepEqual(refusalOf(row({ npm: npm({ build: { kind: "runner" } }) })), {
      kind: "BuildMismatch",
      row: "linux-x86_64",
      build: "runner",
      cLibrary: "glibc",
    });
  });

  test("UnknownValue: release, c_library, npm.cpu and npm.platform outside their sets", () => {
    assert.deepEqual(refusalOf(row({ release: "later" })), { kind: "UnknownValue", row: "linux-x86_64", field: "release", value: "later" });

    assert.deepEqual(refusalOf(row({ c_library: "bionic" })), {
      kind: "UnknownValue",
      row: "linux-x86_64",
      field: "c_library",
      value: "bionic",
    });

    assert.deepEqual(refusalOf(row({ npm: npm({ cpu: "ia32" }) })), { kind: "UnknownValue", row: "linux-x86_64", field: "npm.cpu", value: "ia32" });

    assert.deepEqual(refusalOf(row({ npm: npm({ platform: "win32-x64-msvc" }) })), {
      kind: "UnknownValue",
      row: "linux-x86_64",
      field: "npm.platform",
      value: "win32-x64-msvc",
    });
  });

  test("PlatformMismatch: a platform that the C library and the CPU do not give", () => {
    assert.deepEqual(refusalOf(row({ npm: npm({ platform: "linux-x64-musl" }) })), {
      kind: "PlatformMismatch",
      row: "linux-x86_64",
      platform: "linux-x64-musl",
      expected: "linux-x64-gnu",
    });
  });

  test("RepeatedName and RepeatedPlatform", () => {
    assert.deepEqual(refusalOf(row(), row()), { kind: "RepeatedName", row: "linux-x86_64" });
    assert.deepEqual(refusalOf(row(), row({ name: "other" })), { kind: "RepeatedPlatform", platform: "linux-x64-gnu" });
  });
});

describe("expectedPlatform", () => {
  test("gives the napi platform of each C library", () => {
    assert.equal(expectedPlatform("glibc", "arm64"), "linux-arm64-gnu");
    assert.equal(expectedPlatform("musl", "x64"), "linux-x64-musl");
    assert.equal(expectedPlatform("libsystem", "arm64"), "darwin-arm64");
  });
});

describe("platformListRefusedText", () => {
  test("names the file and the row", () => {
    assert.equal(
      platformListRefusedText({ kind: "PlatformMismatch", row: "r", platform: "linux-x64-musl", expected: "linux-x64-gnu" }),
      ".github/release-platforms.json: row r: npm.platform is linux-x64-musl; its C library and CPU give linux-x64-gnu",
    );
  });
});

describe("the committed platform list", () => {
  test("reads, and lists each platform of the loader once", () => {
    const read = readPlatformList();

    assert.ok(read.ok, read.ok ? "" : platformListRefusedText(read.error));
    assert.deepEqual(read.value.map((entry) => entry.platform).toSorted(), Object.keys(NATIVE_PLATFORMS).toSorted());
  });

  test("publishes no musl platform before the musl measurement", () => {
    const read = readPlatformList();

    assert.ok(read.ok);
    assert.deepEqual(
      read.value.filter((entry) => entry.cLibrary === "musl").map((entry) => entry.release),
      ["hold", "hold"],
    );
  });
});
