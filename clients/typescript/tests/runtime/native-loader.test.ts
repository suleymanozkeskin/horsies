// The outcomes of the addon loader that need no built addon. Runs on Node and on Bun.

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { Worker } from "node:worker_threads";

import {
  addonSource,
  linuxLibc,
  nativePlatform,
  openAddon,
} from "../../horsies/src/native/load.ts";

const GLIBC_MAPS = [
  "55d0c0a00000-55d0c0a01000 r--p 00000000 08:01 1 /usr/bin/node",
  "7f1c2c000000-7f1c2c028000 r--p 00000000 08:01 2 /usr/lib/x86_64-linux-gnu/libc.so.6",
].join("\n");

const MUSL_MAPS = [
  "55d0c0a00000-55d0c0a01000 r--p 00000000 08:01 1 /usr/local/bin/node",
  "7f1c2c000000-7f1c2c014000 r--p 00000000 08:01 2 /lib/ld-musl-x86_64.so.1",
].join("\n");

describe("nativePlatform", () => {
  test("gives darwin-arm64 for macOS on arm64", () => {
    assert.deepEqual(nativePlatform({ kind: "darwin", arch: "arm64" }), {
      kind: "supported",
      platform: "darwin-arm64",
    });
  });

  test("refuses macOS on x64", () => {
    assert.deepEqual(nativePlatform({ kind: "darwin", arch: "x64" }), {
      kind: "unsupported",
      host: "darwin-x64",
    });
  });

  test("gives the gnu package for Linux with glibc", () => {
    assert.deepEqual(nativePlatform({ kind: "linux", arch: "x64", libc: "glibc" }), {
      kind: "supported",
      platform: "linux-x64-gnu",
    });
  });

  test("gives the musl package for Linux with musl", () => {
    assert.deepEqual(nativePlatform({ kind: "linux", arch: "arm64", libc: "musl" }), {
      kind: "supported",
      platform: "linux-arm64-musl",
    });
  });

  test("refuses Linux when the C library is undetected", () => {
    assert.deepEqual(nativePlatform({ kind: "linux", arch: "x64", libc: "undetected" }), {
      kind: "unsupported",
      host: "linux-x64-undetected",
    });
  });

  test("refuses a Linux CPU with no package", () => {
    assert.deepEqual(nativePlatform({ kind: "linux", arch: "ia32", libc: "glibc" }), {
      kind: "unsupported",
      host: "linux-ia32-glibc",
    });
  });

  test("refuses another operating system", () => {
    assert.deepEqual(nativePlatform({ kind: "other", platform: "win32", arch: "x64" }), {
      kind: "unsupported",
      host: "win32-x64",
    });
  });
});

describe("linuxLibc", () => {
  test("gives glibc when libc.so.6 is mapped", () => {
    assert.equal(linuxLibc({ kind: "read", text: GLIBC_MAPS }), "glibc");
  });

  test("gives glibc when the versioned library of glibc 2.28 is mapped", () => {
    assert.equal(
      linuxLibc({ kind: "read", text: "7f1c2c000000-7f1c2c028000 r-xp 0 08:01 2 /usr/lib64/libc-2.28.so" }),
      "glibc",
    );
  });

  test("gives glibc when the versioned library of glibc 2.31 is mapped", () => {
    assert.equal(
      linuxLibc({ kind: "read", text: "7f1c2c000000-7f1c2c028000 r-xp 0 08:01 2 /usr/lib/x86_64-linux-gnu/libc-2.31.so" }),
      "glibc",
    );
  });

  test("a file that only starts like the versioned library is not glibc", () => {
    assert.equal(linuxLibc({ kind: "read", text: "7f1c2c000000-7f1c2c028000 r-xp 0 08:01 2 /opt/libc-2.31.so.bak" }), "undetected");
  });

  test("gives musl when the musl loader is mapped", () => {
    assert.equal(linuxLibc({ kind: "read", text: MUSL_MAPS }), "musl");
  });

  test("gives undetected when neither library is mapped", () => {
    assert.equal(linuxLibc({ kind: "read", text: "55d0c0a00000-55d0c0a01000 r--p 0 0:0 1 /x" }), "undetected");
  });

  test("gives undetected when both libraries are mapped", () => {
    assert.equal(linuxLibc({ kind: "read", text: `${GLIBC_MAPS}\n${MUSL_MAPS}` }), "undetected");
  });

  test("gives undetected when the map could not be read", () => {
    assert.equal(linuxLibc({ kind: "readFailed", cause: "no such file" }), "undetected");
  });
});

describe("addonSource", () => {
  test("takes the path that the variable names, on any host", () => {
    assert.deepEqual(addonSource("/builds/horsies.node", { kind: "unsupported", host: "win32-x64" }), {
      kind: "pathVariable",
      path: "/builds/horsies.node",
    });
  });

  test("takes the platform package of a supported host when the variable is unset", () => {
    assert.deepEqual(addonSource(undefined, { kind: "supported", platform: "linux-x64-gnu" }), {
      kind: "platformPackage",
      packageName: "@horsies/native-linux-x64-gnu",
    });
  });

  test("gives the unsupported host when the variable is unset", () => {
    assert.deepEqual(addonSource(undefined, { kind: "unsupported", host: "win32-x64" }), {
      kind: "unsupported",
      host: "win32-x64",
    });
  });
});

describe("openAddon", () => {
  test("fails with the cause when the file does not exist", () => {
    const path = "/nonexistent/horsies-loader-check/horsies.node";
    const opened = openAddon({ kind: "pathVariable", path });

    assert.equal(opened.kind, "openFailed");
    assert.match(opened.kind === "openFailed" ? opened.cause : "", /horsies-loader-check/u);
  });

  test("fails with the cause when the platform package is not installed", () => {
    const opened = openAddon({ kind: "platformPackage", packageName: "@horsies/native-not-a-platform" });

    assert.equal(opened.kind, "openFailed");
    assert.match(opened.kind === "openFailed" ? opened.cause : "", /@horsies\/native-not-a-platform/u);
  });
});

describe("loadAddon", () => {
  test("answers outsideMainThread in a worker thread and loads nothing", async () => {
    const worker = new Worker(new URL("native-loader-worker.ts", import.meta.url));

    const answer = new Promise<string>((resolve, reject) => {
      worker.once("message", (kind: string) => {
        resolve(kind);
      });
      worker.once("error", reject);
    });

    const exited = new Promise<number>((resolve) => {
      worker.once("exit", resolve);
    });

    assert.equal(await answer, "outsideMainThread");
    assert.equal(await exited, 0);
  });
});
