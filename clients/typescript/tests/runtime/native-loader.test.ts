// The outcomes of the addon loader and of its dev-only entry that need no built addon. Runs on Node and
// on Bun.

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";

import {
  DEV_ADDON_PATH_VARIABLE,
  devAddonPath,
  devLoadFailureText,
  devLoadOf,
  loadDevAddon,
} from "../../horsies/src/native/dev-addon.ts";
import { openAddonFile, platformPackageName } from "../../horsies/src/native/addon-file.ts";
import { linuxLibc, loadedFrom, nativePlatform } from "../../horsies/src/native/load.ts";
import type { AddonLoad } from "../../horsies/src/native/load.ts";

const GLIBC_MAPS = [
  "55d0c0a00000-55d0c0a01000 r--p 00000000 08:01 1 /usr/bin/node",
  "7f1c2c000000-7f1c2c028000 r--p 00000000 08:01 2 /usr/lib/x86_64-linux-gnu/libc.so.6",
].join("\n");

const MUSL_MAPS = [
  "55d0c0a00000-55d0c0a01000 r--p 00000000 08:01 1 /usr/local/bin/node",
  "7f1c2c000000-7f1c2c014000 r--p 00000000 08:01 2 /lib/ld-musl-x86_64.so.1",
].join("\n");

/** An absolute path where no file exists. */
const MISSING_BUILD_PATH = "/nonexistent/horsies-loader-check/horsies.node";

/** The longest time a child case may take. */
const CHILD_CASE_BOUND_MS = 30_000;

const CHILD = fileURLToPath(new URL("native-loader-child.ts", import.meta.url));

/**
 * Run one case of `native-loader-child.ts` in a new process of this runtime, with the dev addon
 * variable unset; its JSON line.
 */
function runChild(name: string): Readonly<Record<string, unknown>> {
  const environment = { ...process.env };

  delete environment[DEV_ADDON_PATH_VARIABLE];

  const result = spawnSync(process.execPath, [CHILD, name], {
    encoding: "utf8",
    env: environment,
    timeout: CHILD_CASE_BOUND_MS,
  });

  assert.equal(result.status, 0, `the child case ${name} ended with ${String(result.status)}: ${result.stderr}`);

  // SAFETY: the child writes one JSON object (`CaseOutcome`) as its only stdout line.
  return JSON.parse(result.stdout) as Readonly<Record<string, unknown>>;
}

/** The kind that a loader in a new worker thread answers; `loader` names the loader (see the worker). */
async function workerAnswer(loader: "package" | "dev"): Promise<string> {
  const worker = new Worker(new URL("native-loader-worker.ts", import.meta.url), { workerData: loader });

  const answer = new Promise<string>((resolve, reject) => {
    worker.once("message", (kind: string) => {
      resolve(kind);
    });
    worker.once("error", reject);
  });

  const exited = new Promise<number>((resolve) => {
    worker.once("exit", resolve);
  });

  const kind = await answer;

  assert.equal(await exited, 0);

  return kind;
}

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

describe("platformPackageName", () => {
  test("is the napi package name with the platform", () => {
    assert.equal(platformPackageName("linux-x64-gnu"), "@horsies/native-linux-x64-gnu");
  });
});

describe("loadedFrom", () => {
  test("an opened file is loaded from its source", () => {
    const exports = {};

    assert.deepEqual(loadedFrom({ kind: "devBuild", path: "/a.node" }, { kind: "opened", exports }), {
      kind: "loaded",
      exports,
      source: { kind: "devBuild", path: "/a.node" },
    });
  });

  test("a file that did not open is loadFailed with its source and cause", () => {
    const source = { kind: "platformPackage", packageName: "@horsies/native-linux-x64-gnu" } as const;

    assert.deepEqual(loadedFrom(source, { kind: "openFailed", cause: "no such file" }), {
      kind: "loadFailed",
      source,
      cause: "no such file",
    });
  });
});

describe("openAddonFile", () => {
  test("a file that does not exist fails with the cause", () => {
    const opened = openAddonFile(MISSING_BUILD_PATH);

    assert.equal(opened.kind, "openFailed");
    assert.match(opened.kind === "openFailed" ? opened.cause : "", /horsies-loader-check/u);
  });
});

describe("keptLoad", () => {
  test("runs the first load once and gives its outcome to every later call", () => {
    assert.deepEqual(runChild("kept-load"), {
      first: "platformUnsupported",
      later: "platformUnsupported",
      same: true,
      firstCalls: 1,
      laterCalls: 0,
    });
  });

  test("the package loader answers outsideMainThread in a worker thread and loads nothing", async () => {
    assert.equal(await workerAnswer("package"), "outsideMainThread");
  });
});

describe("devAddonPath", () => {
  test("an absolute path is named", () => {
    assert.deepEqual(devAddonPath("/builds/horsies.node"), { kind: "named", path: "/builds/horsies.node" });
  });

  test("an unset variable is notSet", () => {
    assert.deepEqual(devAddonPath(undefined), { kind: "notSet" });
  });

  test("a relative path is notAbsolute", () => {
    assert.deepEqual(devAddonPath("bridge/horsies.node"), { kind: "notAbsolute", path: "bridge/horsies.node" });
  });

  test("an empty value is notAbsolute", () => {
    assert.deepEqual(devAddonPath(""), { kind: "notAbsolute", path: "" });
  });
});

describe("loadDevAddon", () => {
  test("an unset variable is pathNotSet and loads nothing", () => {
    assert.deepEqual(loadDevAddon(undefined), { kind: "pathNotSet" });
  });

  test("a relative path is pathNotAbsolute and loads nothing", () => {
    assert.deepEqual(loadDevAddon("horsies.node"), { kind: "pathNotAbsolute", path: "horsies.node" });
  });

  test("a file that does not exist is loadFailed, and the package loader keeps that load", () => {
    assert.deepEqual(runChild("dev-missing-file"), {
      dev: "loadFailed",
      causeNamesPath: true,
      kept: "loadFailed",
      keptSource: "devBuild",
    });
  });

  test("after a load of the platform package it is otherLoadKept with that load", () => {
    const outcome = runChild("package-first");

    assert.equal(outcome["dev"], "otherLoadKept");
    assert.equal(outcome["keptIsSame"], true);
    assert.ok(outcome["kept"] === "loadFailed" || outcome["kept"] === "platformUnsupported", JSON.stringify(outcome));
  });

  test("in a worker thread it is outsideMainThread and loads nothing", async () => {
    assert.equal(await workerAnswer("dev"), "outsideMainThread");
  });
});

describe("devLoadOf", () => {
  const exports = {};
  const devSource = { kind: "devBuild", path: MISSING_BUILD_PATH } as const;
  const packageSource = { kind: "platformPackage", packageName: "@horsies/native-linux-x64-gnu" } as const;

  test("a kept load of the same dev build is loaded", () => {
    assert.deepEqual(devLoadOf(MISSING_BUILD_PATH, { kind: "loaded", exports, source: devSource }), {
      kind: "loaded",
      exports,
      path: MISSING_BUILD_PATH,
    });
  });

  test("a kept failure of the same dev build is loadFailed", () => {
    assert.deepEqual(devLoadOf(MISSING_BUILD_PATH, { kind: "loadFailed", source: devSource, cause: "no file" }), {
      kind: "loadFailed",
      path: MISSING_BUILD_PATH,
      cause: "no file",
    });
  });

  test("a kept load of another dev build is otherLoadKept", () => {
    const kept: AddonLoad = { kind: "loaded", exports, source: { kind: "devBuild", path: "/other/horsies.node" } };

    assert.deepEqual(devLoadOf(MISSING_BUILD_PATH, kept), { kind: "otherLoadKept", kept });
  });

  test("a kept load or failure of the platform package is otherLoadKept", () => {
    const loaded: AddonLoad = { kind: "loaded", exports, source: packageSource };
    const failed: AddonLoad = { kind: "loadFailed", source: packageSource, cause: "not installed" };

    assert.deepEqual(devLoadOf(MISSING_BUILD_PATH, loaded), { kind: "otherLoadKept", kept: loaded });
    assert.deepEqual(devLoadOf(MISSING_BUILD_PATH, failed), { kind: "otherLoadKept", kept: failed });
  });

  test("a kept platformUnsupported is otherLoadKept", () => {
    const kept: AddonLoad = { kind: "platformUnsupported", host: "win32-x64" };

    assert.deepEqual(devLoadOf(MISSING_BUILD_PATH, kept), { kind: "otherLoadKept", kept });
  });

  test("a kept outsideMainThread is outsideMainThread", () => {
    assert.deepEqual(devLoadOf(MISSING_BUILD_PATH, { kind: "outsideMainThread", threadId: 4 }), {
      kind: "outsideMainThread",
      threadId: 4,
    });
  });
});

describe("devLoadFailureText", () => {
  test("names the variable when it is unset", () => {
    assert.match(devLoadFailureText({ kind: "pathNotSet" }), /set HORSIES_NATIVE_ADDON_PATH/u);
  });

  test("names the variable and the relative path", () => {
    assert.equal(
      devLoadFailureText({ kind: "pathNotAbsolute", path: "horsies.node" }),
      'HORSIES_NATIVE_ADDON_PATH must be an absolute path; it is "horsies.node"',
    );
  });

  test("names the thread of a worker", () => {
    assert.equal(
      devLoadFailureText({ kind: "outsideMainThread", threadId: 4 }),
      "the dev addon loads only on the main thread; this is thread 4",
    );
  });

  test("names the path and the cause of a failed load", () => {
    assert.equal(
      devLoadFailureText({ kind: "loadFailed", path: "/a.node", cause: "no such file" }),
      "the dev addon did not load from /a.node: no such file",
    );
  });

  test("names the source of the load that the process kept", () => {
    const kept: AddonLoad = {
      kind: "loadFailed",
      source: { kind: "platformPackage", packageName: "@horsies/native-linux-x64-gnu" },
      cause: "not installed",
    };

    assert.equal(
      devLoadFailureText({ kind: "otherLoadKept", kept }),
      "this process loaded the addon from another source first: @horsies/native-linux-x64-gnu",
    );
  });

  test("requireDevAddon throws the text when the variable is unset", () => {
    const outcome = runChild("require-unset");

    assert.equal(outcome["threw"], true);
    assert.match(String(outcome["message"]), /set HORSIES_NATIVE_ADDON_PATH/u);
  });
});
