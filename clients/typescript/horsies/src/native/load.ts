// The loader of the native addon.
//
// - Importing this module loads nothing. The first load of a process loads the addon; every later
//   call gives the outcome of that first load.
// - Only the main thread of a process loads the addon. In a `worker_threads` isolate, a load
//   answers `outsideMainThread` and loads nothing.
// - The package loads the addon from the platform package `@horsies/native-<platform>` of the host
//   (`loadAddon`). No environment variable chooses the file. The checks before the open (main thread,
//   platform, C library) are in this module. The open step comes from the import
//   `#native/platform-open` of `package.json` `imports`: on Bun its `bun` condition
//   (`platform-open/bun.ts`, a literal `require` per platform, which `bun build --compile` embeds);
//   on every other runtime its `default` condition (`platform-open/node.ts`, a resolve and
//   `process.dlopen`). Tests and dev builds load a build of
//   `clients/typescript/bridge` through the dev-only entry `dev-addon.ts`, which is not in the
//   published package.

import { closeSync, openSync, readSync } from "node:fs";
import { isMainThread, threadId } from "node:worker_threads";

import { openPlatformPackage } from "#native/platform-open";

import { describeThrown, platformPackageName } from "./addon-file.ts";
import type { AddonOpen, NativePlatform } from "./addon-file.ts";
import { impossibleValueDefect } from "./call/impossible-value.ts";

/** The C library of a Linux process. `undetected`: the process report shows neither library. */
export type LinuxLibc = "glibc" | "musl" | "undetected";

/** The facts of the host that choose the platform package. */
export type HostSystem =
  | { readonly kind: "darwin"; readonly arch: NodeJS.Architecture }
  | { readonly kind: "linux"; readonly arch: NodeJS.Architecture; readonly libc: LinuxLibc }
  | { readonly kind: "other"; readonly platform: NodeJS.Platform; readonly arch: NodeJS.Architecture };

/** The platform package of a host, or the host text when no package serves it. */
export type PlatformLookup =
  | { readonly kind: "supported"; readonly platform: NativePlatform }
  | { readonly kind: "unsupported"; readonly host: string };

/**
 * Where the addon file comes from: the platform package of the host, or a dev build that the
 * dev-only entry (`dev-addon.ts`) names.
 */
export type AddonSource =
  | { readonly kind: "platformPackage"; readonly packageName: string }
  | { readonly kind: "devBuild"; readonly path: string };

/** The outcome of a load. */
export type AddonLoad =
  | { readonly kind: "loaded"; readonly exports: object; readonly source: AddonSource }
  | { readonly kind: "outsideMainThread"; readonly threadId: number }
  | { readonly kind: "platformUnsupported"; readonly host: string }
  | { readonly kind: "loadFailed"; readonly source: AddonSource; readonly cause: string };

/** The kept outcome of the first load of this module instance. */
type LoadSlot = { readonly kind: "empty" } | { readonly kind: "filled"; readonly load: AddonLoad };

let loadSlot: LoadSlot = { kind: "empty" };

/**
 * Load the addon from the platform package of the host at the first load of the process; give the
 * kept outcome at every later call. Effects of the first load on the main thread: on Linux it reads
 * `/proc/self/maps`; it opens the addon file of one platform package. It starts no core runtime: the addon
 * starts nothing at load.
 */
export function loadAddon(): AddonLoad {
  return keptLoad(loadFromPlatformPackage);
}

/**
 * The outcome of the first load of this module instance. At the first call on the main thread it
 * runs `firstLoad` and keeps its outcome; in a `worker_threads` isolate it keeps `outsideMainThread`
 * and runs nothing. Every later call gives the kept outcome and runs nothing, whatever `firstLoad` it
 * passes. `loadAddon` passes the platform package load; the dev-only entry passes its own.
 */
export function keptLoad(firstLoad: () => AddonLoad): AddonLoad {
  switch (loadSlot.kind) {
    case "filled":
      return loadSlot.load;
    case "empty": {
      const load: AddonLoad = isMainThread ? firstLoad() : { kind: "outsideMainThread", threadId };

      loadSlot = { kind: "filled", load };

      return load;
    }

    default:
      return unreachable(loadSlot);
  }
}

/** Load the platform package of this host. Effects: as `loadAddon`. */
function loadFromPlatformPackage(): AddonLoad {
  const lookup = nativePlatform(hostSystem());

  switch (lookup.kind) {
    case "unsupported":
      return { kind: "platformUnsupported", host: lookup.host };
    case "supported": {
      const packageName = platformPackageName(lookup.platform);

      return loadedFrom({ kind: "platformPackage", packageName }, openPlatformPackage(lookup.platform));
    }

    default:
      return unreachable(lookup);
  }
}

/** The load outcome of opening the file of `source`. Pure. */
export function loadedFrom(source: AddonSource, opened: AddonOpen): AddonLoad {
  switch (opened.kind) {
    case "opened":
      return { kind: "loaded", exports: opened.exports, source };
    case "openFailed":
      return { kind: "loadFailed", source, cause: opened.cause };
    default:
      return unreachable(opened);
  }
}

/** The platform package of a host. Pure. */
export function nativePlatform(host: HostSystem): PlatformLookup {
  switch (host.kind) {
    case "darwin":
      return host.arch === "arm64"
        ? { kind: "supported", platform: "darwin-arm64" }
        : { kind: "unsupported", host: `darwin-${host.arch}` };
    case "linux":
      return linuxPlatform(host.arch, host.libc);
    case "other":
      return { kind: "unsupported", host: `${host.platform}-${host.arch}` };
    default:
      return unreachable(host);
  }
}

/** The Linux platform packages, by C library and CPU. */
const LINUX_PLATFORMS: Readonly<
  Record<"glibc" | "musl", Readonly<Record<"arm64" | "x64", NativePlatform>>>
> = {
  glibc: { arm64: "linux-arm64-gnu", x64: "linux-x64-gnu" },
  musl: { arm64: "linux-arm64-musl", x64: "linux-x64-musl" },
};

function linuxPlatform(arch: NodeJS.Architecture, libc: LinuxLibc): PlatformLookup {
  const host = `linux-${arch}-${libc}`;

  switch (libc) {
    case "undetected":
      return { kind: "unsupported", host };
    case "glibc":
    case "musl":
      switch (arch) {
        case "arm64":
        case "x64":
          return { kind: "supported", platform: LINUX_PLATFORMS[libc][arch] };
        default:
          return { kind: "unsupported", host };
      }

    default:
      return unreachable(libc);
  }
}

/** The host facts of this process. Effects: on Linux it reads `/proc/self/maps`. */
export function hostSystem(): HostSystem {
  switch (process.platform) {
    case "darwin":
      return { kind: "darwin", arch: process.arch };
    case "linux":
      return { kind: "linux", arch: process.arch, libc: linuxLibc(readProcessMaps()) };
    default:
      return { kind: "other", platform: process.platform, arch: process.arch };
  }
}

/** The outcome of a read of the memory map of this process (`/proc/self/maps`). */
export type ProcessMapsRead =
  | { readonly kind: "read"; readonly text: string }
  | { readonly kind: "readFailed"; readonly cause: string };

/** The mapped file of a glibc before 2.34: `libc.so.6` links to it, and the map shows this name. */
const GLIBC_VERSIONED_FILE = /\/libc-2\.\d+\.so$/u;

/** The most bytes of `/proc/self/maps` that the C library check reads. */
const PROCESS_MAPS_READ_BOUND_BYTES = 1_048_576;

/**
 * Read `/proc/self/maps`, at most `PROCESS_MAPS_READ_BOUND_BYTES`. A longer map is cut at the bound.
 * The file reports size 0, so the read runs to its end.
 */
function readProcessMaps(): ProcessMapsRead {
  const buffer = Buffer.alloc(PROCESS_MAPS_READ_BOUND_BYTES);
  let filled = 0;

  try {
    const descriptor = openSync("/proc/self/maps", "r");

    try {
      let lastRead = 1;

      while (lastRead > 0 && filled < buffer.length) {
        lastRead = readSync(descriptor, buffer, filled, buffer.length - filled, null);
        filled += lastRead;
      }
    } finally {
      closeSync(descriptor);
    }
  } catch (cause) {
    return { kind: "readFailed", cause: describeThrown(cause) };
  }

  return { kind: "read", text: buffer.toString("utf8", 0, filled) };
}

/**
 * The C library of the process, from its memory map: `musl` when a mapped file is the musl loader
 * or library, `glibc` when a mapped file is the glibc library: `libc.so.6` (glibc 2.34 and later)
 * or `libc-2.<minor>.so` (earlier glibc, where `libc.so.6` is a link to that file and the map shows
 * the target). Neither, both, or a failed read give
 * `undetected`. Pure.
 */
export function linuxLibc(maps: ProcessMapsRead): LinuxLibc {
  switch (maps.kind) {
    case "readFailed":
      return "undetected";
    case "read": {
      const lines = maps.text.split("\n");
      const musl = lines.some((line) => line.includes("/ld-musl-") || line.includes("/libc.musl-"));

      const glibc = lines.some(
        (line) => line.endsWith("/libc.so.6") || GLIBC_VERSIONED_FILE.test(line),
      );

      if (musl === glibc) {
        return "undetected";
      }

      return musl ? "musl" : "glibc";
    }

    default:
      return unreachable(maps);
  }
}

/** Throw the defect of an impossible value. The loader has no addon to report it to, so it throws. */
function unreachable(value: never): never {
  throw impossibleValueDefect(value);
}
