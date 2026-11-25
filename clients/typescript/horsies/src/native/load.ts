// The loader of the native addon.
//
// - Importing this module loads nothing. The first call of `loadAddon` loads the addon; every later
//   call gives the outcome of that first call.
// - Only the main thread of a process loads the addon. In a `worker_threads` isolate, `loadAddon`
//   answers `outsideMainThread` and loads nothing.
// - The addon file comes from one of two sources:
//   - the file that the environment variable `HORSIES_NATIVE_ADDON_PATH` names (a build of
//     `clients/typescript/bridge`, for tests and development);
//   - else the platform package `@horsies/native-<platform>` of the host.

import { closeSync, openSync, readSync } from "node:fs";
import { createRequire } from "node:module";
import { isMainThread, threadId } from "node:worker_threads";

/** The environment variable that names an addon file to load in place of the platform package. */
export const ADDON_PATH_VARIABLE = "HORSIES_NATIVE_ADDON_PATH";

/** The name prefix of the platform packages. Equal to `napi.packageName` of `bridge/package.json`, plus `-`. */
const PLATFORM_PACKAGE_PREFIX = "@horsies/native-";

/** The platforms that have a platform package: the targets of `napi.targets` in `bridge/package.json`. */
export type NativePlatform =
  | "darwin-arm64"
  | "linux-arm64-gnu"
  | "linux-arm64-musl"
  | "linux-x64-gnu"
  | "linux-x64-musl";

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

/** Where the addon file comes from. */
export type AddonSource =
  | { readonly kind: "pathVariable"; readonly path: string }
  | { readonly kind: "platformPackage"; readonly packageName: string };

/** The outcome of opening one addon source. */
export type AddonOpen =
  | { readonly kind: "opened"; readonly exports: object }
  | { readonly kind: "openFailed"; readonly cause: string };

/** The outcome of `loadAddon`. */
export type AddonLoad =
  | { readonly kind: "loaded"; readonly exports: object; readonly source: AddonSource }
  | { readonly kind: "outsideMainThread"; readonly threadId: number }
  | { readonly kind: "platformUnsupported"; readonly host: string }
  | { readonly kind: "loadFailed"; readonly source: AddonSource; readonly cause: string };

/** The kept outcome of the first `loadAddon` call of this module instance. */
type LoadSlot = { readonly kind: "empty" } | { readonly kind: "filled"; readonly load: AddonLoad };

let loadSlot: LoadSlot = { kind: "empty" };

/**
 * Load the addon at the first call; give the kept outcome at every later call.
 * Effects of the first call on the main thread: on Linux it reads `/proc/self/maps`; it resolves and
 * opens one addon file. It starts no core runtime: the addon starts nothing at load.
 */
export function loadAddon(): AddonLoad {
  switch (loadSlot.kind) {
    case "filled":
      return loadSlot.load;
    case "empty": {
      const load = firstLoad();

      loadSlot = { kind: "filled", load };

      return load;
    }

    default:
      return unreachable(loadSlot);
  }
}

function firstLoad(): AddonLoad {
  if (!isMainThread) {
    return { kind: "outsideMainThread", threadId };
  }

  const source = addonSource(process.env[ADDON_PATH_VARIABLE], nativePlatform(hostSystem()));

  switch (source.kind) {
    case "unsupported":
      return { kind: "platformUnsupported", host: source.host };
    case "pathVariable":
    case "platformPackage": {
      const opened = openAddon(source);

      switch (opened.kind) {
        case "opened":
          return { kind: "loaded", exports: opened.exports, source };
        case "openFailed":
          return { kind: "loadFailed", source, cause: opened.cause };
        default:
          return unreachable(opened);
      }
    }

    default:
      return unreachable(source);
  }
}

/**
 * The source of the addon: the path that the variable names, else the platform package of the host.
 * A host with no platform package and no variable gives `unsupported`. Pure.
 */
export function addonSource(
  pathVariable: string | undefined,
  lookup: PlatformLookup,
): AddonSource | Extract<PlatformLookup, { kind: "unsupported" }> {
  if (pathVariable !== undefined) {
    return { kind: "pathVariable", path: pathVariable };
  }

  switch (lookup.kind) {
    case "supported":
      return { kind: "platformPackage", packageName: `${PLATFORM_PACKAGE_PREFIX}${lookup.platform}` };
    case "unsupported":
      return lookup;
    default:
      return unreachable(lookup);
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
 * or library, `glibc` when a mapped file is `libc.so.6`. Neither, both, or a failed read give
 * `undetected`. Pure.
 */
export function linuxLibc(maps: ProcessMapsRead): LinuxLibc {
  switch (maps.kind) {
    case "readFailed":
      return "undetected";
    case "read": {
      const lines = maps.text.split("\n");
      const musl = lines.some((line) => line.includes("/ld-musl-") || line.includes("/libc.musl-"));
      const glibc = lines.some((line) => line.endsWith("/libc.so.6"));

      if (musl === glibc) {
        return "undetected";
      }

      return musl ? "musl" : "glibc";
    }

    default:
      return unreachable(maps);
  }
}

/**
 * Open the addon file of a source with `process.dlopen`. Effects: resolves a package name against
 * this module's location; opens the shared object, which registers the Node-API module in this
 * environment. A failure of either step is `openFailed` with its cause.
 */
export function openAddon(source: AddonSource): AddonOpen {
  const module: { exports: object } = { exports: {} };

  try {
    const path =
      source.kind === "pathVariable"
        ? source.path
        : createRequire(import.meta.url).resolve(source.packageName);

    process.dlopen(module, path);
  } catch (cause) {
    return { kind: "openFailed", cause: describeThrown(cause) };
  }

  return { kind: "opened", exports: module.exports };
}

function describeThrown(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

function unreachable(value: never): never {
  throw new Error(`unreachable value: ${JSON.stringify(value)}`);
}
