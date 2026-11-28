// The facts that the loader (`load.ts`) and the two open steps of the platform package
// (`platform-open/node.ts`, `platform-open/bun.ts`) share: the platform names, the package name of a
// platform, and the outcome of opening one addon file.

/** The name prefix of the platform packages. Equal to `napi.packageName` of `bridge/package.json`, plus `-`. */
const PLATFORM_PACKAGE_PREFIX = "@horsies/native-";

/** The platforms that have a platform package: the `npm.platform` values of `.github/release-platforms.json`. */
export type NativePlatform =
  | "darwin-arm64"
  | "linux-arm64-gnu"
  | "linux-arm64-musl"
  | "linux-x64-gnu"
  | "linux-x64-musl";

/** The outcome of opening one addon file. */
export type AddonOpen =
  | { readonly kind: "opened"; readonly exports: object }
  | { readonly kind: "openFailed"; readonly cause: string };

/** The name of the platform package of `platform`. Pure. */
export function platformPackageName(platform: NativePlatform): string {
  return `${PLATFORM_PACKAGE_PREFIX}${platform}`;
}

/**
 * Open the addon file `path` with `process.dlopen`. Effects: opens the shared object, which registers
 * the Node-API module in this environment. A failure is `openFailed` with its cause.
 */
export function openAddonFile(path: string): AddonOpen {
  const module: { exports: object } = { exports: {} };

  try {
    process.dlopen(module, path);
  } catch (cause) {
    return { kind: "openFailed", cause: describeThrown(cause) };
  }

  return { kind: "opened", exports: module.exports };
}

/** The message of a thrown value. Pure. */
export function describeThrown(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}
