// The open step of the platform package on Node and on every runtime that is not Bun: the
// `default` condition of the import `#native/platform-open` (`package.json` `imports`).

import { createRequire } from "node:module";

import { describeThrown, openAddonFile, platformPackageName } from "../addon-file.ts";
import type { AddonOpen, NativePlatform } from "../addon-file.ts";

/**
 * Resolve the main file of the platform package of `platform` against this module's location, and
 * open it with `process.dlopen`. A package that does not resolve, or a file that does not open, is
 * `openFailed` with its cause.
 */
export function openPlatformPackage(platform: NativePlatform): AddonOpen {
  let path: string;

  try {
    path = createRequire(import.meta.url).resolve(platformPackageName(platform));
  } catch (cause) {
    return { kind: "openFailed", cause: describeThrown(cause) };
  }

  return openAddonFile(path);
}
