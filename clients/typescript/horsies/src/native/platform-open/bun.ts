// The open step of the platform package on Bun: the `bun` condition of the import
// `#native/platform-open` (`package.json` `imports`).
//
// Each platform has one `require` of its package name as a literal. `bun build --compile` follows a
// literal `require` and embeds the `.node` file of each platform package that is installed, so a
// compiled single-file program carries the addon. A `require` in a `try` block of a platform package
// that is not installed builds, and throws only when it runs. Bun defines `require` in an ES module.

import { describeThrown } from "../addon-file.ts";
import type { AddonOpen, NativePlatform } from "../addon-file.ts";
import { impossibleValueDefect } from "../call/impossible-value.ts";

/**
 * Require the platform package of `platform`. A package that is not installed, a file that does not
 * open, or a package that gives no object is `openFailed` with its cause. Each `require` is in a `try`
 * block of its own: `bun build --compile` then builds when a platform package is not installed.
 */
export function openPlatformPackage(platform: NativePlatform): AddonOpen {
  switch (platform) {
    case "darwin-arm64":
      try {
        const exports: unknown = require("@horsies/native-darwin-arm64");

        return exports instanceof Object ? { kind: "opened", exports } : notAnObject(String(exports));
      } catch (cause) {
        return { kind: "openFailed", cause: describeThrown(cause) };
      }

    case "linux-arm64-gnu":
      try {
        const exports: unknown = require("@horsies/native-linux-arm64-gnu");

        return exports instanceof Object ? { kind: "opened", exports } : notAnObject(String(exports));
      } catch (cause) {
        return { kind: "openFailed", cause: describeThrown(cause) };
      }

    case "linux-arm64-musl":
      try {
        const exports: unknown = require("@horsies/native-linux-arm64-musl");

        return exports instanceof Object ? { kind: "opened", exports } : notAnObject(String(exports));
      } catch (cause) {
        return { kind: "openFailed", cause: describeThrown(cause) };
      }

    case "linux-x64-gnu":
      try {
        const exports: unknown = require("@horsies/native-linux-x64-gnu");

        return exports instanceof Object ? { kind: "opened", exports } : notAnObject(String(exports));
      } catch (cause) {
        return { kind: "openFailed", cause: describeThrown(cause) };
      }

    case "linux-x64-musl":
      try {
        const exports: unknown = require("@horsies/native-linux-x64-musl");

        return exports instanceof Object ? { kind: "opened", exports } : notAnObject(String(exports));
      } catch (cause) {
        return { kind: "openFailed", cause: describeThrown(cause) };
      }

    default:
      throw impossibleValueDefect(platform);
  }
}

/** The outcome of a platform package that gives `given`, which is not an object. Pure. */
function notAnObject(given: string): AddonOpen {
  return { kind: "openFailed", cause: `the platform package gave ${given}, not the addon exports object` };
}
