// The selection of the open step of the platform package (`#native/platform-open` of the `imports` map of
// `horsies/package.json`) and the outcomes of both open steps that need no installed platform package. Runs on Node
// and on Bun. The workspace installs no platform package, so each open fails with its cause.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

import { platformPackageName } from "../../horsies/src/native/addon-file.ts";
import type { NativePlatform } from "../../horsies/src/native/addon-file.ts";
import { openPlatformPackage as openOnBun } from "../../horsies/src/native/platform-open/bun.ts";
import { openPlatformPackage as openOnNode } from "../../horsies/src/native/platform-open/node.ts";
import { ClientDefect } from "../../horsies/src/thrown-errors.ts";
import { NATIVE_PLATFORMS } from "../packages/platforms.ts";

const LOADER_FILE = fileURLToPath(new URL("../../horsies/src/native/load.ts", import.meta.url));

const BUN_OPEN_FILE = fileURLToPath(new URL("../../horsies/src/native/platform-open/bun.ts", import.meta.url));

const onBun = process.versions["bun"] !== undefined;

// SAFETY: the keys of a `Record<NativePlatform, true>` are the platform names.
const PLATFORMS = Object.keys(NATIVE_PLATFORMS) as readonly NativePlatform[];

/** The cause of an open that failed, or the kind of an open that did not fail. */
function causeOf(opened: ReturnType<typeof openOnNode>): string {
  return opened.kind === "openFailed" ? opened.cause : opened.kind;
}

describe("the import #native/platform-open", () => {
  test("resolves to the Bun open step on Bun and to the Node open step on Node", () => {
    const resolved = createRequire(LOADER_FILE).resolve("#native/platform-open");
    const expected = onBun ? "/native/platform-open/bun.ts" : "/native/platform-open/node.ts";

    assert.ok(resolved.endsWith(expected), `${resolved} is not ${expected}`);
  });
});

describe("the Bun open step", () => {
  test("each platform name maps to a literal require of its platform package", () => {
    const source = readFileSync(BUN_OPEN_FILE, "utf8");

    for (const platform of PLATFORMS) {
      const literal = `case "${platform}":\n      try {\n        const exports: unknown = require("${platformPackageName(platform)}");`;

      assert.ok(source.includes(literal), `bun.ts has no literal require of ${platformPackageName(platform)} for ${platform}`);
    }
  });

  test("it requires no other specifier", () => {
    const specifiers = [...readFileSync(BUN_OPEN_FILE, "utf8").matchAll(/require\("([^"]+)"\)/gu)].map((match) => match[1]);

    assert.deepEqual(specifiers.toSorted(), PLATFORMS.map(platformPackageName).toSorted());
  });

  test("a platform package that is not installed fails with the cause that names it", { skip: onBun ? false : "Node defines no require in an ES module" }, () => {
    for (const platform of PLATFORMS) {
      assert.match(causeOf(openOnBun(platform)), new RegExp(platformPackageName(platform).replace("/", "\\/"), "u"));
    }
  });

  test("a platform name outside the closed type throws the impossible-value defect", () => {
    // SAFETY: the test passes a value outside the type on purpose.
    assert.throws(() => openOnBun("windows-x64" as NativePlatform), ClientDefect);
  });
});

describe("the Node open step", () => {
  test("a platform package that is not installed fails with the cause that names it", () => {
    for (const platform of PLATFORMS) {
      assert.match(causeOf(openOnNode(platform)), new RegExp(platformPackageName(platform).replace("/", "\\/"), "u"));
    }
  });
});
