import { describe, expect, test } from "bun:test";

import { PACKAGE_VERSION } from "../../../horsies/src/_catalog/package_version.ts";
import { INSTRUMENTATION_SCOPE_VERSION } from "../../../horsies/src/telemetry/otel-api.ts";
import { readHorsiesManifest } from "../../packages/manifest.ts";

describe("the package version module", () => {
  test("the generated version is the version of package.json", () => {
    const manifest = readHorsiesManifest();

    expect(manifest.ok ? manifest.value.version : manifest.error).toBe(PACKAGE_VERSION);
  });

  test("the instrumentation scope version is the generated version", () => {
    expect(INSTRUMENTATION_SCOPE_VERSION).toBe(PACKAGE_VERSION);
  });
});
