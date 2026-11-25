import { defineConfig } from "oxlint";

// The lint rules of the client's own code. Every rule is an error.
// - Base set: the `correctness` category of Oxlint 1.93.0, with Oxlint's default plugins
//   (`eslint`, `typescript`, `unicorn`, `oxc`). It is not type-aware.
// - `lint/anti-slop/` holds the anti-slop rules that `@cloudflare/oxlint-config` does not
//   contain (vendored; provenance and the rule comparison in `lint/anti-slop/UPSTREAM.md`).
//
// `@cloudflare/oxlint-config` (type-aware through `tsrs`) replaces the base set in #2354.
// Its packages are newer than the 10-day `minimumReleaseAge` of the install. The change:
// add `import cloudflare from "@cloudflare/oxlint-config";`, replace `categories` below with
// `extends: [cloudflare]` and `env: cloudflare.env`, and put `...cloudflare.ignorePatterns`
// first in `ignorePatterns`.
export default defineConfig({
  categories: { correctness: "error" },
  ignorePatterns: ["lint/anti-slop/**"],
  jsPlugins: [{ name: "anti-slop", specifier: "./lint/anti-slop/index.ts" }],
  rules: {
    "anti-slop/no-runtime-typeof": "error",
    "anti-slop/no-shape-in-symbol-names": "error",
    "anti-slop/no-unknown-parameters": "error",
    "anti-slop/require-readable-spacing": "error",
    "anti-slop/require-safety-comment-for-type-assertion": "error",
  },
  overrides: [
    {
      // The codec is the boundary that parses stored text into typed values and
      // checks the values that a sender passes. Its input is unparsed by
      // definition: it takes `unknown` and narrows it with `typeof`.
      files: ["horsies/src/codec/**"],
      rules: {
        "anti-slop/no-runtime-typeof": "off",
        "anti-slop/no-unknown-parameters": "off",
      },
    },
    {
      // A test writer of `WireJson` values: it tells the members of the union
      // apart by their runtime type to write each one as JSON text.
      files: ["tests/unit/generated/support/wire-text.ts"],
      rules: {
        "anti-slop/no-runtime-typeof": "off",
      },
    },
  ],
});
