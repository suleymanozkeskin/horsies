import cloudflare from "@cloudflare/oxlint-config";
import { defineConfig } from "oxlint";

// The lint rules of the client's own code. Every rule is an error.
// - `@cloudflare/oxlint-config` gives the base set, type-aware through `tsrs`.
// - `lint/anti-slop/` holds the anti-slop rules that the base set does not contain
//   (vendored; provenance and the rule comparison in `lint/anti-slop/UPSTREAM.md`).
export default defineConfig({
  extends: [cloudflare],
  env: cloudflare.env,
  ignorePatterns: [...cloudflare.ignorePatterns, "lint/anti-slop/**"],
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
  ],
});
