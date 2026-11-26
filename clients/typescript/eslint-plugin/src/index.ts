// The ESLint plugin of the horsies TypeScript client. In a flat config: `plugins: { horsies }` and the rule
// `"horsies/no-dropped-result": "error"` (README.md).

import manifest from "../package.json" with { type: "json" };
import { noDroppedResult } from "./no-dropped-result.ts";

/**
 * The published type of the plugin object: its `meta` only, as typescript-eslint types its own plugin
 * (`CompatiblePlugin`). The `Plugin` type of ESLint 10 and the `FlatConfig.Plugin` type of typescript-eslint both
 * accept it. The object also holds `rules`; the rule module type of typescript-eslint is not assignable to the rule
 * type of ESLint 10, so a type with `rules` would refuse `plugins: { horsies }` in `defineConfig`.
 *
 * `meta` names the package and its version, so a cached lint run sees a new version.
 */
export interface HorsiesPlugin {
  readonly meta: { readonly name: string; readonly version: string; readonly namespace: string };
}

const pluginObject = {
  meta: { name: manifest.name, version: manifest.version, namespace: "horsies" },
  rules: { "no-dropped-result": noDroppedResult },
};

const plugin: HorsiesPlugin = pluginObject;

export default plugin;
