// The dev-only start module of the examples and the showcase: it runs them on the source of the package and the dev
// build of the addon, in this workspace, where the package is not built. It is not part of any example: an app that
// installs `horsies` from npm runs with no start module.
//
//   export HORSIES_NATIVE_ADDON_PATH="$PWD/bridge/horsies.darwin-arm64.node"   # from clients/typescript
//   node --import "$PWD/examples/dev/source-package.ts" examples/quick-start.ts worker
//   bun --preload "$PWD/examples/dev/source-package.ts" examples/quick-start.ts worker
//
// What it does, before the entry of the process loads:
// - On Node, it maps the package names (`horsies`, `horsies/zod`, `horsies/rerun/pg`, `horsies/rerun/postgres`) to
//   their source entries with a module resolve hook. Bun applies the `paths` of `examples/tsconfig.json` itself.
// - On the main thread, it loads the dev build of the addon that `HORSIES_NATIVE_ADDON_PATH` names. A worker thread
//   loads no addon.
//
// The runtime keeps `--import` and `--preload` in `process.execArgv`, and a worker starts each child with the flags
// of its own process, so every child of a worker runs this module first too.

import * as nodeModule from "node:module";
import { isMainThread } from "node:worker_threads";

import { requireDevAddon } from "../../horsies/src/native/dev-addon.ts";

/** The source entry of each package name of the `exports` map of `horsies/package.json`. */
const SOURCE_ENTRIES: ReadonlyMap<string, string> = new Map([
  ["horsies", new URL("../../horsies/src/index.ts", import.meta.url).href],
  ["horsies/zod", new URL("../../horsies/src/zod.ts", import.meta.url).href],
  ["horsies/rerun/pg", new URL("../../horsies/src/rerun/pg.ts", import.meta.url).href],
  ["horsies/rerun/postgres", new URL("../../horsies/src/rerun/postgres.ts", import.meta.url).href],
]);

/** The runtime of this process: Bun when `process.versions.bun` is set, else Node. */
type Runtime = "node" | "bun";

function runtimeOfProcess(): Runtime {
  return process.versions["bun"] === undefined ? "node" : "bun";
}

/** Map each package name to its source entry for the module loads of this thread. Effects: a Node resolve hook. */
function mapPackageNames(runtime: Runtime): void {
  switch (runtime) {
    case "node":
      nodeModule.registerHooks({
        resolve: (specifier, context, nextResolve) => {
          const source = SOURCE_ENTRIES.get(specifier);

          return source === undefined ? nextResolve(specifier, context) : { url: source, shortCircuit: true };
        },
      });

      return;
    case "bun":
      // Bun reads the `paths` of the nearest tsconfig.json when it runs a file.
      return;
    default: {
      const unexpected: never = runtime;

      throw new TypeError(`source-package: a runtime outside its type: ${String(unexpected)}`);
    }
  }
}

mapPackageNames(runtimeOfProcess());

if (isMainThread) {
  // Throws, with the variable and the fix, when the dev addon does not load: no example can run without it.
  void requireDevAddon();
}
