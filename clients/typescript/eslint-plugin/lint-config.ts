// The flat config of README.md: the parser and the plugin of typescript-eslint (`typescript-eslint`, whose plugin
// type ESLint 10 accepts), `horsies/no-dropped-result` and
// `@typescript-eslint/switch-exhaustiveness-check`, every rule an error. The tests lint the fixture project with it
// (tests/typescript-7-project.test.ts), and `lint-client.ts` lints the client's own code with it.

import type { Linter } from "eslint";
import { defineConfig } from "eslint/config";
import tseslint from "typescript-eslint";

import horsies from "./src/index.ts";

/**
 * Where the parser finds the project of a file:
 * - `projectService`: the nearest tsconfig.json of the file (README.md), under `rootDir`.
 * - `projects`: the named tsconfig files, relative to `rootDir`.
 */
export type ProjectSource =
  | { readonly kind: "projectService"; readonly rootDir: string }
  | { readonly kind: "projects"; readonly rootDir: string; readonly tsconfigFiles: readonly string[] };

function parserOptionsOf(source: ProjectSource): Linter.ParserOptions {
  switch (source.kind) {
    case "projectService":
      return { projectService: true, tsconfigRootDir: source.rootDir };
    case "projects":
      return { project: [...source.tsconfigFiles], tsconfigRootDir: source.rootDir };
    default: {
      const unreachable: never = source;

      return unreachable;
    }
  }
}

/** The config of README.md for the TypeScript files under the project source. Pure. */
export function lintConfig(source: ProjectSource): Linter.Config[] {
  return defineConfig({
    files: ["**/*.ts"],
    languageOptions: { parser: tseslint.parser, parserOptions: parserOptionsOf(source) },
    plugins: { horsies, "@typescript-eslint": tseslint.plugin },
    rules: {
      "horsies/no-dropped-result": "error",
      "@typescript-eslint/switch-exhaustiveness-check": "error",
    },
  });
}
