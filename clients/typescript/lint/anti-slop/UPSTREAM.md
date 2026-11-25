# Vendored anti-slop rules

Source: [dmmulroy/anti-slop](https://github.com/dmmulroy/anti-slop), commit
`63dbb09f73438e550a6991dc6d778f979ffbf183` (2026-11-20). License: MIT, kept in `LICENSE`. The ESLint Stylistic
engine under `vendor/eslint-stylistic/` keeps its own `LICENSE` and `UPSTREAM.md`.

## Rules in this folder

`@cloudflare/oxlint-config` 0.2.0 is the base rule set of the client. These are the anti-slop rules that it does not
contain:

| Rule | Note |
|---|---|
| `no-runtime-typeof` | Off in `horsies/src/codec/**` (`oxlint.config.ts`): the codec parses unparsed input |
| `no-shape-in-symbol-names` | |
| `no-unknown-parameters` | Off in `horsies/src/codec/**`, for the same reason. A parameter named `cause` and the subject of a type predicate stay allowed |
| `require-readable-spacing` | Whitespace-only autofix (`oxlint --fix`) |
| `require-safety-comment-for-type-assertion` | Marker `SAFETY:` |

Not copied:
- The 12 generic rules that `@cloudflare/oxlint-config` contains as `quality/*` (through `@nkzw/oxlint-config`
  4.1.0), and `oxc/no-accumulating-spread`, which it enables.
- `no-module-mocking`: it matches only Vitest `vi` and Jest `jest`. The client's tests run on `bun test`.
- The five Effect rules (`src/effect/`): the client does not use Effect.
- `require-readable-spacing-cli.test.ts`: it runs `pnpm exec oxlint`.
- The shared helpers that only the rules above use.

## Copied files

From upstream `src/`, unchanged:
- `rules/<rule>.ts` and `rules/<rule>.test.ts` for each rule in the table
- `shared/function-parameters.ts`
- `vendor/eslint-stylistic/` (all five files)

Changed: `index.ts` registers only the five rules of the table. Upstream registers 18.

## Tests

The `RuleTester` of `oxlint/plugins-dev` refuses to parse on Bun. `tests/lint/anti-slop-rules.test.ts` runs each
`rules/*.test.ts` file in a Node process, so `bun test` covers every rule here. `bun check` type-checks this folder
through `lint/tsconfig.json`. `oxlint` does not lint this folder (`ignorePatterns`).

## Update

1. Choose an upstream commit. Compare each copied file with the same file at that commit.
2. Port the changes. Keep the change to `index.ts`.
3. Update the commit in this file. Keep both `LICENSE` files.
4. Keep `@oxlint/plugins` and `oxlint` at the same exact version in `clients/typescript/package.json`.
5. Run `bun check` and `bun test` in `clients/typescript/`.
