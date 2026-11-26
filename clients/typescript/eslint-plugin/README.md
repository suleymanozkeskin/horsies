# @horsies/eslint-plugin

ESLint rules for the horsies TypeScript client. The package has one rule, `horsies/no-dropped-result`. The config
below also turns on `@typescript-eslint/switch-exhaustiveness-check` of typescript-eslint: a `switch` over a union
that misses a member is an error. The version of the package is the version of `horsies`.

## Install

The rules read types. They need typescript-eslint and the compiler API of TypeScript. TypeScript 7 has
no compiler API. Install the API of TypeScript 6.0 under the name `typescript`, and TypeScript 7 under a second name
for `tsc`, as the TypeScript 7.0 release notes show:

```json
{
  "devDependencies": {
    "@horsies/eslint-plugin": "<the version of horsies>",
    "@typescript/native": "npm:typescript@^7.0.2",
    "eslint": "^10.0.0",
    "typescript": "npm:@typescript/typescript6@^6.0.2",
    "typescript-eslint": "^8.72.2"
  }
}
```

`tsc` then runs TypeScript 7. ESLint and typescript-eslint import the TypeScript 6.0 API.

## Config

`eslint.config.js`:

```js
import horsies from "@horsies/eslint-plugin";
import { defineConfig } from "eslint/config";
import tseslint from "typescript-eslint";

export default defineConfig({
  files: ["**/*.ts"],
  languageOptions: { parser: tseslint.parser, parserOptions: { projectService: true } },
  plugins: { horsies, "@typescript-eslint": tseslint.plugin },
  rules: {
    "horsies/no-dropped-result": "error",
    "@typescript-eslint/switch-exhaustiveness-check": "error",
  },
});
```

The rules need type information: `parserOptions.projectService` (or `parserOptions.project`). Without it, the lint
stops with the error of typescript-eslint: "You have used a rule which requires type information".

## Rules

### `no-dropped-result`

An expression statement whose value is a horsies `Result`, or a `Promise` of a `Result`, is an error. The statement
drops the value, so the code never reads which error the call gave.

```ts
await app.send(report, { n: 1 }); // error: the statement drops the Result
app.send(report, { n: 1 }); // error: the statement drops a Promise of a Result
await handle.get(); // error: the statement drops the Result
await Promise.all([app.send(report, { n: 1 }), app.send(report, { n: 2 })]); // error: the statement drops an array of Results

void (await app.send(report, { n: 1 })); // passes: `void` is the explicit discard

const sent = await app.send(report, { n: 1 }); // passes: the code reads the Result

if (!sent.ok) {
  return sent.error;
}
```

Passes:
- `void expression`: the explicit discard.
- An assignment: `outcome = await handle.get();` stores the value.
- A value that is not in an expression statement: a declaration, a `return`, an argument.

How the rule finds the type: horsies marks `Ok` and `Err` with a property that exists only in the types. Its key is a
`unique symbol` and its type is `"horsies.Result"`. The rule reads this mark from the type checker. It does not read
the name of a type, of a method or of an import, so it finds the `Result` of every call and of every installed copy
of horsies. A union that has a `Result` member is a `Result` for the rule (`flag ? parse(text) : undefined`). An array
or a tuple whose element type is a `Result` (`Result[]`, `[Result, Result]`) holds a `Result` for the rule, to a nesting
depth of 3. The settled array of `Promise.allSettled` over Results (`PromiseSettledResult<Result>[]`) is such a collection; the rule does not read its `value`.

`@typescript-eslint/no-floating-promises` refuses a `Promise` that is not awaited, of any type. This rule refuses a
`Result` that is dropped, awaited or not. The two rules can run together.

### `@typescript-eslint/switch-exhaustiveness-check`

A rule of typescript-eslint, with its default options. A `switch` over a union (for example the `kind` of a
`ReadError` or of a send error) that has no `case` for a member is an error, also when the `switch` has a `default`.
Write a `case` for each member, and a `never` check in `default`, so that a new member fails the type check:

```ts
switch (error.kind) {
  case "waitTimeout":
    return "the task is still running";
  // ... a case for each other kind
  default: {
    const unreachable: never = error;

    return unreachable;
  }
}
```

## Oxlint

Oxlint 1.93.0 gives JavaScript plugins no type information: `context.sourceCode.parserServices` is an empty object.
The type-aware rules of Oxlint run in a separate program and do not take custom rules. Loaded as an Oxlint
JavaScript plugin, `horsies/no-dropped-result` stops with the error above, and `oxlint` exits with status 1. A project
that lints with Oxlint runs ESLint with the config above next to Oxlint.
