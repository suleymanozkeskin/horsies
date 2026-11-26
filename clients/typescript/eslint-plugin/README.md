# @horsies/eslint-plugin

ESLint rules for the horsies TypeScript client. The package has one rule, `horsies/no-dropped-result`. Its version is
the version of `horsies`.

## Install

The rule reads types. It needs the parser of typescript-eslint and the compiler API of TypeScript. TypeScript 7 has
no compiler API. Install the API of TypeScript 6.0 under the name `typescript`, and TypeScript 7 under a second name
for `tsc`, as the TypeScript 7.0 release notes show:

```json
{
  "devDependencies": {
    "@horsies/eslint-plugin": "<the version of horsies>",
    "@typescript-eslint/parser": "^8.72.2",
    "@typescript/native": "npm:typescript@^7.0.2",
    "eslint": "^10.0.0",
    "typescript": "npm:@typescript/typescript6@^6.0.2"
  }
}
```

`tsc` then runs TypeScript 7. ESLint and typescript-eslint import the TypeScript 6.0 API.

## Config

`eslint.config.js`:

```js
import horsies from "@horsies/eslint-plugin";
import parser from "@typescript-eslint/parser";
import { defineConfig } from "eslint/config";

export default defineConfig({
  files: ["**/*.ts"],
  languageOptions: { parser, parserOptions: { projectService: true } },
  plugins: { horsies },
  rules: { "horsies/no-dropped-result": "error" },
});
```

The rule needs type information: `parserOptions.projectService` (or `parserOptions.project`). Without it, the rule
stops the lint with the error of typescript-eslint: "You have used a rule which requires type information".

## Rules

### `no-dropped-result`

An expression statement whose value is a horsies `Result`, or a `Promise` of a `Result`, is an error. The statement
drops the value, so the code never reads which error the call gave.

```ts
await app.send(report, { n: 1 }); // error: the statement drops the Result
app.send(report, { n: 1 }); // error: the statement drops a Promise of a Result
await handle.get(); // error: the statement drops the Result

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
of horsies. A union that has a `Result` member is a `Result` for the rule (`flag ? parse(text) : undefined`).

`@typescript-eslint/no-floating-promises` refuses a `Promise` that is not awaited, of any type. This rule refuses a
`Result` that is dropped, awaited or not. The two rules can run together.

## Oxlint

Oxlint 1.93.0 gives JavaScript plugins no type information: `context.sourceCode.parserServices` is an empty object.
The type-aware rules of Oxlint run in a separate program and do not take custom rules. Loaded as an Oxlint
JavaScript plugin, this rule stops with the error above, and `oxlint` exits with status 1. A project that lints with
Oxlint runs ESLint with this one rule next to Oxlint.
