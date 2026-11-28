// The cases of `no-dropped-result`, one per outcome of the rule. Each case is linted as tests/fixtures/file.ts in the
// project of tests/fixtures/tsconfig.json, where `horsies` is the source of the client. Runs on Node (`node --test`).

import { after, describe, it } from "node:test";
import { fileURLToPath } from "node:url";

import { RuleTester } from "@typescript-eslint/rule-tester";

import { noDroppedResult } from "../src/no-dropped-result.ts";

RuleTester.afterAll = after;

RuleTester.describe = describe;

RuleTester.it = it;

RuleTester.itOnly = it.only;

const fixtureFolder = fileURLToPath(new URL("fixtures/", import.meta.url));

const ruleTester = new RuleTester({
  languageOptions: {
    parserOptions: { project: "./tsconfig.json", tsconfigRootDir: fixtureFolder },
  },
});

// The calls of the client that the cases drop: a send (a Promise of a Result), a read of the handle (a Promise of a
// Result), and a function of the user that gives a Result.
const header = `
import { defineTask, h, type Horsies, type Result, type TaskHandle } from "horsies";

const report = defineTask({ name: "report", input: { n: h.int() }, output: h.int() });

declare const app: Horsies;
declare const handle: TaskHandle<number, string>;
declare function parseCount(text: string): Result<number, "notANumber">;
declare const flag: boolean;
`;

ruleTester.run("no-dropped-result", noDroppedResult, {
  valid: [
    { name: "void discards a Result", code: `${header}void parseCount("1");` },
    { name: "void discards an awaited Result", code: `${header}void (await app.send(report, { n: 1 }));` },
    {
      name: "an awaited Result that is used",
      code: `${header}const sent = await app.send(report, { n: 1 });\nif (!sent.ok) { throw new Error("the send was refused"); }`,
    },
    { name: "a returned Result", code: `${header}export function send() { return app.send(report, { n: 1 }); }` },
    { name: "an assigned Result", code: `${header}let outcome; outcome = await handle.get();\nexport { outcome };` },
    { name: "void discards a Promise.all of sends", code: `${header}void (await Promise.all([app.send(report, { n: 1 }), app.send(report, { n: 2 })]));` },
    {
      name: "an array of Results that is used",
      code: `${header}const reads = await Promise.all([handle.get(), handle.get()]);\nconsole.log(reads.length);`,
    },
    { name: "void discards a Promise.allSettled of sends", code: `${header}void (await Promise.allSettled([app.send(report, { n: 1 })]));` },
    { name: "a settled array of values that are not Results", code: `await Promise.allSettled([Promise.resolve(1)]);` },
    { name: "an array of values that are not Results", code: `declare const numbers: number[];\nnumbers.map((n) => n + 1);` },
    { name: "a tuple of values that are not Results", code: `declare function pair(): [number, string];\npair();` },
    { name: "a value that is not a Result", code: `${header}console.log(parseCount("1"));` },
    {
      name: "the shape of a Result without the brand",
      code: `declare function parseLike(): { ok: true; value: number } | { ok: false; error: string };\nparseLike();`,
    },
    {
      name: "a method named send that gives no Result",
      code: `declare const mailer: { send(text: string): Promise<number> };\nawait mailer.send("hello");`,
    },
    {
      name: "a unique symbol key with a text that is not the brand",
      code: `declare const otherBrand: unique symbol;\ndeclare function marked(): { readonly [otherBrand]?: "other.Result" };\nmarked();`,
    },
    {
      name: "the brand text under a computed key that is a text",
      code: `declare const key: "brand";\ndeclare function marked(): { readonly [key]?: "horsies.Result" };\nmarked();`,
    },
    {
      name: "the brand text under a named key",
      code: `declare function marked(): { readonly brand?: "horsies.Result" };\nmarked();`,
    },
  ],
  invalid: [
    {
      name: "a dropped Result",
      code: `${header}parseCount("1");`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped awaited Result of a send",
      code: `${header}await app.send(report, { n: 1 });`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped awaited Result of a read",
      code: `${header}await handle.get();`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped Promise of a Result",
      code: `${header}app.send(report, { n: 1 });`,
      errors: [{ messageId: "droppedPromisedResult" }],
    },
    {
      name: "a dropped union with a Result member",
      code: `${header}flag ? parseCount("1") : undefined;`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped tuple of Results",
      code: `${header}declare const pair: [Result<number, "a">, Result<string, "b">];\npair;`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped array of Results",
      code: `${header}declare const many: Result<number, "a">[];\nmany;`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped readonly array of Results",
      code: `${header}declare const many: readonly Result<number, "a">[];\nmany;`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped array of arrays of Results",
      code: `${header}declare const grid: Result<number, "a">[][];\ngrid;`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped awaited Promise.all of sends",
      code: `${header}await Promise.all([app.send(report, { n: 1 }), app.send(report, { n: 2 })]);`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped awaited array of reads",
      code: `${header}declare const handles: TaskHandle<number, string>[];\nawait Promise.all(handles.map((each) => each.get()));`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped awaited Promise.allSettled of sends",
      code: `${header}await Promise.allSettled([app.send(report, { n: 1 }), app.send(report, { n: 2 })]);`,
      errors: [{ messageId: "droppedResult" }],
    },
    {
      name: "a dropped Promise.allSettled of reads that is not awaited",
      code: `${header}Promise.allSettled([handle.get()]);`,
      errors: [{ messageId: "droppedPromisedResult" }],
    },
    {
      name: "a dropped Promise.all of sends that is not awaited",
      code: `${header}Promise.all([app.send(report, { n: 1 }), app.send(report, { n: 2 })]);`,
      errors: [{ messageId: "droppedPromisedResult" }],
    },
    {
      name: "the brand of a second copy of horsies",
      code: `declare const copyBrand: unique symbol;\ndeclare function copied(): { readonly ok: true; readonly [copyBrand]?: "horsies.Result" };\ncopied();`,
      errors: [{ messageId: "droppedResult" }],
    },
  ],
});
