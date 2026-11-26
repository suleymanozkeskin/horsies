// Every path to the codec reader, and the check that runs in front of it.
//
// The reader takes a `JsonText` only, and it does not check the core's strict read rule again. Two functions make a
// `JsonText`: `jsonTextCheckedByCore` (a path on which the core's strict check ran) and `writtenJsonText` (a writer of
// the codec). No other module may make one: a cast to `JsonText`, or a type alias of it, outside
// `codec/json-text.ts` fails this test.
//
// Each module that names a maker has one row here:
// - `checkedBy`: the function on that path that runs the check before any reader call. The module names it.
// - `proof`: the test that proves it. For a `jsonTextCheckedByCore` path, the test feeds the path a text that the
//   core's strict check refuses and shows that the path ends with the core's refusal and never calls the reader. For a
//   `writtenJsonText` path, the test feeds the writer each value whose text would break the strict read rule and shows
//   that the writer gives no such text.
// A module that adds a path fails this test until its row and its proof test exist.

import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

import { SOURCE_DIR } from "./support/files.ts";

type Maker = "jsonTextCheckedByCore" | "writtenJsonText";

type ReaderPath = {
  readonly module: string;
  readonly maker: Maker;
  readonly checkedBy: string;
  readonly proof: { readonly file: string; readonly test: string };
};

/** The module that defines the two makers, the only module that may cast to `JsonText`. */
const DEFINING_MODULE = "codec/json-text.ts";

const MAKERS: readonly Maker[] = ["jsonTextCheckedByCore", "writtenJsonText"];

/** The tests folder: a proof file is named relative to it. */
const TESTS_DIR: string = fileURLToPath(new URL("../../", import.meta.url));

const READER_PATHS: readonly ReaderPath[] = [
  {
    module: "workflow/handle.ts",
    maker: "jsonTextCheckedByCore",
    checkedBy: "nodeResult",
    proof: {
      file: "unit/workflow/handle.test.ts",
      test: "a stored text that the core's strict check refuses ends as storedResultUnreadable and never reaches the reader",
    },
  },
  {
    module: "workflow/child-spec.ts",
    maker: "jsonTextCheckedByCore",
    checkedBy: "inputText",
    proof: {
      file: "runtime/native/workflows-addon.test.ts",
      test: "a stored input that the core's strict read refuses fails the node and never calls the build",
    },
  },
  {
    module: "read/handle.ts",
    maker: "jsonTextCheckedByCore",
    checkedBy: "getResult",
    proof: {
      file: "unit/read/handle.test.ts",
      test: "a stored result text that the core's strict check refuses ends as the core's refusal and never reaches the reader",
    },
  },
  {
    module: "child/serve.ts",
    maker: "jsonTextCheckedByCore",
    checkedBy: "build_task_input",
    proof: {
      file: "unit/child/serve.test.ts",
      test: "the reader of a child gets only the input text that the core posts after its strict check",
    },
  },
  {
    module: "seats/isolate.ts",
    maker: "jsonTextCheckedByCore",
    checkedBy: "build_task_input",
    proof: {
      file: "unit/seats/isolate.test.ts",
      test: "the reader of a seat isolate gets only the input text that the core posts after its strict check",
    },
  },
  {
    module: "codec/error-data.ts",
    maker: "writtenJsonText",
    checkedBy: "errorDataText",
    proof: {
      file: "unit/codec/error-data.test.ts",
      test: "the error data writer makes a JsonText of strict JSON only",
    },
  },
  {
    module: "testing/task-runtime.ts",
    maker: "writtenJsonText",
    checkedBy: "engineKeysKeptText",
    proof: {
      file: "unit/testing/run.test.ts",
      test: "the stored workflow context of a test runtime is written by the codec writer, which makes strict JSON only",
    },
  },
  {
    module: "codec/writer.ts",
    maker: "writtenJsonText",
    checkedBy: "encodeValue",
    proof: {
      file: "unit/codec/writer.test.ts",
      test: "the writer makes no JsonText for a value whose text breaks the strict read rule",
    },
  },
];

/** The forms that make a `JsonText` with no maker: a cast, an angle-bracket cast, a type alias. */
const CAST_FORMS: readonly RegExp[] = [/\bas\s+JsonText\b/, /<\s*JsonText\s*>/, /\btype\s+\w+\s*=\s*JsonText\b/];

function sourceModules(): readonly string[] {
  return readdirSync(SOURCE_DIR, { recursive: true, encoding: "utf8" })
    .filter((name) => name.endsWith(".ts"))
    .map((name) => name.split("\\").join("/"))
    .toSorted();
}

function sourceOf(module: string): string {
  return readFileSync(`${SOURCE_DIR}${module}`, "utf8");
}

function names(text: string, name: string): boolean {
  return new RegExp(`\\b${name}\\b`).test(text);
}

/** The cast forms that a source text holds. */
function castsIn(text: string): readonly string[] {
  return CAST_FORMS.filter((form) => form.test(text)).map((form) => form.source);
}

describe("paths to the codec reader", () => {
  test("each module that makes a JsonText has a row", () => {
    const found = sourceModules()
      .filter((module) => module !== DEFINING_MODULE)
      .flatMap((module) =>
        MAKERS.filter((maker) => names(sourceOf(module), maker)).map((maker) => `${module} ${maker}`),
      );

    const listed = READER_PATHS.map((path) => `${path.module} ${path.maker}`);

    assert.deepEqual(found.toSorted(), listed.toSorted());
  });

  test("each row names a function of its module that checks, and a proof test that exists", () => {
    for (const path of READER_PATHS) {
      assert(names(sourceOf(path.module), path.checkedBy), `${path.module} names ${path.checkedBy}`);

      const proofFile = `${TESTS_DIR}${path.proof.file}`;

      assert(existsSync(proofFile), `${path.proof.file} exists`);
      assert(readFileSync(proofFile, "utf8").includes(`test(${JSON.stringify(path.proof.test)}`), path.proof.test);
    }
  });

  test("no module but codec/json-text.ts casts to JsonText or aliases it", () => {
    const casts = sourceModules()
      .filter((module) => module !== DEFINING_MODULE)
      .flatMap((module) => castsIn(sourceOf(module)).map((form) => `${module}: ${form}`));

    assert.deepEqual(casts, []);
  });

  test("the cast scan finds each form", () => {
    for (const text of [
      "const t = s as JsonText;",
      "const t = s as  JsonText;",
      "const t = <JsonText>s;",
      "type Text = JsonText;",
    ]) {
      assert.notEqual(castsIn(text).length, 0, text);
    }

    assert.deepEqual(castsIn('import type { JsonText } from "./json-text.ts";\nfunction f(t: JsonText) {}'), []);
  });
});
