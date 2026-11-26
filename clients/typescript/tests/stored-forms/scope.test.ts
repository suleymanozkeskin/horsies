// The scope of the stored-form tests: which vector files this client reads, and that every row of those files is
// one that the tests read. A new file, section, row key or vector type fails here until a test reads it.

import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import { describe, test } from "node:test";

import { isString } from "../../horsies/src/codec/values.ts";
import { STORED_FORMS_DIR } from "./files.ts";
import {
  CORE_ONLY_FILES,
  declaredWires,
  layoutProblems,
  type Notation,
  type Parameter,
  resultEnvelopeFile,
  specDataFile,
  taskInputFile,
  vectorDocument,
  type VectorFileName,
} from "./vectors.ts";

const IN_SCOPE_FILES: readonly VectorFileName[] = [
  "error_data.json",
  "result_envelope.json",
  "task_input.json",
  "workflow_spec_data.json",
];

describe("the vector files", () => {
  test("the folder holds the four files of this client and the two files that only the core writes", () => {
    const files = readdirSync(STORED_FORMS_DIR).filter((name) => name.endsWith(".json")).toSorted();

    assert.deepEqual(files, [...IN_SCOPE_FILES, ...CORE_ONLY_FILES].toSorted());
  });

  for (const fileName of IN_SCOPE_FILES) {
    test(`${fileName}: every section and row key is read by these tests`, () => {
      assert.deepEqual(layoutProblems(fileName, vectorDocument(fileName)), []);
    });
  }

  test("every vector type of a row that this client expresses has a declared wire type", () => {
    const taskInput = taskInputFile();

    const parameters: readonly Parameter[] = [
      ...taskInput.vectors.filter((row) => row.typescript_cannot_express === undefined).flatMap((row) => row.parameters),
      ...taskInput.refused_values.flatMap((row) => row.parameters),
      ...taskInput.typed_refused_texts.flatMap((row) => row.parameters),
    ];

    const okTypes = resultEnvelopeFile()
      .vectors.filter((row) => row.typescript_cannot_express === undefined)
      .flatMap((row) => (row.ok === undefined ? [] : [row.ok.type]));

    for (const type of new Set([...parameters.map((parameter) => parameter.type), ...okTypes])) {
      assert.doesNotThrow(() => declaredWires(type, undefined), type);
    }
  });

  test("the rows that this client cannot express are the two bytes rows, each with its reason", () => {
    const skipped = [
      ...taskInputFile().vectors.filter((row) => row.typescript_cannot_express !== undefined),
      ...resultEnvelopeFile().vectors.filter((row) => row.typescript_cannot_express !== undefined),
    ];

    assert.deepEqual(
      skipped.map((row) => row.name),
      ["bytes_utf8", "ok_bytes"],
    );
    assert(skipped.every((row) => (row.typescript_cannot_express ?? "").length > 0));
  });

  // The core's strict check runs before every path to the codec reader, so these texts never reach it. The test of
  // these rows through the core's check needs a built addon.
  test("the refused texts of task_input.json each name the core's strict-rule refusal", () => {
    for (const row of taskInputFile().refused_texts) {
      assert(isString(row.core_refusal) && row.core_refusal.length > 0, row.name);
    }
  });

  test("the core alone reads workflow spec data: its refused texts each name the core's field path", () => {
    for (const row of specDataFile().refused_texts) {
      assert(isString(row.core_field_path) && row.core_field_path.length > 0, row.name);
    }
  });
});

/** A one-section file of `workflow_spec_data.json` with the given rows, for the tests of the layout check. */
function specDocument(rows: readonly Notation[], extra: Readonly<Record<string, Notation>> = {}): Notation {
  return { form: "workflow spec data", vectors: rows, refused_texts: [], ...extra };
}

describe("the layout check", () => {
  test("a file whose rows hold only read keys has no problem", () => {
    assert.deepEqual(layoutProblems("workflow_spec_data.json", specDocument([{ name: "a", document: {}, text: "" }])), []);
  });

  test("a row key that the tests do not read", () => {
    assert.deepEqual(layoutProblems("workflow_spec_data.json", specDocument([{ name: "a", typescript_skip: "x" }])), [
      "workflow_spec_data.json vectors a: the key typescript_skip is not read by these tests",
    ]);
  });

  test("a key of a nested member that the tests do not read", () => {
    const document: Notation = {
      form: "task input",
      vectors: [{ name: "a", parameters: [{ name: "v", type: "i64", unit: "ms" }], text: "" }],
      refused_values: [],
      refused_texts: [],
      typed_refused_texts: [],
    };

    assert.deepEqual(layoutProblems("task_input.json", document), [
      "task_input.json vectors a.parameters[0]: the key unit is not read by these tests",
    ]);
  });

  test("a section that the tests do not read, and a missing section", () => {
    assert.deepEqual(layoutProblems("workflow_spec_data.json", { form: "workflow spec data", vectors: [], refused: [] }), [
      "workflow_spec_data.json: the section refused is not read by these tests",
      "workflow_spec_data.json: the section refused_texts is missing or not a list",
    ]);
  });

  test("another form name, and a row without a name", () => {
    assert.deepEqual(layoutProblems("workflow_spec_data.json", specDocument([{ text: "" }], { form: "task input" })), [
      'workflow_spec_data.json: the form is not "workflow spec data"',
      "workflow_spec_data.json vectors row 0: no name",
    ]);
  });

  test("a file or a row that is not an object", () => {
    assert.deepEqual(layoutProblems("workflow_spec_data.json", []), ["workflow_spec_data.json: not an object"]);
    assert.deepEqual(layoutProblems("workflow_spec_data.json", specDocument([1])), [
      "workflow_spec_data.json vectors row 0: no name",
      "workflow_spec_data.json vectors row 0: not an object",
    ]);
  });
});
