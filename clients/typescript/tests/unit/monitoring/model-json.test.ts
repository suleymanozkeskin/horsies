// The core's JSON text as the values of the decoders: integers keep every digit, objects are maps, and a text that is
// not JSON or does not decode is a defect.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { decodeStatusCount } from "../../../horsies/src/_catalog/monitoring_models.ts";
import {
  decodeCoreModel,
  jsonValueOfCoreText,
  listItems,
  textItem,
  wireJsonOfCoreText,
} from "../../../horsies/src/monitoring/model-json.ts";

describe("wireJsonOfCoreText", () => {
  test("an integer is a bigint with every digit and a float is a number", () => {
    assert.deepEqual(wireJsonOfCoreText("read", "[12345678901234567890, 7, 1.5]"), [12345678901234567890n, 7n, 1.5]);
  });

  test("an object is a map that keeps a __proto__ key as an entry", () => {
    const value = wireJsonOfCoreText("read", '{"b": "x", "__proto__": null}');

    assert.deepEqual(value, new Map<string, null | string>([["b", "x"], ["__proto__", null]]));
  });

  test("a text that is not JSON is a defect that names the read", () => {
    assert.throws(() => wireJsonOfCoreText("listTasks", "{"), { name: "ClientDefect", subject: "listTasks" });
  });
});

describe("jsonValueOfCoreText", () => {
  test("an integer in the safe range is a number and one past it a bigint", () => {
    assert.deepEqual(jsonValueOfCoreText("read", '{"a": 7, "b": 9007199254740993, "c": [1.25, "t", null, true]}'), {
      a: 7,
      b: 9007199254740993n,
      c: [1.25, "t", null, true],
    });
  });
});

describe("decodeCoreModel", () => {
  test("gives the decoded model", () => {
    assert.deepEqual(decodeCoreModel("taskStatusCounts", '{"status": "PENDING", "count": 3}', decodeStatusCount), {
      status: "PENDING",
      count: 3,
    });
  });

  test("a text that does not decode is a defect that names the model and the place", () => {
    assert.throws(() => decodeCoreModel("taskStatusCounts", '{"status": "PENDING"}', decodeStatusCount), {
      name: "ClientDefect",
      subject: "taskStatusCounts",
      rule: "the core's StatusCount does not decode: fieldMissing at $.count",
    });
  });
});

describe("listItems and textItem", () => {
  test("a list gives its items and a string its text", () => {
    const items = listItems("workflowNames", wireJsonOfCoreText("workflowNames", '["a", "b"]'));

    assert.deepEqual(
      items.map((item) => textItem("workflowNames", item)),
      ["a", "b"],
    );
  });

  test("a value that is not a list is a defect", () => {
    assert.throws(() => listItems("workflowNames", wireJsonOfCoreText("workflowNames", "{}")), { name: "ClientDefect" });
  });

  test("an item that is not a string is a defect", () => {
    for (const text of ["7", "null", "true", "[]", "{}"]) {
      assert.throws(() => textItem("workflowNames", wireJsonOfCoreText("workflowNames", text)), { name: "ClientDefect" }, text);
    }
  });
});
