// The event dispatcher on the core's event wire vectors: the text of each vector, read with the dispatcher's own
// parser (`wireItemsOf`) and decoded with the generated decoder, reaches a handler of its event type as that type.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { type AnyEvent, EVENT_TYPES } from "../../../../horsies/src/_catalog/events.ts";
import type { WireJson } from "../../../../horsies/src/_catalog/wire_json.ts";
import { isString } from "../../../../horsies/src/codec/values.ts";
import { eventDispatch } from "../../../../horsies/src/events/handler-child.ts";
import { wireItemsOf } from "../../../../horsies/src/events/wire-text.ts";
import { wireText } from "../../generated/support/wire-text.ts";

const VECTORS_URL = new URL("../../../../../../core/tests/fixtures/event_wire/vectors.json", import.meta.url);

/** One vector: its name, the event type of its tag, and the compact wire text of its event, as the core writes it. */
type VectorText = { readonly name: string; readonly eventType: AnyEvent["kind"]; readonly text: string };

/** The wire value of the vector file, read with the dispatcher's parser (integers as `bigint`). */
function vectorFile(): WireJson {
  const read = wireItemsOf(`[${readFileSync(VECTORS_URL, "utf8")}]`);
  const [document] = read.kind === "items" ? read.items : assert.fail(`the vector file does not read: ${read.cause}`);

  return document ?? assert.fail("the vector file is empty");
}

/** The name, the event type and the wire text of each vector of the file. */
function vectorTexts(): readonly VectorText[] {
  const document = vectorFile();
  const vectors = document instanceof Map ? document.get("vectors") : undefined;

  assert.ok(vectors instanceof Array, "the vector file has a list of vectors");

  return vectors.map((vector: WireJson) => {
    assert.ok(vector instanceof Map, "a vector is an object");

    const name = vector.get("name");
    const wire = vector.get("wire");

    assert.ok(isString(name), "a vector has a name");
    assert.ok(wire instanceof Array, `the vector ${name} has wire items`);

    const eventType = EVENT_TYPES.find((type) => type === wire[0]);

    assert.ok(eventType !== undefined, `the vector ${name} names an event type`);

    return { name, eventType, text: wireText(wire) };
  });
}

describe("the event dispatcher on the core's wire vectors", () => {
  test("each vector reaches a handler of its type, decoded as that type", async () => {
    const texts = vectorTexts();

    assert.ok(texts.length >= EVENT_TYPES.length, "the file has a vector of each event type");

    for (const { name, eventType, text } of texts) {
      const seen: AnyEvent["kind"][] = [];
      const dispatch = eventDispatch([{ name: "probe", eventType, handler: (event) => void seen.push(event.kind) }]);

      assert.deepEqual(await dispatch({ index: 0, wireJson: text }), { kind: "handled" }, name);
      assert.deepEqual(seen, [eventType], name);
    }
  });
});
