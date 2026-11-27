// The carrier between the propagator and the core: a key reads as written first, then with ASCII case ignored; a
// carrier that another client's propagator wrote reads through the extract; a propagator that writes no key gives no
// carrier.

import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";

import * as otel from "@opentelemetry/api";

import { CARRIER_GETTER, extractedContextOf, sendTraceOf } from "../../../../horsies/src/telemetry/carrier.ts";
import type { OtelApi } from "../../../../horsies/src/telemetry/otel-api.ts";
import { installSdk, loadedApi, type TestSdk } from "../support.ts";

const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

describe("the getter of a stored carrier", () => {
  test("a key as written is read as written, also when another key equals it with ASCII case ignored", () => {
    const carrier = new Map([
      ["TraceParent", "other"],
      ["traceparent", TRACEPARENT],
    ]);

    assert.equal(CARRIER_GETTER.get(carrier, "traceparent"), TRACEPARENT);
  });

  test("a key that differs only in ASCII case reads", () => {
    assert.equal(CARRIER_GETTER.get(new Map([["TRACEPARENT", TRACEPARENT]]), "traceparent"), TRACEPARENT);
    assert.equal(CARRIER_GETTER.get(new Map([["traceparent", TRACEPARENT]]), "TraceParent"), TRACEPARENT);
  });

  test("a key that is not in the carrier reads as undefined", () => {
    assert.equal(CARRIER_GETTER.get(new Map([["baggage", "a=b"]]), "traceparent"), undefined);
  });

  test("case is folded for ASCII only: a non-ASCII capital is not the lower-case letter", () => {
    assert.equal(CARRIER_GETTER.get(new Map([["TRACEPARENTİ", "x"]]), "traceparenti"), undefined);
  });

  test("the keys are the stored keys", () => {
    assert.deepEqual(CARRIER_GETTER.keys(new Map([["a", "1"], ["B", "2"]])), ["a", "B"]);
  });
});

describe("the context of a stored carrier", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("a carrier with an upper-case key gives the span of its traceparent", () => {
    const extracted = extractedContextOf(loadedApi(), new Map([["TRACEPARENT", TRACEPARENT]]));
    const span = otel.trace.getSpan(extracted);

    assert.equal(span?.spanContext().traceId, "0af7651916cd43dd8448eb211c80319c");
    assert.equal(span?.spanContext().spanId, "b7ad6b7169203331");
  });
});

describe("the trace of a send", () => {
  let sdk: TestSdk;

  beforeEach(() => {
    sdk = installSdk();
  });

  afterEach(() => {
    sdk.remove();
  });

  test("a propagator that writes no key gives no carrier, though the context holds a valid span", () => {
    const api = loadedApi();
    const silent: OtelApi = { ...api, propagation: { ...api.propagation, inject: () => undefined } };
    const span = otel.trace.getTracer("caller").startSpan("caller");

    assert.deepEqual(sendTraceOf(silent, otel.trace.setSpan(otel.ROOT_CONTEXT, span)), { kind: "noCarrier" });
    assert.equal(sendTraceOf(api, otel.trace.setSpan(otel.ROOT_CONTEXT, span)).kind, "carrier");
    span.end();
  });
});
