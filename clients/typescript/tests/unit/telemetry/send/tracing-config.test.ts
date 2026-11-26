// The tracing step of `Horsies.create`: the defaults of the setting, no API load with `none`, the refusal
// `CONFIG_OTEL_API_MISSING` with `otel` and no API, and the load of the API module.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { createHorsies } from "../../../../horsies/src/app.ts";
import { appInternals } from "../../../../horsies/src/app/internals.ts";
import { NO_OP_OBSERVER } from "../../../../horsies/src/app/observers.ts";
import { defineTask } from "../../../../horsies/src/contract/public.ts";
import { tracingPartsOf } from "../../../../horsies/src/telemetry/create-hook.ts";
import { type OtelApiLoad, requireOtelApi } from "../../../../horsies/src/telemetry/otel-api.ts";
import { tracingSettingOf } from "../../../../horsies/src/telemetry/tracing-config.ts";
import { h } from "../../../../horsies/src/wire/public.ts";
import { DATABASE_URL, fakeEnvironment } from "../../app/support.ts";
import { API, installSdk, recordedWarnings } from "../support.ts";

const listed = defineTask({ name: "listed", input: {}, output: h.null() });

/** A loader that counts its calls and gives `load`. */
function countingLoader(load: OtelApiLoad): { readonly loader: () => OtelApiLoad; readonly calls: { count: number } } {
  const calls = { count: 0 };

  return {
    calls,
    loader: () => {
      calls.count += 1;

      return load;
    },
  };
}

const MISSING: OtelApiLoad = { kind: "otelApiMissing", cause: "Cannot find module '@opentelemetry/api'" };

function environmentOf(loader: () => OtelApiLoad): Parameters<typeof tracingPartsOf>[1] {
  return { loadApi: loader, warnings: recordedWarnings().warnings, clock: () => 0 };
}

describe("the tracing setting", () => {
  test("left out and none are no tracing", () => {
    assert.deepEqual(tracingSettingOf(undefined), { kind: "none" });
    assert.deepEqual(tracingSettingOf({ kind: "none" }), { kind: "none" });
  });

  test("otel takes the defaults of the Python client", () => {
    assert.deepEqual(tracingSettingOf({ kind: "otel" }), {
      kind: "otel",
      parentage: "childOfSend",
      workerEndedSpans: "makeSpans",
    });
  });

  test("otel keeps the given options", () => {
    assert.deepEqual(tracingSettingOf({ kind: "otel", parentage: "linkedToSend", workerEndedSpans: "noSpans" }), {
      kind: "otel",
      parentage: "linkedToSend",
      workerEndedSpans: "noSpans",
    });
  });
});

describe("the tracing step", () => {
  test("none never loads the API and gives the observer that does nothing", () => {
    const { loader, calls } = countingLoader(MISSING);
    const parts = tracingPartsOf({ kind: "none" }, environmentOf(loader));

    assert.equal(calls.count, 0);
    assert.equal(parts.ok ? parts.value.sendObserver : undefined, NO_OP_OBSERVER);
  });

  test("otel with no API is CONFIG_OTEL_API_MISSING on the tracing path, with the cause and the fixes", () => {
    const { loader, calls } = countingLoader(MISSING);
    const parts = tracingPartsOf(tracingSettingOf({ kind: "otel" }), environmentOf(loader));
    const refusal = parts.ok ? undefined : parts.error;

    assert.equal(calls.count, 1);
    assert.equal(refusal?.code, "HRS-220");
    assert.equal(refusal?.path, "tracing");
    assert.match(refusal?.message ?? "", /^tracing: .*@opentelemetry\/api does not load \(Cannot find module/u);
    assert.equal(refusal?.help.length, 2);
  });

  test("otel with the API gives an observer of its own", () => {
    const { loader } = countingLoader({ kind: "loaded", api: API });
    const parts = tracingPartsOf(tracingSettingOf({ kind: "otel" }), environmentOf(loader));

    assert.notEqual(parts.ok ? parts.value.sendObserver : NO_OP_OBSERVER, NO_OP_OBSERVER);
  });

  test("the package's load gives the API, which shares the globals that the app's SDK sets", () => {
    const sdk = installSdk();

    try {
      const loaded = requireOtelApi();
      const api = loaded.kind === "loaded" ? loaded.api : assert.fail("the API does not load");

      api.trace.getTracer("test").startSpan("probe", { kind: api.SpanKind.CONSUMER, attributes: {} }, api.ROOT_CONTEXT).end();
      assert.deepEqual(
        sdk.spans().map((span) => span.name),
        ["probe"],
      );
    } finally {
      sdk.remove();
    }
  });
});

describe("Horsies.create and the tracing step", () => {
  test("the refusal reaches created.error.refusals, after the config parse refusals", () => {
    const { loader } = countingLoader(MISSING);

    const created = createHorsies(
      { broker: { databaseUrl: { kind: "fromEnv", name: "NOT_SET" } }, tracing: { kind: "otel" }, tasks: [listed] },
      fakeEnvironment({ otelApi: loader }),
    );

    const codes = created.ok ? [] : created.error.refusals.map((refused) => refused.code);

    assert.equal(codes.at(-1), "HRS-220");
    assert.ok(codes.length >= 2, `the config refusal and the tracing refusal: ${codes.join(", ")}`);
  });

  test("an app with no tracing never loads the API", () => {
    const { loader, calls } = countingLoader(MISSING);
    const created = createHorsies({ broker: { databaseUrl: DATABASE_URL }, tasks: [listed] }, fakeEnvironment({ otelApi: loader }));

    assert.equal(created.ok, true);
    assert.equal(calls.count, 0);
    assert.deepEqual(created.ok ? appInternals(created.value).telemetry.tracing : undefined, { kind: "none" });
  });

  test("an app with otel tracing keeps its setting for the worker and the children", () => {
    const { loader } = countingLoader({ kind: "loaded", api: API });

    const created = createHorsies(
      { broker: { databaseUrl: DATABASE_URL }, tracing: { kind: "otel", workerEndedSpans: "noSpans" }, tasks: [listed] },
      fakeEnvironment({ otelApi: loader }),
    );

    assert.deepEqual(created.ok ? appInternals(created.value).telemetry.tracing : undefined, {
      kind: "otel",
      parentage: "childOfSend",
      workerEndedSpans: "noSpans",
    });
  });
});
