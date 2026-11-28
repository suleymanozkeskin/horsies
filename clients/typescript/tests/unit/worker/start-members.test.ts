// The start members of the worker manifest: the text in the manifest's own forms, the task facts, the handler list,
// the worker-ended spans switch and the child launch; and the writer of manifest wire values.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { WireJson } from "../../../horsies/src/_catalog/wire_json.ts";
import { MAX_MANIFEST_DEPTH, manifestText } from "../../../horsies/src/worker/manifest-text.ts";
import {
  childLaunch,
  eventHandlers,
  startMembersText,
  taskManifests,
  workerEndedSpans,
} from "../../../horsies/src/worker/start-members.ts";
import { contextOf, implementedApp } from "./support.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";

const COMMAND = { program: "/usr/local/bin/node", args: ["/srv/app/main.js", "--horsies-child"] };

describe("the start members", () => {
  test("the text has the manifest's member names and forms", () => {
    const text = startMembersText({
      profile: { kind: "NAMED", name: "io" },
      metricsEndpoint: { kind: "LISTEN_ADDRESS", host: "0.0.0.0", port: 9464 },
      tasks: new Map([["ping", { execution: "ISOLATED", error_codes: [] }]]),
      handlers: { kind: "NO_HANDLERS" },
      workerEndedSpans: "NO_SPANS",
      childLaunch: childLaunch(COMMAND, "INFO"),
    });

    assert.equal(
      text,
      '{"profile":{"kind":"NAMED","name":"io"},' +
        '"metrics_endpoint":{"kind":"LISTEN_ADDRESS","host":"0.0.0.0","port":9464},' +
        '"tasks":{"ping":{"execution":"ISOLATED","error_codes":[]}},' +
        '"handlers":{"kind":"NO_HANDLERS"},' +
        '"worker_ended_spans":"NO_SPANS",' +
        '"child_launch":{"command":{"program":"/usr/local/bin/node","args":["/srv/app/main.js","--horsies-child"]},' +
        '"preload":{"kind":"NO_PRELOAD_PROGRAM"},"program":{"kind":"HOST"},"log_level":"INFO"}}',
    );
  });

  test("no profile and no metrics endpoint have their own forms", () => {
    const parsed: unknown = JSON.parse(
      startMembersText({
        profile: { kind: "NOT_GIVEN" },
        metricsEndpoint: { kind: "NO_METRICS_ENDPOINT" },
        tasks: new Map(),
        handlers: { kind: "NO_HANDLERS" },
        workerEndedSpans: "MAKE_SPANS",
        childLaunch: childLaunch(COMMAND, "DEBUG"),
      }),
    );

    assert.deepEqual(parsed, {
      profile: { kind: "NOT_GIVEN" },
      metrics_endpoint: { kind: "NO_METRICS_ENDPOINT" },
      tasks: {},
      handlers: { kind: "NO_HANDLERS" },
      worker_ended_spans: "MAKE_SPANS",
      child_launch: {
        command: COMMAND,
        preload: { kind: "NO_PRELOAD_PROGRAM" },
        program: { kind: "HOST" },
        log_level: "DEBUG",
      },
    });
  });

  test("each task has the execution of its contract and its declared codes; ISOLATED and open codes by default", () => {
    assert.deepEqual(
      taskManifests(contextOf(implementedApp())),
      new Map([
        ["ping", { execution: "ISOLATED", error_codes: [] }],
        ["charge", { execution: "SHARED_ASYNC", error_codes: ["CARD_DECLINED", "BANK_DOWN"] }],
      ]),
    );
  });

  test("no handler is NO_HANDLERS", () => {
    assert.deepEqual(eventHandlers([]), { kind: "NO_HANDLERS" });
  });

  test("handlers are listed with their names and event types, in order", () => {
    assert.deepEqual(
      eventHandlers([
        { name: "alerts", eventType: "AttemptEnded" },
        { name: "audit", eventType: "TaskClaimed" },
      ]),
      {
        kind: "HANDLERS",
        handlers: [
          { name: "alerts", event_type: "AttemptEnded" },
          { name: "audit", event_type: "TaskClaimed" },
        ],
      },
    );
  });

  test("OpenTelemetry tracing makes the worker-ended spans by its switch; no tracing makes none", () => {
    assert.equal(workerEndedSpans({ kind: "otelTracing", parentage: "childOfSend", workerEndedSpans: "makeSpans" }), "MAKE_SPANS");
    assert.equal(workerEndedSpans({ kind: "otelTracing", parentage: "childOfSend", workerEndedSpans: "noSpans" }), "NO_SPANS");
    assert.equal(workerEndedSpans({ kind: "noTracing" }), "NO_SPANS");
  });

  test("the child launch has no preload program and the host-child program RUST", () => {
    assert.deepEqual(childLaunch(COMMAND, "WARNING"), {
      command: COMMAND,
      preload: { kind: "NO_PRELOAD_PROGRAM" },
      program: { kind: "HOST" },
      log_level: "WARNING",
    });
  });
});

describe("the manifest text", () => {
  test("each member of a wire value has its JSON form", () => {
    const value: WireJson = new Map<string, WireJson>([
      ["null", null],
      ["flag", false],
      ["count", 18_446_744_073_709_551_615n],
      ["text", 'a "quoted" text'],
      ["list", [true, 1n]],
      ["__proto__", new Map()],
    ]);

    assert.equal(
      manifestText(value),
      '{"null":null,"flag":false,"count":18446744073709551615,"text":"a \\"quoted\\" text","list":[true,1],"__proto__":{}}',
    );
  });

  test("a number is a defect of the package: a manifest wire value holds no float", () => {
    assert.throws(() => manifestText([1.5]), ClientDefect);
  });

  test("a value deeper than the bound is a defect", () => {
    let deep: WireJson = null;

    for (let level = 0; level <= MAX_MANIFEST_DEPTH + 1; level += 1) {
      deep = [deep];
    }

    assert.throws(() => manifestText(deep), ClientDefect);
  });
});
