import { describe, expect, test } from "bun:test";

import { schedulerManifestToWire } from "../../../horsies/src/_catalog/scheduler_manifest.ts";
import type { SchedulerManifest } from "../../../horsies/src/_catalog/scheduler_manifest.ts";
import { wireText } from "./support/wire-text.ts";

// The expected text is the text of Python's `dump_scheduler_manifest` for the
// same manifest. The manifest holds every variant of every tagged type.

const MANIFEST: SchedulerManifest = {
  manifest_version: 1,
  enabled: true,
  check_interval_seconds: 1,
  schedules: [
    {
      name: "nightly-report",
      activation: {
        kind: "ENABLED",
        pattern_json: String.raw`{"type":"daily","time":"09:00:00"}`,
        timezone: "Europe/Berlin",
        catch_up_missed: true,
        max_catch_up_runs: 100,
        send: {
          task_name: "reports.nightly",
          queue_name: "default",
          priority: 100,
          input_json: String.raw`{"format":"pdf"}`,
          task_options: { kind: "ENCODED", json: String.raw`{"timeout_ms":5000}` },
          retention_class_key: "standard_30d",
          retain_rerun_input: false,
        },
      },
    },
    {
      name: "stock-refresh",
      activation: {
        kind: "ENABLED",
        pattern_json: String.raw`{"type":"interval","seconds":null,"minutes":5,"hours":null,"days":null}`,
        timezone: "UTC",
        catch_up_missed: false,
        max_catch_up_runs: 1,
        send: {
          task_name: "stock.refresh",
          queue_name: "urgent",
          priority: 1,
          input_json: "{}",
          task_options: { kind: "NO_OPTIONS" },
          retention_class_key: "forever",
          retain_rerun_input: true,
        },
      },
    },
    { name: "paused-digest", activation: { kind: "DISABLED" } },
  ],
};

const MANIFEST_TEXT = [
  String.raw`{"manifest_version":1,`,
  String.raw`"enabled":true,`,
  String.raw`"check_interval_seconds":1,`,
  String.raw`"schedules":[{"name":"nightly-report",`,
  String.raw`"activation":{"kind":"ENABLED",`,
  String.raw`"pattern_json":"{\"type\":\"daily\",\"time\":\"09:00:00\"}",`,
  String.raw`"timezone":"Europe/Berlin",`,
  String.raw`"catch_up_missed":true,`,
  String.raw`"max_catch_up_runs":100,`,
  String.raw`"send":{"task_name":"reports.nightly",`,
  String.raw`"queue_name":"default",`,
  String.raw`"priority":100,`,
  String.raw`"input_json":"{\"format\":\"pdf\"}",`,
  String.raw`"task_options":{"kind":"ENCODED","json":"{\"timeout_ms\":5000}"},`,
  String.raw`"retention_class_key":"standard_30d",`,
  String.raw`"retain_rerun_input":false}}},`,
  String.raw`{"name":"stock-refresh",`,
  String.raw`"activation":{"kind":"ENABLED",`,
  String.raw`"pattern_json":"{\"type\":\"interval\",\"seconds\":null,\"minutes\":5,\"hours\":null,\"days\":null}",`,
  String.raw`"timezone":"UTC",`,
  String.raw`"catch_up_missed":false,`,
  String.raw`"max_catch_up_runs":1,`,
  String.raw`"send":{"task_name":"stock.refresh",`,
  String.raw`"queue_name":"urgent",`,
  String.raw`"priority":1,`,
  String.raw`"input_json":"{}",`,
  String.raw`"task_options":{"kind":"NO_OPTIONS"},`,
  String.raw`"retention_class_key":"forever",`,
  String.raw`"retain_rerun_input":true}}},`,
  String.raw`{"name":"paused-digest","activation":{"kind":"DISABLED"}}]}`,
].join("");

describe("schedulerManifestToWire", () => {
  test("gives Python's text of a manifest with every variant", () => {
    expect(wireText(schedulerManifestToWire(MANIFEST))).toBe(MANIFEST_TEXT);
  });

  test("throws a defect for a number that is not a safe integer, with its field", () => {
    const manifest: SchedulerManifest = { ...MANIFEST, check_interval_seconds: Number.NaN };

    expect(() => schedulerManifestToWire(manifest)).toThrow(
      "integerToWire: SchedulerManifest.check_interval_seconds is NaN, not a safe integer",
    );
  });
});
