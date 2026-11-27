import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";

import type { Execution } from "../../../horsies/src/_catalog/monitoring_models.ts";
import type { TaskManifest } from "../../../horsies/src/_catalog/worker_manifest.ts";

const WORKER_MANIFEST_TEXT = readFileSync(new URL("../../../horsies/src/_catalog/worker_manifest.ts", import.meta.url), "utf8");

describe("the types that generated modules share", () => {
  test("the worker manifest imports Execution from the monitoring models and declares none", () => {
    expect(WORKER_MANIFEST_TEXT).toContain('\nimport type { Execution } from "./monitoring_models.ts";\n');
    expect(WORKER_MANIFEST_TEXT).not.toContain("export type Execution");
  });

  test("a task manifest holds the Execution of the monitoring models", () => {
    const execution: Execution = "SHARED_THREAD";
    const task: TaskManifest = { execution, error_codes: [] };

    expect(task.execution).toBe(execution);
  });
});
