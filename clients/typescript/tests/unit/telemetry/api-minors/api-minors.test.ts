// Two minors of `@opentelemetry/api`: the installed one and the floor of the peer range (1.7.0, installed under the
// name `@opentelemetry/api-floor`). Each minor runs the probe in a process of its own (`api-minors-probe.ts`): the API
// keeps one registration container per process, so two minors cannot register in one. The probe checks that the module
// fits `OtelApi` (at typecheck), and runs the send span, the carrier, the whole-number counter and the attempt span.

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

import { MINOR_NAMES } from "./minors.ts";

/** The probe process of the checks of one minor. */
const PROBE_PATH = fileURLToPath(new URL("./api-minors-probe.ts", import.meta.url));

/** The longest run of a probe process, in milliseconds. */
const PROBE_BOUND_MS = 60_000;

describe("the two minors of the OpenTelemetry API", () => {
  for (const minor of MINOR_NAMES) {
    test(`the ${minor} minor: the send span, the carrier, the counter and the attempt span`, () => {
      const run = spawnSync(process.execPath, [PROBE_PATH, minor], { encoding: "utf8", timeout: PROBE_BOUND_MS });

      assert.equal(run.status, 0, `the probe of the ${minor} minor failed:\n${run.stderr}${run.stdout}`);
    });
  }
});
