// expect: TS2345 at 27
// A send error switch that misses a code fails at the `assertNever` line (a new catalog code does the same).

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

function assertNever(value: never): never {
  throw new Error(`no case for ${String(value)}`);
}

const report = defineTask({ name: "report", input: {}, output: h.null() });

export async function send(): Promise<void> {
  const sent = await app.send(report, {});

  if (!sent.ok) {
    switch (sent.error.code) {
      case "ENQUEUE_FAILED":
      case "IDEMPOTENCY_KEY_CONFLICT":
      case "VALIDATION_FAILED":
      case "PAYLOAD_TOO_LARGE":
      case "SEND_SUPPRESSED":
        break;
      default:
        assertNever(sent.error);
    }
  }
}
