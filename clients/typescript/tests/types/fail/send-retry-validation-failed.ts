// expect: TS2345 at 15
// `retrySend` takes only `ENQUEUE_FAILED` and `IDEMPOTENCY_KEY_CONFLICT`.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const report = defineTask({ name: "report", input: {}, output: h.null() });

export async function send(): Promise<void> {
  const sent = await app.send(report, {});

  if (!sent.ok && sent.error.code === "VALIDATION_FAILED") {
    void (await app.retrySend(sent.error));
  }
}
