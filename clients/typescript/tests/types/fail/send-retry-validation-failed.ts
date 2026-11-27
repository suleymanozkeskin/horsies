// expect: TS2345 at 15
// `resend` takes only `ENQUEUE_FAILED`.

import { defineTask, type Horsies } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

const report = defineTask({ name: "report", input: {}, output: h.null() });

export async function send(): Promise<void> {
  const sent = await app.send(report, {});

  if (!sent.ok && sent.error.code === "VALIDATION_FAILED") {
    void (await app.resend(sent.error));
  }
}
