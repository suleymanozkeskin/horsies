// `chaos`: send the export drills. The first attempt of a drawn export ends its child process; the worker records
// `WORKER_CRASHED` and the retry policy runs the export again. A restart of the worker during the run shows the
// recovery of the claimed tasks too.

import type { Result } from "horsies";

import { flakyExport } from "../contracts/analytics.ts";
import { err, ok } from "../result.ts";
import { CHAOS_EXPORT_COUNT } from "../tuning.ts";
import { sendErrorText, withScenario } from "./context.ts";
import { bullet, heading, say } from "./output.ts";

export async function runChaos(): Promise<Result<void, string>> {
  heading("Acme Clothing — chaos");

  return withScenario(async (context) => {
    for (let index = 0; index < CHAOS_EXPORT_COUNT; index += 1) {
      const sent = await context.showcase.app.send(flakyExport, { exportId: `chaos-export-${String(index)}` });

      if (!sent.ok) {
        return err(`send flaky_export: ${sendErrorText(sent.error)}`);
      }
    }

    bullet(`submitted ${String(CHAOS_EXPORT_COUNT)} deterministic export drills`);
    say("restart the worker during this bounded run to exercise recovery");

    return ok(undefined);
  });
}
