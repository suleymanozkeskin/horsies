// A test entry: the showcase app with one schedule of a 1 s interval in place of the schedule table, and a check
// every second, for `tests/scheduler.test.ts`. The schedule sends `prewarm_search` with the campaign id that
// `SCHEDULE_MARKER_VARIABLE` names, so the test finds its own slots. Every other part is the app of `src/app.ts`.
//
//   ACME_DATABASE_URL=... ACME_TEST_SCHEDULE_MARKER=... <runtime> tests/compressed-schedule-entry.ts scheduler

import { cli, taskSchedule } from "horsies";

import { appRefusedText, buildShowcase } from "../src/app.ts";
import { prewarmSearch } from "../src/contracts/promotions.ts";
import { processDatabaseSettings, settingsRefusedText } from "../src/settings.ts";

/** The variable that names the campaign id of the compressed schedule. */
export const SCHEDULE_MARKER_VARIABLE = "ACME_TEST_SCHEDULE_MARKER";

/** The name of the compressed schedule of `marker`. Pure. */
export function compressedScheduleName(marker: string): string {
  return `compressed-${marker}`;
}

async function main(): Promise<number> {
  const settings = processDatabaseSettings();
  const marker = process.env[SCHEDULE_MARKER_VARIABLE] ?? "";

  if (!settings.ok || marker === "") {
    process.stderr.write(`compressed schedule entry: ${settings.ok ? `${SCHEDULE_MARKER_VARIABLE} is not set` : settingsRefusedText(settings.error)}\n`);

    return 1;
  }

  const built = buildShowcase(settings.value, {
    schedules: [
      taskSchedule({
        name: compressedScheduleName(marker),
        task: prewarmSearch,
        input: { campaignId: marker },
        pattern: { kind: "interval", every: Temporal.Duration.from({ seconds: 1 }) },
      }),
    ],
    checkIntervalSeconds: 1,
  });

  if (!built.ok) {
    process.stderr.write(`compressed schedule entry: ${appRefusedText(built.error)}\n`);

    return 1;
  }

  return cli.run(built.value.app);
}

// The module is an entry only when a process runs it, not when a test imports its names.
if (process.argv[1] !== undefined && import.meta.filename === process.argv[1]) {
  process.exitCode = await main();
}
