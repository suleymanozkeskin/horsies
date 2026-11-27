// The CLI report: the commands of `cli.run` and the flags of each one, written from the flag tables that the command
// parsers read. A change of a command or a flag changes the report; the committed report is
// `tests/public-api/report/cli.txt`. Run from `clients/typescript/`, on Node or on Bun:
//
//   bun tests/public-api/cli-report.ts check   compares the report with the committed file
//   bun tests/public-api/cli-report.ts write   writes the committed file
//
// Exit status: 0 when the report is the committed one (or is written); 1 when it differs; 2 for a wrong command.

import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { CHECK_FLAGS, CHECK_USAGE } from "../../horsies/src/cli/check-command.ts";
import { METRICS_FLAG_OPTIONS, METRICS_USAGE } from "../../horsies/src/cli/metrics-command.ts";
import { COMMAND_NAMES, type CommandName } from "../../horsies/src/cli/run.ts";
import { SCHEDULER_FLAGS, SCHEDULER_USAGE } from "../../horsies/src/cli/scheduler-command.ts";
import { WEB_FLAG_OPTIONS, WEB_USAGE } from "../../horsies/src/cli/web-command.ts";
import { WORKER_FLAGS, WORKER_USAGE } from "../../horsies/src/cli/worker-command.ts";
import { compareReport } from "./report.ts";
import { REPORT_DIR } from "./report.ts";

/** The committed CLI report. */
export const CLI_REPORT_FILE = join(REPORT_DIR, "cli.txt");

/** The command that writes the report again. */
export const CLI_REPORT_COMMAND = "(cd clients/typescript && bun tests/public-api/cli-report.ts write)";

/** One flag of a command: `parseArgs` gives its type as `string` or `boolean`. */
type FlagTable = { readonly [name: string]: { readonly type: "string" | "boolean" } };

/** The usage line and the flag table of one command. */
type CommandFacts = { readonly usage: string; readonly flags: FlagTable };

/** The facts of every command, keyed by command name: a new command is a type error here until it is listed. */
const COMMAND_FACTS: { readonly [Name in CommandName]: CommandFacts } = {
  worker: { usage: WORKER_USAGE, flags: WORKER_FLAGS },
  check: { usage: CHECK_USAGE, flags: CHECK_FLAGS },
  scheduler: { usage: SCHEDULER_USAGE, flags: SCHEDULER_FLAGS },
  web: { usage: WEB_USAGE, flags: WEB_FLAG_OPTIONS },
  metrics: { usage: METRICS_USAGE, flags: METRICS_FLAG_OPTIONS },
};

/** The text of the report: one block per command, the flags in name order. Pure. */
export function cliReportText(): string {
  const blocks = COMMAND_NAMES.toSorted().map((name) => {
    const { usage, flags } = COMMAND_FACTS[name];

    const flagLines = Object.entries(flags)
      .toSorted(([left], [right]) => left.localeCompare(right))
      .map(([flag, option]) => `  --${flag} (${option.type})`);

    return [`command ${name}`, `  usage: ${usage}`, ...flagLines].join("\n");
  });

  return `${blocks.join("\n\n")}\n`;
}

function main(command: string | undefined): number {
  const report = cliReportText();

  switch (command) {
    case "write":
      writeFileSync(CLI_REPORT_FILE, report);
      console.log("wrote tests/public-api/report/cli.txt");

      return 0;
    case "check": {
      let committed: string | undefined;

      try {
        committed = readFileSync(CLI_REPORT_FILE, "utf8");
      } catch {
        committed = undefined;
      }

      const comparison = compareReport(committed, report);

      if (comparison.state === "same") {
        console.log("cli.txt: same as the committed report");

        return 0;
      }

      console.error(`The CLI differs from the committed CLI report (${comparison.state}). To write the report: ${CLI_REPORT_COMMAND}`);

      return 1;
    }

    default:
      console.error("usage: bun tests/public-api/cli-report.ts check|write");

      return 2;
  }
}

if (import.meta.main) {
  process.exitCode = main(process.argv[2]);
}
