// The one entry of every process of the showcase: the producer scenarios, the operator commands, and, through
// `cli.run(app)`, the worker, each of its children, the scheduler, the check, the monitoring server and the metrics
// server.
//
//   <runtime> src/main.ts seed                     the Acme tables and the catalog
//   <runtime> src/main.ts worker [--profile acme]  a worker; each child is this entry again, in child mode
//   <runtime> src/main.ts scheduler                the schedule table (src/schedules.ts)
//   <runtime> src/main.ts steady --orders 10       a scenario (see `SCENARIO_NAMES`)
//   <runtime> src/main.ts report                   the monitoring and health reads
//   <runtime> src/main.ts web --auth none          the monitoring web page and API on 127.0.0.1:8600
//   <runtime> src/main.ts check                    the app and every workflow, with no database
//
// The worker starts each child with this entry and `--horsies-child`; every line before `cli.run` runs in the child
// too, so the child builds the same app from the same environment.

import { cli } from "horsies";

import { appRefusedText, buildShowcase } from "./app.ts";
import { OPERATION_NAMES, operationNameOf, runOperation } from "./operations/index.ts";
import { runScenario, SCENARIO_NAMES, scenarioNameOf } from "./scenarios/index.ts";
import { processDatabaseSettings, settingsRefusedText } from "./settings.ts";

/** The commands of `cli.run`, for the usage text. */
const HORSIES_COMMANDS: readonly string[] = ["worker", "scheduler", "check", "web", "metrics"];

/** The command that prints the usage text. */
const HELP_COMMAND = "help";

/** The usage text of the entry. Pure. */
function usageText(): string {
  return [
    "usage: <entry> <command> [flags]",
    `  scenarios: ${SCENARIO_NAMES.join(", ")}`,
    `  operations: ${OPERATION_NAMES.join(", ")}`,
    `  horsies: ${HORSIES_COMMANDS.join(", ")}`,
  ].join("\n");
}

/** Run the horsies command of `argv` (or serve a child of the worker) for the app of the environment. */
async function runHorsiesCommand(): Promise<number> {
  const settings = processDatabaseSettings();

  if (!settings.ok) {
    process.stderr.write(`acme: ${settingsRefusedText(settings.error)}\n`);

    return 1;
  }

  const built = buildShowcase(settings.value);

  if (!built.ok) {
    process.stderr.write(`acme: the app is refused:\n${appRefusedText(built.error)}\n`);

    return 1;
  }

  return cli.run(built.value.app);
}

/** Run the command of the arguments after the entry script: its exit code. */
async function main(args: readonly string[]): Promise<number> {
  const [command, ...rest] = args;

  if (command === undefined || command === HELP_COMMAND) {
    process.stdout.write(`${usageText()}\n`);

    return command === undefined ? 2 : 0;
  }

  const scenario = scenarioNameOf(command);

  if (scenario.kind === "scenario") {
    return runScenario(scenario.name, rest);
  }

  const operation = operationNameOf(command);

  if (operation.kind === "operation") {
    return runOperation(operation.name, rest);
  }

  return runHorsiesCommand();
}

process.exitCode = await main(process.argv.slice(2));
