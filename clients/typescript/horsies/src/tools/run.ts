// The `horsies` tool of the npm package: `get-docs` and `skills install`, as the Python client's CLI and the Rust
// client's shipped binary have them. The worker, the check, the scheduler, the web UI and the metrics endpoint of an
// app run from the app's own entry through `cli.run(app)`, because they need the app's task implementations; this
// tool refuses those commands by name and says where they run. No module of this tool loads the native addon.
//
// Exit codes: 0 when a command succeeds; 1 when a command fails; 2 for arguments that do not parse and for a command
// of the app's entry.

import type { CommandName } from "../cli/run.ts";
import type { CommandExitCode } from "../cli/usage.ts";
import { processToolOutput, reportUsageError, TOOL_FAILED, TOOL_PASSED, TOOL_USAGE, type ToolOutput } from "./command-line.ts";
import { type GetDocsDeps, processGetDocsDeps, runGetDocs } from "./docs-command.ts";
import { docsErrorText, packageVersionOf } from "./docs.ts";
import { packageFolder } from "./files.ts";
import { runSkillsInstall, SKILLS_INSTALL_USAGE } from "./skills-command.ts";

/** Each command of `cli.run(app)`, by itself: the record names each member of `CommandName` once. */
const APP_ENTRY_COMMAND_SET: { readonly [Name in CommandName]: Name } = {
  worker: "worker",
  check: "check",
  scheduler: "scheduler",
  web: "web",
  metrics: "metrics",
};

/** The commands that run from the app's own entry, which this tool refuses by name. */
export const APP_ENTRY_COMMANDS: readonly CommandName[] = Object.values(APP_ENTRY_COMMAND_SET);

/** The usage line of the tool. */
export const TOOL_USAGE_LINE = "horsies [-h] [-V] {get-docs,skills} ...";

/** The help of the tool. */
export const TOOL_HELP: readonly string[] = [
  `usage: ${TOOL_USAGE_LINE}`,
  "",
  "The horsies tools of the TypeScript client: the docs and the agent skill.",
  "",
  "commands:",
  "  get-docs        Fetch the docs of this release for AI agents: the shared pages and the TypeScript pages",
  "  skills install  Copy the skill folder of this release to <target>/horsies/",
  "",
  "options:",
  "  -h, --help      show this help message and exit",
  "  -V, --version   show the version of the package and exit",
  "",
  "The worker, the check, the scheduler, the web UI and the metrics endpoint of an app run from the app's own entry,",
  "which imports the app's task implementations. End the entry with `process.exitCode = await cli.run(app)`, then run:",
  "  <your entry> worker --profile <name>",
  "  <your entry> check [--live]",
  "",
  "Examples:",
  "  horsies get-docs",
  "  horsies skills install --target .claude/skills",
];

/** The command of the tool's arguments. */
export type ToolCommand =
  | { readonly kind: "help" }
  | { readonly kind: "version" }
  | { readonly kind: "getDocs"; readonly args: readonly string[] }
  | { readonly kind: "skillsInstall"; readonly args: readonly string[] }
  | { readonly kind: "appEntryCommand"; readonly command: CommandName }
  | { readonly kind: "usageError"; readonly command: string | undefined; readonly usage: string; readonly message: string };

/** The command of `args`, the arguments after the tool's name. Pure. */
export function toolCommandOf(args: readonly string[]): ToolCommand {
  const [command, ...rest] = args;

  switch (command) {
    case undefined:
      return { kind: "usageError", command: undefined, usage: TOOL_USAGE_LINE, message: "the following arguments are required: command" };
    case "-h":
    case "--help":
      return { kind: "help" };
    case "-V":
    case "--version":
      return { kind: "version" };
    case "get-docs":
      return { kind: "getDocs", args: rest };
    case "skills":
      return skillsCommandOf(rest);
    default: {
      const appCommand = APP_ENTRY_COMMANDS.find((known) => known === command);

      return appCommand === undefined
        ? {
            kind: "usageError",
            command: undefined,
            usage: TOOL_USAGE_LINE,
            message: `argument command: invalid choice: ${JSON.stringify(command)} (choose from get-docs, skills)`,
          }
        : { kind: "appEntryCommand", command: appCommand };
    }
  }
}

/** The command of the arguments after `skills`. Pure. */
function skillsCommandOf(args: readonly string[]): ToolCommand {
  const [subcommand, ...rest] = args;

  switch (subcommand) {
    case "install":
      return { kind: "skillsInstall", args: rest };
    case undefined:
      return { kind: "usageError", command: "skills", usage: SKILLS_INSTALL_USAGE, message: "the following arguments are required: install" };
    default:
      return {
        kind: "usageError",
        command: "skills",
        usage: SKILLS_INSTALL_USAGE,
        message: `argument skills_command: invalid choice: ${JSON.stringify(subcommand)} (choose from install)`,
      };
  }
}

/** The refusal of a command of the app's own entry, given to this tool. Pure. */
export function appEntryRefusalText(command: CommandName): string {
  return (
    `\`horsies ${command}\` is not a command of this tool: the ${command} of an app runs from the app's own entry, ` +
    `which imports the app's task implementations. End the entry with \`process.exitCode = await cli.run(app)\` and ` +
    `run \`<your entry> ${command}\`.`
  );
}

/** What the tool reads and writes outside its arguments. */
export interface ToolDeps {
  readonly output: ToolOutput;
  /** The folder of the installed package: its `package.json` and its skill folder. */
  readonly packageFolder: string;
  /** The deps of `get-docs`. */
  readonly getDocs: GetDocsDeps;
}

/** The deps of the tool in this process. */
export function processToolDeps(): ToolDeps {
  const output = processToolOutput();
  const folder = packageFolder();

  return { output, packageFolder: folder, getDocs: processGetDocsDeps(output, folder) };
}

/**
 * Write `horsies <version>` with the version of the package's own `package.json`, as the Rust tool's `--version`.
 * Exit codes: 0; 1 when the version cannot be read, with `error: <message>` on stderr.
 */
function writeVersion(deps: ToolDeps): CommandExitCode {
  const version = packageVersionOf(deps.packageFolder);

  if (!version.ok) {
    deps.output.writeError(`error: ${docsErrorText(version.error)}`);

    return TOOL_FAILED;
  }

  deps.output.writeOut(`horsies ${version.value}`);

  return TOOL_PASSED;
}

/** Run the command of `args` (the arguments after the tool's name) and give its exit code. */
export function runTool(args: readonly string[], deps: ToolDeps): CommandExitCode {
  const command = toolCommandOf(args);

  switch (command.kind) {
    case "help":
      for (const line of TOOL_HELP) {
        deps.output.writeOut(line);
      }

      return TOOL_PASSED;
    case "version":
      return writeVersion(deps);
    case "getDocs":
      return runGetDocs(command.args, deps.getDocs);
    case "skillsInstall":
      return runSkillsInstall(command.args, deps);
    case "appEntryCommand":
      deps.output.writeError(`error: ${appEntryRefusalText(command.command)}`);

      return TOOL_USAGE;
    case "usageError":
      return reportUsageError(deps.output, command.command, command.usage, { kind: "usageError", message: command.message });
  }
}
