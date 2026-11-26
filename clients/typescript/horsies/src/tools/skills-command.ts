// The command `horsies skills install`: its flags, its output lines and its exit codes, as Python's
// `horsies skills install` (`--target`, `--force`) gives them.

import type { ParseArgsOptionsConfig } from "node:util";

import type { CommandExitCode } from "../cli/usage.ts";
import {
  parseToolFlags,
  reportUsageError,
  switchFlagOf,
  textFlagOf,
  TOOL_FAILED,
  TOOL_PASSED,
  type ToolOutput,
} from "./command-line.ts";
import { installErrorText, installOutcomeText, installSkill, packagedSkillFiles } from "./skills.ts";

/** The usage line of `horsies skills install`. */
export const SKILLS_INSTALL_USAGE = "horsies skills install [-h] --target TARGET [--force]";

/** The help of `horsies skills install`. */
export const SKILLS_INSTALL_HELP: readonly string[] = [
  `usage: ${SKILLS_INSTALL_USAGE}`,
  "",
  "Copy the skill folder of this release to <target>/horsies/.",
  "",
  "options:",
  "  -h, --help       show this help message and exit",
  "  --target TARGET  Skills directory of your agent tool, for example .claude/skills. The skill is written to",
  "                   <target>/horsies/",
  "  --force          Replace an existing copy that differs from the packaged skill",
];

const SKILLS_INSTALL_FLAGS: ParseArgsOptionsConfig = {
  target: { type: "string" },
  force: { type: "boolean" },
  help: { type: "boolean", short: "h" },
};

/** What `skills install` reads and writes outside its arguments. */
export interface SkillsInstallDeps {
  readonly output: ToolOutput;
  /** The folder of the installed package, which holds `.agents/skills/horsies/`. */
  readonly packageFolder: string;
}

/**
 * Run `horsies skills install` with the arguments after `install`. Exit codes: 0 when the folder is written or already
 * current, with one line on stdout; 1 on an install error, with `error: <message>` on stderr; 2 for arguments that do
 * not parse. `--help` prints the help and gives 0. Effects: reads the package's skill files; writes the target folder.
 */
export function runSkillsInstall(args: readonly string[], deps: SkillsInstallDeps): CommandExitCode {
  const flags = parseToolFlags(args, SKILLS_INSTALL_FLAGS);

  if (!flags.ok) {
    return reportUsageError(deps.output, "skills install", SKILLS_INSTALL_USAGE, flags.error);
  }

  if (switchFlagOf(flags.value, "help")) {
    for (const line of SKILLS_INSTALL_HELP) {
      deps.output.writeOut(line);
    }

    return TOOL_PASSED;
  }

  const target = textFlagOf(flags.value, "target");

  if (target.kind === "notGiven") {
    return reportUsageError(deps.output, "skills install", SKILLS_INSTALL_USAGE, {
      kind: "usageError",
      message: "the following arguments are required: --target",
    });
  }

  const packaged = packagedSkillFiles(deps.packageFolder);
  const installed = packaged.ok ? installSkill(target.text, packaged.value, switchFlagOf(flags.value, "force")) : packaged;

  if (!installed.ok) {
    deps.output.writeError(`error: ${installErrorText(installed.error)}`);

    return TOOL_FAILED;
  }

  deps.output.writeOut(installOutcomeText(installed.value));

  return TOOL_PASSED;
}
