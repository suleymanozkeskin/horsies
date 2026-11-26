// The public names of the CLI runner. `src/index.ts` exports this module.

import type { Horsies } from "../app.ts";
import { processCliDeps, runCli } from "./run.ts";

/** The runner of the one entry of every process of an app. */
export interface Cli {
  /**
   * Run the command that the argument after the entry script names for `app`, then end the process with the
   * command's exit code: 0 after an operator stop or a passing check, 1 for a failure, 2 for arguments that do not
   * parse. The commands are the entries of the command table (`COMMANDS`, `cli/run.ts`); with no command or an unknown
   * one, the usage line on stderr names each of them. Every process of the app runs the same entry: the worker starts
   * each child with it.
   */
  run(app: Horsies): Promise<number>;
}

/** The CLI runner: `process.exitCode = await cli.run(app)` at the end of the entry. */
export const cli: Cli = Object.freeze({ run: async (app: Horsies): Promise<number> => runCli(app, processCliDeps()) });
