// The load check of one built addon file, on the runtime that runs this script (Node 26 or Bun 1.4):
//
//   node tests/packages/addon-check.ts <absolute path of the .node file>
//   bun tests/packages/addon-check.ts <absolute path of the .node file>
//
// The file opens with `process.dlopen`, `coreState()` answers `not_started`, and the export object has no test-only
// export. Exit status: 0 when every check passes; 1 when a check fails; 2 when the argument is not an absolute path.

import { isAbsolute } from "node:path";

import { openAddonFile } from "../../horsies/src/native/load.ts";
import { processCoreView } from "../../horsies/src/native/process-core/addon.ts";
import { coreStateProblem, testOnlyExportsIn } from "./addon-exports.ts";

/** The name and version of the runtime that runs this script. */
function runtimeText(): string {
  const bun = process.versions["bun"];

  return bun === undefined ? `node ${process.version}` : `bun ${bun}`;
}

/** The problems of the loaded export object. Effects: calls `coreState()` of the addon, which starts nothing. */
function exportProblems(exports: object): readonly string[] {
  const problems = testOnlyExportsIn(exports).map((name) => `the addon exports the test-only ${name}`);
  let stateProblem: string | "none";

  try {
    stateProblem = coreStateProblem(processCoreView(exports).coreState());
  } catch (cause) {
    stateProblem = `coreState() did not answer: ${cause instanceof Error ? cause.message : String(cause)}`;
  }

  return stateProblem === "none" ? problems : [...problems, stateProblem];
}

/** Run the check; the exit status. Effects: opens the addon file; writes stdout and stderr. */
function main(args: readonly string[]): number {
  const [path] = args;

  if (args.length !== 1 || path === undefined || !isAbsolute(path)) {
    console.error("give one argument: the absolute path of the .node file");

    return 2;
  }

  const opened = openAddonFile(path);

  if (opened.kind === "openFailed") {
    console.error(`FAIL (${runtimeText()}): ${path} did not open: ${opened.cause}`);

    return 1;
  }

  const problems = exportProblems(opened.exports);

  for (const problem of problems) {
    console.error(`FAIL (${runtimeText()}): ${problem}`);
  }

  if (problems.length > 0) {
    return 1;
  }

  console.log(`OK (${runtimeText()}): ${path} loads, coreState() is not_started, no test-only export`);

  return 0;
}

process.exitCode = main(process.argv.slice(2));
