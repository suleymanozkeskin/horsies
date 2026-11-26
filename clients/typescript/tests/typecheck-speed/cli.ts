// The typecheck-speed check. Each command runs `sessionsPerMeasurement` sessions of
// `runsPerSession` runs of `bun check` on the control project.
//   bun tests/typecheck-speed/cli.ts check   exit 1 when the median of the session medians is
//                                            above the baseline median plus the baseline noise
//   bun tests/typecheck-speed/cli.ts record  writes the baseline of this machine
// Exit 2: no verdict (no baseline for this machine, a failed run, a file error, a wrong argument).
// The project is written to a new folder in the temporary directory and removed after the runs.
// The check measures the control project only. The app project on the horsies API joins in #2376.
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

import storedFile from "./baseline.json" with { type: "json" };
import {
  type BaselineKey,
  currentMachine,
  describeBaseline,
  describeMachine,
  findBaseline,
  judge,
  parseBaseline,
  projectSha256,
  type StoredBaselineFile,
  storedBaselineOf,
  withRecordedBaseline,
  writeBaselineFile,
} from "./baseline.ts";
import { controlProjectFiles } from "./control-project.ts";
import {
  bunCheckCommand,
  checkRunTimeout,
  createProjectFolder,
  describeRunFailure,
  medianOf,
  millisecondsText,
  removeProjectFolder,
  runSession,
  runsPerSession,
  sessionsPerMeasurement,
  spreadOf,
  writeProjectFiles,
} from "./measure.ts";

const exitCodes = { success: 0, slower: 1, noVerdict: 2 } as const;

const baselinePath = fileURLToPath(new URL("baseline.json", import.meta.url));
const recordedFile: StoredBaselineFile = storedFile;

type Command = "check" | "record";

type Measurement =
  | { readonly kind: "measured"; readonly sessionMedians: readonly Temporal.Duration[] }
  | { readonly kind: "notMeasured"; readonly reason: string };

function assertNever(value: never): never {
  throw new Error(`typecheck speed: unexpected variant ${JSON.stringify(value)}`);
}

function parseCommand(argument: string | undefined): { readonly kind: "command"; readonly command: Command } | { readonly kind: "usage" } {
  switch (argument) {
    case "check":
      return { kind: "command", command: "check" };
    case "record":
      return { kind: "command", command: "record" };
    case undefined:
    default:
      return { kind: "usage" };
  }
}

/** Writes the project to `folder` and runs `sessionsPerMeasurement` sessions there. */
function measureInFolder(folder: string, files: ReadonlyMap<string, string>): Measurement {
  const write = writeProjectFiles(folder, files);

  if (write.kind === "notWritten") {
    return { kind: "notMeasured", reason: `${write.file} was not written: ${write.cause}` };
  }

  const sessionMedians: Temporal.Duration[] = [];

  for (let session = 1; session <= sessionsPerMeasurement; session += 1) {
    const result = runSession(bunCheckCommand, folder, runsPerSession, checkRunTimeout);

    if (result.kind === "runFailed") {
      return { kind: "notMeasured", reason: describeRunFailure(result.failure) };
    }

    console.log(`session ${session}: median ${millisecondsText(result.median)}; runs ${result.runs.map(millisecondsText).join(", ")}`);
    sessionMedians.push(result.median);
  }

  return { kind: "measured", sessionMedians };
}

/** Measures in a new temporary folder, then removes the folder. A folder left behind does not change the outcome. */
function measureProject(files: ReadonlyMap<string, string>): Measurement {
  const folder = createProjectFolder(tmpdir());

  if (folder.kind === "notCreated") {
    return { kind: "notMeasured", reason: `no project folder: ${folder.cause}` };
  }

  const measurement = measureInFolder(folder.path, files);
  const removal = removeProjectFolder(folder.path);

  if (removal.kind === "notRemoved") {
    console.error(`the project folder ${removal.path} was not removed: ${removal.cause}`);
  }

  return measurement;
}

function recordedMachines(): string {
  return recordedFile.entries.length === 0 ? "none" : recordedFile.entries.map((entry) => describeMachine(entry.machine)).join("; ");
}

function check(key: BaselineKey, files: ReadonlyMap<string, string>): number {
  const lookup = findBaseline(recordedFile, key);

  if (lookup.kind === "noMatch") {
    console.error("no baseline for this machine, project and procedure; record one with `bun run typecheck-speed:record`");
    console.error(`recorded machines: ${recordedMachines()}`);

    return exitCodes.noVerdict;
  }

  const parsed = parseBaseline(lookup.stored);

  if (parsed.kind === "unreadable") {
    console.error(`the baseline is unreadable: ${parsed.text}: ${parsed.cause}`);

    return exitCodes.noVerdict;
  }

  const measurement = measureProject(files);

  if (measurement.kind === "notMeasured") {
    console.error(measurement.reason);

    return exitCodes.noVerdict;
  }

  console.log(`baseline: ${describeBaseline(parsed.baseline)}`);
  const verdict = judge(parsed.baseline, medianOf(measurement.sessionMedians));

  switch (verdict.kind) {
    case "withinNoise":
      console.log(`within the noise: median ${millisecondsText(verdict.measured)}, limit ${millisecondsText(verdict.limit)}`);

      return exitCodes.success;
    case "slower":
      console.error(`slower than the baseline: median ${millisecondsText(verdict.measured)}, limit ${millisecondsText(verdict.limit)}`);

      return exitCodes.slower;
    default:
      return assertNever(verdict);
  }
}

function record(key: BaselineKey, files: ReadonlyMap<string, string>): number {
  const measurement = measureProject(files);

  if (measurement.kind === "notMeasured") {
    console.error(measurement.reason);

    return exitCodes.noVerdict;
  }

  const entry = storedBaselineOf(key, measurement.sessionMedians);
  const write = writeBaselineFile(baselinePath, withRecordedBaseline(recordedFile, entry));

  if (write.kind === "notWritten") {
    console.error(`${baselinePath} was not written: ${write.cause}`);

    return exitCodes.noVerdict;
  }

  const median = millisecondsText(medianOf(measurement.sessionMedians));
  console.log(`recorded in ${baselinePath}: median ${median}, noise ${millisecondsText(spreadOf(measurement.sessionMedians))}`);

  return exitCodes.success;
}

function main(argument: string | undefined): number {
  const parsedCommand = parseCommand(argument);

  if (parsedCommand.kind === "usage") {
    console.error("usage: bun tests/typecheck-speed/cli.ts check|record");

    return exitCodes.noVerdict;
  }

  const machine = currentMachine();

  if (machine.kind === "cpuModelNotReported") {
    console.error("the operating system reports no CPU model, so no baseline can match this machine");

    return exitCodes.noVerdict;
  }

  const files = controlProjectFiles();

  const key: BaselineKey = {
    machine: machine.machine,
    projectSha256: projectSha256(files),
    runsPerSession,
    sessions: sessionsPerMeasurement,
  };

  console.log(`control project: ${files.size} files, sha256 ${key.projectSha256}`);
  console.log(`machine: ${describeMachine(key.machine)}`);

  switch (parsedCommand.command) {
    case "check":
      return check(key, files);
    case "record":
      return record(key, files);
    default:
      return assertNever(parsedCommand.command);
  }
}

process.exitCode = main(process.argv[2]);
