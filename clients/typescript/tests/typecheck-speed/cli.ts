// The typecheck-speed check. Each command runs `sessionsPerMeasurement` sessions. A session runs `runsPerSession`
// runs of `bun check` on the control project, then the same on the app project, and takes the ratio of the app
// median to the control median. The CPU speed cancels in the ratio; the operating system and the CPU architecture do
// not, so baseline.json holds one baseline per platform, architecture and Bun binary.
//   bun tests/typecheck-speed/cli.ts check   exit 1 when the median of the session ratios is
//                                            above the baseline median plus the baseline noise
//   bun tests/typecheck-speed/cli.ts record  writes the baseline of this platform, architecture and Bun binary
// Exit 2: no verdict (no baseline for this platform, architecture, Bun binary and these projects, a failed run, a file
// error, a wrong argument).
// The control project is written to a new folder in the temporary directory. The app project is written to a new
// folder next to this file, because its tsconfig refers to the package by a relative path. Both folders are removed
// after the runs.
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

import { appProjectFiles } from "./app-project.ts";
import storedFile from "./baseline.json" with { type: "json" };
import {
  type BaselineKey,
  currentBun,
  currentMachine,
  describeBaseline,
  describeMachine,
  findBaseline,
  judge,
  type MachineIdentity,
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
  millisecondsText,
  removeProjectFolder,
  runSession,
  runsPerSession,
  sessionsPerMeasurement,
  writeProjectFiles,
} from "./measure.ts";
import { medianOfRatios, type Ratio, ratioOf, ratioText, spreadOfRatios } from "./ratio.ts";

const exitCodes = { success: 0, slower: 1, noVerdict: 2 } as const;

const baselinePath = fileURLToPath(new URL("baseline.json", import.meta.url));
const recordedFile: StoredBaselineFile = storedFile;

type Command = "check" | "record";

type Measurement =
  | { readonly kind: "measured"; readonly sessionRatios: readonly Ratio[] }
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

/** The folder of this file: the parent of the app project folder. */
const appProjectParent = fileURLToPath(new URL(".", import.meta.url));

/** The two project folders of a measurement, both written. */
interface ProjectFolders {
  readonly control: string;
  readonly app: string;
}

/** One session on each project, and the ratio of the app median to the control median. */
function measureSession(folders: ProjectFolders, session: number): { readonly kind: "measured"; readonly ratio: Ratio } | { readonly kind: "notMeasured"; readonly reason: string } {
  const control = runSession(bunCheckCommand, folders.control, runsPerSession, checkRunTimeout);

  if (control.kind === "runFailed") {
    return { kind: "notMeasured", reason: `control project: ${describeRunFailure(control.failure)}` };
  }

  const app = runSession(bunCheckCommand, folders.app, runsPerSession, checkRunTimeout);

  if (app.kind === "runFailed") {
    return { kind: "notMeasured", reason: `app project: ${describeRunFailure(app.failure)}` };
  }

  const ratio = ratioOf(app.median, control.median);

  if (ratio.kind === "controlTimeZero") {
    return { kind: "notMeasured", reason: "the control median is zero, so the ratio is not defined" };
  }

  console.log(`session ${session}: control ${millisecondsText(control.median)}, app ${millisecondsText(app.median)}, ratio ${ratioText(ratio.ratio)}`);

  return { kind: "measured", ratio: ratio.ratio };
}

function measureInFolders(folders: ProjectFolders, controlFiles: ReadonlyMap<string, string>, appFiles: ReadonlyMap<string, string>): Measurement {
  for (const [folder, files] of [
    [folders.control, controlFiles],
    [folders.app, appFiles],
  ] as const) {
    const write = writeProjectFiles(folder, files);

    if (write.kind === "notWritten") {
      return { kind: "notMeasured", reason: `${write.file} was not written: ${write.cause}` };
    }
  }

  const sessionRatios: Ratio[] = [];

  for (let session = 1; session <= sessionsPerMeasurement; session += 1) {
    const result = measureSession(folders, session);

    if (result.kind === "notMeasured") {
      return result;
    }

    sessionRatios.push(result.ratio);
  }

  return { kind: "measured", sessionRatios };
}

function removeFolder(folder: string): void {
  const removal = removeProjectFolder(folder);

  if (removal.kind === "notRemoved") {
    console.error(`the project folder ${removal.path} was not removed: ${removal.cause}`);
  }
}

/** Measures in two new folders, then removes them. A folder left behind does not change the outcome. */
function measureProjects(controlFiles: ReadonlyMap<string, string>, appFiles: ReadonlyMap<string, string>): Measurement {
  const control = createProjectFolder(tmpdir());

  if (control.kind === "notCreated") {
    return { kind: "notMeasured", reason: `no control project folder: ${control.cause}` };
  }

  const app = createProjectFolder(appProjectParent);

  if (app.kind === "notCreated") {
    removeFolder(control.path);

    return { kind: "notMeasured", reason: `no app project folder: ${app.cause}` };
  }

  const measurement = measureInFolders({ control: control.path, app: app.path }, controlFiles, appFiles);
  removeFolder(control.path);
  removeFolder(app.path);

  return measurement;
}

function recordedBaselines(): string {
  return recordedFile.entries.length === 0
    ? "none"
    : recordedFile.entries.map((entry) => `bun ${entry.bun}, recorded on ${describeMachine(entry.recordedOn)}`).join("; ");
}

function check(key: BaselineKey, controlFiles: ReadonlyMap<string, string>, appFiles: ReadonlyMap<string, string>): number {
  const lookup = findBaseline(recordedFile, key);

  if (lookup.kind === "noMatch") {
    console.error(
      `no baseline for ${key.platform} ${key.arch}, this Bun binary and these projects; record one with \`bun run typecheck-speed:record\``,
    );
    console.error(`recorded baselines: ${recordedBaselines()}`);

    return exitCodes.noVerdict;
  }

  const parsed = parseBaseline(lookup.stored);

  if (parsed.kind === "unreadable") {
    console.error(`the baseline is unreadable: ${parsed.text}: ${parsed.cause}`);

    return exitCodes.noVerdict;
  }

  const measurement = measureProjects(controlFiles, appFiles);

  if (measurement.kind === "notMeasured") {
    console.error(measurement.reason);

    return exitCodes.noVerdict;
  }

  console.log(`baseline: ${describeBaseline(parsed.baseline)}`);
  const verdict = judge(parsed.baseline, medianOfRatios(measurement.sessionRatios));

  switch (verdict.kind) {
    case "withinNoise":
      console.log(`within the noise: ratio ${ratioText(verdict.measured)}, limit ${ratioText(verdict.limit)}`);

      return exitCodes.success;
    case "slower":
      console.error(`slower than the baseline: ratio ${ratioText(verdict.measured)}, limit ${ratioText(verdict.limit)}`);

      return exitCodes.slower;
    default:
      return assertNever(verdict);
  }
}

function record(
  key: BaselineKey,
  machine: MachineIdentity,
  controlFiles: ReadonlyMap<string, string>,
  appFiles: ReadonlyMap<string, string>,
): number {
  const measurement = measureProjects(controlFiles, appFiles);

  if (measurement.kind === "notMeasured") {
    console.error(measurement.reason);

    return exitCodes.noVerdict;
  }

  const entry = storedBaselineOf(key, machine, measurement.sessionRatios);
  const write = writeBaselineFile(baselinePath, withRecordedBaseline(recordedFile, entry));

  if (write.kind === "notWritten") {
    console.error(`${baselinePath} was not written: ${write.cause}`);

    return exitCodes.noVerdict;
  }

  const median = ratioText(medianOfRatios(measurement.sessionRatios));
  console.log(`recorded in ${baselinePath}: median ${median}, noise ${ratioText(spreadOfRatios(measurement.sessionRatios))}`);

  return exitCodes.success;
}

function main(argument: string | undefined): number {
  const parsedCommand = parseCommand(argument);

  if (parsedCommand.kind === "usage") {
    console.error("usage: bun tests/typecheck-speed/cli.ts check|record");

    return exitCodes.noVerdict;
  }

  const controlFiles = controlProjectFiles();
  const appFiles = appProjectFiles();

  const key: BaselineKey = {
    platform: process.platform,
    arch: process.arch,
    bun: currentBun(),
    controlSha256: projectSha256(controlFiles),
    appSha256: projectSha256(appFiles),
    runsPerSession,
    sessions: sessionsPerMeasurement,
  };

  console.log(`control project: ${controlFiles.size} files, sha256 ${key.controlSha256}`);
  console.log(`app project: ${appFiles.size} files, sha256 ${key.appSha256}`);
  console.log(`bun: ${key.bun}, platform: ${key.platform} ${key.arch}`);

  switch (parsedCommand.command) {
    case "check":
      return check(key, controlFiles, appFiles);
    case "record":
      return recordOnThisMachine(key, controlFiles, appFiles);
    default:
      return assertNever(parsedCommand.command);
  }
}

/** Records on this machine. Refused when the system reports no CPU model: the entry names the machine. */
function recordOnThisMachine(key: BaselineKey, controlFiles: ReadonlyMap<string, string>, appFiles: ReadonlyMap<string, string>): number {
  const machine = currentMachine();

  if (machine.kind === "cpuModelNotReported") {
    console.error("the operating system reports no CPU model, so the baseline cannot name the machine that records it");

    return exitCodes.noVerdict;
  }

  console.log(`machine: ${describeMachine(machine.machine)}`);

  return record(key, machine.machine, controlFiles, appFiles);
}

process.exitCode = main(process.argv[2]);
