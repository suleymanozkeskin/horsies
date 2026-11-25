// Wall time of `bun check` on a generated project: the folder of the project, the runs, and
// the statistics of the runs. The verdict against a baseline is in `baseline.ts`.
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

/** Runs of a session after its warm-up run. Odd, so the median is one run. */
export const runsPerSession = 9;

/**
 * Sessions of a recording and of a check. Odd, so the median is one session. A check uses the
 * statistic of the baseline: the median of the session medians.
 */
export const sessionsPerMeasurement = 5;

/** Runs before the measured runs of a session. Their times are not used. */
export const warmUpRuns = 1;

/** Wall-time bound of one run. */
export const checkRunTimeout: Temporal.Duration = Temporal.Duration.from({ seconds: 120 });

/** Bound of the output that one run can write, stdout and stderr each. */
export const checkOutputLimitBytes = 1_048_576;

const folderPrefix = "horsies-typecheck-speed-";

/** The program that one run starts, in the project folder. */
export interface CheckCommand {
  readonly executable: string;
  readonly args: readonly string[];
}

/** `bun check` of the Bun binary that runs this script. */
export const bunCheckCommand: CheckCommand = { executable: process.execPath, args: ["check", "--no-pretty"] };

export type CheckRunFailure =
  | { readonly kind: "failed"; readonly exitCode: number; readonly output: string }
  | { readonly kind: "timedOut"; readonly bound: Temporal.Duration }
  | { readonly kind: "outputTooLarge"; readonly limitBytes: number }
  | { readonly kind: "killed"; readonly signal: string }
  | { readonly kind: "didNotStart"; readonly cause: string };

export type CheckRun = { readonly kind: "passed"; readonly wallTime: Temporal.Duration } | CheckRunFailure;

export type Session =
  | { readonly kind: "measured"; readonly median: Temporal.Duration; readonly runs: readonly Temporal.Duration[] }
  | { readonly kind: "runFailed"; readonly failure: CheckRunFailure };

export type ProjectFolder = { readonly kind: "created"; readonly path: string } | { readonly kind: "notCreated"; readonly cause: string };

export type ProjectWrite = { readonly kind: "written" } | { readonly kind: "notWritten"; readonly file: string; readonly cause: string };

export type FolderRemoval = { readonly kind: "removed" } | { readonly kind: "notRemoved"; readonly path: string; readonly cause: string };

/** The message of a caught error, or its text. */
export function describeCause(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

/** A wall time in whole microseconds. */
export function durationFromMilliseconds(milliseconds: number): Temporal.Duration {
  return Temporal.Duration.from({ microseconds: Math.round(milliseconds * 1000) });
}

/** A duration as milliseconds with one decimal, for example `29.5 ms`. */
export function millisecondsText(duration: Temporal.Duration): string {
  return `${duration.total("millisecond").toFixed(1)} ms`;
}

/** Creates an empty folder under `parent`. Writes no file. */
export function createProjectFolder(parent: string): ProjectFolder {
  try {
    return { kind: "created", path: mkdtempSync(join(parent, folderPrefix)) };
  } catch (error) {
    return { kind: "notCreated", cause: describeCause(error) };
  }
}

/** Writes each file under `folder`. After `notWritten`, the files before the failed file stay written. */
export function writeProjectFiles(folder: string, files: ReadonlyMap<string, string>): ProjectWrite {
  for (const [file, source] of files) {
    const path = join(folder, file);

    try {
      mkdirSync(dirname(path), { recursive: true });
      writeFileSync(path, source);
    } catch (error) {
      return { kind: "notWritten", file, cause: describeCause(error) };
    }
  }

  return { kind: "written" };
}

/** Removes `folder` and its content. */
export function removeProjectFolder(folder: string): FolderRemoval {
  try {
    rmSync(folder, { recursive: true, force: true });

    return { kind: "removed" };
  } catch (error) {
    return { kind: "notRemoved", path: folder, cause: describeCause(error) };
  }
}

function spawnErrorCode(error: Error): string {
  return "code" in error ? String(error.code) : "none";
}

/** Runs `command` once in `folder` and measures its wall time, process start included. */
export function runCheckOnce(command: CheckCommand, folder: string, timeout: Temporal.Duration): CheckRun {
  const started = performance.now();

  const result = spawnSync(command.executable, [...command.args], {
    cwd: folder,
    encoding: "utf8",
    timeout: timeout.total("millisecond"),
    maxBuffer: checkOutputLimitBytes,
  });

  const wallTime = durationFromMilliseconds(performance.now() - started);

  if (result.error !== undefined) {
    switch (spawnErrorCode(result.error)) {
      case "ETIMEDOUT":
        return { kind: "timedOut", bound: timeout };
      case "ENOBUFS":
        return { kind: "outputTooLarge", limitBytes: checkOutputLimitBytes };
      default:
        return { kind: "didNotStart", cause: result.error.message };
    }
  }

  if (result.status === null) {
    return { kind: "killed", signal: result.signal ?? "no signal reported" };
  }

  if (result.status !== 0) {
    return { kind: "failed", exitCode: result.status, output: `${result.stdout}${result.stderr}` };
  }

  return { kind: "passed", wallTime };
}

function assertOddCount(values: readonly Temporal.Duration[]): void {
  if (values.length % 2 !== 1) {
    throw new Error(`typecheck speed: a median needs an odd count of values, got ${values.length}`);
  }
}

function sortedDurations(values: readonly Temporal.Duration[]): Temporal.Duration[] {
  return [...values].sort((one, two) => Temporal.Duration.compare(one, two));
}

/** The middle value of an odd count of durations. */
export function medianOf(values: readonly Temporal.Duration[]): Temporal.Duration {
  assertOddCount(values);
  const middle = sortedDurations(values)[(values.length - 1) / 2];

  if (middle === undefined) {
    throw new Error("typecheck speed: no middle value");
  }

  return middle;
}

/** The slowest value minus the fastest value. */
export function spreadOf(values: readonly Temporal.Duration[]): Temporal.Duration {
  const sorted = sortedDurations(values);
  const fastest = sorted[0];
  const slowest = sorted.at(-1);

  if (fastest === undefined || slowest === undefined) {
    throw new Error("typecheck speed: the spread of no values");
  }

  return slowest.subtract(fastest);
}

/** `warmUpRuns` runs, then `runs` measured runs. Stops at the first failed run. */
export function runSession(command: CheckCommand, folder: string, runs: number, timeout: Temporal.Duration): Session {
  const measured: Temporal.Duration[] = [];

  for (let run = 0; run < warmUpRuns + runs; run += 1) {
    const outcome = runCheckOnce(command, folder, timeout);

    if (outcome.kind !== "passed") {
      return { kind: "runFailed", failure: outcome };
    }

    if (run >= warmUpRuns) {
      measured.push(outcome.wallTime);
    }
  }

  return { kind: "measured", median: medianOf(measured), runs: measured };
}

/** One line that names a failed run. */
export function describeRunFailure(failure: CheckRunFailure): string {
  switch (failure.kind) {
    case "failed":
      return `the check exited with ${failure.exitCode}:\n${failure.output}`;
    case "timedOut":
      return `the check ran longer than ${millisecondsText(failure.bound)}`;
    case "outputTooLarge":
      return `the check wrote more than ${failure.limitBytes} bytes`;
    case "killed":
      return `the check was stopped by ${failure.signal}`;
    case "didNotStart":
      return `the check did not start: ${failure.cause}`;
    default:
      return assertNever(failure);
  }
}

function assertNever(value: never): never {
  throw new Error(`typecheck speed: unexpected variant ${JSON.stringify(value)}`);
}
