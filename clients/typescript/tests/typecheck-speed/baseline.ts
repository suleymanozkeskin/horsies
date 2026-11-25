// The recorded baselines of the typecheck-speed check and the verdict of a session against
// one of them. A baseline compares only on the machine, with the Bun binary, that recorded it.
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { availableParallelism, cpus } from "node:os";

import { describeCause, medianOf, millisecondsText, spreadOf } from "./measure.ts";

/** The machine and the Bun binary of a measurement. Times of another machine or Bun do not compare. */
export interface MachineIdentity {
  readonly platform: string;
  readonly arch: string;
  readonly cpuModel: string;
  readonly logicalCores: number;
  readonly bun: string;
}

/** One baseline as `baseline.json` stores it. */
export interface StoredBaseline {
  readonly machine: MachineIdentity;
  /** SHA-256 of the generated project (`projectSha256`). */
  readonly projectSha256: string;
  readonly runsPerSession: number;
  /** The median of each recorded session, as ISO 8601 durations (`PT0.029512S`). */
  readonly sessionMedians: readonly string[];
}

export interface StoredBaselineFile {
  readonly entries: readonly StoredBaseline[];
}

/** What a measurement must match to compare with a stored baseline. */
export interface BaselineKey {
  readonly machine: MachineIdentity;
  readonly projectSha256: string;
  readonly runsPerSession: number;
  /** The count of session medians. */
  readonly sessions: number;
}

/** The center and the run-to-run noise of a baseline. */
export interface Baseline {
  /** The median of the session medians. */
  readonly median: Temporal.Duration;
  /** The slowest session median minus the fastest session median. */
  readonly noise: Temporal.Duration;
}

export type MachineLookup = { readonly kind: "identified"; readonly machine: MachineIdentity } | { readonly kind: "cpuModelNotReported" };

export type BaselineLookup = { readonly kind: "found"; readonly stored: StoredBaseline } | { readonly kind: "noMatch" };

export type BaselineParse =
  | { readonly kind: "parsed"; readonly baseline: Baseline }
  | { readonly kind: "unreadable"; readonly text: string; readonly cause: string };

export type Verdict =
  | { readonly kind: "withinNoise"; readonly measured: Temporal.Duration; readonly limit: Temporal.Duration }
  | { readonly kind: "slower"; readonly measured: Temporal.Duration; readonly limit: Temporal.Duration };

export type BaselineFileWrite = { readonly kind: "written" } | { readonly kind: "notWritten"; readonly cause: string };

/** The facts that the operating system and the runtime report about this process's machine. */
export interface MachineFacts {
  readonly platform: string;
  readonly arch: string;
  readonly cpuModels: readonly string[];
  readonly logicalCores: number;
  readonly bun: string;
}

/** The identity of a machine. The first CPU's model names the CPU. */
export function machineFrom(facts: MachineFacts): MachineLookup {
  const cpuModel = facts.cpuModels[0];

  if (cpuModel === undefined) {
    return { kind: "cpuModelNotReported" };
  }

  return {
    kind: "identified",
    machine: { platform: facts.platform, arch: facts.arch, cpuModel, logicalCores: facts.logicalCores, bun: facts.bun },
  };
}

/** The machine that runs this process, and its Bun binary. */
export function currentMachine(): MachineLookup {
  return machineFrom({
    platform: process.platform,
    arch: process.arch,
    cpuModels: cpus().map((cpu) => cpu.model),
    logicalCores: availableParallelism(),
    bun: `${Bun.version}+${Bun.revision.slice(0, 9)}`,
  });
}

/** SHA-256 over each path and its content, in path order. */
export function projectSha256(files: ReadonlyMap<string, string>): string {
  const hash = createHash("sha256");
  const ordered = [...files].sort(([one], [two]) => (one < two ? -1 : one > two ? 1 : 0));

  for (const [path, source] of ordered) {
    hash.update(path);
    hash.update("\0");
    hash.update(source);
    hash.update("\0");
  }

  return hash.digest("hex");
}

export function sameMachine(one: MachineIdentity, two: MachineIdentity): boolean {
  return (
    one.platform === two.platform &&
    one.arch === two.arch &&
    one.cpuModel === two.cpuModel &&
    one.logicalCores === two.logicalCores &&
    one.bun === two.bun
  );
}

/** The stored baseline with the machine, the project, the run count and the session count of `key`. */
export function findBaseline(file: StoredBaselineFile, key: BaselineKey): BaselineLookup {
  const stored = file.entries.find(
    (entry) =>
      sameMachine(entry.machine, key.machine) &&
      entry.projectSha256 === key.projectSha256 &&
      entry.runsPerSession === key.runsPerSession &&
      entry.sessionMedians.length === key.sessions,
  );

  return stored === undefined ? { kind: "noMatch" } : { kind: "found", stored };
}

/** Reads the session medians of a stored baseline. Needs an odd, non-zero count of them. */
export function parseBaseline(stored: StoredBaseline): BaselineParse {
  if (stored.sessionMedians.length % 2 !== 1) {
    return { kind: "unreadable", text: `${stored.sessionMedians.length} session medians`, cause: "the count is not odd" };
  }

  const medians: Temporal.Duration[] = [];

  for (const text of stored.sessionMedians) {
    try {
      medians.push(Temporal.Duration.from(text));
    } catch (error) {
      return { kind: "unreadable", text, cause: describeCause(error) };
    }
  }

  return { kind: "parsed", baseline: { median: medianOf(medians), noise: spreadOf(medians) } };
}

/** Slower when the measured median is above the baseline median plus the noise. */
export function judge(baseline: Baseline, measured: Temporal.Duration): Verdict {
  const limit = baseline.median.add(baseline.noise);

  return Temporal.Duration.compare(measured, limit) > 0 ? { kind: "slower", measured, limit } : { kind: "withinNoise", measured, limit };
}

/** A stored baseline of `key` with the given session medians, one per session of `key`. */
export function storedBaselineOf(key: BaselineKey, sessionMedians: readonly Temporal.Duration[]): StoredBaseline {
  return {
    machine: key.machine,
    projectSha256: key.projectSha256,
    runsPerSession: key.runsPerSession,
    sessionMedians: sessionMedians.map((median) => median.toString()),
  };
}

/** The file with `entry` in place of the entry of the same machine. Entries of other machines stay. */
export function withRecordedBaseline(file: StoredBaselineFile, entry: StoredBaseline): StoredBaselineFile {
  return { entries: [...file.entries.filter((stored) => !sameMachine(stored.machine, entry.machine)), entry] };
}

export function writeBaselineFile(path: string, file: StoredBaselineFile): BaselineFileWrite {
  try {
    writeFileSync(path, `${JSON.stringify(file, null, 2)}\n`);

    return { kind: "written" };
  } catch (error) {
    return { kind: "notWritten", cause: describeCause(error) };
  }
}

export function describeMachine(machine: MachineIdentity): string {
  return `${machine.platform} ${machine.arch}, ${machine.cpuModel}, ${machine.logicalCores} cores, bun ${machine.bun}`;
}

export function describeBaseline(baseline: Baseline): string {
  return `median ${millisecondsText(baseline.median)}, noise ${millisecondsText(baseline.noise)}`;
}
