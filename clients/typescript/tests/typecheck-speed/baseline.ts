// The recorded baselines of the typecheck-speed check and the verdict of a measurement against
// one of them. A baseline is a ratio of the app time to the control time, so it compares on any
// machine; it compares only with the Bun binary and the two projects that recorded it.
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { availableParallelism, cpus } from "node:os";

import { describeCause } from "./measure.ts";
import { medianOfRatios, type Ratio, ratioText, spreadOfRatios, sumOfRatios } from "./ratio.ts";

/** The machine that recorded a baseline. It is information only: a baseline is a ratio, so it compares on any machine. */
export interface MachineIdentity {
  readonly platform: string;
  readonly arch: string;
  readonly cpuModel: string;
  readonly logicalCores: number;
}

/**
 * What a measurement must match to compare with a stored baseline. The ratio depends on the Bun binary that runs
 * `bun check` and on the two projects, not on the machine.
 */
export interface BaselineKey {
  /** The Bun binary: version and revision. */
  readonly bun: string;
  /** SHA-256 of the control project (`projectSha256`). */
  readonly controlSha256: string;
  /** SHA-256 of the app project (`projectSha256`). */
  readonly appSha256: string;
  readonly runsPerSession: number;
  /** The count of session ratios. */
  readonly sessions: number;
}

/** One baseline as `baseline.json` stores it. */
export interface StoredBaseline {
  readonly bun: string;
  readonly controlSha256: string;
  readonly appSha256: string;
  readonly runsPerSession: number;
  readonly recordedOn: MachineIdentity;
  /** The ratio of each recorded session: the app median divided by the control median. */
  readonly sessionRatios: readonly number[];
}

export interface StoredBaselineFile {
  readonly entries: readonly StoredBaseline[];
}

/** The center and the run-to-run noise of a baseline. */
export interface Baseline {
  /** The median of the session ratios. */
  readonly median: Ratio;
  /** The largest session ratio minus the smallest session ratio. */
  readonly noise: Ratio;
}

export type MachineLookup = { readonly kind: "identified"; readonly machine: MachineIdentity } | { readonly kind: "cpuModelNotReported" };

export type BaselineLookup = { readonly kind: "found"; readonly stored: StoredBaseline } | { readonly kind: "noMatch" };

export type BaselineParse =
  | { readonly kind: "parsed"; readonly baseline: Baseline }
  | { readonly kind: "unreadable"; readonly text: string; readonly cause: string };

export type Verdict =
  | { readonly kind: "withinNoise"; readonly measured: Ratio; readonly limit: Ratio }
  | { readonly kind: "slower"; readonly measured: Ratio; readonly limit: Ratio };

export type BaselineFileWrite = { readonly kind: "written" } | { readonly kind: "notWritten"; readonly cause: string };

/** The facts that the operating system reports about this process's machine. */
export interface MachineFacts {
  readonly platform: string;
  readonly arch: string;
  readonly cpuModels: readonly string[];
  readonly logicalCores: number;
}

/** The identity of a machine. The first CPU's model names the CPU. */
export function machineFrom(facts: MachineFacts): MachineLookup {
  const cpuModel = facts.cpuModels[0];

  if (cpuModel === undefined) {
    return { kind: "cpuModelNotReported" };
  }

  return {
    kind: "identified",
    machine: { platform: facts.platform, arch: facts.arch, cpuModel, logicalCores: facts.logicalCores },
  };
}

/** The machine that runs this process. */
export function currentMachine(): MachineLookup {
  return machineFrom({
    platform: process.platform,
    arch: process.arch,
    cpuModels: cpus().map((cpu) => cpu.model),
    logicalCores: availableParallelism(),
  });
}

/** The Bun binary that runs this process: its version and the start of its revision. */
export function currentBun(): string {
  return `${Bun.version}+${Bun.revision.slice(0, 9)}`;
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

/** The stored baseline with the Bun binary, the two projects, the run count and the session count of `key`. */
export function findBaseline(file: StoredBaselineFile, key: BaselineKey): BaselineLookup {
  const stored = file.entries.find(
    (entry) =>
      entry.bun === key.bun &&
      entry.controlSha256 === key.controlSha256 &&
      entry.appSha256 === key.appSha256 &&
      entry.runsPerSession === key.runsPerSession &&
      entry.sessionRatios.length === key.sessions,
  );

  return stored === undefined ? { kind: "noMatch" } : { kind: "found", stored };
}

/** Reads the session ratios of a stored baseline. Needs an odd, non-zero count of finite ratios above 0. */
export function parseBaseline(stored: StoredBaseline): BaselineParse {
  if (stored.sessionRatios.length % 2 !== 1) {
    return { kind: "unreadable", text: `${stored.sessionRatios.length} session ratios`, cause: "the count is not odd" };
  }

  const ratios: Ratio[] = [];

  for (const value of stored.sessionRatios) {
    if (!Number.isFinite(value) || value <= 0) {
      return { kind: "unreadable", text: String(value), cause: "a session ratio must be a finite number above 0" };
    }

    ratios.push({ appPerControl: value });
  }

  return { kind: "parsed", baseline: { median: medianOfRatios(ratios), noise: spreadOfRatios(ratios) } };
}

/** Slower when the measured ratio is above the baseline median plus the noise. */
export function judge(baseline: Baseline, measured: Ratio): Verdict {
  const limit = sumOfRatios(baseline.median, baseline.noise);

  return measured.appPerControl > limit.appPerControl ? { kind: "slower", measured, limit } : { kind: "withinNoise", measured, limit };
}

/** A stored baseline of `key` with the given session ratios, one per session of `key`. */
export function storedBaselineOf(key: BaselineKey, recordedOn: MachineIdentity, sessionRatios: readonly Ratio[]): StoredBaseline {
  return {
    bun: key.bun,
    controlSha256: key.controlSha256,
    appSha256: key.appSha256,
    runsPerSession: key.runsPerSession,
    recordedOn,
    sessionRatios: sessionRatios.map((ratio) => ratio.appPerControl),
  };
}

/** The file with `entry` in place of the entry of the same Bun binary. Entries of other Bun binaries stay. */
export function withRecordedBaseline(file: StoredBaselineFile, entry: StoredBaseline): StoredBaselineFile {
  return { entries: [...file.entries.filter((stored) => stored.bun !== entry.bun), entry] };
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
  return `${machine.platform} ${machine.arch}, ${machine.cpuModel}, ${machine.logicalCores} cores`;
}

export function describeBaseline(baseline: Baseline): string {
  return `median ${ratioText(baseline.median)}, noise ${ratioText(baseline.noise)}`;
}
