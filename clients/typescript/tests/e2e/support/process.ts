// A worker process of a test, or a command that ends by itself, such as `check --live` (Rust
// `clients/rust/tests/support/src/process.rs`, Python `clients/python/tests/e2e/helpers/worker.py`).
//
// - The process writes its output to a file, never to a pipe: a pipe that nothing reads blocks the worker. Its
//   children inherit the file.
// - The process starts in a new process group (`detached: true`), with stdin closed.
// - A stop sends `SIGTERM` to the process, which drains its children, and kills the whole group after
//   `STOP_WAIT_MS`. The children of a worker lead their own sessions; they end when the worker ends.
// - A wait that does not reach its condition fails the test: an `AssertionError` with the subject, the cause and the
//   last lines of the output.

import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { closeSync, openSync, readFileSync, statSync } from "node:fs";

/** The longest wait for the exit after `SIGTERM`: a test profile drains in 5 s, and the stop steps after it are bounded. */
export const STOP_WAIT_MS = 60_000;

/** The longest wait for the exit after `SIGKILL`. */
export const KILL_WAIT_MS = 10_000;

/** The interval of the exit, log and condition polls. */
export const POLL_INTERVAL_MS = 100;

/** The lines of a failure report. */
export const REPORT_LINES = 200;

/** The largest output file that a log read reads. A larger one fails the test. */
export const LOG_READ_MAX_BYTES = 64 * 1024 * 1024;

/** How a process stands: running, or exited with a code or a signal. */
export type ExitState =
  | { readonly kind: "running" }
  | { readonly kind: "exitedWithCode"; readonly code: number }
  | { readonly kind: "endedBySignal"; readonly signal: string };

/** How a stop ended. */
export type StopEnd =
  | { readonly kind: "alreadyExited"; readonly exit: ExitState }
  | { readonly kind: "exitedAfterStop"; readonly exit: ExitState }
  | { readonly kind: "killedAfterStopWait"; readonly exit: ExitState };

/** What a process runs: the program, its arguments and the extra environment. */
export type ProcessCommand = {
  readonly program: string;
  readonly args: readonly string[];
  readonly env: Readonly<Record<string, string>>;
};

/** A process of a test. */
export class TestProcess {
  readonly label: string;
  readonly pid: number;
  readonly logPath: string;
  readonly #child: ChildProcess;
  #exit: ExitState = { kind: "running" };

  private constructor(label: string, child: ChildProcess, pid: number, logPath: string) {
    this.label = `${label}[pid=${String(pid)}]`;
    this.pid = pid;
    this.logPath = logPath;
    this.#child = child;
    child.on("exit", (code, signal) => {
      this.#exit = signal === null ? { kind: "exitedWithCode", code: code ?? 0 } : { kind: "endedBySignal", signal };
    });
    child.unref();
  }

  /** Start `command` in a new process group, its output appended to `logPath`. Fails the test when the start fails. */
  static start(label: string, command: ProcessCommand, logPath: string): TestProcess {
    const output = openSync(logPath, "a");

    try {
      const child = spawn(command.program, [...command.args], {
        detached: true,
        stdio: ["ignore", output, output],
        env: { ...process.env, ...command.env },
      });

      if (child.pid === undefined) {
        assert.fail(`${label}: ${command.program} did not start`);
      }

      return new TestProcess(label, child, child.pid, logPath);
    } finally {
      closeSync(output);
    }
  }

  /** How the process stands now. */
  exitState(): ExitState {
    return this.#exit;
  }

  /** Every line of the output so far. */
  logLines(): readonly string[] {
    const size = statSync(this.logPath).size;

    if (size > LOG_READ_MAX_BYTES) {
      assert.fail(`${this.label}: the output file holds ${String(size)} bytes, over ${String(LOG_READ_MAX_BYTES)}`);
    }

    return readFileSync(this.logPath, "utf8").split("\n");
  }

  /** The number of output lines that contain `needle`. */
  logCount(needle: string): number {
    return this.logLines().filter((line) => line.includes(needle)).length;
  }

  /** The label, how it stands, and the last `REPORT_LINES` output lines, for a failure message. */
  report(): string {
    const tail = this.logLines().slice(-REPORT_LINES).join("\n");

    return `${this.label} (${exitText(this.#exit)}) output, last ${String(REPORT_LINES)} lines:\n${tail}`;
  }

  /** Wait until `probe` answers true, every `POLL_INTERVAL_MS`. Fails the test when the process exits first or `limitMs` passes. */
  async waitUntil(limitMs: number, subject: string, probe: () => Promise<boolean>): Promise<void> {
    const deadline = performance.now() + limitMs;

    for (;;) {
      if (this.#exit.kind !== "running") {
        assert.fail(`${subject}: the process exited first\n${this.report()}`);
      }

      if (await probe()) {
        return;
      }

      if (performance.now() >= deadline) {
        assert.fail(`${subject}: not reached within ${String(limitMs)} ms\n${this.report()}`);
      }

      await pause(POLL_INTERVAL_MS);
    }
  }

  /** Wait until an output line contains `needle`. Fails the test as `waitUntil` does. */
  async waitForLog(needle: string, limitMs: number): Promise<void> {
    await this.waitUntil(limitMs, `the output line ${JSON.stringify(needle)}`, async () => this.logCount(needle) > 0);
  }

  /** Wait until the process exits by itself, at most `limitMs`; how it stands then. */
  async waitExit(limitMs: number): Promise<ExitState> {
    const deadline = performance.now() + limitMs;

    while (this.#exit.kind === "running" && performance.now() < deadline) {
      await pause(POLL_INTERVAL_MS);
    }

    return this.#exit;
  }

  /** Send `signal` to the process alone. A process that exited gets nothing. */
  signal(signal: "SIGTERM" | "SIGINT" | "SIGKILL"): void {
    if (this.#exit.kind === "running") {
      this.#child.kill(signal);
    }
  }

  /** Kill the process and every process of its group with `SIGKILL`: the crash of a worker. Does not wait. */
  killGroup(): void {
    killGroupOf(this.pid);
  }

  /** Stop the process: `SIGTERM`, the exit within `STOP_WAIT_MS`, else `SIGKILL` to the group. */
  async stop(): Promise<StopEnd> {
    if (this.#exit.kind !== "running") {
      return { kind: "alreadyExited", exit: this.#exit };
    }

    this.signal("SIGTERM");

    const stopped = await this.waitExit(STOP_WAIT_MS);

    if (stopped.kind !== "running") {
      killGroupOf(this.pid);

      return { kind: "exitedAfterStop", exit: stopped };
    }

    killGroupOf(this.pid);

    return { kind: "killedAfterStopWait", exit: await this.waitExit(KILL_WAIT_MS) };
  }
}

/** Kill every process of the group `pgid` with `SIGKILL`. A group that has ended gets nothing. */
export function killGroupOf(pgid: number): void {
  try {
    process.kill(-pgid, "SIGKILL");
  } catch (cause) {
    if (!(cause instanceof Error && "code" in cause && cause.code === "ESRCH")) {
      throw cause;
    }
  }
}

/** Whether the process `pid` exists (signal 0): `EPERM` names a process of another user, which exists. */
export function processAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);

    return true;
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && (cause.code === "ESRCH" || cause.code === "EPERM")) {
      return cause.code === "EPERM";
    }

    throw cause;
  }
}

/** The text of an exit state, for a message. Pure. */
export function exitText(exit: ExitState): string {
  switch (exit.kind) {
    case "running":
      return "running";
    case "exitedWithCode":
      return `exit ${String(exit.code)}`;
    case "endedBySignal":
      return `ended by ${exit.signal}`;
  }
}

/** Wait `milliseconds`. */
export async function pause(milliseconds: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, milliseconds));
}
