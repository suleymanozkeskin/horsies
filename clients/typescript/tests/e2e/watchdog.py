#!/usr/bin/env python3
"""Memory and wall-time watchdog of one end-to-end layer process (macOS and Linux).

Runs a command in a new session and polls every 100 ms the memory of its process tree: every descendant by parent
pid, plus every pid that was ever in the tree and is still alive (a child that the death of its parent reparented,
such as a worker child that leads its own session). For each process it reads the resident size, and on macOS also
the physical footprint (`proc_pid_rusage`, RUSAGE_INFO_V2; the footprint counts compressed pages, which the resident
size does not). When the sum of either reaches the bound, or the wall time reaches its bound, it sends SIGKILL to
every tracked pid.

When the command ends, it names each process of the tree that is still alive, waits up to LEFTOVER_GRACE_SECONDS for
each to end by itself, then kills what is still there.

Usage: watchdog.py --max-mb 2048 --max-seconds 1800 [--report FILE] -- CMD ...
Exit: the command's exit code; 137 when the watchdog killed the tree at a bound; 3 when the command ended and left
processes that did not end within the grace.
The last line on stderr is a JSON summary.
"""
import argparse
import ctypes
import json
import os
import signal
import subprocess
import sys
import time

POLL_SECONDS = 0.1
LEFTOVER_GRACE_SECONDS = 3.0
EXIT_KILLED_AT_BOUND = 137
EXIT_LEFT_PROCESSES = 3
RUSAGE_INFO_V2 = 2
PHYS_FOOTPRINT_INDEX = 7  # after the 16-byte uuid: user, system, idle, intr, pageins, wired, resident, footprint
RESIDENT_INDEX = 6
PS_ROW_LIMIT = 100_000


class RusageV2(ctypes.Structure):
    _fields_ = [("uuid", ctypes.c_uint8 * 16), ("values", ctypes.c_uint64 * 32)]


def darwin_memory_reader():
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib")
    libproc.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    libproc.proc_pid_rusage.restype = ctypes.c_int

    def memory_of(pid):
        """(resident bytes, footprint bytes) of pid, or None when it is gone."""
        info = RusageV2()
        if libproc.proc_pid_rusage(pid, RUSAGE_INFO_V2, ctypes.byref(info)) != 0:
            return None
        return info.values[RESIDENT_INDEX], info.values[PHYS_FOOTPRINT_INDEX]

    return memory_of


def linux_memory_reader():
    page_bytes = os.sysconf("SC_PAGE_SIZE")

    def memory_of(pid):
        """(resident bytes, resident bytes) of pid from /proc/<pid>/statm, or None when it is gone."""
        try:
            with open(f"/proc/{pid}/statm", "r") as statm:
                fields = statm.read().split()
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            return None
        if len(fields) < 2:
            return None
        resident = int(fields[1]) * page_bytes
        return resident, resident

    return memory_of


def memory_reader():
    if sys.platform == "darwin":
        return darwin_memory_reader()
    if sys.platform.startswith("linux"):
        return linux_memory_reader()
    sys.exit(f"watchdog.py: no memory reader for the platform {sys.platform}")


def process_table():
    out = subprocess.run(["ps", "-axo", "pid=,ppid="], capture_output=True, text=True).stdout
    children = {}
    for line in out.splitlines()[:PS_ROW_LIMIT]:
        parts = line.split()
        if len(parts) == 2:
            children.setdefault(int(parts[1]), []).append(int(parts[0]))
    return children


def tree_of(root, children):
    found, stack = set(), [root]
    while stack:
        pid = stack.pop()
        if pid in found:
            continue
        found.add(pid)
        stack.extend(children.get(pid, []))
    return found


def described(pids):
    rows = []
    for pid in sorted(pids):
        row = subprocess.run(["ps", "-o", "stat=,command=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
        if row:
            rows.append({"pid": pid, "ps": row[:200]})
    return rows


def kill_all(pids, own_pids):
    for pid in pids:
        if pid in own_pids:
            continue
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


def watch(proc, bound, max_seconds, memory_of):
    """Poll the tree until the command ends or a bound is hit. Returns (tracked pids, peaks, killed reason)."""
    started = time.monotonic()
    tracked = {proc.pid}
    peak_resident = peak_footprint = 0
    killed = None
    while proc.poll() is None:
        children = process_table()
        for pid in list(tracked):
            tracked |= tree_of(pid, children)
        resident = footprint = 0
        alive = set()
        for pid in tracked:
            mem = memory_of(pid)
            if mem is None:
                continue
            alive.add(pid)
            resident += mem[0]
            footprint += mem[1]
        tracked = alive | {proc.pid}
        peak_resident = max(peak_resident, resident)
        peak_footprint = max(peak_footprint, footprint)
        if resident >= bound or footprint >= bound:
            killed = "memory"
        elif time.monotonic() - started >= max_seconds:
            killed = "time"
        if killed:
            kill_all(tracked, {os.getpid()})
            break
        time.sleep(POLL_SECONDS)
    return tracked, peak_resident, peak_footprint, killed


def leftovers(tracked, own_pids, memory_of):
    children = process_table()
    found = set()
    for pid in list(tracked):
        for left in tree_of(pid, children):
            if left not in own_pids and left != 1 and memory_of(left) is not None:
                found.add(left)
    return found


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-mb", type=int, default=2048)
    parser.add_argument("--max-seconds", type=float, required=True)
    parser.add_argument("--report")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    memory_of = memory_reader()
    started = time.monotonic()
    proc = subprocess.Popen(command, start_new_session=True)
    tracked, peak_resident, peak_footprint, killed = watch(proc, args.max_mb * 1024 * 1024, args.max_seconds, memory_of)
    code = proc.wait()
    own_pids = {os.getpid(), proc.pid}
    at_exit = leftovers(tracked, own_pids, memory_of)
    at_exit_rows = described(at_exit)
    grace_end = time.monotonic() + LEFTOVER_GRACE_SECONDS
    still = at_exit
    while still and time.monotonic() < grace_end:
        time.sleep(POLL_SECONDS)
        still = leftovers(tracked, own_pids, memory_of)
    after_grace_rows = described(still)
    kill_all(still, own_pids)
    summary = {
        "command": " ".join(os.path.basename(c) if i == 0 else c for i, c in enumerate(command[:6])),
        "exit": code if code >= 0 else f"signal {-code}",
        "killed": killed,
        "peakResidentMb": round(peak_resident / 1048576),
        "peakFootprintMb": round(peak_footprint / 1048576),
        "seconds": round(time.monotonic() - started, 1),
        "leftoverAtExit": at_exit_rows,
        "leftoverAfterGrace": after_grace_rows,
    }
    print(json.dumps(summary), file=sys.stderr)
    if args.report:
        with open(args.report, "a") as report:
            report.write(json.dumps(summary) + "\n")
    if killed in ("memory", "time"):
        sys.exit(EXIT_KILLED_AT_BOUND)
    if code != 0:
        sys.exit(code if code > 0 else 128 - code)
    sys.exit(EXIT_LEFT_PROCESSES if still else 0)


if __name__ == "__main__":
    main()
