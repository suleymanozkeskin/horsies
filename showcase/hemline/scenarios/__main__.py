# showcase/hemline/scenarios/__main__.py
"""Scenario dispatch.

    uv run python -m showcase.hemline.scenarios <name>

Run from the repository root, the same directory the worker, scheduler, and
web processes are started from.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from typing import Final

from . import seed, steady

# Scenarios are watched live and are usually piped — into a terminal
# multiplexer, a process manager, or a log file, where Python block-buffers
# stdout and a running scenario looks like a hung one. Every scenario line goes
# through `say()`, which flushes.

SCENARIOS: Final[dict[str, Callable[[], int]]] = {
    'seed': seed.run,
    'steady': steady.run,
}


def main() -> int:
    """Parse the scenario name and run it. Returns a process exit code."""
    parser = argparse.ArgumentParser(
        prog='python -m showcase.hemline.scenarios',
        description='Hemline demo scenarios.',
    )
    parser.add_argument(
        'scenario',
        choices=sorted(SCENARIOS),
        help='which scenario to run',
    )
    arguments = parser.parse_args()
    return SCENARIOS[arguments.scenario]()


if __name__ == '__main__':
    raise SystemExit(main())
