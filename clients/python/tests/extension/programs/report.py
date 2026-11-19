"""The report of one case of a program, and the entry point of a program.

A program runs the case that its command line names and writes the case's
report, one JSON object, to the report path. This module imports no horsies
module, so a case can measure the process before its first horsies import.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Final

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type Report = dict[str, JsonValue]

DATABASE_URL_VARIABLE: Final = 'HORSIES_EXTENSION_DATABASE_URL'
"""The environment variable that gives a case the URL of its database."""

USAGE_EXIT_CODE: Final = 2
"""The exit code of a program whose command line names no case of it."""


def run_case(cases: Mapping[str, Callable[[], Report]]) -> None:
    """Run the case that ``sys.argv`` names (``<case> <report path>``) and
    write its report as JSON to the report path.

    A command line that names no case of ``cases`` writes the usage to stderr
    and exits with ``USAGE_EXIT_CODE``. An exception of the case propagates:
    the program exits with it, and the test shows its stderr.
    """
    match sys.argv[1:]:
        case [name, report_path] if name in cases:
            report = cases[name]()
            Path(report_path).write_text(json.dumps(report), encoding='utf-8')
        case _:
            sys.stderr.write(
                f'usage: <case> <report path>; cases: {", ".join(sorted(cases))}\n',
            )
            raise SystemExit(USAGE_EXIT_CODE)
