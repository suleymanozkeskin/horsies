"""The cross-client test harnesses of horsies: parity, perf and the
stored-form vectors, each with its own tests.

The harnesses use test support of the Python client: ``sqlalchemy_url`` and
``session_database.base_database_url`` of its package ``tests``, which is in
``clients/python/``. An import of this package appends that folder to
``sys.path``, so every entry point of a harness finds the package ``tests``:
a pytest run, ``python -m crossclient.perf``, and the processes that a harness
starts with this package on their import path. The folder goes last, not
first: an installed ``horsies`` keeps its place before the sources in
``clients/python/horsies/``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Final

PYTHON_CLIENT_PROJECT: Final = Path(__file__).resolve().parents[1] / 'clients' / 'python'
"""The folder that holds the package ``tests`` of the Python client."""

if str(PYTHON_CLIENT_PROJECT) not in sys.path:
    sys.path.append(str(PYTHON_CLIENT_PROJECT))
