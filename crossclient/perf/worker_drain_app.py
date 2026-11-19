"""The app module that ``horsies worker`` loads in the ``claim-finish`` scenario.

The worker process and each of its children import this module
(``WORKER_APP_LOCATOR`` in ``worker_drain.py``). It builds the app of the drain
from the database URL that the harness puts in the environment of the worker
(``DRAIN_DSN_VARIABLE``). The harness does not import it.
"""

from __future__ import annotations

from crossclient.perf.worker_drain import drain_app, drain_dsn_from_environment

app = drain_app(drain_dsn_from_environment()).app
