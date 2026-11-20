"""The app module that ``horsies worker`` loads in a drain with tracing: the
levels ``tracing``, ``handler`` and ``worker-ended-spans``.

The worker process and each of its children import this module
(``TELEMETRY_APP_LOCATOR`` in ``telemetry_levels.py``). It builds the app of
the drain from the database URL and the ``AppTelemetry`` that the harness
puts in the environment of the worker. The harness does not import it.
"""

from __future__ import annotations

from crossclient.perf.telemetry_apps import telemetry_drain_app
from crossclient.perf.telemetry_levels import app_telemetry_from_environment
from crossclient.perf.worker_drain import drain_dsn_from_environment

app = telemetry_drain_app(
    drain_dsn_from_environment(),
    app_telemetry_from_environment(),
).app
