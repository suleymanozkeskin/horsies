"""Whether the OpenTelemetry API that `horsies[otel]` installs is present.

The check finds the module specs of the API modules that horsies uses. It
imports no API module: `opentelemetry` is a namespace package, so the only
import is that of the namespace, which runs no code. A process with
`NoTracing` never calls this check.
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from typing import Final

OTEL_EXTRA: Final = 'horsies[otel]'
"""The extra that installs the OpenTelemetry API."""

OTEL_EXTRA_INSTALL: Final = "pip install 'horsies[otel]'"
"""The install command that a refusal names."""

OTEL_API_MODULES: Final = (
    'opentelemetry.trace',
    'opentelemetry.metrics',
    'opentelemetry.propagate',
)
"""The API modules that the spans and the producer metrics use, each part
of the `opentelemetry-api` distribution."""


@dataclass(frozen=True, slots=True)
class OtelApiInstalled:
    """Every module of `OTEL_API_MODULES` can be imported."""


@dataclass(frozen=True, slots=True)
class OtelApiMissing:
    """`module` (the first of `OTEL_API_MODULES` that is absent) cannot be
    imported."""

    module: str


type OtelApiPresence = OtelApiInstalled | OtelApiMissing
"""Whether the OpenTelemetry API is installed."""


def otel_api_presence() -> OtelApiPresence:
    """Whether each module of `OTEL_API_MODULES` can be imported. Imports
    no API module (see the module doc); reads the import path only.

    Outcomes: `OtelApiInstalled`, or `OtelApiMissing` with the first module
    that is absent. A namespace `opentelemetry` with no API module in it (for
    example only `opentelemetry-proto` installed) is `OtelApiMissing`.
    """
    for module in OTEL_API_MODULES:
        match _module_found(module):
            case True:
                pass
            case False:
                return OtelApiMissing(module=module)
    return OtelApiInstalled()


def _module_found(module: str) -> bool:
    """Whether the import system finds a spec for the dotted name `module`.

    `importlib.util.find_spec` imports the parent package of a dotted name;
    it raises `ModuleNotFoundError` when the parent is absent, which is the
    same answer as a spec that is not found.
    """
    try:
        return importlib.util.find_spec(module) is not None
    except ModuleNotFoundError:
        return False
