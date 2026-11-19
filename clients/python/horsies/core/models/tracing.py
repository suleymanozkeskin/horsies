"""The tracing setting of `AppConfig`: `OtelTracing` or `NoTracing`.

`OtelTracing` traces the sends and workflow starts of the app and each
attempt that a worker child runs, with the OpenTelemetry API, and records the
producer metrics with the OpenTelemetry meter. The API comes with the extra
`horsies[otel]`; the user's SDK, set in the process, decides what is
exported. `NoTracing` (the default) makes no span and records no metric, and
the client does not import `opentelemetry`.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, ClassVar, Literal, Union, assert_never

from pydantic import BaseModel, ConfigDict, Field

from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.telemetry.otel_api import (
    OTEL_EXTRA,
    OTEL_EXTRA_INSTALL,
    OtelApiInstalled,
    OtelApiMissing,
    otel_api_presence,
)


class ChildOfSend(BaseModel):
    """The span of an attempt is a child of the span that sent the task:
    the send and its attempts are one trace. The default."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['child_of_send'] = 'child_of_send'


class LinkedToSend(BaseModel):
    """The span of an attempt starts a trace of its own, with a link to the
    span that sent the task."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['linked_to_send'] = 'linked_to_send'


AttemptParentage = Annotated[
    Union[ChildOfSend, LinkedToSend],
    Field(discriminator='kind'),
]
"""How the span of an attempt relates to the span that sent its task."""


class WorkerEndedSpans(str, Enum):
    """Whether the handler child of a worker makes the span of each attempt
    that the worker ended: an isolated task killed at its timeout, a shared
    async child killed for a blocked loop, a child lost with its exit seen.
    The task child of such an attempt is gone, so the span that it opened is
    never exported.

    - `MAKE_SPANS`: the handler child makes the span. The default. The
      worker starts its handler child also when the app has no event
      handler; set the tracer provider in `@app.on_handler_child_start`.
    - `NO_SPANS`: no span for these attempts.
    """

    MAKE_SPANS = 'MAKE_SPANS'
    NO_SPANS = 'NO_SPANS'


class OtelTracing(BaseModel):
    """Spans and producer metrics through the OpenTelemetry API.

    Needs the extra `horsies[otel]`; a config with `OtelTracing` and no
    OpenTelemetry API is refused when it is built (`HRS-220`).

    `parentage` places the span of each attempt: `ChildOfSend()` (the
    default) or `LinkedToSend()`. A task sent with no span has an attempt
    span with no parent in both.

    `worker_ended_spans` makes the span of each attempt that the worker
    ended, in the handler child: `WorkerEndedSpans.MAKE_SPANS` (the default)
    or `WorkerEndedSpans.NO_SPANS`.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['otel_tracing'] = 'otel_tracing'
    parentage: AttemptParentage = Field(default_factory=ChildOfSend)
    worker_ended_spans: WorkerEndedSpans = WorkerEndedSpans.MAKE_SPANS


class NoTracing(BaseModel):
    """No span and no producer metric. The client does not import
    `opentelemetry`."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_tracing'] = 'no_tracing'


TracingSetting = Annotated[
    Union[OtelTracing, NoTracing],
    Field(discriminator='kind'),
]
"""The tracing setting of `AppConfig.tracing`."""


def worker_ended_spans_of(setting: OtelTracing | NoTracing) -> WorkerEndedSpans:
    """Whether the handler child makes the spans of the attempts that the
    worker ended under `setting`: never with `NoTracing`. Pure."""
    match setting:
        case NoTracing():
            return WorkerEndedSpans.NO_SPANS
        case OtelTracing(worker_ended_spans=spans):
            return spans
        case _ as unreachable:
            assert_never(unreachable)


def tracing_errors(setting: OtelTracing | NoTracing) -> list[ConfigurationError]:
    """The refusals of the tracing setting `setting`: `OtelTracing` with the
    OpenTelemetry API not installed (`HRS-220`, which names the extra). No
    refusal for `NoTracing`, which does not look for the API.

    Reads the import path for `OtelTracing` (`otel_api_presence`).
    """
    match setting:
        case NoTracing():
            return []
        case OtelTracing():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match otel_api_presence():
        case OtelApiInstalled():
            return []
        case OtelApiMissing(module=module):
            return [
                ConfigurationError(
                    message=f'tracing=OtelTracing() needs the OpenTelemetry API ({OTEL_EXTRA})',
                    code=ErrorCode.CONFIG_OTEL_API_MISSING,
                    notes=[f'the module {module} cannot be imported'],
                    help_text=(
                        f'install the extra: {OTEL_EXTRA_INSTALL}, '
                        'or set tracing=NoTracing()'
                    ),
                ),
            ]


def tracing_label(setting: OtelTracing | NoTracing) -> str:
    """The text of a tracing setting in the config log and in the success
    text of `horsies check`: `OpenTelemetry` or `off`. Pure."""
    match setting:
        case OtelTracing():
            return 'OpenTelemetry'
        case NoTracing():
            return 'off'
        case _ as unreachable:
            assert_never(unreachable)
