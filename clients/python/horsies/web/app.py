# pyright: reportUnusedFunction=false
# Route handlers are registered by their decorator, not called by name.
"""FastAPI application factory for the monitoring UI.

The app mounts the core's monitoring handler (``horsies._core.MonitoringHandler``):
every route, guard, read cache, the schema gate, the event stream and the
page run in the core. What lives here is the user's authorization policy,
which only Python can call. For each request the app asks the handler which
verdicts the request needs, calls the policy for those and no other, and
passes the verdicts with the request.

Mounting anywhere works because the page is told its own base path per
request from the ASGI ``root_path`` rather than at build time.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import (
    Annotated,
    Any,
    AsyncGenerator,
    ClassVar,
    Final,
    Literal,
    Self,
    Union,
    assert_never,
)
from urllib.parse import quote

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from horsies import __version__
from horsies._core import (
    AccessGrant,
    EventFrame,
    EventStream,
    EventStreamEnded,
    EventStreamRefused,
    MonitoringHandler,
    MonitoringRequest,
    MonitoringResponse,
    trace_link_refusal,
)
from horsies.core.app import Horsies
from horsies.core.fleet_metrics import fleet_label_names
from horsies.core.errors import ConfigurationError, HorsiesErrorCode
from horsies.web.auth import INTENT_HEADER, MonitoringAuthPolicy
from horsies.web.schema import SchemaState

EVENT_HEARTBEAT_MS: Final = 15_000
"""How long an event stream waits without an event before it sends a
heartbeat frame, which keeps an intermediary proxy from closing it."""

SERVED_METHODS: Final = ('GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS')
"""The methods that reach the handler: every route of the app answers in the
core, a method that no route takes included (405)."""

type Verdict = Literal['ALLOWED', 'REFUSED']
"""The answer of one call of the user's policy."""


TRACE_ID_PLACEHOLDER: Final = '{trace_id}'
"""The placeholder of a trace link template that the page fills with the 32
hex digits of a task's trace id."""


class TraceLinkTemplate(BaseModel):
    """The link of a task's trace id in the task detail of the page: an
    ``http`` or ``https`` URL with one ``{trace_id}``, at most 2,048 bytes of
    printable ASCII with no space, for example
    ``https://grafana.example.com/explore?traceId={trace_id}``.

    The core checks the template (``horsies._core.trace_link_refusal``); a
    template that it refuses raises ``ConfigurationError`` (``HRS-221``) with
    the rule that does not hold.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['trace_link_template'] = 'trace_link_template'
    template: str

    @model_validator(mode='after')
    def check_template(self) -> Self:
        match trace_link_refusal(self.template):
            case None:
                return self
            case str() as refusal:
                raise ConfigurationError(
                    message='invalid trace link template',
                    code=HorsiesErrorCode.CONFIG_INVALID_TRACE_LINK,
                    notes=[refusal],
                    help_text=(
                        'use an http or https URL with one '
                        f'{TRACE_ID_PLACEHOLDER}, for example '
                        f'https://grafana.example.com/explore?traceId={TRACE_ID_PLACEHOLDER}'
                    ),
                )


class NoTraceLink(BaseModel):
    """The task detail shows the trace id as text."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_trace_link'] = 'no_trace_link'


TraceLinkSetting = Annotated[
    Union[TraceLinkTemplate, NoTraceLink],
    Field(discriminator='kind'),
]
"""The trace link of the page."""


def trace_link_template_text(setting: TraceLinkTemplate | NoTraceLink) -> str | None:
    """The template text that the core handler takes, or ``None`` for
    ``NoTraceLink``. Pure."""
    match setting:
        case TraceLinkTemplate(template=template):
            return template
        case NoTraceLink():
            return None
        case _ as unreachable:
            assert_never(unreachable)


class MonitoringUIConfig(BaseModel):
    """Adopter-facing customization of the served page.

    ``custom_css_url`` is injected after everything else, so a stylesheet it
    points at overrides any design token the app ships. Overriding CSS custom
    properties is the entire customization contract — there is no build-time
    theming.

    ``trace_link`` is the link of a task's trace id in the task detail
    (``TraceLinkTemplate``), or ``NoTraceLink`` (the default) for the id as
    text.
    """

    custom_css_url: str | None = None
    trace_link: TraceLinkSetting = Field(default_factory=NoTraceLink)


class MetaResponse(BaseModel):
    """What the SPA needs on boot to decide what to render: the body of
    ``GET /api/meta``.

    ``actions_enabled`` is the server's verdict — the deployment's own switch
    ANDed with schema compatibility — while ``can_act`` is this caller's
    policy verdict. A schema the tool cannot write through forces actions off
    for everyone and names itself in ``actions_disabled_reason``.
    ``schema_state`` is the probe's verdict itself, so a client never derives
    it from the version numbers. ``trace_link_template`` is the template of
    the trace link of a task detail, or ``None`` for the trace id as text.
    """

    horsies_version: str
    base_path: str
    actions_enabled: bool
    can_act: bool
    schema_version: int | None
    expected_schema_version: int
    schema_compatible: bool
    schema_state: SchemaState
    actions_disabled_reason: str | None
    trace_link_template: str | None


class MonitoringEvents:
    """The event layer of one monitoring app (``monitoring_app.state.events``).

    ``close`` ends every open event stream of the app after its degraded
    frame. The app's lifespan calls it at shutdown; a host process or a test
    can call it without waiting for the lifespan.
    """

    def __init__(self, handler: MonitoringHandler) -> None:
        self._handler = handler

    async def close(self) -> None:
        """End every event stream of the app. Runs to its end when the caller leaves."""
        await self._handler.close_events_async()


def path_inside_mount(scope: Mapping[str, Any]) -> str:
    """The request path inside the mount, percent-encoded as it came on the wire.

    The ASGI ``raw_path`` holds the whole path, the mount path included; the
    mount path (``root_path``) is cut as Starlette cuts it from ``path``. A
    scope without ``raw_path`` gives ``path`` encoded again. Pure.
    """
    root_path: str = scope.get('root_path', '')
    raw_path: object = scope.get('raw_path')
    match raw_path:
        case bytes():
            path = raw_path.decode('latin-1')
        case _:
            path = quote(str(scope['path']))
    if not root_path or not path.startswith(root_path):
        return path or '/'
    if path == root_path:
        return '/'
    if path[len(root_path)] == '/':
        return path[len(root_path) :]
    return path


def _verdict(answer: object) -> Verdict:
    """A policy answer as a verdict: a true value allows. Pure."""
    match bool(answer):
        case True:
            return 'ALLOWED'
        case False:
            return 'REFUSED'


async def ask_policy(
    need: Literal['PUBLIC', 'VIEW', 'VIEW_REPORT_ACT', 'VIEW_AND_ACT'],
    policy: MonitoringAuthPolicy,
    request: Request,
) -> AccessGrant:
    """Call the user's policy for the verdicts that ``need`` names, in order.

    ``can_act`` is asked only after ``can_view`` allowed. Effects: the calls
    of the user's policy. A policy that raises raises here.
    """
    match need:
        case 'PUBLIC':
            return AccessGrant(view='NOT_ASKED', act='NOT_ASKED')
        case 'VIEW':
            return AccessGrant(
                view=_verdict(await policy.can_view(request)), act='NOT_ASKED',
            )
        case 'VIEW_REPORT_ACT' | 'VIEW_AND_ACT':
            match _verdict(await policy.can_view(request)):
                case 'REFUSED':
                    return AccessGrant(view='REFUSED', act='NOT_ASKED')
                case 'ALLOWED':
                    return AccessGrant(
                        view='ALLOWED', act=_verdict(await policy.can_act(request)),
                    )
        case _ as unreachable:
            assert_never(unreachable)


async def read_bounded_body(request: Request, bound: int) -> bytes:
    """The request body, read up to ``bound + 1`` bytes.

    A body over ``bound`` stops the read; the handler refuses it with 413.
    """
    chunks: list[bytes] = []
    read = 0
    async for chunk in request.stream():
        chunks.append(chunk)
        read += len(chunk)
        if read > bound:
            break
    return b''.join(chunks)[: bound + 1]


def to_starlette_response(response: MonitoringResponse) -> Response:
    """The handler's response as a Starlette response. Starlette writes the
    content length of the body. Pure."""
    sent = Response(content=response.body, status_code=response.status)
    for name, value in response.headers:
        match name:
            case 'content-length':
                pass
            case _:
                sent.headers.append(name, value)
    return sent


async def stream_frames(stream: EventStream) -> AsyncIterator[str]:
    """The frames of an event stream until it ends. The stream is closed when
    the client leaves."""
    try:
        while True:
            match await stream.next_async():
                case EventFrame(text=text):
                    yield text
                case EventStreamEnded():
                    return
    finally:
        stream.close()


async def one_frame(frame: str) -> AsyncIterator[str]:
    """One frame, then the end."""
    yield frame


async def event_stream_response(handler: MonitoringHandler) -> StreamingResponse:
    """The event stream of one client, after an allowed view."""
    match await handler.subscribe_events_async():
        case EventStream() as stream:
            frames = stream_frames(stream)
        case EventStreamRefused(frame=frame):
            frames = one_frame(frame)
        case _ as unreachable:
            assert_never(unreachable)
    return StreamingResponse(
        frames,
        media_type='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        },
    )


async def one_response(
    handler: MonitoringHandler,
    request: Request,
    path: str,
    grant: AccessGrant,
) -> Response:
    """The handler's one response to a request with its verdicts."""
    query = bytes(request.scope.get('query_string', b'')).decode('latin-1')
    answer = await handler.handle_async(
        MonitoringRequest(
            method=request.method,
            path=path,
            raw_query=query or None,
            intent_header=request.headers.get(INTENT_HEADER),
            content_type=request.headers.get('content-type'),
            body=await read_bounded_body(request, handler.max_request_body_bytes),
            base_path=str(request.scope.get('root_path', '')),
            grant=grant,
        ),
    )
    return to_starlette_response(answer)


async def serve_request(
    handler: MonitoringHandler,
    policy: MonitoringAuthPolicy,
    request: Request,
) -> Response:
    """Answer one request: the policy's verdicts, then the handler's answer.

    The event stream opens only after an allowed view; a refused view of it
    is the handler's 403.
    """
    path = path_inside_mount(request.scope)
    access = handler.route_access(request.method, path)
    grant = await ask_policy(access.need, policy, request)
    match access.form:
        case 'EVENT_STREAM' if grant.view == 'ALLOWED':
            return await event_stream_response(handler)
        case 'EVENT_STREAM' | 'ONE_RESPONSE':
            return await one_response(handler, request, path, grant)


def create_monitoring_app(
    app: Horsies,
    *,
    auth_policy: MonitoringAuthPolicy,
    config: MonitoringUIConfig | None = None,
    actions_enabled: bool = True,
) -> FastAPI:
    """Build the monitoring application for a horsies app.

    ``auth_policy`` is required and has no default: a deployment must choose
    consciously. When mounting this under a host application, the mount must
    sit behind that application's own authentication — ``AllowAll()`` states
    that it does.

    No schema browser and no CORS middleware are exposed: the SPA is the only
    intended client and it is served same-origin.

    ``GET /metrics`` serves the fleet metrics of the app's database behind
    ``can_view``. Its ``queue`` and ``schedule`` label values are the queues and
    the schedules of ``app.config``; other names are ``(unknown)``. A queue or
    schedule name that cannot be a label value of its own (empty,
    ``(unknown)``, or past the bound of names) counts in the ``(unknown)``
    series, and the first request logs one WARNING line that names it.
    """
    broker = app.get_broker()
    ui_config = config or MonitoringUIConfig()
    names = fleet_label_names(app)
    handler = MonitoringHandler(
        broker._core_handle,  # pyright: ignore[reportPrivateUsage]
        actions_enabled=actions_enabled,
        custom_css_url=ui_config.custom_css_url,
        trace_link_template=trace_link_template_text(ui_config.trace_link),
        client_version=__version__,
        heartbeat_ms=EVENT_HEARTBEAT_MS,
        queue_names=list(names.queues),
        schedule_names=list(names.schedules),
    )
    events = MonitoringEvents(handler)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
        yield
        await events.close()

    monitoring_app = FastAPI(
        title='horsies monitoring',
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    # Exposed so a host process (or a test) can shut the event layer down
    # without waiting for the ASGI lifespan to run.
    monitoring_app.state.events = events

    # One route takes every path: the page, its assets, the API and its 404s
    # are the handler's.
    @monitoring_app.api_route(
        '/{full_path:path}', methods=list(SERVED_METHODS), include_in_schema=False,
    )
    async def serve(request: Request) -> Response:
        """Answer one request through the handler."""
        return await serve_request(handler, auth_policy, request)

    return monitoring_app
