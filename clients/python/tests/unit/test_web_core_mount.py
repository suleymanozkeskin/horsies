"""Unit tests of ``create_monitoring_app`` on the core's monitoring handler.

The app asks the handler which verdicts a request needs, calls the user's
policy for those and no other, and passes the verdicts with the request. No
database is reachable here, so every request below is answered before a
read: a refused verdict, a path id that is not a UUID, the page, an unknown
API path. Each expected answer (the status, the body and the policy calls)
is the answer of the Python app before the switch to the core's handler.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Final

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.web import (
    INTENT_HEADER,
    INTENT_VALUE,
    Actions,
    AllowAll,
    MonitoringUIConfig,
    create_monitoring_app,
)
from horsies.web.app import ask_policy, path_inside_mount, read_bounded_body

pytestmark = [pytest.mark.unit]

asyncio_tests = pytest.mark.asyncio(loop_scope='function')

UNREACHABLE_URL: Final = 'postgresql://postgres:none@127.0.0.1:1/none'
"""Nothing listens here; no request below reaches the database."""

ACT_HEADERS: Final = {INTENT_HEADER: INTENT_VALUE}


def make_horsies_app() -> Horsies:
    """A horsies app whose broker can never connect."""
    return Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL))))


@dataclass
class RecordingPolicy:
    """A policy with fixed answers that records each call, in order."""

    view: bool
    act: bool
    calls: list[str] = field(default_factory=list[str])

    async def can_view(self, request: Request) -> bool:
        _ = request
        self.calls.append('can_view')
        return self.view

    async def can_act(self, request: Request) -> bool:
        _ = request
        self.calls.append('can_act')
        return self.act


@dataclass(frozen=True, slots=True)
class Answer:
    """What one request got: the status, the body (its JSON value when it is
    JSON, else its text) and the policy calls."""

    status: int
    body: object
    calls: tuple[str, ...]


async def answer_of(
    method: str,
    path: str,
    *,
    view: bool,
    act: bool,
    actions: Actions = Actions.ENABLED,
) -> Answer:
    """The answer of one request to a new monitoring app."""
    policy = RecordingPolicy(view=view, act=act)
    built = create_monitoring_app(
        make_horsies_app(), authorization_policy=policy, actions=actions,
    )
    try:
        async with AsyncClient(transport=ASGITransport(app=built), base_url='http://test') as client:
            response = await client.request(method, path, headers=ACT_HEADERS)
    finally:
        await built.state.events.close()
    body: object = (
        response.json()
        if response.headers.get('content-type', '').startswith('application/json')
        else response.text
    )
    return Answer(status=response.status_code, body=body, calls=tuple(policy.calls))


class TestHardening:
    """The SPA is the only client; nothing else is exposed."""

    def test_no_schema_browser_and_no_cors(self) -> None:
        built = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        assert built.docs_url is None
        assert built.redoc_url is None
        assert built.openapi_url is None
        assert all(middleware.cls is not CORSMiddleware for middleware in built.user_middleware)

    def test_authorization_policy_has_no_default(self) -> None:
        with pytest.raises(TypeError):
            create_monitoring_app(make_horsies_app())  # type: ignore[call-arg]


class TestPathInsideMount:
    """The handler gets the path inside the mount as it came on the wire."""

    def test_the_raw_path_without_a_mount(self) -> None:
        scope = {'path': '/api/tasks/a b', 'raw_path': b'/api/tasks/a%20b', 'root_path': ''}

        assert path_inside_mount(scope) == '/api/tasks/a%20b'

    def test_the_mount_path_is_cut(self) -> None:
        scope = {
            'path': '/monitoring/api/meta',
            'raw_path': b'/monitoring/api/meta',
            'root_path': '/monitoring',
        }

        assert path_inside_mount(scope) == '/api/meta'

    def test_the_mount_itself_is_the_root(self) -> None:
        scope = {'path': '/monitoring', 'raw_path': b'/monitoring', 'root_path': '/monitoring'}

        assert path_inside_mount(scope) == '/'

    def test_a_path_that_only_starts_like_the_mount_is_kept(self) -> None:
        scope = {'path': '/monitoring2/x', 'raw_path': b'/monitoring2/x', 'root_path': '/monitoring'}

        assert path_inside_mount(scope) == '/monitoring2/x'

    def test_a_path_without_the_mount_is_kept(self) -> None:
        """httpx's ASGI transport sends the root path apart from the path."""
        scope = {'path': '/api/meta', 'raw_path': b'/api/meta', 'root_path': '/monitoring'}

        assert path_inside_mount(scope) == '/api/meta'

    def test_a_scope_without_raw_path_encodes_the_path(self) -> None:
        scope = {'path': '/api/workers/a b/history', 'root_path': ''}

        assert path_inside_mount(scope) == '/api/workers/a%20b/history'


def bare_request() -> Request:
    return Request({'type': 'http', 'method': 'GET', 'path': '/', 'headers': []})


class TestAskPolicy:
    """The policy is called for the verdicts that the route needs, in order."""

    pytestmark = [asyncio_tests]

    async def test_a_public_route_calls_nothing(self) -> None:
        policy = RecordingPolicy(view=True, act=True)

        grant = await ask_policy('PUBLIC', policy, bare_request())

        assert (grant.view, grant.act) == ('NOT_ASKED', 'NOT_ASKED')
        assert policy.calls == []

    @pytest.mark.parametrize(('view', 'verdict'), [(True, 'ALLOWED'), (False, 'REFUSED')])
    async def test_a_read_calls_can_view_only(self, view: bool, verdict: str) -> None:
        policy = RecordingPolicy(view=view, act=True)

        grant = await ask_policy('VIEW', policy, bare_request())

        assert (grant.view, grant.act) == (verdict, 'NOT_ASKED')
        assert policy.calls == ['can_view']

    @pytest.mark.parametrize('need', ['VIEW_REPORT_ACT', 'VIEW_AND_ACT'])
    async def test_a_refused_view_does_not_ask_to_act(self, need: Any) -> None:
        policy = RecordingPolicy(view=False, act=True)

        grant = await ask_policy(need, policy, bare_request())

        assert (grant.view, grant.act) == ('REFUSED', 'NOT_ASKED')
        assert policy.calls == ['can_view']

    @pytest.mark.parametrize(('act', 'verdict'), [(True, 'ALLOWED'), (False, 'REFUSED')])
    async def test_an_allowed_view_asks_to_act(self, act: bool, verdict: str) -> None:
        policy = RecordingPolicy(view=True, act=act)

        grant = await ask_policy('VIEW_AND_ACT', policy, bare_request())

        assert (grant.view, grant.act) == ('ALLOWED', verdict)
        assert policy.calls == ['can_view', 'can_act']


class TestBoundedBody:
    """The app reads at most one byte past the handler's bound."""

    pytestmark = [asyncio_tests]

    @staticmethod
    def request_of(chunks: list[bytes]) -> Request:
        pending = list(chunks)

        async def receive() -> dict[str, Any]:
            match pending:
                case [first, *rest]:
                    pending[:] = rest
                    return {'type': 'http.request', 'body': first, 'more_body': bool(rest)}
                case _:
                    return {'type': 'http.request', 'body': b'', 'more_body': False}

        return Request({'type': 'http', 'method': 'POST', 'path': '/', 'headers': []}, receive)

    async def test_a_body_within_the_bound_is_read_whole(self) -> None:
        body = await read_bounded_body(self.request_of([b'{"a":', b'1}']), 16)

        assert body == b'{"a":1}'

    async def test_a_body_over_the_bound_stops_one_byte_past_it(self) -> None:
        body = await read_bounded_body(self.request_of([b'x' * 10, b'y' * 10, b'z' * 10]), 12)

        assert body == b'x' * 10 + b'yyy'


REFUSED_VIEW_PATHS: Final = (
    ('GET', '/api/meta'),
    ('GET', '/api/tasks'),
    ('GET', '/api/tasks/stats'),
    ('GET', '/api/tasks/00000000-0000-4000-8000-000000000001'),
    ('GET', '/api/workflows'),
    ('GET', '/api/workflows/00000000-0000-4000-8000-000000000002/tasks/0'),
    ('GET', '/api/workers'),
    ('GET', '/api/workers/ping'),
    ('GET', '/api/workers/w1/history'),
    ('GET', '/api/events'),
    ('POST', '/api/tasks/00000000-0000-4000-8000-000000000001/cancel'),
    ('POST', '/api/workflows/00000000-0000-4000-8000-000000000002/resume'),
)

ACTION_PATHS: Final = (
    '/api/tasks/00000000-0000-4000-8000-000000000001/cancel',
    '/api/workflows/00000000-0000-4000-8000-000000000002/pause',
    '/api/workflows/00000000-0000-4000-8000-000000000002/resume',
    '/api/workflows/00000000-0000-4000-8000-000000000002/cancel',
)


NOT_AUTHORIZED: Final = {'detail': 'Not authorized.'}

READ_REFUSALS: Final = (
    (
        'GET',
        '/api/tasks/not-a-uuid',
        400,
        {
            'code': 'INVALID_INPUT',
            'detail': (
                'Path parameter task_id: Input should be a valid UUID, '
                'invalid character: found `n` at 1.'
            ),
        },
        ('can_view',),
    ),
    (
        'GET',
        '/api/workflows/00000000-0000-4000-8000-000000000002/tasks/2147483648',
        404,
        {'detail': 'Workflow task not found.'},
        ('can_view',),
    ),
    (
        'GET',
        '/api/workflows/00000000-0000-4000-8000-000000000002/tasks/x',
        422,
        {
            'detail': [
                {
                    'type': 'int_parsing',
                    'loc': ['path', 'task_index'],
                    'msg': 'Input should be a valid integer, unable to parse string as an integer',
                    'input': 'x',
                },
            ],
        },
        ('can_view',),
    ),
    (
        'GET',
        '/api/tasks?offset=500&limit=1',
        400,
        {'detail': 'offset + limit must be <= 500; got 501'},
        ('can_view',),
    ),
    (
        'GET',
        '/api/tasks?limit=0',
        422,
        {
            'detail': [
                {
                    'type': 'greater_than_equal',
                    'loc': ['query', 'limit'],
                    'msg': 'Input should be greater than or equal to 1',
                    'input': '0',
                    'ctx': {'ge': 1},
                },
            ],
        },
        ('can_view',),
    ),
    (
        'GET',
        '/api/tasks?status=NOT_A_STATUS',
        400,
        {'detail': "Unknown status 'NOT_A_STATUS'."},
        ('can_view',),
    ),
    (
        'GET',
        '/api/workers/ping?timeout_seconds=99',
        422,
        {
            'detail': [
                {
                    'type': 'less_than_equal',
                    'loc': ['query', 'timeout_seconds'],
                    'msg': 'Input should be less than or equal to 10',
                    'input': '99',
                    'ctx': {'le': 10.0},
                },
            ],
        },
        ('can_view',),
    ),
    ('GET', '/api/unknown', 404, {'detail': 'Not found.'}, ()),
    ('GET', '/api', 404, {'detail': 'Not found.'}, ()),
)
"""Requests that the Python app refused before any read: the method, the
path, and Python's status, body and policy calls."""


class TestTheCoreAnswersAsPythonDid:
    """Requests answered before any read: the core's handler gives the status
    and the body of Python's app, after the same policy calls."""

    pytestmark = [asyncio_tests]

    @pytest.mark.parametrize(('method', 'path'), REFUSED_VIEW_PATHS)
    async def test_a_refused_view(self, method: str, path: str) -> None:
        core = await answer_of(method, path, view=False, act=True)

        assert core == Answer(status=403, body=NOT_AUTHORIZED, calls=('can_view',))

    @pytest.mark.parametrize('path', ACTION_PATHS)
    async def test_a_refused_act(self, path: str) -> None:
        core = await answer_of('POST', path, view=True, act=False)

        assert core == Answer(
            status=403, body=NOT_AUTHORIZED, calls=('can_view', 'can_act'),
        )

    @pytest.mark.parametrize('path', ACTION_PATHS)
    async def test_an_action_while_actions_are_disabled(self, path: str) -> None:
        core = await answer_of('POST', path, view=True, act=True, actions=Actions.DISABLED)

        assert core == Answer(status=403, body=NOT_AUTHORIZED, calls=('can_view',))

    @pytest.mark.parametrize(
        ('method', 'path', 'status', 'body', 'calls'),
        READ_REFUSALS,
        ids=[f'{method}-{path}' for method, path, *_ in READ_REFUSALS],
    )
    async def test_a_request_refused_before_any_read(
        self,
        method: str,
        path: str,
        status: int,
        body: object,
        calls: tuple[str, ...],
    ) -> None:
        core = await answer_of(method, path, view=True, act=True)

        assert core == Answer(status=status, body=body, calls=calls)


_INJECTED: Final = re.compile(
    r'<base href="[^"]*"><script>window\.__HORSIES_UI__ = [^<]*</script>'
    r'(<link rel="stylesheet" href="[^"]*">)?</head>',
)


PYTHON_INJECTED_BLOCK: Final = (
    '<base href="/monitoring/"><script>window.__HORSIES_UI__ = '
    '{"basePath": "/monitoring", "apiBase": "/monitoring/api"}</script>'
    '<link rel="stylesheet" href="/static/theme.css"></head>'
)
"""The block that the Python app injected into the page under the mount
``/monitoring``, with the stylesheet ``/static/theme.css``."""


async def injected_block(path: str) -> tuple[int, str, str]:
    """The status, the content type and the injected block of the page,
    under the mount ``/monitoring``."""
    host = FastAPI()
    built = create_monitoring_app(
        make_horsies_app(),
        authorization_policy=RecordingPolicy(view=False, act=False),
        ui=MonitoringUIConfig(custom_css_url='/static/theme.css'),
    )
    host.mount('/monitoring', built)
    try:
        async with AsyncClient(transport=ASGITransport(app=host), base_url='http://test') as client:
            response = await client.get(path)
    finally:
        await built.state.events.close()
    found = _INJECTED.search(response.text)
    return (
        response.status_code,
        response.headers['content-type'],
        found.group(0) if found is not None else '',
    )


class TestThePage:
    """The page carries no data and needs no verdict; its injected base path,
    config and stylesheet link are Python's."""

    pytestmark = [asyncio_tests]

    @pytest.mark.parametrize('path', ['/monitoring/', '/monitoring/tasks/some/client/route'])
    async def test_the_injected_block_is_pythons(self, path: str) -> None:
        core = await injected_block(path)

        assert core == (200, 'text/html; charset=utf-8', PYTHON_INJECTED_BLOCK)


class TestFleetMetrics:
    """`GET /metrics` is the fleet text of the core's handler behind the view
    verdict; its label names come from the app config."""

    pytestmark = [asyncio_tests]

    async def test_a_refused_view_is_403(self) -> None:
        core = await answer_of('GET', '/metrics', view=False, act=True)

        assert core == Answer(status=403, body=NOT_AUTHORIZED, calls=('can_view',))

    async def test_an_allowed_view_is_the_fleet_text(self) -> None:
        core = await answer_of('GET', '/metrics', view=True, act=False)

        assert core.status == 200
        assert core.calls == ('can_view',)
        assert isinstance(core.body, str)
        assert '# TYPE horsies_queue_tasks gauge\n' in core.body


class TestFleetMetricLabels:
    """A queue name that cannot be a label value of its own does not stop the
    build: the core counts its samples in the `(unknown)` series."""

    def test_a_queue_name_that_is_the_fallback_text_is_not_refused(self) -> None:
        app = Horsies(
            AppConfig(
                broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)),
                queue_mode=QueueMode.CUSTOM,
                custom_queues=[CustomQueueConfig(name='(unknown)')],
            )
        )

        mounted = create_monitoring_app(app, authorization_policy=AllowAll())

        assert isinstance(mounted, FastAPI)
