# pyright: reportPrivateUsage=false
# These tests deliberately exercise module-private rules and seams.
"""Unit tests for the shell of the monitoring app (``create_monitoring_app``
on the core's monitoring handler): hardening, gating, and serving.

These exercise the app over httpx's ASGI transport without a database. The
few paths that do reach the broker are pointed at a port nothing listens on,
which is how the 503 mapping is proven. The page and its assets are the web
UI build that the core embeds.

Covered: the authorization gate on every API route, the intent-header gate
on actions, unknown-API 404s, page serving with per-request config
injection, and mount-anywhere base paths.

The core's tests of the same rules are in ``core/src/web/tests.rs``.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import uuid

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.web.schema import EXPECTED_SCHEMA_VERSION
from horsies.web import (
    INTENT_HEADER,
    INTENT_VALUE,
    Actions,
    AllowAll,
    MonitoringUIConfig,
    TraceLinkTemplate,
    TrustedHeader,
    ViewOnly,
    create_monitoring_app,
)

pytestmark = [pytest.mark.unit]

# Applied per class: the module also holds synchronous tests.
asyncio_tests = pytest.mark.asyncio(loop_scope='function')

# Nothing listens here, so any route that reaches the database fails fast.
UNREACHABLE_URL = 'postgresql://postgres:none@127.0.0.1:1/none'

ACT_HEADERS = {INTENT_HEADER: INTENT_VALUE}


def make_horsies_app() -> Horsies:
    """A horsies app whose broker can never connect."""
    return Horsies(
        AppConfig(database=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)))
    )


def client_for(app: FastAPI) -> AsyncClient:
    """An httpx client speaking ASGI directly to the app."""
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test')


class TestMeta:
    """What the SPA reads on boot to decide what to render."""

    pytestmark = [asyncio_tests]

    async def test_reports_version_and_capabilities(self) -> None:
        """With no reachable schema the app reports itself read-only."""
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get('/api/meta')

        assert response.status_code == 200
        body = response.json()
        assert body['horsies_version']
        assert body['base_path'] == '/'
        assert body['can_act'] is True
        assert body['schema_version'] is None
        assert body['expected_schema_version'] == EXPECTED_SCHEMA_VERSION
        assert body['schema_compatible'] is False
        assert body['schema_state'] == 'UNKNOWN'
        assert body['actions'] == 'DISABLED'
        assert body['actions_disabled_reason'] == 'SCHEMA_UNKNOWN'
        assert body['trace_link_template'] is None

    async def test_reports_the_trace_link_template(self) -> None:
        template = 'https://grafana.example.com/explore?traceId={trace_id}'
        app = create_monitoring_app(
            make_horsies_app(),
            authorization_policy=AllowAll(),
            ui=MonitoringUIConfig(trace_link=TraceLinkTemplate(template=template)),
        )

        async with client_for(app) as client:
            body = (await client.get('/api/meta')).json()

        assert body['trace_link_template'] == template


    async def test_view_only_deployment_reports_no_action_capability(self) -> None:
        app = create_monitoring_app(
            make_horsies_app(), authorization_policy=ViewOnly(), actions=Actions.DISABLED
        )

        async with client_for(app) as client:
            body = (await client.get('/api/meta')).json()

        assert body['actions'] == 'DISABLED'
        assert body['can_act'] is False

    async def test_meta_itself_requires_authorization(self) -> None:
        """The SPA's not-authorized screen is driven by this 403."""
        app = create_monitoring_app(
            make_horsies_app(),
            authorization_policy=TrustedHeader('X-Forwarded-User', actions=Actions.DISABLED),
        )

        async with client_for(app) as client:
            response = await client.get('/api/meta')

        assert response.status_code == 403
        assert response.json() == {'detail': 'Not authorized.'}


class TestAuthorizationGate:
    """Every API route is gated; static assets are not."""

    pytestmark = [asyncio_tests]

    @pytest.mark.parametrize(
        'path',
        [
            '/api/meta',
            '/api/tasks',
            '/api/tasks/status-counts',
            '/api/tasks/facets',
            '/api/tasks/breakdown',
            '/api/tasks/00000000-0000-4000-8000-000000000001',
            '/api/workflows',
            '/api/workflows/names',
            '/api/workflows/00000000-0000-4000-8000-000000000002',
            '/api/workflows/00000000-0000-4000-8000-000000000002/tasks/0',
            '/api/workers',
            '/api/workers/ping',
            '/api/workers/schedules',
            '/api/workers/w1/history',
            '/api/events',
        ],
    )
    async def test_unauthorized_reads_are_refused(self, path: str) -> None:
        app = create_monitoring_app(
            make_horsies_app(),
            authorization_policy=TrustedHeader('X-Forwarded-User', actions=Actions.DISABLED),
        )

        async with client_for(app) as client:
            response = await client.get(path)

        assert response.status_code == 403

    @pytest.mark.parametrize(
        'path',
        [
            '/api/tasks/some-id/cancel',
            '/api/workflows/run-id/cancel',
            '/api/workflows/run-id/pause',
            '/api/workflows/run-id/resume',
        ],
    )
    async def test_actions_refuse_a_view_only_policy(self, path: str) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=ViewOnly())

        async with client_for(app) as client:
            response = await client.post(path, json={}, headers=ACT_HEADERS)

        assert response.status_code == 403

    @pytest.mark.parametrize(
        'path',
        [
            '/api/tasks/some-id/cancel',
            '/api/workflows/run-id/cancel',
            '/api/workflows/run-id/pause',
            '/api/workflows/run-id/resume',
        ],
    )
    async def test_actions_refuse_a_missing_intent_header(self, path: str) -> None:
        """A cross-site form post cannot set a custom header."""
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.post(path, json={})

        assert response.status_code == 403

    async def test_actions_refuse_when_disabled_server_side(self) -> None:
        app = create_monitoring_app(
            make_horsies_app(), authorization_policy=AllowAll(), actions=Actions.DISABLED
        )

        async with client_for(app) as client:
            response = await client.post(
                '/api/tasks/some-id/cancel', json={}, headers=ACT_HEADERS
            )

        assert response.status_code == 403

    async def test_static_assets_are_not_gated(self) -> None:
        """An unauthorized viewer gets the shell, then meta's 403 inside it."""
        app = create_monitoring_app(
            make_horsies_app(),
            authorization_policy=TrustedHeader('X-Forwarded-User', actions=Actions.DISABLED),
        )

        async with client_for(app) as client:
            response = await client.get('/')

        assert response.status_code == 200
        assert 'text/html' in response.headers['content-type']


class TestUnreachableDatabase:
    """A failed dependency is 503, and the message names the surface."""

    pytestmark = [asyncio_tests]

    @pytest.mark.parametrize(
        'path,surface',
        [
            ('/api/tasks', 'Task list'),
            ('/api/tasks/status-counts', 'Task stats'),
            ('/api/tasks/facets', 'Task facets'),
            ('/api/tasks/breakdown', 'Task breakdown'),
            (
                '/api/tasks/00000000-0000-4000-8000-000000000001',
                'Task detail',
            ),
            ('/api/workflows', 'Workflow runs'),
            ('/api/workflows/names', 'Workflow names'),
            (
                '/api/workflows/00000000-0000-4000-8000-000000000002',
                'Workflow run',
            ),
            (
                '/api/workflows/00000000-0000-4000-8000-000000000002/tasks/0',
                'Workflow task',
            ),
            ('/api/workers/schedules', 'Schedule state'),
        ],
    )
    async def test_read_routes_report_service_unavailable(
        self, path: str, surface: str
    ) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get(path)

        assert response.status_code == 503
        assert response.json()['detail'].startswith(f'{surface} query failed:')


class TestRequestValidation:
    """Bad column names are 400; bad shapes are FastAPI's 422."""

    pytestmark = [asyncio_tests]

    @pytest.mark.parametrize(
        'path',
        [
            '/api/tasks?status=NOT_A_STATUS',
            '/api/tasks?sort_field=drop_table',
            '/api/tasks/breakdown?group_by=nonsense',
            '/api/tasks/facets?status=NOPE',
        ],
    )
    async def test_unknown_allowlist_values_are_bad_requests(self, path: str) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get(path)

        assert response.status_code == 400

    @pytest.mark.parametrize(
        'path',
        [
            '/api/tasks?sort_direction=sideways',
            '/api/tasks?limit=0',
            '/api/tasks?limit=201',
            '/api/tasks?offset=-1',
            '/api/tasks/breakdown?limit=501',
            '/api/workflows?limit=201',
            '/api/workers/ping?timeout_seconds=99',
            '/api/workers/w1/history?limit=1001',
        ],
    )
    async def test_out_of_range_values_are_unprocessable(self, path: str) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get(path)

        assert response.status_code == 422


    @pytest.mark.parametrize('node_index', ['2147483648', '-2147483649', '1' * 40])
    async def test_a_node_index_outside_the_integer_column_is_not_found(
        self, node_index: str,
    ) -> None:
        """An index that names no node is 404 with no read: with an
        unreachable database a read would answer 503."""
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get(
                f'/api/workflows/{uuid.uuid4()}/tasks/{node_index}',
            )

        assert response.status_code == 404
        assert response.json() == {'detail': 'Workflow task not found.'}

    async def test_task_page_reach_past_five_hundred_is_rejected(self) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get('/api/tasks?offset=500&limit=1')

        assert response.status_code == 400
        assert 'offset + limit must be <= 500' in response.json()['detail']

    @pytest.mark.parametrize(
        ('path', 'code'),
        [
            ('/api/tasks?limit=201', 422),
            ('/api/tasks?limit=0', 422),
            ('/api/tasks?offset=-1', 422),
            ('/api/tasks/breakdown?limit=501', 422),
            ('/api/workflows?limit=201', 422),
        ],
    )
    async def test_http_page_bounds_keep_their_answer(
        self, path: str, code: int,
    ) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get(path)

        assert response.status_code == code


class TestStaticServing:
    """The SPA shell, its assets, and the config injected into the page."""

    pytestmark = [asyncio_tests]

    async def test_index_is_served_with_the_runtime_config(self) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get('/')

        assert response.status_code == 200
        assert 'window.__HORSIES_UI__' in response.text
        assert json.dumps({'basePath': '/', 'apiBase': '/api'}) in response.text

    async def test_a_base_href_is_injected(self) -> None:
        """Relative asset URLs need a directory to resolve against."""
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            body = (await client.get('/')).text

        assert '<base href="/">' in body

    async def test_a_deep_link_without_a_trailing_slash_still_gets_the_base(
        self,
    ) -> None:
        """The case the tag exists for: assets must not resolve against /workflows."""
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            body = (await client.get('/workflows/some-run-id')).text

        assert '<base href="/">' in body

    async def test_the_base_href_precedes_any_asset_reference(self) -> None:
        """A base tag only governs URLs that appear after it."""
        app = create_monitoring_app(
            make_horsies_app(),
            authorization_policy=AllowAll(),
            ui=MonitoringUIConfig(custom_css_url='brand/override.css'),
        )

        async with client_for(app) as client:
            body = (await client.get('/')).text

        assert body.index('<base href=') < body.index('override.css')

    async def test_config_is_injected_inside_head(self) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            body = (await client.get('/')).text

        assert body.index('window.__HORSIES_UI__') < body.index('</head>')


    async def test_client_side_routes_fall_back_to_the_page(self) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get('/workflows?run=abc')

        assert response.status_code == 200
        assert 'window.__HORSIES_UI__' in response.text

    async def test_unknown_api_paths_are_not_swallowed_by_the_spa(self) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get('/api/does-not-exist')

        assert response.status_code == 404
        assert 'window.__HORSIES_UI__' not in response.text

    async def test_paths_cannot_escape_the_static_tree(self) -> None:
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get('/../../pyproject.toml')

        assert response.status_code == 200
        assert 'window.__HORSIES_UI__' in response.text
        assert '[build-system]' not in response.text

    async def test_custom_css_is_injected_after_the_config(self) -> None:
        """Adopter CSS lands last so it overrides any shipped token."""
        app = create_monitoring_app(
            make_horsies_app(),
            authorization_policy=AllowAll(),
            ui=MonitoringUIConfig(custom_css_url='/brand/override.css'),
        )

        async with client_for(app) as client:
            body = (await client.get('/')).text

        assert '/brand/override.css' in body
        assert body.index('window.__HORSIES_UI__') < body.index('override.css')
        assert body.index('override.css') < body.index('</head>')


class TestMountedAnywhere:
    """The page learns its own base path per request, not at build time."""

    pytestmark = [asyncio_tests]

    async def test_mounted_app_reports_its_mount_path(self) -> None:
        host = FastAPI()
        host.mount(
            '/monitoring',
            create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll()),
        )

        async with client_for(host) as client:
            page = await client.get('/monitoring/')
            meta = await client.get('/monitoring/api/meta')

        assert (
            json.dumps({'basePath': '/monitoring', 'apiBase': '/monitoring/api'})
            in page.text
        )
        assert meta.json()['base_path'] == '/monitoring'

    async def test_mounted_api_routes_answer_under_the_mount(self) -> None:
        host = FastAPI()
        host.mount(
            '/monitoring',
            create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll()),
        )

        async with client_for(host) as client:
            response = await client.get('/monitoring/api/tasks/status-counts')

        assert response.status_code == 503


class TestSchemaGate:
    """A database whose schema this build cannot verify refuses every action."""

    pytestmark = [asyncio_tests]

    @pytest.mark.parametrize(
        'path',
        [
            '/api/tasks/some-id/cancel',
            '/api/workflows/run-id/cancel',
            '/api/workflows/run-id/pause',
            '/api/workflows/run-id/resume',
        ],
    )
    async def test_unverifiable_schema_refuses_actions_regardless_of_policy(
        self, path: str
    ) -> None:
        """An unreachable database is reported as unknown, never as absent."""
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.post(path, json={}, headers=ACT_HEADERS)

        assert response.status_code == 409
        body = response.json()
        assert body['code'] == 'SCHEMA_UNKNOWN'
        assert 'Cannot reach the database' in body['detail']
        assert 'no horsies schema' not in body['detail']

    async def test_reads_are_still_attempted(self) -> None:
        """Only writes are gated; reads fail on their own terms."""
        app = create_monitoring_app(make_horsies_app(), authorization_policy=AllowAll())

        async with client_for(app) as client:
            response = await client.get('/api/tasks')

        assert response.status_code == 503

    async def test_authorization_is_checked_before_the_schema(self) -> None:
        """An unauthorized caller learns nothing about the database."""
        app = create_monitoring_app(make_horsies_app(), authorization_policy=ViewOnly())

        async with client_for(app) as client:
            response = await client.post(
                '/api/tasks/some-id/cancel', json={}, headers=ACT_HEADERS
            )

        assert response.status_code == 403


def test_missing_extra_raises_a_clear_import_error() -> None:
    """Importing the package without fastapi must name the extra to install.

    Run in a subprocess with fastapi blocked, because this process has it.
    """
    program = textwrap.dedent(
        """
        import sys

        class Blocker:
            def find_module(self, name, path=None):
                return None
            def find_spec(self, name, path=None, target=None):
                if name == 'fastapi' or name.startswith('fastapi.'):
                    raise ImportError('blocked for test')
                return None

        sys.modules.pop('fastapi', None)
        sys.meta_path.insert(0, Blocker())
        try:
            import horsies.web
        except ImportError as exc:
            print(str(exc))
        else:
            print('NO ERROR RAISED')
        """
    )

    completed = subprocess.run(
        [sys.executable, '-c', program],
        capture_output=True,
        text=True,
        check=False,
    )

    assert "pip install 'horsies[web]'" in completed.stdout
    assert 'NO ERROR RAISED' not in completed.stdout


def test_monitoring_package_does_not_import_the_web_layer() -> None:
    """horsies.monitoring must stay usable without the extra."""
    completed = subprocess.run(
        [
            sys.executable,
            '-c',
            'import horsies.monitoring, sys; ' "print('horsies.web' in sys.modules)",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.stdout.strip() == 'False'


