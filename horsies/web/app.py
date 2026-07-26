# pyright: reportUnusedFunction=false
# Route handlers are registered by their decorator, not called by name.
"""FastAPI application factory for the monitoring UI.

The app is a thin HTTP skin: every read goes through ``horsies.monitoring``
and every action through its task or workflow primitive. What lives here is
transport — authorization enforcement, status-code mapping, the event stream,
and serving the single-page app.

Mounting anywhere works because the SPA is told its own base path per request
from the ASGI ``root_path`` rather than at build time.
"""

from __future__ import annotations

import json
import os
import re
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel

from horsies import __version__
from horsies.core.app import Horsies
from horsies.core.logging import get_logger
from horsies.core.utils.url import to_psycopg_url
from horsies.web.auth import MonitoringAuthPolicy, act_guard, view_guard
from horsies.web.events import EventBroadcaster
from horsies.web.routes import actions, events, tasks, workers, workflows
from horsies.web.schema import (
    SCHEMA_INCOMPATIBLE,
    SCHEMA_UNKNOWN,
    SchemaIncompatible,
    SchemaProbe,
    SchemaState,
    schema_guard,
)

STATIC_DIR = os.path.join(os.path.dirname(__file__), 'static')
INDEX_FILE = 'index.html'

ASSETS_MISSING_DETAIL = (
    'horsies web UI assets are not built. '
    'Run: cd webui && bun install && bun run build'
)

_HEAD_CLOSE = re.compile(r'</head\s*>', re.IGNORECASE)

logger = get_logger('web')


class MonitoringUIConfig(BaseModel):
    """Adopter-facing customization of the served page.

    ``custom_css_url`` is injected after everything else, so a stylesheet it
    points at overrides any design token the app ships. Overriding CSS custom
    properties is the entire customization contract — there is no build-time
    theming.
    """

    custom_css_url: str | None = None


class MetaResponse(BaseModel):
    """What the SPA needs on boot to decide what to render.

    ``actions_enabled`` is the server's verdict — the deployment's own switch
    ANDed with schema compatibility — while ``can_act`` is this caller's
    policy verdict. A schema the tool cannot write through forces actions off
    for everyone and names itself in ``actions_disabled_reason``.
    """

    horsies_version: str
    base_path: str
    actions_enabled: bool
    can_act: bool
    schema_version: int | None
    expected_schema_version: int
    schema_compatible: bool
    actions_disabled_reason: str | None


def _disabled_reason(state: SchemaState) -> str | None:
    """Why actions are off, distinguishing an unknown schema from a foreign one."""
    match state:
        case SchemaState.MATCH:
            return None
        case SchemaState.UNKNOWN:
            return SCHEMA_UNKNOWN
        case _:
            return SCHEMA_INCOMPATIBLE


def _ui_config(root_path: str) -> dict[str, str]:
    """The runtime config injected into the served page."""
    return {
        'basePath': root_path or '/',
        'apiBase': f'{root_path}/api',
    }


def _read_index() -> str | None:
    """Read the built page once, or None when the build is absent."""
    path = os.path.join(STATIC_DIR, INDEX_FILE)
    if not os.path.isfile(path):
        return None
    with open(path, encoding='utf-8') as handle:
        return handle.read()


def _base_href(root_path: str) -> str:
    """The directory every relative asset URL resolves against.

    The SPA builds with a relative base, so a deep link served at a path
    without a trailing slash would otherwise resolve its assets against the
    wrong directory. The trailing slash is what makes this a directory.
    """
    return f'{root_path}/' if root_path else '/'


def _inject(index_html: str, root_path: str, custom_css_url: str | None) -> str:
    """Put the base path, runtime config, and any adopter stylesheet into the page."""
    block = (
        f'<base href="{_base_href(root_path)}">'
        f'<script>window.__HORSIES_UI__ = '
        f'{json.dumps(_ui_config(root_path))}</script>'
    )
    if custom_css_url is not None:
        block += f'<link rel="stylesheet" href="{custom_css_url}">'

    replaced, count = _HEAD_CLOSE.subn(f'{block}</head>', index_html, count=1)
    return replaced if count else index_html + block


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
    """
    broker = app.get_broker()
    ui_config = config or MonitoringUIConfig()
    schema_probe = SchemaProbe(broker)
    broadcaster = EventBroadcaster(
        to_psycopg_url(broker.config.effective_session_database_url)
    )
    index_html = _read_index()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
        yield
        await broadcaster.close()

    monitoring_app = FastAPI(
        title='horsies monitoring',
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    # Exposed so a host process (or a test) can shut the event layer down
    # without waiting for the ASGI lifespan to run.
    monitoring_app.state.events = broadcaster

    view_only = Depends(view_guard(auth_policy))
    may_act = Depends(act_guard(auth_policy, actions_enabled=actions_enabled))

    @monitoring_app.exception_handler(SchemaIncompatible)
    async def _on_schema_incompatible(request: Request, exc: Exception) -> JSONResponse:
        """Render the action refusal a foreign schema produces."""
        detail = exc.detail if isinstance(exc, SchemaIncompatible) else str(exc)
        code = exc.code if isinstance(exc, SchemaIncompatible) else SCHEMA_INCOMPATIBLE
        logger.warning(
            f'monitoring action refused path={request.url.path} '
            f'http=409 result={code}'
        )
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={'code': code, 'detail': detail},
        )

    @monitoring_app.get('/api/meta', dependencies=[view_only])
    async def read_meta(request: Request) -> MetaResponse:
        """Version, schema verdict, and this caller's action verdict."""
        schema = await schema_probe.status()
        return MetaResponse(
            horsies_version=__version__,
            base_path=request.scope.get('root_path', '') or '/',
            actions_enabled=actions_enabled and schema.compatible,
            can_act=await auth_policy.can_act(request),
            schema_version=schema.version,
            expected_schema_version=schema.expected_version,
            schema_compatible=schema.compatible,
            actions_disabled_reason=_disabled_reason(schema.state),
        )

    monitoring_app.include_router(
        tasks.build_router(broker), prefix='/api', dependencies=[view_only]
    )
    monitoring_app.include_router(
        workflows.build_router(broker), prefix='/api', dependencies=[view_only]
    )
    monitoring_app.include_router(
        workers.build_router(app, broker), prefix='/api', dependencies=[view_only]
    )
    monitoring_app.include_router(
        events.build_router(broadcaster), prefix='/api', dependencies=[view_only]
    )
    # The schema gate sits after the authorization gates: an unauthorized
    # caller learns nothing about the database, and an authorized one is
    # refused regardless of what its policy permits.
    monitoring_app.include_router(
        actions.build_router(broker),
        prefix='/api',
        dependencies=[view_only, may_act, Depends(schema_guard(schema_probe))],
    )

    # Registered last so it never shadows an API route. Static assets carry no
    # data and are served without the authorization dependency; an
    # unauthorized viewer receives the shell and then the not-authorized state
    # its first /api/meta call produces.
    @monitoring_app.get('/{full_path:path}', include_in_schema=False)
    async def serve_spa(request: Request, full_path: str) -> Any:
        """Serve a built asset, or the page itself for any client-side route."""
        if full_path == 'api' or full_path.startswith('api/'):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail='Not found.'
            )
        if index_html is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={'detail': ASSETS_MISSING_DETAIL},
            )

        asset = _resolve_asset(full_path)
        if asset is not None:
            return FileResponse(asset)
        return HTMLResponse(
            _inject(
                index_html,
                request.scope.get('root_path', ''),
                ui_config.custom_css_url,
            )
        )

    return monitoring_app


def _resolve_asset(relative_path: str) -> str | None:
    """Resolve a request path to a built file inside the static tree.

    Returns None for the page itself and for anything that is not a real file
    under the static directory, including paths that try to escape it.
    """
    if not relative_path or relative_path == INDEX_FILE:
        return None
    candidate = os.path.realpath(os.path.join(STATIC_DIR, relative_path))
    root = os.path.realpath(STATIC_DIR)
    if not candidate.startswith(root + os.sep):
        return None
    return candidate if os.path.isfile(candidate) else None
