"""The authorization, intent and schema guards of the Python routes before the switch.

FastAPI dependencies of the reference app (``tests/dualrun/reference/web_app.py``).
The core's monitoring handler runs these guards since ``create_monitoring_app``
mounts it. No production code imports them.
"""

from __future__ import annotations

from typing import Awaitable, Callable

from fastapi import HTTPException, Request, status

from horsies.web.auth import INTENT_HEADER, INTENT_VALUE, MonitoringAuthPolicy
from horsies.web.schema import SchemaIncompatible, SchemaProbe

_NOT_AUTHORIZED = 'Not authorized.'


def view_guard(
    policy: MonitoringAuthPolicy,
) -> Callable[[Request], Awaitable[None]]:
    """Build the dependency gating every ``/api`` route."""

    async def guard(request: Request) -> None:
        if not await policy.can_view(request):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=_NOT_AUTHORIZED,
            )

    return guard


def act_guard(
    policy: MonitoringAuthPolicy,
    *,
    actions_enabled: bool,
) -> Callable[[Request], Awaitable[None]]:
    """Build the dependency gating every action route.

    Authorization is checked before intent: a caller who may not act learns
    that, rather than being told to resend with a header that would not help.
    """

    async def guard(request: Request) -> None:
        if not actions_enabled or not await policy.can_act(request):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=_NOT_AUTHORIZED,
            )
        if request.headers.get(INTENT_HEADER) != INTENT_VALUE:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f'Missing {INTENT_HEADER}: {INTENT_VALUE} header.',
            )

    return guard


def schema_guard(probe: SchemaProbe) -> Callable[[], Awaitable[None]]:
    """Build the dependency that refuses actions on a foreign schema.

    Applied to every action route, so a new action cannot be added without
    inheriting the rule.
    """

    async def guard() -> None:
        status = await probe.status()
        if not status.compatible:
            raise SchemaIncompatible(status)

    return guard
