"""Authorization policies for the monitoring app.

horsies never owns identity. A deployment supplies a policy that answers two
questions per request — may this caller read monitoring data, and may it act —
and the app enforces the answer on every ``/api`` route. The three policies
here cover the deployment shapes horsies documents; anything else is the
adopter's to implement against ``MonitoringAuthorizationPolicy``.

Mutating requests carry a second, non-identity check: the
``X-Horsies-Intent: action`` header. It is a CSRF guard, not authentication —
a browser cannot attach it to a cross-site form post.
"""

from __future__ import annotations

from enum import Enum
from typing import Protocol, assert_never

from fastapi import Request

# Mutating requests must opt in explicitly. A cross-site form post cannot set
# a custom header, so its absence is treated as an untrusted request.
INTENT_HEADER = 'X-Horsies-Intent'
INTENT_VALUE = 'action'


class Actions(str, Enum):
    """Whether the action routes (task cancel, workflow pause, resume and
    cancel) run. Disabled actions answer 403 without asking the policy's
    ``can_act``. ``GET /api/meta`` reports the value as ``actions``."""

    ENABLED = 'ENABLED'
    DISABLED = 'DISABLED'


class MonitoringAuthorizationPolicy(Protocol):
    """The authorization contract the monitoring app enforces."""

    async def can_view(self, request: Request) -> bool:
        """Whether this request may read monitoring data."""
        ...

    async def can_act(self, request: Request) -> bool:
        """Whether this request may cancel, retry, pause or resume."""
        ...


class AllowAll:
    """Authorize every request.

    For mounts that already sit behind the host application's own
    authentication. Selecting it asserts that something in front of this app
    has authenticated the caller.
    """

    async def can_view(self, request: Request) -> bool:
        return True

    async def can_act(self, request: Request) -> bool:
        return True


class ViewOnly:
    """Authorize reads and refuse every action."""

    async def can_view(self, request: Request) -> bool:
        return True

    async def can_act(self, request: Request) -> bool:
        return False


class TrustedHeader:
    """Authorize a request carrying a non-empty identity header.

    The reverse proxy in front of this app is the identity boundary: it
    authenticates the caller and sets the header.

    SECURITY: the proxy MUST strip or overwrite this header on incoming
    requests. A proxy that forwards a client-supplied header makes this policy
    trivially spoofable. horsies cannot verify that the proxy does so — it is
    the deployment's invariant to uphold.
    """

    def __init__(self, header_name: str, *, actions: Actions) -> None:
        self.header_name = header_name
        self.actions = actions

    async def can_view(self, request: Request) -> bool:
        return bool((request.headers.get(self.header_name) or '').strip())

    async def can_act(self, request: Request) -> bool:
        match self.actions:
            case Actions.ENABLED:
                return await self.can_view(request)
            case Actions.DISABLED:
                return False
            case _ as unreachable:
                assert_never(unreachable)
