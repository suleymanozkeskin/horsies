"""Authorization policies for the monitoring app.

horsies never owns identity. A deployment supplies a policy that answers two
questions per request — may this caller read monitoring data, and may it act —
and the app enforces the answer on every ``/api`` route. The three policies
here cover the deployment shapes horsies documents; anything else is the
adopter's to implement against ``MonitoringAuthPolicy``.

Mutating requests carry a second, non-identity check: the
``X-Horsies-Intent: action`` header. It is a CSRF guard, not authentication —
a browser cannot attach it to a cross-site form post.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Protocol

from fastapi import HTTPException, Request, status

# Mutating requests must opt in explicitly. A cross-site form post cannot set
# a custom header, so its absence is treated as an untrusted request.
INTENT_HEADER = 'X-Horsies-Intent'
INTENT_VALUE = 'action'

_NOT_AUTHORIZED = 'Not authorized.'


class MonitoringAuthPolicy(Protocol):
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

    def __init__(self, header_name: str, *, allow_actions: bool) -> None:
        self.header_name = header_name
        self.allow_actions = allow_actions

    async def can_view(self, request: Request) -> bool:
        return bool((request.headers.get(self.header_name) or '').strip())

    async def can_act(self, request: Request) -> bool:
        return self.allow_actions and await self.can_view(request)


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
