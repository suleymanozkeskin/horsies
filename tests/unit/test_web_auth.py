"""Unit tests for the monitoring authorization policies and guards.

Covers each shipped policy's verdicts and the two dependencies that enforce
them: the read gate on every API route, and the action gate that additionally
demands the intent header.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from horsies.web.auth import (
    INTENT_HEADER,
    INTENT_VALUE,
    AllowAll,
    TrustedHeader,
    ViewOnly,
    act_guard,
    view_guard,
)

pytestmark = [pytest.mark.unit, pytest.mark.asyncio(loop_scope='function')]


def make_request(**headers: str) -> Request:
    """A minimal request carrying the given headers."""
    return Request(
        {
            'type': 'http',
            'method': 'POST',
            'path': '/api/tasks/x/cancel',
            'headers': [
                (key.lower().encode(), value.encode()) for key, value in headers.items()
            ],
        }
    )


class TestAllowAll:
    """Everything is authorized; the deployment vouched for the caller."""

    async def test_permits_reads_and_actions(self) -> None:
        policy = AllowAll()

        assert await policy.can_view(make_request()) is True
        assert await policy.can_act(make_request()) is True


class TestViewOnly:
    """Reads pass, actions never do."""

    async def test_permits_reads_and_refuses_actions(self) -> None:
        policy = ViewOnly()

        assert await policy.can_view(make_request()) is True
        assert await policy.can_act(make_request()) is False


class TestTrustedHeader:
    """Identity is whatever the proxy asserts in the configured header."""

    async def test_absent_header_is_unauthorized(self) -> None:
        policy = TrustedHeader('X-Forwarded-User', allow_actions=True)

        assert await policy.can_view(make_request()) is False
        assert await policy.can_act(make_request()) is False

    async def test_empty_and_whitespace_headers_are_unauthorized(self) -> None:
        policy = TrustedHeader('X-Forwarded-User', allow_actions=True)

        assert await policy.can_view(make_request(**{'X-Forwarded-User': ''})) is False
        assert (
            await policy.can_view(make_request(**{'X-Forwarded-User': '   '})) is False
        )

    async def test_present_header_authorizes_reads(self) -> None:
        policy = TrustedHeader('X-Forwarded-User', allow_actions=False)

        request = make_request(**{'X-Forwarded-User': 'alex'})

        assert await policy.can_view(request) is True

    async def test_actions_need_both_identity_and_the_flag(self) -> None:
        identified = make_request(**{'X-Forwarded-User': 'alex'})

        assert (
            await TrustedHeader('X-Forwarded-User', allow_actions=False).can_act(
                identified
            )
            is False
        )
        assert (
            await TrustedHeader('X-Forwarded-User', allow_actions=True).can_act(
                identified
            )
            is True
        )

    async def test_header_name_is_matched_case_insensitively(self) -> None:
        """HTTP header names are case-insensitive; the policy must not care."""
        policy = TrustedHeader('X-Forwarded-User', allow_actions=False)

        assert await policy.can_view(make_request(**{'x-forwarded-user': 'a'})) is True


class TestViewGuard:
    """The dependency every API route carries."""

    async def test_passes_an_authorized_request(self) -> None:
        guard = view_guard(AllowAll())

        assert await guard(make_request()) is None

    async def test_rejects_an_unauthorized_request(self) -> None:
        guard = view_guard(TrustedHeader('X-Forwarded-User', allow_actions=False))

        with pytest.raises(HTTPException) as raised:
            await guard(make_request())

        assert raised.value.status_code == 403
        assert raised.value.detail == 'Not authorized.'


class TestActGuard:
    """Actions need authorization and an explicit statement of intent."""

    async def test_passes_when_authorized_and_intent_is_declared(self) -> None:
        guard = act_guard(AllowAll(), actions_enabled=True)

        assert await guard(make_request(**{INTENT_HEADER: INTENT_VALUE})) is None

    async def test_rejects_when_the_policy_refuses(self) -> None:
        guard = act_guard(ViewOnly(), actions_enabled=True)

        with pytest.raises(HTTPException) as raised:
            await guard(make_request(**{INTENT_HEADER: INTENT_VALUE}))

        assert raised.value.status_code == 403
        assert raised.value.detail == 'Not authorized.'

    async def test_rejects_when_actions_are_disabled_server_side(self) -> None:
        guard = act_guard(AllowAll(), actions_enabled=False)

        with pytest.raises(HTTPException) as raised:
            await guard(make_request(**{INTENT_HEADER: INTENT_VALUE}))

        assert raised.value.status_code == 403

    async def test_rejects_a_missing_intent_header(self) -> None:
        guard = act_guard(AllowAll(), actions_enabled=True)

        with pytest.raises(HTTPException) as raised:
            await guard(make_request())

        assert raised.value.status_code == 403
        assert INTENT_HEADER in raised.value.detail

    async def test_rejects_a_wrong_intent_value(self) -> None:
        guard = act_guard(AllowAll(), actions_enabled=True)

        with pytest.raises(HTTPException) as raised:
            await guard(make_request(**{INTENT_HEADER: 'something-else'}))

        assert raised.value.status_code == 403
