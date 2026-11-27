"""Unit tests for the monitoring authorization policies: the verdicts of
each shipped policy. The core's handler enforces them on every API route.
"""

from __future__ import annotations

import pytest
from starlette.requests import Request

from horsies.web.authorization import (
    Actions,
    AllowAll,
    TrustedHeader,
    ViewOnly,
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
        policy = TrustedHeader('X-Forwarded-User', actions=Actions.ENABLED)

        assert await policy.can_view(make_request()) is False
        assert await policy.can_act(make_request()) is False

    async def test_empty_and_whitespace_headers_are_unauthorized(self) -> None:
        policy = TrustedHeader('X-Forwarded-User', actions=Actions.ENABLED)

        assert await policy.can_view(make_request(**{'X-Forwarded-User': ''})) is False
        assert (
            await policy.can_view(make_request(**{'X-Forwarded-User': '   '})) is False
        )

    async def test_blank_comma_member_is_unauthorized(self) -> None:
        """Two copies of the header, one blank, arrive joined as ``alice, ``."""
        policy = TrustedHeader('X-Forwarded-User', actions=Actions.ENABLED)

        for value in ('alice, ', ', alice', 'alice,,bob', ','):
            request = make_request(**{'X-Forwarded-User': value})
            assert await policy.can_view(request) is False
            assert await policy.can_act(request) is False

    async def test_repeated_header_with_a_blank_copy_is_unauthorized(self) -> None:
        policy = TrustedHeader('X-Forwarded-User', actions=Actions.ENABLED)
        request = Request(
            {
                'type': 'http',
                'method': 'GET',
                'path': '/api/meta',
                'headers': [
                    (b'x-forwarded-user', b'alice'),
                    (b'x-forwarded-user', b''),
                ],
            }
        )

        assert await policy.can_view(request) is False

    async def test_comma_members_that_are_all_present_are_authorized(self) -> None:
        policy = TrustedHeader('X-Forwarded-User', actions=Actions.DISABLED)

        request = make_request(**{'X-Forwarded-User': 'alice, bob'})

        assert await policy.can_view(request) is True

    async def test_present_header_authorizes_reads(self) -> None:
        policy = TrustedHeader('X-Forwarded-User', actions=Actions.DISABLED)

        request = make_request(**{'X-Forwarded-User': 'alex'})

        assert await policy.can_view(request) is True

    async def test_actions_need_both_identity_and_the_flag(self) -> None:
        identified = make_request(**{'X-Forwarded-User': 'alex'})

        assert (
            await TrustedHeader('X-Forwarded-User', actions=Actions.DISABLED).can_act(
                identified
            )
            is False
        )
        assert (
            await TrustedHeader('X-Forwarded-User', actions=Actions.ENABLED).can_act(
                identified
            )
            is True
        )

    async def test_header_name_is_matched_case_insensitively(self) -> None:
        """HTTP header names are case-insensitive; the policy must not care."""
        policy = TrustedHeader('X-Forwarded-User', actions=Actions.DISABLED)

        assert await policy.can_view(make_request(**{'x-forwarded-user': 'a'})) is True


