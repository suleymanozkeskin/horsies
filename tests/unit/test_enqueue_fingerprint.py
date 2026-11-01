"""`enqueue_fingerprint` text is a stored comparator.

`enqueue_sha` holds this hash, and a resend with the same task id compares
against the stored value. The hash covers `json.dumps` text with the default
`ensure_ascii=True`, so non-ASCII input is hashed in its escaped form. A change
to that text gives a different hash for a stored row, and its resend then
reports a payload mismatch. That is why this function keeps its own
`json.dumps` call and does not use `dumps_canonical_json`.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from horsies.core.utils.fingerprint import enqueue_fingerprint

pytestmark = [pytest.mark.unit]

SENT_AT = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)


def fingerprint_of(kwargs_json: str) -> str:
    return enqueue_fingerprint(
        task_name='billing.charge',
        queue_name='default',
        priority=50,
        args_json=None,
        kwargs_json=kwargs_json,
        sent_at=SENT_AT,
        good_until=None,
        enqueue_delay_seconds=None,
        task_options=None,
    )


class TestPinnedDigests:
    """Wire-stability anchors: each digest was taken before any codec change."""

    def test_ascii_input(self) -> None:
        assert fingerprint_of('{"city":"Bonn"}') == (
            '438a55a330e5ad741568aebcdf06b8367a0e6218eef0b61174f5a39fbeb20dd9'
        )

    def test_non_ascii_input_is_hashed_in_its_escaped_form(self) -> None:
        assert fingerprint_of('{"city":"Köln"}') == (
            '15d17990f9a9feb1f7bfa09d58d682185d33efed35697f6885b7156bb7a15c50'
        )
