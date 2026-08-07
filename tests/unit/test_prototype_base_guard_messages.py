"""The base-version guard must refuse with a version a reader can act on.

The guard is designed to be re-litigated at every base move, so its messages
move too. They previously did not: the constant advanced to 30 while the two
refusals still read v27 and v26 — and they disagreed with each other, so they
had never been updated together. A guard that refuses correctly and then names
a version nobody can act on is right about the outcome and wrong in what it
reports.

Deriving the version from the constant fixes it structurally. This pins that
derivation so a hardcoded literal cannot creep back in.
"""

from __future__ import annotations

import re

from tests.task_history_prototypes import schema


def _refusal_messages() -> list[str]:
    source = re.sub(r'\s+', ' ', _guard_source())
    return re.findall(r"raise RuntimeError\((.*?)\)", source)


def _guard_source() -> str:
    from pathlib import Path

    return Path(schema.__file__).read_text()


class TestRefusalMessagesTrackTheConstant:
    def test_no_hardcoded_schema_version_literal_remains(self) -> None:
        # The exact regression: 'schema v27 program' / 'at schema v26' while
        # the constant said 30.
        assert re.findall(r"schema v\d+", _guard_source()) == []

    def test_both_refusals_interpolate_the_expected_constant(self) -> None:
        messages = _refusal_messages()
        version_refusals = [
            m for m in messages if 'task-history prototypes require' in m
        ]

        assert len(version_refusals) == 2
        for message in version_refusals:
            assert '_EXPECTED_BASE_SCHEMA_VERSION' in message

    def test_both_refusals_report_what_was_found(self) -> None:
        # A refusal that states only the requirement makes the reader go
        # looking for the actual value; stating both makes it actionable.
        messages = _refusal_messages()
        version_refusals = [
            m for m in messages if 'task-history prototypes require' in m
        ]

        for message in version_refusals:
            assert 'found v' in message

    def test_the_constant_is_a_plain_integer(self) -> None:
        # Guards against the guard being widened to a range, which the
        # comment at the site rules out explicitly. Read from source rather
        # than imported: the name is private to its module, and a static
        # check is what a widening would have to get past anyway.
        assignment = re.search(
            r'_EXPECTED_BASE_SCHEMA_VERSION\s*=\s*(.+)', _guard_source()
        )

        assert assignment is not None
        assert assignment.group(1).strip().isdigit()
