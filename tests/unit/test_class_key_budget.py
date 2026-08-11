"""The class-key budget matches the names actually built from a key.

`MAX_RETENTION_CLASS_KEY_LENGTH` is written as arithmetic over suffix
lengths, which is a copy of what the name builders do. These tests close
that gap: they call the real builders, so changing a suffix without
changing the constant fails here instead of drifting until a deployment
meets it.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from horsies.core.history.ddl.classes import finite_class_parent_name
from horsies.core.history.names import (
    MAX_RETENTION_CLASS_KEY_LENGTH,
    POSTGRES_IDENTIFIER_LIMIT,
)
from horsies.core.history.partitions.catalog import (
    daily_leaf_name,
    leaf_enqueued_index_name,
    leaf_id_index_name,
)

_ANCHOR = datetime(2026, 8, 11, tzinfo=UTC)


def _longest_name_for(key: str) -> str:
    """The longest relation name a class key produces, via the builders."""
    parent = finite_class_parent_name(key)
    leaf = daily_leaf_name(parent, _ANCHOR)
    return max(
        (parent, leaf, leaf_id_index_name(leaf), leaf_enqueued_index_name(leaf)),
        key=len,
    )


def test_a_key_at_the_budget_fits_every_derived_name() -> None:
    """The constant is not conservative by luck — the limit is exact."""
    longest = _longest_name_for('k' * MAX_RETENTION_CLASS_KEY_LENGTH)

    assert len(longest) == POSTGRES_IDENTIFIER_LIMIT


def test_one_character_past_the_budget_overflows() -> None:
    """A looser bound would not be safe: the next key up already breaks."""
    longest = _longest_name_for('k' * (MAX_RETENTION_CLASS_KEY_LENGTH + 1))

    assert len(longest) > POSTGRES_IDENTIFIER_LIMIT


@pytest.mark.parametrize('key_length', [30, 31])
def test_both_leaf_indexes_collide_once_truncated(key_length: int) -> None:
    """The band the property probe cannot see.

    These keys build a valid leaf, so nothing refuses them, but both of
    its index names truncate to the same bytes and the second CREATE
    INDEX would fail on a duplicate relation name.
    """
    parent = finite_class_parent_name('k' * key_length)
    leaf = daily_leaf_name(parent, _ANCHOR)

    truncated_id = leaf_id_index_name(leaf)[:POSTGRES_IDENTIFIER_LIMIT]
    truncated_enqueued = leaf_enqueued_index_name(leaf)[
        :POSTGRES_IDENTIFIER_LIMIT
    ]

    assert truncated_id == truncated_enqueued
    assert key_length > MAX_RETENTION_CLASS_KEY_LENGTH, (
        'the budget must already refuse this key'
    )
