"""Regression tests for mapping key collision detection in serde."""

from __future__ import annotations

import pytest

from horsies.core.codec.serde import dumps_json, to_jsonable
from horsies.core.types.result import is_err


@pytest.mark.unit
class TestMappingKeyCollision:
    """to_jsonable must reject mappings where distinct keys collide after str()."""

    def test_int_and_str_key_collision(self) -> None:
        """int key 1 and str key '1' both become '1' — must error."""
        result = to_jsonable({1: "a", "1": "b"})
        assert is_err(result)
        assert "key collision" in str(result.err_value).lower()
        assert "'1'" in str(result.err_value)

    def test_str_before_int_key_collision(self) -> None:
        """Reverse order: str key '1' inserted first, then int 1 collides."""
        result = to_jsonable({"1": "a", 1: "b"})
        assert is_err(result)
        assert "key collision" in str(result.err_value).lower()
        assert "'1'" in str(result.err_value)

    def test_two_int_keys_no_collision(self) -> None:
        """Distinct int keys that produce distinct strings are fine."""
        result = to_jsonable({1: "a", 2: "b"})
        assert not is_err(result)
        assert result.ok_value == {"1": "a", "2": "b"}

    def test_tuple_key_collision(self) -> None:
        """tuple(1,2) and str '(1, 2)' collide after str()."""
        result = to_jsonable({(1, 2): "a", "(1, 2)": "b"})
        assert is_err(result)
        assert "key collision" in str(result.err_value).lower()

    def test_nested_mapping_collision(self) -> None:
        """Collision inside a nested mapping is detected."""
        result = to_jsonable({"outer": {1: "x", "1": "y"}})
        assert is_err(result)
        assert "key collision" in str(result.err_value).lower()

    def test_string_keys_no_collision(self) -> None:
        """Normal string-keyed dicts pass through without error."""
        result = to_jsonable({"a": 1, "b": 2, "c": 3})
        assert not is_err(result)
        assert result.ok_value == {"a": 1, "b": 2, "c": 3}

    def test_collision_propagates_through_dumps_json(self) -> None:
        """Full dumps_json path also rejects colliding keys."""
        result = dumps_json({1: "a", "1": "b"})
        assert is_err(result)
        assert "key collision" in str(result.err_value).lower()

    def test_empty_mapping_ok(self) -> None:
        """Empty mapping has no keys to collide."""
        result = to_jsonable({})
        assert not is_err(result)
        assert result.ok_value == {}


@pytest.mark.unit
class TestDumpsJsonSurrogate:
    """dumps_json must fail closed on text that is not UTF-8 encodable.

    ensure_ascii=False lets json.dumps emit lone UTF-16 surrogates verbatim;
    those would otherwise pass as Ok and crash far downstream in the Postgres
    TEXT insert.
    """

    def test_lone_surrogate_in_value_rejected(self) -> None:
        """A lone surrogate inside a string value is rejected at encode time."""
        result = dumps_json({'s': '\ud800test'})
        assert is_err(result)
        assert 'utf-8' in str(result.err_value).lower()

    def test_lone_surrogate_in_key_rejected(self) -> None:
        """A lone surrogate inside a mapping key is rejected."""
        result = dumps_json({'\udfff': 'v'})
        assert is_err(result)
        assert 'utf-8' in str(result.err_value).lower()

    def test_lone_surrogate_in_list_rejected(self) -> None:
        """A lone surrogate inside a list element is rejected."""
        result = dumps_json(['ok', '\ud83d'])  # lone half of a surrogate pair
        assert is_err(result)
        assert 'utf-8' in str(result.err_value).lower()

    def test_valid_unicode_round_trips(self) -> None:
        """Legitimate non-ASCII (accents, emoji, CJK) still serializes and is UTF-8 encodable."""
        result = dumps_json({'s': 'héllo \U0001F600 日本語'})
        assert not is_err(result)
        assert result.ok_value.encode('utf-8')  # encodable, no raise
