"""Regression tests for strict mapping-key and sequence handling in serde.

JSON object keys must be strings and JSON arrays come only from lists.
``to_jsonable`` rejects non-str dict keys and non-list sequences (tuple,
set, ...) instead of silently coercing them (str(key) / tuple->list), which
would be lossy on round-trip. Matches the strict-serde fence in
``_validate_json_native``.
"""

from __future__ import annotations

import pytest

from horsies.core.codec.serde import dumps_json, to_jsonable
from horsies.core.types.result import is_err


@pytest.mark.unit
class TestMappingKeyRejection:
    """to_jsonable rejects non-string mapping keys rather than str()-coercing."""

    def test_int_key_rejected(self) -> None:
        result = to_jsonable({1: "a"})
        assert is_err(result)
        assert "key must be str" in str(result.err_value).lower()

    def test_int_and_str_keys_rejected(self) -> None:
        """A dict mixing int and str keys is rejected on the non-str key."""
        result = to_jsonable({1: "a", "1": "b"})
        assert is_err(result)
        assert "key must be str" in str(result.err_value).lower()

    def test_two_int_keys_rejected(self) -> None:
        """Previously str()-coerced to {'1','2'}; now rejected (lossy)."""
        result = to_jsonable({1: "a", 2: "b"})
        assert is_err(result)
        assert "key must be str" in str(result.err_value).lower()

    def test_tuple_key_rejected(self) -> None:
        result = to_jsonable({(1, 2): "a"})
        assert is_err(result)
        assert "key must be str" in str(result.err_value).lower()

    def test_nested_mapping_non_str_key_rejected(self) -> None:
        """A non-str key inside a nested mapping is detected."""
        result = to_jsonable({"outer": {1: "x"}})
        assert is_err(result)
        assert "key must be str" in str(result.err_value).lower()

    def test_string_keys_ok(self) -> None:
        """Normal string-keyed dicts pass through without error."""
        result = to_jsonable({"a": 1, "b": 2, "c": 3})
        assert not is_err(result)
        assert result.ok_value == {"a": 1, "b": 2, "c": 3}

    def test_rejection_propagates_through_dumps_json(self) -> None:
        """Full dumps_json path also rejects non-str keys."""
        result = dumps_json({1: "a"})
        assert is_err(result)
        assert "key must be str" in str(result.err_value).lower()

    def test_empty_mapping_ok(self) -> None:
        result = to_jsonable({})
        assert not is_err(result)
        assert result.ok_value == {}


@pytest.mark.unit
class TestSequenceRejection:
    """to_jsonable accepts only lists for JSON arrays; tuple/set/etc. are
    rejected rather than silently coerced to a list (lossy on round-trip)."""

    def test_tuple_rejected(self) -> None:
        result = to_jsonable((1, 2))
        assert is_err(result)
        assert "type tuple" in str(result.err_value).lower()

    def test_empty_tuple_rejected(self) -> None:
        result = to_jsonable(())
        assert is_err(result)
        assert "type tuple" in str(result.err_value).lower()

    def test_set_rejected(self) -> None:
        result = to_jsonable({1, 2, 3})
        assert is_err(result)

    def test_list_ok(self) -> None:
        result = to_jsonable([1, 2, 3])
        assert not is_err(result)
        assert result.ok_value == [1, 2, 3]

    def test_nested_tuple_in_list_rejected(self) -> None:
        result = to_jsonable([1, (2, 3)])
        assert is_err(result)
        assert "type tuple" in str(result.err_value).lower()

    def test_tuple_rejected_through_dumps_json(self) -> None:
        result = dumps_json((1, 2))
        assert is_err(result)
        assert "type tuple" in str(result.err_value).lower()


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
