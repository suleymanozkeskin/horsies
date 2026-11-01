"""Tests for the strict JSON I/O boundary (codec/json_io.py).

dumps_json is strict: it rejects non-JSON-native values (tuple, set, non-str
dict keys, lone surrogates, non-finite floats) rather than coercing them, and
emits no class tags. loads_json rejects non-RFC-8259 constants (NaN/Infinity),
numeric literals outside the finite binary64 range, and lone surrogates.
loads_json_bytes is the stored-payload form: strict UTF-8, and empty is an
error. dumps_canonical_json is the one canonical form for hashed or stored text.
"""

from __future__ import annotations

import sys

import pytest

from horsies.core.codec.json_io import (
    dumps_canonical_json,
    dumps_json,
    loads_json,
    loads_json_bytes,
)
from horsies.core.codec.json_value import (
    JSON_TEXT_TOO_DEEP,
    MAX_JSON_DEPTH,
    JsonValue,
    escape_lone_surrogates,
)
from horsies.core.codec.typed import _scan_reserved_keys  # pyright: ignore[reportPrivateUsage]
from horsies.core.types.result import is_err


@pytest.mark.unit
class TestEscapeLoneSurrogates:
    """Free text in a library-built error must encode."""

    def test_a_lone_surrogate_becomes_an_escape_that_encodes(self) -> None:
        escaped = escape_lone_surrogates(f'/data/a{chr(0xdc80)}b')

        assert escaped == '/data/a\\udc80b'
        assert not is_err(dumps_json(escaped))

    def test_a_python_surrogate_pair_becomes_two_escapes(self) -> None:
        pair = chr(0xd83d) + chr(0xde00)

        assert escape_lone_surrogates(pair) == '\\ud83d\\ude00'

    @pytest.mark.parametrize(
        'text',
        ['', 'plain', 'caf\u00e9', 'a\ufffdb', chr(0x1f600)],
        ids=['empty', 'ascii', 'non-ascii', 'replacement-char', 'astral'],
    )
    def test_text_that_encodes_does_not_change(self, text: str) -> None:
        assert escape_lone_surrogates(text) == text


@pytest.mark.unit
class TestDumpsJsonStrictMapping:
    """Non-string mapping keys are rejected, not str()-coerced."""

    def test_int_key_rejected(self) -> None:
        result = dumps_json({1: 'a'})
        assert is_err(result)
        assert 'key must be str' in str(result.err_value).lower()

    def test_mixed_int_and_str_keys_rejected(self) -> None:
        result = dumps_json({1: 'a', '1': 'b'})
        assert is_err(result)
        assert 'key must be str' in str(result.err_value).lower()

    def test_tuple_key_rejected(self) -> None:
        result = dumps_json({(1, 2): 'a'})
        assert is_err(result)
        assert 'key must be str' in str(result.err_value).lower()

    def test_nested_non_str_key_rejected(self) -> None:
        result = dumps_json({'outer': {1: 'x'}})
        assert is_err(result)
        assert 'key must be str' in str(result.err_value).lower()

    def test_string_keys_ok(self) -> None:
        result = dumps_json({'a': 1, 'b': 2})
        assert not is_err(result)
        assert result.ok_value == '{"a":1,"b":2}'

    def test_empty_mapping_ok(self) -> None:
        result = dumps_json({})
        assert not is_err(result)
        assert result.ok_value == '{}'


@pytest.mark.unit
class TestDumpsJsonStrictSequence:
    """Only lists become JSON arrays; tuple/set are rejected, not coerced."""

    def test_tuple_rejected(self) -> None:
        result = dumps_json((1, 2))
        assert is_err(result)
        assert 'tuple' in str(result.err_value).lower()

    def test_empty_tuple_rejected(self) -> None:
        result = dumps_json(())
        assert is_err(result)
        assert 'tuple' in str(result.err_value).lower()

    def test_set_rejected(self) -> None:
        result = dumps_json({1, 2, 3})
        assert is_err(result)

    def test_nested_tuple_in_list_rejected(self) -> None:
        result = dumps_json([1, (2, 3)])
        assert is_err(result)
        assert 'tuple' in str(result.err_value).lower()

    def test_list_ok(self) -> None:
        result = dumps_json([1, 2, 3])
        assert not is_err(result)
        assert result.ok_value == '[1,2,3]'


@pytest.mark.unit
class TestDumpsJsonPrimitives:
    """Primitives and nesting serialize; non-finite floats are rejected."""

    def test_primitives(self) -> None:
        assert dumps_json(None).ok_value == 'null'
        assert dumps_json(True).ok_value == 'true'
        assert dumps_json(42).ok_value == '42'
        assert dumps_json('hi').ok_value == '"hi"'

    def test_non_finite_float_rejected(self) -> None:
        assert is_err(dumps_json(float('nan')))
        assert is_err(dumps_json(float('inf')))


@pytest.mark.unit
class TestDumpsJsonSurrogate:
    """dumps_json fails closed on text that is not UTF-8 encodable.

    ensure_ascii=False lets json.dumps emit lone UTF-16 surrogates verbatim;
    those would otherwise pass as Ok and crash far downstream in the Postgres
    TEXT insert.
    """

    def test_lone_surrogate_in_value_rejected(self) -> None:
        result = dumps_json({'s': '\ud800test'})
        assert is_err(result)
        assert 'utf-8' in str(result.err_value).lower()

    def test_lone_surrogate_in_key_rejected(self) -> None:
        result = dumps_json({'\udfff': 'v'})
        assert is_err(result)
        assert 'utf-8' in str(result.err_value).lower()

    def test_lone_surrogate_in_list_rejected(self) -> None:
        result = dumps_json(['ok', '\ud83d'])  # lone half of a surrogate pair
        assert is_err(result)
        assert 'utf-8' in str(result.err_value).lower()

    def test_valid_unicode_round_trips(self) -> None:
        result = dumps_json({'s': 'héllo \U0001F600 日本語'})
        assert not is_err(result)
        assert result.ok_value.encode('utf-8')  # encodable, no raise


@pytest.mark.unit
class TestLoadsJson:
    """loads_json basics and non-RFC-8259 constant rejection."""

    def test_empty_returns_none(self) -> None:
        assert loads_json(None).ok_value is None
        assert loads_json('').ok_value is None

    def test_round_trip(self) -> None:
        result = loads_json('{"a":[1,2],"b":"x"}')
        assert not is_err(result)
        assert result.ok_value == {'a': [1, 2], 'b': 'x'}

    def test_nan_rejected(self) -> None:
        assert is_err(loads_json('NaN'))

    def test_infinity_rejected(self) -> None:
        assert is_err(loads_json('Infinity'))

    def test_malformed_rejected(self) -> None:
        assert is_err(loads_json('{bad'))

    @pytest.mark.parametrize(
        'document',
        [
            '[1e9999]',
            '[-1e9999]',
            '[1E400]',
            '{"a":[{"b":1.8e308}]}',
            '[' + '1' * 400 + '.0]',
        ],
    )
    def test_number_outside_finite_binary64_rejected(self, document: str) -> None:
        result = loads_json(document)
        assert is_err(result)
        assert 'finite binary64' in str(result.err_value)

    def test_overflow_rejection_text_holds_no_input_value(self) -> None:
        result = loads_json('[1234e9999]')
        assert is_err(result)
        assert '1234e9999' not in str(result.err_value)

    @pytest.mark.parametrize(
        ('document', 'expected'),
        [
            ('[1.5]', 1.5),
            ('[1.7976931348623157e308]', 1.7976931348623157e308),
            ('[-1.7976931348623157e308]', -1.7976931348623157e308),
            ('[5e-324]', 5e-324),
            ('[1e-9999]', 0.0),
        ],
    )
    def test_finite_float_keeps_its_value(
        self, document: str, expected: float,
    ) -> None:
        result = loads_json(document)
        assert not is_err(result)
        assert result.ok_value == [expected]

    @pytest.mark.parametrize(
        'document',
        [
            r'"\ud800"',
            r'"\uDFFF"',
            r'{"x":"\ud800"}',
            r'{"\ud800":1}',
            r'[["a\ud800b"]]',
            r'"\ude00\ud83d"',
            r'"\ud83d\u0041"',
            r'"\\\ud800"',
            '"\ud800"',
            '[' * 900 + r'"\ud800"' + ']' * 900,
        ],
    )
    def test_lone_surrogate_rejected(self, document: str) -> None:
        result = loads_json(document)
        assert is_err(result)
        assert 'lone surrogate' in str(result.err_value)

    def test_lone_surrogate_rejection_text_holds_no_input_value(self) -> None:
        result = loads_json(r'{"secret":"abc\ud800"}')
        assert is_err(result)
        assert 'abc' not in str(result.err_value)
        assert 'secret' not in str(result.err_value)

    @pytest.mark.parametrize(
        ('document', 'expected'),
        [
            (r'"\ud83d\ude00"', '😀'),
            (r'"\uD83D\uDE00"', '😀'),
            ('"😀"', '😀'),
            (r'"\u00e9"', 'é'),
            (r'"\\ud800"', '\\ud800'),
            (r'{"\ud83d\ude00":"\ud83d\ude00"}', {'😀': '😀'}),
        ],
    )
    def test_valid_text_keeps_its_value(
        self, document: str, expected: object,
    ) -> None:
        result = loads_json(document)
        assert not is_err(result)
        assert result.ok_value == expected

    def test_integer_literal_stays_an_integer_past_the_float_range(self) -> None:
        digits = '1' * 400
        result = loads_json(f'[{digits}]')
        assert not is_err(result)
        assert result.ok_value == [int(digits)]


def _nested_text(depth: int) -> str:
    """Return a JSON text with `depth` nested lists around one number."""
    return '[' * depth + '1' + ']' * depth


def _nested_value(depth: int) -> JsonValue:
    """Return a value with `depth` nested lists around one number."""
    value: JsonValue = 1
    for _ in range(depth):
        value = [value]
    return value


def _text_that_alternates(depth: int) -> str:
    """Return a JSON text with `depth` levels, of lists and dicts in turn."""
    opening = ('[', '{"key":')
    closing = (']', '}')
    levels = range(depth)
    return (
        ''.join(opening[level % 2] for level in levels)
        + '1'
        + ''.join(closing[level % 2] for level in reversed(levels))
    )


@pytest.mark.unit
class TestDumpsJsonDepthBound:
    def test_a_value_at_the_bound_is_accepted(self) -> None:
        assert not is_err(dumps_json(_nested_value(MAX_JSON_DEPTH)))

    def test_one_level_more_is_refused(self) -> None:
        result = dumps_json(_nested_value(MAX_JSON_DEPTH + 1))

        assert is_err(result)
        assert 'nested deeper than' in str(result.err_value)

    def test_a_list_that_holds_itself_is_refused(self) -> None:
        """Before, this raised `RecursionError` out of a `SerdeResult`."""
        value: list[object] = []
        value.append(value)

        result = dumps_json(value)

        assert is_err(result)
        assert 'or it holds itself' in str(result.err_value)

    def test_a_dict_that_holds_itself_is_refused(self) -> None:
        value: dict[str, object] = {}
        value['self'] = value

        result = dumps_json(value)

        assert is_err(result)
        assert 'or it holds itself' in str(result.err_value)

    def test_a_scalar_below_the_deepest_list_does_not_count(self) -> None:
        """Only a list or a dict adds a level."""
        wide: list[JsonValue] = [1, 'text', None, True, 0.5]
        value: JsonValue = wide
        for _ in range(MAX_JSON_DEPTH - 1):
            value = [value, 1]

        assert not is_err(dumps_json(value))


@pytest.mark.unit
class TestLoadsJsonDepthBound:
    @pytest.mark.parametrize(
        'document',
        [
            _nested_text(MAX_JSON_DEPTH),
            _text_that_alternates(MAX_JSON_DEPTH),
            '[[1],' + _nested_text(MAX_JSON_DEPTH - 1) + ']',
        ],
        ids=['lists', 'lists-and-dicts', 'more-brackets-than-the-bound'],
    )
    def test_a_document_at_the_bound_is_accepted(self, document: str) -> None:
        assert not is_err(loads_json(document))

    @pytest.mark.parametrize(
        'document',
        [
            _nested_text(MAX_JSON_DEPTH + 1),
            _text_that_alternates(MAX_JSON_DEPTH + 1),
            '[' + '[1],' * 500 + _nested_text(MAX_JSON_DEPTH) + ']',
            _nested_text(5_000),
        ],
        ids=['lists', 'lists-and-dicts', 'in-the-last-branch', 'parser-accepts-it'],
    )
    def test_a_document_past_the_bound_is_refused(self, document: str) -> None:
        result = loads_json(document)

        assert is_err(result)
        assert str(result.err_value) == f'JSON parse failed: {JSON_TEXT_TOO_DEEP}'

    def test_a_document_too_deep_for_the_parser_is_refused(self) -> None:
        """`json.loads` raises `RecursionError` at this depth."""
        result = loads_json(_nested_text(200_000))

        assert is_err(result)
        assert str(result.err_value) == f'JSON parse failed: {JSON_TEXT_TOO_DEEP}'

    def test_a_stored_payload_past_the_bound_is_refused(self) -> None:
        result = loads_json_bytes(_nested_text(MAX_JSON_DEPTH + 1).encode())

        assert is_err(result)
        assert str(result.err_value) == f'JSON parse failed: {JSON_TEXT_TOO_DEEP}'

    def test_a_wide_document_with_many_brackets_is_accepted(self) -> None:
        """The count of brackets exceeds the bound, the nesting does not."""
        document = '[' + ','.join(['{"key":[1]}'] * MAX_JSON_DEPTH) + ']'

        result = loads_json(document)

        assert not is_err(result)
        assert result.ok_value == [{'key': [1]}] * MAX_JSON_DEPTH

    @pytest.mark.parametrize(
        'document',
        [
            '"' + '[' * (MAX_JSON_DEPTH + 1) + '"',
            '["' + '{' * (MAX_JSON_DEPTH + 1) + '"]',
        ],
        ids=['string', 'string-in-a-list'],
    )
    def test_brackets_in_a_string_are_not_nesting(self, document: str) -> None:
        assert not is_err(loads_json(document))

    def test_each_walker_works_at_the_bound(self) -> None:
        """A value that `loads_json` returns stays inside the recursion limit."""
        result = loads_json(_nested_text(MAX_JSON_DEPTH))

        assert not is_err(result)
        _scan_reserved_keys(result.ok_value)
        assert not is_err(dumps_json(result.ok_value))
        assert dumps_canonical_json(result.ok_value) == _nested_text(MAX_JSON_DEPTH)

    def test_the_bound_leaves_room_for_the_stack_of_the_caller(self) -> None:
        assert 3 * MAX_JSON_DEPTH <= sys.getrecursionlimit()


@pytest.mark.unit
class TestLoadsJsonBytes:
    """A stored payload parses as declared, or it is an error."""

    def test_valid_payload(self) -> None:
        result = loads_json_bytes('{"city":"Köln"}'.encode())
        assert not is_err(result)
        assert result.ok_value == {'city': 'Köln'}

    def test_json_null_is_the_value_none(self) -> None:
        result = loads_json_bytes(b'null')
        assert not is_err(result)
        assert result.ok_value is None

    def test_empty_payload_is_an_error_not_null(self) -> None:
        result = loads_json_bytes(b'')
        assert is_err(result)
        assert 'empty' in str(result.err_value)

    @pytest.mark.parametrize(
        'payload',
        [
            b'\xff',
            '[]'.encode('utf-16'),
            '[]'.encode('utf-32'),
        ],
    )
    def test_non_utf8_payload_is_an_error(self, payload: bytes) -> None:
        result = loads_json_bytes(payload)
        assert is_err(result)
        assert 'not UTF-8' in str(result.err_value)

    def test_utf8_byte_order_mark_is_an_error(self) -> None:
        assert is_err(loads_json_bytes(b'\xef\xbb\xbf[]'))

    def test_non_utf8_error_text_holds_no_payload_byte(self) -> None:
        result = loads_json_bytes(b'[\xfe]')
        assert is_err(result)
        assert 'byte 1' in str(result.err_value)
        assert '0xfe' not in str(result.err_value)

    @pytest.mark.parametrize(
        'payload',
        [
            b'{bad',
            b'[NaN]',
            b'[Infinity]',
            b'[1e9999]',
            b'[' + b'1' * 5000 + b']',
            rb'["\ud800"]',
        ],
    )
    def test_strict_parse_failure_is_an_error(self, payload: bytes) -> None:
        assert is_err(loads_json_bytes(payload))


@pytest.mark.unit
class TestDumpsCanonicalJson:
    """Equal content gives equal text, because callers store or hash it."""

    def test_compact_sorted_and_unescaped(self) -> None:
        assert dumps_canonical_json({'b': 1, 'a': ['ü']}) == '{"a":["ü"],"b":1}'

    def test_nested_keys_are_sorted(self) -> None:
        assert dumps_canonical_json({'z': {'y': 1, 'x': 2}}) == (
            '{"z":{"x":2,"y":1}}'
        )

    def test_list_order_is_kept(self) -> None:
        assert dumps_canonical_json(['b', 'a']) == '["b","a"]'

    def test_largest_finite_float_is_kept(self) -> None:
        assert dumps_canonical_json([1.7976931348623157e308]) == (
            '[1.7976931348623157e+308]'
        )

    @pytest.mark.parametrize('value', [float('inf'), float('-inf'), float('nan')])
    def test_non_finite_float_crashes_loud(self, value: float) -> None:
        with pytest.raises(ValueError, match='not JSON compliant'):
            dumps_canonical_json({'x': value})
