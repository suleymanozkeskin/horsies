"""Unit tests for horsies.core.types.result — Ok, Err, Result and helpers.

Adapted from upstream rustedpy/result test suite with additions for
local extensions (__json__, DoException explicit tests).
"""

from __future__ import annotations

from typing import Callable

import pytest

from horsies.core.types.result import (
    DoException,
    Err,
    Ok,
    Result,
    UnwrapError,
    is_err,
    is_ok,
)


# ---------------------------------------------------------------------------
# Helpers (module-level, used across test classes)
# ---------------------------------------------------------------------------


def sq(i: int) -> Result[int, int]:
    return Ok(i * i)


async def sq_async(i: int) -> Result[int, int]:
    return Ok(i * i)


def to_err(i: int) -> Result[int, int]:
    return Err(i)


async def to_err_async(i: int) -> Result[int, int]:
    return Err(i)


sq_lambda: Callable[[int], Result[int, int]] = lambda i: Ok(i * i)
to_err_lambda: Callable[[int], Result[int, int]] = lambda i: Err(i)


# ===========================================================================
# Ok basics
# ===========================================================================


@pytest.mark.unit
class TestOkBasics:
    """Construction, identity checks, accessors."""

    def test_factory_stores_value(self) -> None:
        ok = Ok(1)
        assert ok._value == 1

    def test_is_ok_returns_true(self) -> None:
        assert Ok(1).is_ok() is True

    def test_is_err_returns_false(self) -> None:
        assert Ok(1).is_err() is False

    def test_ok_value_property(self) -> None:
        assert Ok('haha').ok_value == 'haha'

    def test_ok_method_returns_value(self) -> None:
        assert Ok('yay').ok() == 'yay'

    def test_err_method_returns_none(self) -> None:
        assert Ok('yay').err() is None


# ===========================================================================
# Err basics
# ===========================================================================


@pytest.mark.unit
class TestErrBasics:
    """Construction, identity checks, accessors."""

    def test_factory_stores_value(self) -> None:
        err = Err(2)
        assert err._value == 2

    def test_is_ok_returns_false(self) -> None:
        assert Err(2).is_ok() is False

    def test_is_err_returns_true(self) -> None:
        assert Err(2).is_err() is True

    def test_err_value_property(self) -> None:
        assert Err('haha').err_value == 'haha'

    def test_ok_method_returns_none(self) -> None:
        assert Err('nay').ok() is None

    def test_err_method_returns_value(self) -> None:
        assert Err('nay').err() == 'nay'

    def test_err_value_is_exception_chains_cause(self) -> None:
        """UnwrapError.__cause__ is set when inner value is BaseException."""
        res = Err(ValueError('Some Error'))
        assert res.is_ok() is False
        assert res.is_err() is True

        with pytest.raises(UnwrapError):
            res.unwrap()

        try:
            res.unwrap()
        except UnwrapError as exc:
            assert isinstance(exc.__cause__, ValueError)


# ===========================================================================
# Equality
# ===========================================================================


@pytest.mark.unit
class TestEquality:
    def test_ok_eq_ok_same_value(self) -> None:
        assert Ok(1) == Ok(1)

    def test_ok_ne_ok_different_value(self) -> None:
        assert Ok(1) != Ok(2)

    def test_err_eq_err_same_value(self) -> None:
        assert Err(1) == Err(1)

    def test_err_ne_err_different_value(self) -> None:
        assert Err(1) != Err(2)

    def test_ok_ne_err_same_inner(self) -> None:
        assert Ok(1) != Err(1)

    def test_ok_ne_non_result(self) -> None:
        assert Ok(1) != 'abc'

    def test_ok_str_ne_ok_int(self) -> None:
        assert Ok('0') != Ok(0)

    def test_ne_returns_false_for_equal(self) -> None:
        assert not (Ok(1) != Ok(1))

    def test_err_ne_returns_false_for_equal(self) -> None:
        assert not (Err(1) != Err(1))

    def test_err_ne_non_result(self) -> None:
        assert Err(1) != 'abc'


# ===========================================================================
# Hash
# ===========================================================================


@pytest.mark.unit
class TestHash:
    def test_dedup_ok_and_err(self) -> None:
        assert len({Ok(1), Err('2'), Ok(1), Err('2')}) == 2

    def test_distinct_ok_values(self) -> None:
        assert len({Ok(1), Ok(2)}) == 2

    def test_ok_and_err_same_inner_differ(self) -> None:
        assert len({Ok('a'), Err('a')}) == 2


# ===========================================================================
# Repr
# ===========================================================================


@pytest.mark.unit
class TestRepr:
    def test_ok_repr(self) -> None:
        o = Ok(123)
        assert repr(o) == 'Ok(123)'
        assert o == eval(repr(o))

    def test_err_repr(self) -> None:
        n = Err(-1)
        assert repr(n) == 'Err(-1)'
        assert n == eval(repr(n))


# ===========================================================================
# __json__ serialization (local addition)
# ===========================================================================


@pytest.mark.unit
class TestJson:
    def test_ok_json(self) -> None:
        result = Ok(42).__json__()
        assert result == {'_tag': 'Ok', 'value': 42}

    def test_ok_json_with_string(self) -> None:
        result = Ok('hello').__json__()
        assert result == {'_tag': 'Ok', 'value': 'hello'}

    def test_err_json_non_exception(self) -> None:
        result = Err('some error').__json__()
        assert result == {'_tag': 'Err', 'error': 'some error'}

    def test_err_json_exception_payload(self) -> None:
        result = Err(ValueError('bad value')).__json__()
        assert result == {
            '_tag': 'Err',
            'error': {
                'exception_type': 'ValueError',
                'message': 'bad value',
            },
        }

    def test_err_json_non_exception_stringified(self) -> None:
        """Non-exception payloads are str()-ified."""
        result = Err(123).__json__()
        assert result == {'_tag': 'Err', 'error': '123'}


# ===========================================================================
# __iter__
# ===========================================================================


@pytest.mark.unit
class TestIterator:
    def test_ok_iter_yields_value(self) -> None:
        values = list(Ok(42))
        assert values == [42]

    def test_err_iter_raises_do_exception(self) -> None:
        err = Err('fail')
        iterator = iter(err)
        with pytest.raises(DoException) as exc_info:
            next(iterator)
        assert exc_info.value.err is err


# ===========================================================================
# Expect
# ===========================================================================


@pytest.mark.unit
class TestExpect:
    def test_ok_expect_returns_value(self) -> None:
        assert Ok('yay').expect('failure') == 'yay'

    def test_err_expect_raises_unwrap_error(self) -> None:
        with pytest.raises(UnwrapError):
            Err('nay').expect('failure')

    def test_err_expect_message_includes_custom_text(self) -> None:
        with pytest.raises(UnwrapError, match='failure'):
            Err('nay').expect('failure')

    def test_err_expect_chains_base_exception(self) -> None:
        """When inner value is BaseException, it becomes __cause__."""
        inner = RuntimeError('boom')
        with pytest.raises(UnwrapError) as exc_info:
            Err(inner).expect('wrapped')
        assert exc_info.value.__cause__ is inner


# ===========================================================================
# Expect err
# ===========================================================================


@pytest.mark.unit
class TestExpectErr:
    def test_err_expect_err_returns_value(self) -> None:
        assert Err('nay').expect_err('hello') == 'nay'

    def test_ok_expect_err_raises_unwrap_error(self) -> None:
        with pytest.raises(UnwrapError):
            Ok('yay').expect_err('hello')


# ===========================================================================
# Unwrap
# ===========================================================================


@pytest.mark.unit
class TestUnwrap:
    def test_ok_unwrap_returns_value(self) -> None:
        assert Ok('yay').unwrap() == 'yay'

    def test_err_unwrap_raises(self) -> None:
        with pytest.raises(UnwrapError):
            Err('nay').unwrap()

    def test_err_unwrap_chains_base_exception(self) -> None:
        inner = ValueError('original')
        with pytest.raises(UnwrapError) as exc_info:
            Err(inner).unwrap()
        assert exc_info.value.__cause__ is inner

    def test_err_unwrap_non_exception_value_no_chain(self) -> None:
        """Non-BaseException inner values don't set __cause__."""
        with pytest.raises(UnwrapError) as exc_info:
            Err('just a string').unwrap()
        assert exc_info.value.__cause__ is None


# ===========================================================================
# Unwrap err
# ===========================================================================


@pytest.mark.unit
class TestUnwrapErr:
    def test_err_unwrap_err_returns_value(self) -> None:
        assert Err('nay').unwrap_err() == 'nay'

    def test_ok_unwrap_err_raises(self) -> None:
        with pytest.raises(UnwrapError):
            Ok('yay').unwrap_err()


# ===========================================================================
# Unwrap or
# ===========================================================================


@pytest.mark.unit
class TestUnwrapOr:
    def test_ok_returns_value_ignoring_default(self) -> None:
        assert Ok('yay').unwrap_or('some_default') == 'yay'

    def test_err_returns_default(self) -> None:
        assert Err('nay').unwrap_or('another_default') == 'another_default'


# ===========================================================================
# Unwrap or else
# ===========================================================================


@pytest.mark.unit
class TestUnwrapOrElse:
    def test_ok_returns_value_ignoring_op(self) -> None:
        assert Ok('yay').unwrap_or_else(str.upper) == 'yay'

    def test_err_applies_op_to_error(self) -> None:
        assert Err('nay').unwrap_or_else(str.upper) == 'NAY'


# ===========================================================================
# Unwrap or raise
# ===========================================================================


@pytest.mark.unit
class TestUnwrapOrRaise:
    def test_ok_returns_value(self) -> None:
        assert Ok('yay').unwrap_or_raise(ValueError) == 'yay'

    def test_err_raises_given_exception_type(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            Err('nay').unwrap_or_raise(ValueError)
        assert exc_info.value.args == ('nay',)


# ===========================================================================
# Map
# ===========================================================================


@pytest.mark.unit
class TestMap:
    def test_ok_map_transforms_value(self) -> None:
        assert Ok('yay').map(str.upper).ok() == 'YAY'

    def test_err_map_returns_self(self) -> None:
        assert Err('nay').map(str.upper).err() == 'nay'

    def test_ok_map_type_change(self) -> None:
        assert Ok(3).map(str).ok() == '3'

    def test_err_map_preserves_error(self) -> None:
        assert Err(2).map(str).err() == 2


@pytest.mark.unit
class TestMapAsync:
    @pytest.mark.asyncio(loop_scope='function')
    async def test_ok_map_async_transforms(self) -> None:
        async def str_upper_async(s: str) -> str:
            return s.upper()

        result = await Ok('yay').map_async(str_upper_async)
        assert result.ok() == 'YAY'

    @pytest.mark.asyncio(loop_scope='function')
    async def test_err_map_async_returns_self(self) -> None:
        async def str_upper_async(s: str) -> str:
            return s.upper()

        result = await Err('nay').map_async(str_upper_async)
        assert result.err() == 'nay'

    @pytest.mark.asyncio(loop_scope='function')
    async def test_ok_map_async_type_change(self) -> None:
        async def str_async(x: int) -> str:
            return str(x)

        result = await Ok(3).map_async(str_async)
        assert result.ok() == '3'

    @pytest.mark.asyncio(loop_scope='function')
    async def test_err_map_async_preserves_error(self) -> None:
        async def str_async(x: int) -> str:
            return str(x)

        result = await Err(2).map_async(str_async)
        assert result.err() == 2


# ===========================================================================
# Map or
# ===========================================================================


@pytest.mark.unit
class TestMapOr:
    def test_ok_uses_op(self) -> None:
        assert Ok('yay').map_or('hay', str.upper) == 'YAY'

    def test_err_returns_default(self) -> None:
        assert Err('nay').map_or('hay', str.upper) == 'hay'

    def test_ok_type_change(self) -> None:
        assert Ok(3).map_or('-1', str) == '3'

    def test_err_type_change(self) -> None:
        assert Err(2).map_or('-1', str) == '-1'


# ===========================================================================
# Map or else
# ===========================================================================


@pytest.mark.unit
class TestMapOrElse:
    def test_ok_uses_op(self) -> None:
        assert Ok('yay').map_or_else(lambda: 'hay', str.upper) == 'YAY'

    def test_err_uses_default_op(self) -> None:
        assert Err('nay').map_or_else(lambda: 'hay', str.upper) == 'hay'

    def test_ok_type_change(self) -> None:
        assert Ok(3).map_or_else(lambda: '-1', str) == '3'

    def test_err_type_change(self) -> None:
        assert Err(2).map_or_else(lambda: '-1', str) == '-1'


# ===========================================================================
# Map err
# ===========================================================================


@pytest.mark.unit
class TestMapErr:
    def test_ok_returns_self(self) -> None:
        assert Ok('yay').map_err(str.upper).ok() == 'yay'

    def test_err_transforms_error(self) -> None:
        assert Err('nay').map_err(str.upper).err() == 'NAY'


# ===========================================================================
# And then
# ===========================================================================


@pytest.mark.unit
class TestAndThen:
    def test_ok_chain_ok(self) -> None:
        assert Ok(2).and_then(sq).and_then(sq).ok() == 16

    def test_ok_chain_to_err(self) -> None:
        assert Ok(2).and_then(sq).and_then(to_err).err() == 4

    def test_ok_to_err_stops_chain(self) -> None:
        assert Ok(2).and_then(to_err).and_then(sq).err() == 2

    def test_err_short_circuits(self) -> None:
        assert Err(3).and_then(sq).and_then(sq).err() == 3

    def test_lambda_ok_chain(self) -> None:
        assert Ok(2).and_then(sq_lambda).and_then(sq_lambda).ok() == 16

    def test_lambda_ok_chain_to_err(self) -> None:
        assert Ok(2).and_then(sq_lambda).and_then(to_err_lambda).err() == 4

    def test_lambda_ok_to_err_stops_chain(self) -> None:
        assert Ok(2).and_then(to_err_lambda).and_then(sq_lambda).err() == 2

    def test_lambda_err_short_circuits(self) -> None:
        assert Err(3).and_then(sq_lambda).and_then(sq_lambda).err() == 3


@pytest.mark.unit
class TestAndThenAsync:
    @pytest.mark.asyncio(loop_scope='function')
    async def test_ok_chain_ok(self) -> None:
        r1 = await Ok(2).and_then_async(sq_async)
        r2 = await r1.and_then_async(sq_async)
        assert r2.ok() == 16

    @pytest.mark.asyncio(loop_scope='function')
    async def test_ok_chain_to_err(self) -> None:
        r1 = await Ok(2).and_then_async(sq_async)
        r2 = await r1.and_then_async(to_err_async)
        assert r2.err() == 4

    @pytest.mark.asyncio(loop_scope='function')
    async def test_ok_to_err_stops_chain(self) -> None:
        r1 = await Ok(2).and_then_async(to_err_async)
        r2 = await r1.and_then_async(to_err_async)
        assert r2.err() == 2

    @pytest.mark.asyncio(loop_scope='function')
    async def test_err_short_circuits(self) -> None:
        r1 = await Err(3).and_then_async(sq_async)
        r2 = await r1.and_then_async(sq_async)
        assert r2.err() == 3


# ===========================================================================
# Or else
# ===========================================================================


@pytest.mark.unit
class TestOrElse:
    def test_ok_ignores_op(self) -> None:
        assert Ok(2).or_else(sq).or_else(sq).ok() == 2

    def test_ok_ignores_to_err(self) -> None:
        assert Ok(2).or_else(to_err).or_else(sq).ok() == 2

    def test_err_applies_op(self) -> None:
        assert Err(3).or_else(sq).or_else(to_err).ok() == 9

    def test_err_chain_stays_err(self) -> None:
        assert Err(3).or_else(to_err).or_else(to_err).err() == 3

    def test_lambda_ok_ignores(self) -> None:
        assert Ok(2).or_else(sq_lambda).or_else(sq).ok() == 2

    def test_lambda_ok_ignores_to_err(self) -> None:
        assert Ok(2).or_else(to_err_lambda).or_else(sq_lambda).ok() == 2

    def test_lambda_err_applies(self) -> None:
        assert Err(3).or_else(sq_lambda).or_else(to_err_lambda).ok() == 9

    def test_lambda_err_chain_stays_err(self) -> None:
        assert Err(3).or_else(to_err_lambda).or_else(to_err_lambda).err() == 3


# ===========================================================================
# Inspect
# ===========================================================================


@pytest.mark.unit
class TestInspect:
    def test_ok_calls_op(self) -> None:
        collected: list[int] = []
        result = Ok(2).inspect(lambda x: collected.append(x))
        assert result == Ok(2)
        assert collected == [2]

    def test_err_does_not_call_op(self) -> None:
        collected: list[int] = []
        result = Err('e').inspect(lambda x: collected.append(x))
        assert result == Err('e')
        assert collected == []

    def test_ok_inspect_with_regular_fn(self) -> None:
        """op return value is discarded; original result is returned."""
        collected: list[str] = []

        def _add(x: str) -> str:
            collected.append(x)
            return x + x  # return value should be ignored

        assert Ok('hello').inspect(_add) == Ok('hello')
        assert collected == ['hello']


# ===========================================================================
# Inspect err
# ===========================================================================


@pytest.mark.unit
class TestInspectErr:
    def test_err_calls_op(self) -> None:
        collected: list[str] = []
        result = Err('e').inspect_err(lambda x: collected.append(x))
        assert result == Err('e')
        assert collected == ['e']

    def test_ok_does_not_call_op(self) -> None:
        collected: list[str] = []
        result = Ok(2).inspect_err(lambda x: collected.append(x))
        assert result == Ok(2)
        assert collected == []


# ===========================================================================
# Slots
# ===========================================================================


@pytest.mark.unit
class TestSlots:
    def test_ok_rejects_arbitrary_attributes(self) -> None:
        o = Ok('yay')
        with pytest.raises(AttributeError):
            o.some_arbitrary_attribute = 1  # type: ignore[attr-defined]

    def test_err_rejects_arbitrary_attributes(self) -> None:
        n = Err('nay')
        with pytest.raises(AttributeError):
            n.some_arbitrary_attribute = 1  # type: ignore[attr-defined]


# ===========================================================================
# UnwrapError
# ===========================================================================


@pytest.mark.unit
class TestUnwrapError:
    def test_stores_result_on_err_unwrap(self) -> None:
        n = Err('nay')
        with pytest.raises(UnwrapError) as exc_info:
            n.unwrap()
        assert exc_info.value.result is n

    def test_stores_result_on_ok_unwrap_err(self) -> None:
        o = Ok('yay')
        with pytest.raises(UnwrapError) as exc_info:
            o.unwrap_err()
        assert exc_info.value.result is o

    def test_message_in_str(self) -> None:
        with pytest.raises(UnwrapError, match='nay'):
            Err('nay').unwrap()


# ===========================================================================
# Pattern matching
# ===========================================================================


@pytest.mark.unit
class TestPatternMatching:
    def test_match_ok(self) -> None:
        o: Result[str, int] = Ok('yay')
        match o:
            case Ok(value):
                reached = True
            case _:
                reached = False

        assert value == 'yay'
        assert reached is True

    def test_match_err(self) -> None:
        n: Result[int, str] = Err('nay')
        match n:
            case Err(value):
                reached = True
            case _:
                reached = False

        assert value == 'nay'
        assert reached is True

    def test_match_does_not_cross(self) -> None:
        """Ok doesn't match Err pattern and vice versa."""
        o: Result[str, str] = Ok('hello')
        match o:
            case Err(value):
                reached_err = True
            case Ok(value):
                reached_err = False

        assert reached_err is False
        assert value == 'hello'


# ===========================================================================
# Type guard functions
# ===========================================================================


@pytest.mark.unit
class TestTypeGuards:
    def test_is_ok_on_ok(self) -> None:
        assert is_ok(Ok(1)) is True

    def test_is_ok_on_err(self) -> None:
        assert is_ok(Err(1)) is False

    def test_is_err_on_err(self) -> None:
        assert is_err(Err(1)) is True

    def test_is_err_on_ok(self) -> None:
        assert is_err(Ok(1)) is False
