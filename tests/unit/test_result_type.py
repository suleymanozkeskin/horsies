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
    OkErr,
    Result,
    UnwrapError,
    as_async_result,
    as_result,
    do,
    do_async,
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
# OkErr isinstance constant
# ===========================================================================


@pytest.mark.unit
class TestOkErrConstant:
    def test_ok_is_instance(self) -> None:
        assert isinstance(Ok('yay'), OkErr)

    def test_err_is_instance(self) -> None:
        assert isinstance(Err('nay'), OkErr)

    def test_non_result_is_not_instance(self) -> None:
        assert not isinstance(1, OkErr)


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
# as_result (sync decorator)
# ===========================================================================


@pytest.mark.unit
class TestAsResult:
    def test_success_returns_ok(self) -> None:
        @as_result(ValueError)
        def good(value: int) -> int:
            return value

        result = good(123)
        assert isinstance(result, Ok)
        assert result.unwrap() == 123

    def test_caught_exception_returns_err(self) -> None:
        @as_result(IndexError, ValueError)
        def bad(value: int) -> int:
            raise ValueError

        result = bad(123)
        assert isinstance(result, Err)
        assert isinstance(result.unwrap_err(), ValueError)

    def test_uncaught_exception_propagates(self) -> None:
        @as_result(ValueError)
        def raises_index() -> int:
            raise IndexError

        with pytest.raises(IndexError):
            raises_index()

    def test_no_exception_types_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match='requires one or more exception types'):
            @as_result()
            def f() -> int:
                return 1

    def test_non_exception_type_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match='requires one or more exception types'):
            @as_result('not an exception type')  # type: ignore[arg-type]
            def g() -> int:
                return 1

    def test_preserves_function_signature(self) -> None:
        @as_result(ValueError)
        def f(a: int) -> int:
            return a

        res: Result[int, ValueError] = f(123)
        assert res.ok() == 123


# ===========================================================================
# as_async_result (async decorator)
# ===========================================================================


@pytest.mark.unit
class TestAsAsyncResult:
    @pytest.mark.asyncio(loop_scope='function')
    async def test_success_returns_ok(self) -> None:
        @as_async_result(ValueError)
        async def good(value: int) -> int:
            return value

        result = await good(123)
        assert isinstance(result, Ok)
        assert result.unwrap() == 123

    @pytest.mark.asyncio(loop_scope='function')
    async def test_caught_exception_returns_err(self) -> None:
        @as_async_result(IndexError, ValueError)
        async def bad(value: int) -> int:
            raise ValueError

        result = await bad(123)
        assert isinstance(result, Err)
        assert isinstance(result.unwrap_err(), ValueError)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_uncaught_exception_propagates(self) -> None:
        @as_async_result(ValueError)
        async def raises_index() -> int:
            raise IndexError

        with pytest.raises(IndexError):
            await raises_index()

    def test_no_exception_types_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match='requires one or more exception types'):
            @as_async_result()
            async def f() -> int:
                return 1

    def test_non_exception_type_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match='requires one or more exception types'):
            @as_async_result('not an exception type')  # type: ignore[arg-type]
            async def g() -> int:
                return 1


# ===========================================================================
# do (sync do-notation)
# ===========================================================================


@pytest.mark.unit
class TestDo:
    def test_all_ok_returns_final_ok(self) -> None:
        def resx(is_suc: bool) -> Result[str, int]:
            return Ok('hello') if is_suc else Err(1)

        def resy(is_suc: bool) -> Result[bool, int]:
            return Ok(True) if is_suc else Err(2)

        result: Result[float, int] = do(
            Ok(len(x) + int(y) + 0.5)
            for x in resx(True)
            for y in resy(True)
        )
        assert result == Ok(6.5)

    def test_first_err_short_circuits(self) -> None:
        result: Result[float, int] = do(
            Ok(len(x) + int(y) + 0.5)
            for x in Err(1)
            for y in Ok(True)
        )
        assert result == Err(1)

    def test_second_err_short_circuits(self) -> None:
        result: Result[float, int] = do(
            Ok(len(x) + int(y) + 0.5)
            for x in Ok('hello')
            for y in Err(2)
        )
        assert result == Err(2)

    def test_both_err_returns_first(self) -> None:
        result: Result[float, int] = do(
            Ok(len(x) + int(y) + 0.5)
            for x in Err(1)
            for y in Err(2)
        )
        assert result == Err(1)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_async_generator_raises_helpful_type_error(self) -> None:
        """do() with async generators gives a clear error pointing to do_async()."""

        async def aget_resx(is_suc: bool) -> Result[str, int]:
            return Ok('hello') if is_suc else Err(1)

        async def aget_resy(is_suc: bool) -> Result[bool, int]:
            return Ok(True) if is_suc else Err(2)

        with pytest.raises(TypeError) as exc_info:
            do(
                Ok(len(x) + int(y))
                for x in await aget_resx(True)
                for y in await aget_resy(True)
            )

        assert 'async_generator' in str(exc_info.value)

    def test_non_async_type_error_reraised(self) -> None:
        """TypeErrors unrelated to async generators propagate unchanged."""

        def bad_gen() -> Result[int, str]:  # type: ignore[misc]
            raise TypeError('unrelated type error')
            yield  # noqa: F401 — makes it a generator

        with pytest.raises(TypeError, match='unrelated type error'):
            do(bad_gen())


# ===========================================================================
# do_async
# ===========================================================================


@pytest.mark.unit
class TestDoAsync:
    @pytest.mark.asyncio(loop_scope='function')
    async def test_async_generator_all_ok(self) -> None:
        async def aget_resx(is_suc: bool) -> Result[str, int]:
            return Ok('hello') if is_suc else Err(1)

        async def aget_resy(is_suc: bool) -> Result[bool, int]:
            return Ok(True) if is_suc else Err(2)

        def get_resz(is_suc: bool) -> Result[float, int]:
            return Ok(0.5) if is_suc else Err(3)

        result: Result[float, int] = await do_async(
            Ok(len(x) + int(y) + z)
            for x in await aget_resx(True)
            for y in await aget_resy(True)
            for z in get_resz(True)
        )
        assert result == Ok(6.5)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_async_generator_first_err(self) -> None:
        async def aget_resx(is_suc: bool) -> Result[str, int]:
            return Ok('hello') if is_suc else Err(1)

        async def aget_resy(is_suc: bool) -> Result[bool, int]:
            return Ok(True) if is_suc else Err(2)

        def get_resz(is_suc: bool) -> Result[float, int]:
            return Ok(0.5) if is_suc else Err(3)

        result = await do_async(
            Ok(len(x) + int(y) + z)
            for x in await aget_resx(False)
            for y in await aget_resy(True)
            for z in get_resz(True)
        )
        assert result == Err(1)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_async_generator_second_err(self) -> None:
        async def aget_resx(is_suc: bool) -> Result[str, int]:
            return Ok('hello') if is_suc else Err(1)

        async def aget_resy(is_suc: bool) -> Result[bool, int]:
            return Ok(True) if is_suc else Err(2)

        result = await do_async(
            Ok(len(x) + int(y))
            for x in await aget_resx(True)
            for y in await aget_resy(False)
        )
        assert result == Err(2)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_regular_generator_accepted(self) -> None:
        """do_async also accepts regular (non-async) generators for convenience."""
        result = await do_async(
            Ok(x + y)
            for x in Ok(1)
            for y in Ok(2)
        )
        assert result == Ok(3)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_single_async_value(self) -> None:
        """Single-await generator expression (Python makes this non-async)."""

        async def aget_resx(is_suc: bool) -> Result[str, int]:
            return Ok('hello') if is_suc else Err(1)

        assert await do_async(Ok(len(x)) for x in await aget_resx(True)) == Ok(5)
        assert await do_async(Ok(len(x)) for x in await aget_resx(False)) == Err(1)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_swap_order_produces_same_result(self) -> None:
        def foo() -> Result[int, str]:
            return Ok(1)

        async def bar() -> Result[int, str]:
            return Ok(2)

        result1: Result[int, str] = await do_async(
            Ok(x + y)
            for x in foo()
            for y in await bar()
        )

        result2: Result[int, str] = await do_async(
            Ok(x + y)
            for y in await bar()
            for x in foo()
        )

        assert result1 == result2 == Ok(3)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_all_combos_three_sources(self) -> None:
        """Exhaustive pass/fail combinations for three result sources."""

        async def aget_resx(is_suc: bool) -> Result[str, int]:
            return Ok('hello') if is_suc else Err(1)

        async def aget_resy(is_suc: bool) -> Result[bool, int]:
            return Ok(True) if is_suc else Err(2)

        def get_resz(is_suc: bool) -> Result[float, int]:
            return Ok(0.5) if is_suc else Err(3)

        async def _run(s1: bool, s2: bool, s3: bool) -> Result[float, int]:
            return await do_async(
                Ok(len(x) + int(y) + z)
                for x in await aget_resx(s1)
                for y in await aget_resy(s2)
                for z in get_resz(s3)
            )

        assert await _run(True, True, True) == Ok(6.5)
        assert await _run(True, False, True) == Err(2)
        assert await _run(False, True, True) == Err(1)
        assert await _run(False, False, True) == Err(1)
        assert await _run(True, True, False) == Err(3)
        assert await _run(True, False, False) == Err(2)
        assert await _run(False, True, False) == Err(1)
        assert await _run(False, False, False) == Err(1)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_further_processing_in_chain(self) -> None:
        """do_async supports awaiting a processing step in the chain."""

        async def aget_resx(is_suc: bool) -> Result[str, int]:
            return Ok('hello') if is_suc else Err(1)

        async def aget_resy(is_suc: bool) -> Result[bool, int]:
            return Ok(True) if is_suc else Err(2)

        def get_resz(is_suc: bool) -> Result[float, int]:
            return Ok(0.5) if is_suc else Err(3)

        async def process_xyz(x: str, y: bool, z: float) -> Result[float, int]:
            return Ok(len(x) + int(y) + z)

        async def _run(s1: bool, s2: bool, s3: bool) -> Result[float, int]:
            return await do_async(
                Ok(w)
                for x in await aget_resx(s1)
                for y in await aget_resy(s2)
                for z in get_resz(s3)
                for w in await process_xyz(x, y, z)
            )

        assert await _run(True, True, True) == Ok(6.5)
        assert await _run(True, False, True) == Err(2)
        assert await _run(False, True, True) == Err(1)
        assert await _run(True, True, False) == Err(3)


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
