"""The database-section mapping that the producer and worker manifest builders share.

`clients/python/horsies/_catalog/manifest.py` and `clients/python/horsies/_catalog/worker_manifest.py` are
generated as two modules, so each has its own classes for the same Rust types
(`manifest.PoolRecycleNever` is not `worker_manifest.PoolRecycleNever`). The
rules that turn the validated `PostgresConfig` values (the session database,
`pool_recycle`, `tcp_keepalives`, `tcp_user_timeout`) into these types are
written once here. Each function takes the constructors of the target module
as keyword arguments and returns that module's type; the builders pass their
generated classes (`clients/python/horsies/core/manifest.py`, `clients/python/horsies/core/worker_manifest.py`).

Pydantic refuses every user error first, so a value that breaks a rule here is
a defect: it raises `AssertionError`. Every function is pure.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final, Protocol, assert_never

from horsies.core.models.broker import (
    PoolRecycleAfterSeconds,
    PoolRecycleNever,
    SessionDatabaseSameAsDatabaseUrl,
    SessionDatabaseSeparate,
    TcpKeepalivesOff,
    TcpKeepalivesOn,
    TcpUserTimeoutAfter,
    TcpUserTimeoutSystemDefault,
)
from horsies.core.types.result import Err, Ok
from horsies.core.utils.url import check_database_url_scheme

CORE_URL_SCHEME: Final = 'postgresql://'
"""The scheme that the core's connection layer reads."""


def core_database_url(url: str) -> str:
    """`url` with the scheme that the core reads: `postgres://` becomes
    `postgresql://`, and the rest is kept as it is.

    The `PostgresConfig` validators admit only `postgresql://` and
    `postgres://` (`check_database_url_scheme`), so another URL here is a
    defect: it raises `AssertionError`, which names the refusal and never
    the rest of the URL (it can hold a password).
    """
    match check_database_url_scheme(url):
        case Ok():
            _, _, rest = url.partition('://')
            return CORE_URL_SCHEME + rest
        case Err(refusal):
            raise AssertionError(
                f'a validated database URL has the scheme postgresql or '
                f'postgres; got {refusal!r}'
            )


class SeparateSessionClass[S](Protocol):
    """The constructor of the separate session database variant."""

    def __call__(self, *, url: str) -> S: ...


class PoolRecycleAfterSecondsClass[R](Protocol):
    """The constructor of the pool recycle variant with an age in seconds."""

    def __call__(self, *, seconds: int) -> R: ...


class KeepalivesOnClass[K](Protocol):
    """The constructor of the keepalives-on variant with its three values."""

    def __call__(
        self, *, idle_seconds: int, interval_seconds: int, probes: int
    ) -> K: ...


class UserTimeoutAfterClass[T](Protocol):
    """The constructor of the TCP user timeout variant with milliseconds."""

    def __call__(self, *, milliseconds: int) -> T: ...


def session_database_with[S](
    session_database: SessionDatabaseSameAsDatabaseUrl | SessionDatabaseSeparate,
    *,
    same_as_database_url: Callable[[], S],
    separate: SeparateSessionClass[S],
) -> S:
    """`SessionDatabaseSameAsDatabaseUrl` is the main URL; a separate session
    database is the separate variant with the scheme that the core reads
    (`core_database_url`)."""
    match session_database:
        case SessionDatabaseSameAsDatabaseUrl():
            return same_as_database_url()
        case SessionDatabaseSeparate(url=url):
            return separate(url=core_database_url(url.get_secret_value()))
        case _ as unreachable:
            assert_never(unreachable)


def pool_recycle_with[R](
    pool_recycle: PoolRecycleNever | PoolRecycleAfterSeconds,
    *,
    never: Callable[[], R],
    after_seconds: PoolRecycleAfterSecondsClass[R],
) -> R:
    """The manifest form of a validated `pool_recycle`.

    The `PostgresConfig` validator admits only an age of at least 0, so
    another value is a defect and raises `AssertionError`.
    """
    match pool_recycle:
        case PoolRecycleNever():
            return never()
        case PoolRecycleAfterSeconds(seconds=seconds):
            assert seconds >= 0, (
                f'pool_recycle.seconds={seconds} passed the PostgresConfig '
                'validator, which admits only a number of seconds of at least 0'
            )
            return after_seconds(seconds=seconds)
        case _ as unreachable:
            assert_never(unreachable)


def tcp_keepalives_with[K](
    keepalives: TcpKeepalivesOff | TcpKeepalivesOn,
    *,
    off: Callable[[], K],
    on: KeepalivesOnClass[K],
) -> K:
    """The manifest form of the validated keepalive settings: off, or on
    with the idle time, the interval and the probe count.

    The `PostgresConfig` validator admits only values of at least 1, so
    another value is a defect and raises `AssertionError`.
    """
    match keepalives:
        case TcpKeepalivesOff():
            return off()
        case TcpKeepalivesOn(
            idle_seconds=idle_seconds,
            interval_seconds=interval_seconds,
            probes=probes,
        ):
            values = (idle_seconds, interval_seconds, probes)
            assert min(values) >= 1, (
                f'keepalive values {values} passed the PostgresConfig validator, '
                'which admits only values of at least 1'
            )
            return on(
                idle_seconds=idle_seconds,
                interval_seconds=interval_seconds,
                probes=probes,
            )
        case _ as unreachable:
            assert_never(unreachable)


def tcp_user_timeout_with[T](
    tcp_user_timeout: TcpUserTimeoutSystemDefault | TcpUserTimeoutAfter,
    *,
    system_default: Callable[[], T],
    after: UserTimeoutAfterClass[T],
) -> T:
    """The manifest form of a validated `tcp_user_timeout`.

    The `PostgresConfig` validator admits only milliseconds of at least 1,
    so another value is a defect and raises `AssertionError`.
    """
    match tcp_user_timeout:
        case TcpUserTimeoutSystemDefault():
            return system_default()
        case TcpUserTimeoutAfter(milliseconds=milliseconds):
            assert milliseconds > 0, (
                f'tcp_user_timeout.milliseconds={milliseconds} passed the '
                'PostgresConfig validator, which admits only values of at least 1'
            )
            return after(milliseconds=milliseconds)
        case _ as unreachable:
            assert_never(unreachable)
