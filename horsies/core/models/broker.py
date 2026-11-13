from collections.abc import Mapping
from typing import Any, ClassVar, Final, cast

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator
from horsies.core.defaults import DEFAULT_POOL_RECYCLE_S, DEFAULT_TCP_USER_TIMEOUT_MS
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    ValidationReport,
    raise_collected,
)

MOVED_TO_WORKER_PROFILE: Final[Mapping[str, str]] = {
    'worker_child_pool_min_size': 'min_size=N',
    'worker_child_pool_max_size': 'max_size=PoolSize(connections=N)',
    'worker_child_pool_check': 'check=False',
}
"""The child pool fields that 0.7.0 removed from ``PostgresConfig``, and the
argument of the pool of each child kind in a worker profile
(``IsolatedPool``, ``SharedPool``) that replaces each one."""

MOVED_TO_PARENT_POOL: Final[Mapping[str, str]] = {
    'worker_pool_size': (
        'the worker process opens its pool with the resolved pool.max_size '
        'of its worker profile'
    ),
    'worker_max_overflow': (
        'the pool of the worker process has no overflow: it holds at most '
        'pool.max_size connections'
    ),
}
"""The worker pool fields that 0.7.0 removed from ``PostgresConfig``, and the
note of the refusal of each one. ``WorkerProfile.pool`` replaces both."""


class PostgresConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')

    _ENGINE_KWARG_EXCLUDES: ClassVar[set[str]] = {
        'database_url',
        'session_database_url',
        'pgbouncer_transaction_mode',
        'retain_rerun_input_default',
        'pool_pre_ping',
        'worker_pool_pre_ping',
        'tcp_keepalives',
        'tcp_keepalives_idle',
        'tcp_keepalives_interval',
        'tcp_keepalives_count',
        'tcp_user_timeout_ms',
    }

    # SecretStr: repr()/model_dump() of the config must never expose
    # credentials; internal consumers unwrap via get_secret_value().
    database_url: SecretStr = Field(..., description='The URL of the PostgreSQL database')
    session_database_url: SecretStr | None = Field(
        default=None,
        description=(
            'Direct/session-capable PostgreSQL URL for schema DDL, partition '
            'maintenance, and LISTEN/NOTIFY'
        ),
    )
    pgbouncer_transaction_mode: bool = Field(
        default=False,
        description='Disable prepared statements for transaction-pooled PgBouncer URLs',
    )
    # The ping runs at checkout only for a connection that waited in the
    # pool for longer than PRE_PING_AFTER_IDLE_S (register_idle_ping). A
    # connection that dies within that time of its check-in fails the next
    # statement on it, as one that dies after a ping does.
    pool_pre_ping: bool = Field(
        default=True,
        description=(
            'Whether to ping a pooled connection at checkout when it waited '
            'in the pool for longer than PRE_PING_AFTER_IDLE_S (1 s)'
        ),
    )
    # The pre-ping of the worker process's engine (worker_process_config).
    # The worker sends most of its writes as one statement in autocommit:
    # one round trip each. A pre-ping adds a round trip to a checkout of a
    # connection that waited in the pool for longer than
    # PRE_PING_AFTER_IDLE_S. With it off, a dead pooled connection shows at the first
    # statement instead, as a connection error: finalize retries its phase
    # and classifies the replay, the claim loop retries the pass. TCP
    # keepalives and tcp_user_timeout_ms still apply.
    worker_pool_pre_ping: bool = Field(
        default=False,
        description=(
            'Whether the worker process pre-pings its pooled connections; '
            'pool_pre_ping applies to other engines'
        ),
    )
    # Producer-side defaults match SQLAlchemy's (5 + 10). The previous
    # 30 + 30 let a single enqueue-only producer pin up to 60 server
    # connections; several producer processes then pressed Postgres
    # max_connections. High-throughput producers should raise these
    # explicitly. A worker process sizes its own pool in its worker profile
    # (WorkerProfile.pool), and task code in a worker child opens the
    # task_send_pool of its child kind.
    pool_size: int = Field(
        default=5, description='The size of the database connection pool'
    )
    max_overflow: int = Field(
        default=10, description='The maximum number of connections to allow in the pool'
    )
    retain_rerun_input_default: bool = Field(
        default=False,
        description=(
            'App-level default for enqueue-time rerun-input retention; '
            'a per-task value overrides it, and the resolved value is '
            'snapshotted at enqueue'
        ),
    )
    pool_timeout: int = Field(
        default=30, description='The timeout for acquiring a connection from the pool'
    )
    pool_recycle: int = Field(
        default=DEFAULT_POOL_RECYCLE_S,
        description='The number of seconds to recycle connections',
    )
    # TCP keepalives keep idle pooled sockets warm so a server-side or
    # middlebox idle-reap is detected and recycled at the socket layer
    # instead of surfacing as a mid-query OperationalError. pool_pre_ping
    # and pool_recycle are checkout-time guards and cannot catch a
    # connection that dies in-flight. These apply to the broker engine pool
    # and the per-child connection pool; the LISTEN/NOTIFY listener sets its
    # own keepalives independently.
    tcp_keepalives: bool = Field(
        default=True,
        description='Enable libpq TCP keepalives on broker and child-process connections',
    )
    tcp_keepalives_idle: int = Field(
        default=30,
        description='Idle seconds before the first keepalive probe (libpq keepalives_idle)',
    )
    tcp_keepalives_interval: int = Field(
        default=10,
        description='Seconds between keepalive probes (libpq keepalives_interval)',
    )
    tcp_keepalives_count: int = Field(
        default=3,
        description='Unacknowledged probes before the connection is dropped (libpq keepalives_count)',
    )
    # Keepalives probe only an idle connection. While sent data waits for
    # an acknowledgement, the kernel retransmission limit decides instead,
    # and a write to a dead connection (no reset from the server side)
    # blocks for about 15 minutes on Linux. tcp_user_timeout bounds that
    # wait. It does not affect a long query: the client's sent data is
    # acknowledged while it waits for the result.
    tcp_user_timeout_ms: int = Field(
        default=DEFAULT_TCP_USER_TIMEOUT_MS,
        description=(
            'Milliseconds that sent data may stay unacknowledged before the '
            'connection is closed (libpq tcp_user_timeout); 0 uses the '
            'operating system default'
        ),
    )
    echo: bool = Field(default=False, description='Whether to echo the SQL statements')

    @model_validator(mode='before')
    @classmethod
    def reject_moved_pool_fields(cls, data: object) -> object:
        """Fail closed on the pool fields that moved to the worker profiles.

        In 0.7.0 each child kind has its own pool
        (``IsolatedChildren.pool``, ``SharedAsyncChildren.pool``,
        ``SharedThreadChildren.pool``), and the worker process has its own
        (``WorkerProfile.pool``). A config that still names a removed field
        is refused, never silently ignored. Every such field is reported at
        once, so one edit fixes the config.
        """
        if not isinstance(data, dict):
            return data
        fields = cast('dict[str, object]', data)
        report = ValidationReport('broker')
        for name, pool_argument in MOVED_TO_WORKER_PROFILE.items():
            if name in fields:
                report.add(_moved_child_pool_field_error(name, pool_argument))
        for name, note in MOVED_TO_PARENT_POOL.items():
            if name in fields:
                report.add(_moved_parent_pool_field_error(name, note))
        raise_collected(report)
        return fields

    @field_validator('database_url')
    def validate_database_url(cls, v: SecretStr) -> SecretStr:
        raw = v.get_secret_value()
        if not raw.startswith('postgresql+psycopg://'):
            raise ConfigurationError(
                message='invalid database URL scheme',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    f"got: {raw.split('://')[0] if '://' in raw else raw[:20]}://...",
                    'horsies only supports psycopg3 (async PostgreSQL driver)',
                ],
                help_text="use 'postgresql+psycopg://user:pass@host/db'",
            )
        return v

    @field_validator('session_database_url')
    def validate_session_database_url(cls, v: SecretStr | None) -> SecretStr | None:
        if v is None:
            return v
        raw = v.get_secret_value()
        if not raw.startswith('postgresql+psycopg://'):
            raise ConfigurationError(
                message='invalid session database URL scheme',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    f"got: {raw.split('://')[0] if '://' in raw else raw[:20]}://...",
                    'session_database_url must use the psycopg3 PostgreSQL driver',
                ],
                help_text="use 'postgresql+psycopg://user:pass@host/db'",
            )
        return v

    @model_validator(mode='after')
    def validate_pgbouncer_configuration(self) -> 'PostgresConfig':
        if self.pgbouncer_transaction_mode and self.session_database_url is None:
            raise ConfigurationError(
                message='session_database_url required when pgbouncer_transaction_mode=True',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    'PgBouncer transaction pooling cannot support session locks or persistent LISTEN sessions',
                ],
                help_text='use a direct/session-capable Postgres URL for session_database_url',
            )
        if (
            self.pgbouncer_transaction_mode
            and self.session_database_url == self.database_url
        ):
            raise ConfigurationError(
                message='session_database_url must not use the transaction-pool URL',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    'partition maintenance and LISTEN require PostgreSQL session affinity',
                ],
                help_text='use a direct/session-capable Postgres URL for session_database_url',
            )
        if self.tcp_keepalives:
            for name, value in (
                ('tcp_keepalives_idle', self.tcp_keepalives_idle),
                ('tcp_keepalives_interval', self.tcp_keepalives_interval),
                ('tcp_keepalives_count', self.tcp_keepalives_count),
            ):
                if value < 1:
                    raise ConfigurationError(
                        message=f'invalid {name}',
                        code=ErrorCode.CONFIG_INVALID_KEEPALIVE,
                        notes=[f'{name}={value}', f'{name} must be >= 1 when tcp_keepalives is True'],
                        help_text='use positive second/count values, or set tcp_keepalives=False to disable',
                    )
        if self.tcp_user_timeout_ms < 0:
            raise ConfigurationError(
                message='invalid tcp_user_timeout_ms',
                code=ErrorCode.CONFIG_INVALID_KEEPALIVE,
                notes=[
                    f'tcp_user_timeout_ms={self.tcp_user_timeout_ms}',
                    'tcp_user_timeout_ms must be >= 0',
                ],
                help_text='use a positive millisecond value, or 0 for the operating system default',
            )
        return self

    @property
    def effective_session_database_url(self) -> str:
        url = self.session_database_url or self.database_url
        return url.get_secret_value()

    def keepalive_connect_args(self) -> dict[str, int]:
        """libpq keepalive params for psycopg connections, empty when disabled."""
        if not self.tcp_keepalives:
            return {}
        return {
            'keepalives': 1,
            'keepalives_idle': self.tcp_keepalives_idle,
            'keepalives_interval': self.tcp_keepalives_interval,
            'keepalives_count': self.tcp_keepalives_count,
        }

    def tcp_connect_args(self) -> dict[str, int]:
        """libpq TCP params for psycopg connections: the keepalive params
        and tcp_user_timeout."""
        return {
            **self.keepalive_connect_args(),
            'tcp_user_timeout': self.tcp_user_timeout_ms,
        }

    @property
    def pooled_connect_args(self) -> dict[str, object]:
        """psycopg connect_args for pooled broker/child connections.

        Merges the TCP params with the PgBouncer transaction-mode knob
        (prepared statements disabled). Both are valid psycopg connect
        kwargs and coexist.
        """
        args: dict[str, object] = dict(self.tcp_connect_args())
        if self.pgbouncer_transaction_mode:
            args['prepare_threshold'] = None
        return args

    def sqlalchemy_engine_kwargs(self) -> dict[str, Any]:
        """Return only kwargs accepted by SQLAlchemy's engine constructor.

        ``hide_parameters=True`` is fixed: bound values are task arguments
        and results, so statement error text and ``echo`` logs show the
        statement without them. The flag changes the text only; the
        exception's ``params`` attribute still holds the values. Postgres's
        own error text is not affected: a constraint violation's
        ``DETAIL: Failing row contains (...)`` shows column values, each cut
        to 64 bytes.
        """
        return {
            **self.model_dump(
                exclude=self._ENGINE_KWARG_EXCLUDES,
                exclude_none=True,
            ),
            'hide_parameters': True,
        }

    def with_exact_pool(self, connections: int) -> 'PostgresConfig':
        """A copy whose engine pool holds up to ``connections`` connections
        with no overflow: ``pool_size = connections``, ``max_overflow = 0``.
        Every other field, ``pool_pre_ping`` included, is kept.

        A worker child opens the broker of its task code with the
        ``task_send_pool`` of its kind, a producer engine that keeps
        ``pool_pre_ping``. ``connections`` is 1 or more, from a validated
        profile. Below 1 is a defect: ``AssertionError``.
        """
        assert connections >= 1, f'connections must be >= 1; got {connections}'
        return self.model_copy(
            update={'pool_size': connections, 'max_overflow': 0},
        )

    def worker_process_config(self, parent_pool: int) -> 'PostgresConfig':
        """The broker config of the worker process: a pool of up to
        ``parent_pool`` connections with no overflow (``with_exact_pool``),
        the resolved ``WorkerProfile.pool``, and ``worker_pool_pre_ping`` as
        its pre-ping. ``parent_pool`` is 1 or more. Below 1 is a defect:
        ``AssertionError``."""
        return self.with_exact_pool(parent_pool).model_copy(
            update={'pool_pre_ping': self.worker_pool_pre_ping},
        )


def _moved_child_pool_field_error(name: str, pool_argument: str) -> ConfigurationError:
    """The refusal of a removed child pool field: it names the pool field of
    each child kind in a worker profile that replaces it."""
    return ConfigurationError(
        message=f'{name} was removed in 0.7.0',
        code=ErrorCode.CONFIG_INVALID_BROKER_POOL,
        notes=[
            'each child kind of a worker profile has its own connection pool',
        ],
        help_text=(
            'set the pool of each child kind in AppConfig.worker_profiles: '
            f'IsolatedChildren(pool=IsolatedPool({pool_argument})), '
            f'SharedAsyncChildren(pool=SharedPool({pool_argument})) or '
            f'SharedThreadChildren(pool=SharedPool({pool_argument}))'
        ),
    )


def _moved_parent_pool_field_error(name: str, note: str) -> ConfigurationError:
    """The refusal of a removed worker pool field: it names the pool of the
    worker process in a worker profile that replaces it."""
    return ConfigurationError(
        message=f'{name} was removed in 0.7.0',
        code=ErrorCode.CONFIG_INVALID_BROKER_POOL,
        notes=[
            note,
            'task code in a worker child that sends opens the task_send_pool '
            'of its child kind, not this pool',
        ],
        help_text=(
            'set the pool of the worker process in AppConfig.worker_profiles: '
            'WorkerProfile(pool=ParentPool(max_size=PoolSize(connections=N))); '
            'the default, PoolSizeFromSeats(), derives it from the seats of '
            'the profile'
        ),
    )
