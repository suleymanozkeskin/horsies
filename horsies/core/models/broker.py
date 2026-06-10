from typing import Any, ClassVar

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from horsies.core.errors import ConfigurationError, ErrorCode


class PostgresConfig(BaseModel):
    _ENGINE_KWARG_EXCLUDES: ClassVar[set[str]] = {
        'database_url',
        'session_database_url',
        'pgbouncer_transaction_mode',
        'worker_pool_size',
        'worker_max_overflow',
        'worker_child_pool_min_size',
        'worker_child_pool_max_size',
    }

    # SecretStr: repr()/model_dump() of the config must never expose
    # credentials; internal consumers unwrap via get_secret_value().
    database_url: SecretStr = Field(..., description='The URL of the PostgreSQL database')
    session_database_url: SecretStr | None = Field(
        default=None,
        description='Direct/session-capable PostgreSQL URL for LISTEN/NOTIFY and schema DDL',
    )
    pgbouncer_transaction_mode: bool = Field(
        default=False,
        description='Disable prepared statements for transaction-pooled PgBouncer URLs',
    )
    pool_pre_ping: bool = Field(
        default=True, description='Whether to pre-ping the database connection pool'
    )
    # Producer-side defaults match SQLAlchemy's (5 + 10). The previous
    # 30 + 30 let a single enqueue-only producer pin up to 60 server
    # connections; several producer processes then pressed Postgres
    # max_connections. High-throughput producers should raise these
    # explicitly. Worker-side pools are tuned separately below.
    pool_size: int = Field(
        default=5, description='The size of the database connection pool'
    )
    max_overflow: int = Field(
        default=10, description='The maximum number of connections to allow in the pool'
    )
    worker_pool_size: int | None = Field(
        default=3,
        description='Worker coordinator connection pool size; None inherits pool_size',
    )
    worker_max_overflow: int | None = Field(
        default=2,
        description='Worker coordinator overflow connections; None inherits max_overflow',
    )
    worker_child_pool_min_size: int = Field(
        default=0,
        description='Minimum connections kept by each child worker process',
    )
    worker_child_pool_max_size: int = Field(
        default=2,
        description='Maximum connections allowed per child worker process',
    )
    pool_timeout: int = Field(
        default=30, description='The timeout for acquiring a connection from the pool'
    )
    pool_recycle: int = Field(
        default=1800, description='The number of seconds to recycle connections'
    )
    echo: bool = Field(default=False, description='Whether to echo the SQL statements')

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
                    'PgBouncer transaction pooling cannot support persistent LISTEN sessions',
                ],
                help_text='use a direct/session-capable Postgres URL for LISTEN/NOTIFY',
            )
        if self.worker_pool_size is not None and self.worker_pool_size < 1:
            raise ConfigurationError(
                message='invalid worker_pool_size',
                code=ErrorCode.CONFIG_INVALID_BROKER_POOL,
                notes=['worker_pool_size must be >= 1 or None'],
                help_text='set worker_pool_size to a positive integer, or None to inherit pool_size',
            )
        if self.worker_max_overflow is not None and self.worker_max_overflow < 0:
            raise ConfigurationError(
                message='invalid worker_max_overflow',
                code=ErrorCode.CONFIG_INVALID_BROKER_POOL,
                notes=['worker_max_overflow must be >= 0 or None'],
                help_text='set worker_max_overflow to a non-negative integer, or None to inherit max_overflow',
            )
        if self.worker_child_pool_min_size < 0:
            raise ConfigurationError(
                message='invalid worker_child_pool_min_size',
                code=ErrorCode.CONFIG_INVALID_BROKER_POOL,
                notes=['worker_child_pool_min_size must be >= 0'],
                help_text='use 1 for eager per-child connections or 0 for lazy child pools',
            )
        if self.worker_child_pool_max_size < 1:
            raise ConfigurationError(
                message='invalid worker_child_pool_max_size',
                code=ErrorCode.CONFIG_INVALID_BROKER_POOL,
                notes=['worker_child_pool_max_size must be >= 1'],
                help_text='use 1 or 2 for typical workers; increase only for task code that checks out multiple framework connections concurrently',
            )
        if self.worker_child_pool_min_size > self.worker_child_pool_max_size:
            raise ConfigurationError(
                message='invalid worker child pool bounds',
                code=ErrorCode.CONFIG_INVALID_BROKER_POOL,
                notes=[
                    f'worker_child_pool_min_size={self.worker_child_pool_min_size}',
                    f'worker_child_pool_max_size={self.worker_child_pool_max_size}',
                ],
                help_text='worker_child_pool_min_size must be <= worker_child_pool_max_size',
            )
        return self

    @property
    def effective_session_database_url(self) -> str:
        url = self.session_database_url or self.database_url
        return url.get_secret_value()

    @property
    def pooled_connect_args(self) -> dict[str, object]:
        if self.pgbouncer_transaction_mode:
            return {'prepare_threshold': None}
        return {}

    def sqlalchemy_engine_kwargs(self) -> dict[str, Any]:
        """Return only kwargs accepted by SQLAlchemy's engine constructor."""
        return self.model_dump(
            exclude=self._ENGINE_KWARG_EXCLUDES,
            exclude_none=True,
        )

    def worker_runtime_config(self) -> 'PostgresConfig':
        """Return a broker config with worker-specific parent pool sizing."""
        updates: dict[str, int] = {}
        if self.worker_pool_size is not None:
            updates['pool_size'] = self.worker_pool_size
        if self.worker_max_overflow is not None:
            updates['max_overflow'] = self.worker_max_overflow
        return self.model_copy(update=updates)
