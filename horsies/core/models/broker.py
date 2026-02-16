from pydantic import BaseModel, Field, field_validator
from horsies.core.errors import ConfigurationError, ErrorCode


class PostgresConfig(BaseModel):
    database_url: str = Field(..., description='The URL of the PostgreSQL database')
    pool_pre_ping: bool = Field(
        default=True, description='Whether to pre-ping the database connection pool'
    )
    pool_size: int = Field(
        default=30, description='The size of the database connection pool'
    )
    max_overflow: int = Field(
        default=30, description='The maximum number of connections to allow in the pool'
    )
    pool_timeout: int = Field(
        default=30, description='The timeout for acquiring a connection from the pool'
    )
    pool_recycle: int = Field(
        default=1800, description='The number of seconds to recycle connections'
    )
    echo: bool = Field(default=False, description='Whether to echo the SQL statements')

    @field_validator('database_url')
    def validate_database_url(cls, v: str) -> str:
        if not v.startswith('postgresql+psycopg'):
            raise ConfigurationError(
                message='invalid database URL scheme',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    f"got: {v.split('://')[0] if '://' in v else v[:20]}://...",
                    'horsies only supports psycopg3 (async PostgreSQL driver)',
                ],
                help_text="use 'postgresql+psycopg://user:pass@host/db'",
            )
        return v
