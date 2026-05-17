use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Cli {
    /// Database URL to connect to (postgresql://...)
    #[arg(short, long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Direct/session-capable database URL for LISTEN/NOTIFY
    #[arg(long, env = "HORSIES_SESSION_DATABASE_URL")]
    pub session_database_url: Option<String>,

    /// Disable prepared statement caching for transaction-pooled PgBouncer
    #[arg(long, env = "HORSIES_PGBOUNCER_TRANSACTION_MODE")]
    pub pgbouncer_transaction_mode: bool,

    /// Tick rate, i.e. number of ticks per second
    #[arg(short, long, value_name = "FLOAT", default_value_t = 4.0)]
    pub tick_rate: f64,

    /// Frame rate, i.e. number of frames per second
    #[arg(short, long, value_name = "FLOAT", default_value_t = 60.0)]
    pub frame_rate: f64,
}
