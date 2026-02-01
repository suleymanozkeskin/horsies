use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Cli {
    /// Database URL to connect to (postgresql://...)
    #[arg(short, long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Tick rate, i.e. number of ticks per second
    #[arg(short, long, value_name = "FLOAT", default_value_t = 4.0)]
    pub tick_rate: f64,

    /// Frame rate, i.e. number of frames per second
    #[arg(short, long, value_name = "FLOAT", default_value_t = 60.0)]
    pub frame_rate: f64,
}
