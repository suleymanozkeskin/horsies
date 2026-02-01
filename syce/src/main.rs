use clap::Parser;
use cli::Cli;

use crate::app::App;
use crate::errors::Result;

mod action;
mod app;
mod cli;
mod components;
mod db;
mod errors;
mod models;
mod state;
mod theme;
mod tui;

#[tokio::main]
async fn main() -> Result<()> {
    crate::errors::init()?;

    let args = Cli::parse();
    let mut app = App::new(args.tick_rate, args.frame_rate, args.database_url).await?;
    app.run().await?;
    Ok(())
}
