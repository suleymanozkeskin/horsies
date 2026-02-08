use std::io;

#[derive(thiserror::Error, Debug)]
#[allow(dead_code)]
pub enum SyceError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Terminal error: {0}")]
    Terminal(String),

    #[error("Parse error: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, SyceError>;

/// Initialize error handling
pub fn init() -> Result<()> {
    // Simple panic handler that restores terminal
    std::panic::set_hook(Box::new(move |panic_info| {
        // Try to restore terminal on panic
        if let Ok(mut t) = crate::tui::Tui::new() {
            let _ = t.exit();
        }

        eprintln!("{}", panic_info);
        std::process::exit(1);
    }));

    Ok(())
}
