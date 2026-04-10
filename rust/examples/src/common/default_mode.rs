use horsies::AppConfig;

/// Create an AppConfig with QueueMode::Default (single "default" queue).
///
/// This is the equivalent of the Python `instance.py`.
pub fn app_config(db_url: &str) -> AppConfig {
    AppConfig::for_database_url(db_url)
}
