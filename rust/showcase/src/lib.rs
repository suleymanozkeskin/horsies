//! Acme Clothing showcase domain, database store, and settings.

pub mod domain;
pub mod settings;
pub mod store;

pub use settings::{resolve_database_settings, DatabaseSettings, SettingsError};
pub use store::{ensure_database, ensure_schema, Store, StoreError, StoreResult};
