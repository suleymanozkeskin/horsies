//! Acme Clothing showcase domain, database store, and settings.

pub mod app;
pub mod domain;
pub mod settings;
pub mod simulate;
pub mod store;
pub mod tasks;
pub mod tuning;
pub mod workflows;

pub use settings::{resolve_database_settings, DatabaseSettings, SettingsError};
pub use store::{ensure_database, ensure_schema, Store, StoreError, StoreResult};
