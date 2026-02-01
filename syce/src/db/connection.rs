use crate::errors::Result;
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Database connection pool manager
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create a new database connection pool
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;

        Ok(Self { pool })
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Close the connection pool
    pub async fn close(self) {
        self.pool.close().await;
    }
}
