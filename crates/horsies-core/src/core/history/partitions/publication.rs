//! Seam between partition lifecycle and the staged-reader publisher.

use std::future::Future;

use sqlx::PgConnection;

use crate::core::history::errors::HistoryError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoaderRepublished {
    pub absent_leaves: Vec<String>,
}

pub trait LoaderPublication: Send + Sync {
    fn republish(
        &self,
        connection: &mut PgConnection,
    ) -> impl Future<Output = Result<LoaderRepublished, HistoryError>> + Send;

    fn references_leaf(
        &self,
        connection: &mut PgConnection,
        leaf_name: &str,
    ) -> impl Future<Output = Result<bool, HistoryError>> + Send;

    fn needs_republication(
        &self,
        _connection: &mut PgConnection,
    ) -> impl Future<Output = Result<bool, HistoryError>> + Send {
        async { Ok(false) }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnpublishedLoader;

impl LoaderPublication for UnpublishedLoader {
    async fn republish(
        &self,
        _connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, HistoryError> {
        Ok(LoaderRepublished {
            absent_leaves: Vec::new(),
        })
    }

    async fn references_leaf(
        &self,
        _connection: &mut PgConnection,
        _leaf_name: &str,
    ) -> Result<bool, HistoryError> {
        Ok(false)
    }
}
