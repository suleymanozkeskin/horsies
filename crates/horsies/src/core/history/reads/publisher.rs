//! Atomic publication of the staged reader triple and its probe manifest.

use std::collections::HashSet;

use sqlx::PgConnection;

use crate::core::history::errors::HistoryError;
use crate::core::history::names::{
    HEARTBEAT_CLASS_KEY, LEAF_CATALOG, TASK_LOOKUP_MANIFEST, TASK_PROVENANCE_FUNCTION,
};
use crate::core::history::partitions::catalog::read_manifest_leaf_rows;
use crate::core::history::partitions::publication::{LoaderPublication, LoaderRepublished};

use super::detail::staged_detail_published;
use super::lookup_generation::{
    manifest_from_catalog, render_staged_detail_function, render_staged_lookup_function,
    render_staged_provenance_function, LookupManifest,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct StagedLoaderPublisher;

impl LoaderPublication for StagedLoaderPublisher {
    async fn republish(
        &self,
        connection: &mut PgConnection,
    ) -> Result<LoaderRepublished, HistoryError> {
        let selection = read_manifest_leaf_rows(connection).await?;
        let absent: HashSet<String> = selection.absent_relations.iter().cloned().collect();
        let manifest = manifest_from_catalog(&selection.attached, &absent)?;
        sqlx::query(&render_staged_lookup_function(&manifest))
            .execute(&mut *connection)
            .await?;
        sqlx::query(&format!(
            "DROP FUNCTION IF EXISTS {TASK_PROVENANCE_FUNCTION}(uuid)"
        ))
        .execute(&mut *connection)
        .await?;
        sqlx::query(&render_staged_provenance_function(&manifest))
            .execute(&mut *connection)
            .await?;
        sqlx::query(&render_staged_detail_function(&manifest))
            .execute(&mut *connection)
            .await?;
        rewrite_manifest_table(connection, &manifest).await?;

        let mut absent_leaves = selection.absent_relations;
        absent_leaves.sort();
        Ok(LoaderRepublished { absent_leaves })
    }

    async fn references_leaf(
        &self,
        connection: &mut PgConnection,
        leaf_name: &str,
    ) -> Result<bool, HistoryError> {
        let sql = format!(
            "SELECT EXISTS (
                 SELECT 1 FROM {TASK_LOOKUP_MANIFEST} WHERE leaf_name = $1
             )"
        );
        Ok(sqlx::query_scalar(&sql)
            .bind(leaf_name)
            .fetch_one(connection)
            .await?)
    }

    async fn needs_republication(
        &self,
        connection: &mut PgConnection,
    ) -> Result<bool, HistoryError> {
        if !staged_detail_published(connection).await? {
            return Ok(true);
        }
        Ok(!published_manifest_matches_catalog(connection).await?)
    }
}

async fn published_manifest_matches_catalog(
    connection: &mut PgConnection,
) -> Result<bool, HistoryError> {
    let sql = format!(
        "WITH expected AS (
             SELECT
                 leaf_name,
                 row_number() OVER (ORDER BY lower_anchor, leaf_name) - 1
                     AS probe_position,
                 lower_anchor,
                 upper_anchor,
                 min_birth_at
             FROM {LEAF_CATALOG}
             WHERE detached_at IS NULL
               AND dropped_at IS NULL
               AND class_key <> $1
               AND to_regclass(leaf_name) IS NOT NULL
         ),
         difference AS (
             (
                 SELECT leaf_name, probe_position, lower_anchor, upper_anchor, min_birth_at
                 FROM expected
                 EXCEPT
                 SELECT leaf_name, probe_position::bigint, lower_anchor, upper_anchor, min_birth_at
                 FROM {TASK_LOOKUP_MANIFEST}
             )
             UNION ALL
             (
                 SELECT leaf_name, probe_position::bigint, lower_anchor, upper_anchor, min_birth_at
                 FROM {TASK_LOOKUP_MANIFEST}
                 EXCEPT
                 SELECT leaf_name, probe_position, lower_anchor, upper_anchor, min_birth_at
                 FROM expected
             )
         )
         SELECT NOT EXISTS (SELECT 1 FROM difference)"
    );
    Ok(sqlx::query_scalar(&sql)
        .bind(HEARTBEAT_CLASS_KEY)
        .fetch_one(connection)
        .await?)
}

pub async fn published_manifest_absent_leaves(
    connection: &mut PgConnection,
) -> Result<Vec<String>, HistoryError> {
    let sql = format!(
        "SELECT leaf_name FROM {TASK_LOOKUP_MANIFEST}
         WHERE to_regclass(leaf_name) IS NULL ORDER BY leaf_name"
    );
    Ok(sqlx::query_scalar(&sql).fetch_all(connection).await?)
}

async fn rewrite_manifest_table(
    connection: &mut PgConnection,
    manifest: &LookupManifest,
) -> Result<(), HistoryError> {
    sqlx::query(&format!("DELETE FROM {TASK_LOOKUP_MANIFEST}"))
        .execute(&mut *connection)
        .await?;
    let sql = format!(
        "INSERT INTO {TASK_LOOKUP_MANIFEST} (
             leaf_name, probe_position, lower_anchor, upper_anchor,
             min_birth_at, published_at
         ) VALUES ($1, $2, $3, $4, $5, statement_timestamp())"
    );
    for (position, leaf) in manifest.leaves().iter().enumerate() {
        let position = i32::try_from(position)
            .map_err(|_| HistoryError::contract("lookup manifest exceeds integer positions"))?;
        sqlx::query(&sql)
            .bind(leaf.relation_name())
            .bind(position)
            .bind(leaf.lower_anchor())
            .bind(leaf.upper_anchor())
            .bind(leaf.min_birth_at())
            .execute(&mut *connection)
            .await?;
    }
    Ok(())
}
