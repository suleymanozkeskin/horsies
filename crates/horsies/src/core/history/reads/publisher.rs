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
    let mut names = Vec::with_capacity(manifest.leaves().len());
    let mut positions = Vec::with_capacity(manifest.leaves().len());
    let mut lowers = Vec::with_capacity(manifest.leaves().len());
    let mut uppers = Vec::with_capacity(manifest.leaves().len());
    let mut births = Vec::with_capacity(manifest.leaves().len());
    for (position, leaf) in manifest.leaves().iter().enumerate() {
        let position = i32::try_from(position)
            .map_err(|_| HistoryError::contract("lookup manifest exceeds integer positions"))?;
        names.push(leaf.relation_name());
        positions.push(position);
        lowers.push(leaf.lower_anchor());
        uppers.push(leaf.upper_anchor());
        births.push(leaf.min_birth_at());
    }
    sqlx::query(&format!("DELETE FROM {TASK_LOOKUP_MANIFEST}"))
        .execute(&mut *connection)
        .await?;
    sqlx::query(&format!(
        "INSERT INTO {TASK_LOOKUP_MANIFEST} (
             leaf_name, probe_position, lower_anchor, upper_anchor,
             min_birth_at, published_at
         ) SELECT leaf_name, probe_position, lower_anchor, upper_anchor,
                  min_birth_at, statement_timestamp()
           FROM UNNEST($1::text[], $2::int[], $3::timestamptz[],
                       $4::timestamptz[], $5::timestamptz[])
             AS leaves(leaf_name, probe_position, lower_anchor, upper_anchor, min_birth_at)"
    ))
    .bind(names)
    .bind(positions)
    .bind(lowers)
    .bind(uppers)
    .bind(births)
    .execute(connection)
    .await?;
    Ok(())
}
#[cfg(test)]
mod publication_contract_tests {
    use super::*;
    use crate::core::history::reads::lookup_generation::LookupLeaf;
    use chrono::{Duration, Utc};
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn bulk_manifest_preserves_rows_order_and_one_publication_timestamp() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let anchor = chrono::DateTime::from_timestamp(Utc::now().timestamp(), 0).unwrap();
        let mut tx = pool.begin().await.unwrap();
        for count in [0, 1, 60, 240] {
            let leaves = (0..count)
                .map(|i| {
                    LookupLeaf::new(
                        format!("manifest_contract_{i}"),
                        anchor + Duration::days(i),
                        anchor + Duration::days(i + 1),
                        match i % 2 {
                            0 => None,
                            _ => Some(anchor - Duration::days(i + 1)),
                        },
                    )
                    .unwrap()
                })
                .collect();
            let manifest = LookupManifest::new(leaves, None).unwrap();
            rewrite_manifest_table(tx.as_mut(), &manifest)
                .await
                .unwrap();
            let rows: Vec<(String, i32, chrono::DateTime<Utc>, chrono::DateTime<Utc>, Option<chrono::DateTime<Utc>>)> = sqlx::query_as(
                "SELECT leaf_name, probe_position, lower_anchor, upper_anchor, min_birth_at FROM horsies_task_lookup_manifest ORDER BY probe_position",
            ).fetch_all(tx.as_mut()).await.unwrap();
            assert_eq!(rows.len(), count as usize);
            for (position, (row, leaf)) in rows.iter().zip(manifest.leaves()).enumerate() {
                assert_eq!(
                    row,
                    &(
                        leaf.relation_name().to_owned(),
                        position as i32,
                        leaf.lower_anchor(),
                        leaf.upper_anchor(),
                        leaf.min_birth_at()
                    )
                );
            }
            let timestamps: i64 = sqlx::query_scalar(
                "SELECT count(DISTINCT published_at) FROM horsies_task_lookup_manifest",
            )
            .fetch_one(tx.as_mut())
            .await
            .unwrap();
            assert_eq!(timestamps, i64::from(count > 0));
        }
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn failed_publication_restores_manifest_and_reader_definitions_on_rollback() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut tx = pool.begin().await.unwrap();
        let manifest_sql = "SELECT jsonb_agg(to_jsonb(m) ORDER BY probe_position) FROM horsies_task_lookup_manifest m";
        let before: serde_json::Value = sqlx::query_scalar(manifest_sql)
            .fetch_one(tx.as_mut())
            .await
            .unwrap();
        assert!(before.as_array().unwrap().len() > 1);
        let readers_sql = "SELECT jsonb_agg(jsonb_build_array(proname, pg_get_functiondef(oid)) ORDER BY proname) FROM pg_proc WHERE proname IN ('horsies_task_lookup_staged', 'horsies_task_provenance_staged', 'horsies_task_detail_staged')";
        let readers_before: serde_json::Value = sqlx::query_scalar(readers_sql)
            .fetch_one(tx.as_mut())
            .await
            .unwrap();
        sqlx::query("ALTER TABLE horsies_task_lookup_manifest ADD CONSTRAINT test_reject_manifest CHECK (probe_position < 1) NOT VALID").execute(tx.as_mut()).await.unwrap();
        sqlx::query("SAVEPOINT publication_attempt")
            .execute(tx.as_mut())
            .await
            .unwrap();
        assert!(StagedLoaderPublisher.republish(tx.as_mut()).await.is_err());
        sqlx::query("ROLLBACK TO SAVEPOINT publication_attempt")
            .execute(tx.as_mut())
            .await
            .unwrap();
        let after: serde_json::Value = sqlx::query_scalar(manifest_sql)
            .fetch_one(tx.as_mut())
            .await
            .unwrap();
        let readers_after: serde_json::Value = sqlx::query_scalar(readers_sql)
            .fetch_one(tx.as_mut())
            .await
            .unwrap();
        assert_eq!(before, after);
        assert_eq!(readers_before, readers_after);
        tx.rollback().await.unwrap();
    }
}
