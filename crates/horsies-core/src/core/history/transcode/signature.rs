//! Session-independent structural signatures for verification tokens.

use sqlx::PgConnection;

use crate::core::history::partitions::catalog::{pin_utc_timezone, restore_timezone};

use super::TranscodeError;

pub const RELATION_SCHEMA_SIGNATURE_SQL: &str = r#"
SELECT encode(sha256(convert_to(
    jsonb_build_object(
        'relation', jsonb_build_array(
            relation.relkind,
            relation.relpersistence,
            relation.relam,
            relation.reloptions
        ),
        'columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    attribute.attnum,
                    attribute.attname,
                    attribute.atttypid,
                    attribute.atttypmod,
                    attribute.attcollation,
                    attribute.attnotnull,
                    attribute.attidentity,
                    attribute.attgenerated,
                    pg_get_expr(defaults.adbin, defaults.adrelid)
                ) ORDER BY attribute.attnum
            )
            FROM pg_attribute AS attribute
            LEFT JOIN pg_attrdef AS defaults
              ON defaults.adrelid = attribute.attrelid
             AND defaults.adnum = attribute.attnum
            WHERE attribute.attrelid = relation.oid
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
        ), '[]'::jsonb),
        'constraints', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    constraints.conname,
                    constraints.contype,
                    constraints.convalidated,
                    pg_get_constraintdef(constraints.oid, false)
                ) ORDER BY constraints.conname
            )
            FROM pg_constraint AS constraints
            WHERE constraints.conrelid = relation.oid
        ), '[]'::jsonb),
        'indexes', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    indexes.indisvalid,
                    indexes.indisready,
                    pg_get_indexdef(indexes.indexrelid)
                ) ORDER BY indexes.indexrelid
            )
            FROM pg_index AS indexes
            WHERE indexes.indrelid = relation.oid
        ), '[]'::jsonb),
        'triggers', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    triggers.tgenabled,
                    pg_get_triggerdef(triggers.oid, false)
                ) ORDER BY triggers.tgname
            )
            FROM pg_trigger AS triggers
            WHERE triggers.tgrelid = relation.oid
              AND NOT triggers.tgisinternal
        ), '[]'::jsonb)
    )::text,
    'UTF8'
)), 'hex')
FROM pg_class AS relation
WHERE relation.oid = CAST($1 AS oid)
"#;

pub async fn relation_schema_signature(
    connection: &mut PgConnection,
    relation_oid: i64,
) -> Result<Option<String>, TranscodeError> {
    let prior = pin_utc_timezone(&mut *connection).await?;
    let result = sqlx::query_scalar(RELATION_SCHEMA_SIGNATURE_SQL)
        .bind(relation_oid)
        .fetch_optional(&mut *connection)
        .await;
    let restored = restore_timezone(connection, &prior).await;
    match (result, restored) {
        (Ok(signature), Ok(())) => Ok(signature),
        (Err(error), _) => Err(error.into()),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}
