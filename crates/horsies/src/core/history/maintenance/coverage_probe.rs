//! Set-based validation for required partition coverage.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use sqlx::{FromRow, PgConnection};

use super::coverage::DeclaredRetentionClass;
use crate::core::history::commands::{CreateDailyHistoryLeaf, LeafBounds, LeafRef};
use crate::core::history::ddl::classes::{
    finite_class_parent_name, DEFAULT_RETENTION_CLASS_KEY, DEFAULT_RETENTION_DURATION_DAYS,
    FOREVER_CLASS_KEY,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::CreateHourlyHeartbeatLeaf;
use crate::core::history::names::{
    HEARTBEATS_TABLE, HEARTBEAT_CLASS_KEY, LEAF_CATALOG, RETENTION_CLASSES, TASK_HISTORY_FOREVER,
    TASK_HISTORY_PARENT,
};
use crate::core::history::partitions::catalog::INDEX_SCHEMA_VERSION;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoverageClassFault {
    pub class_key: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CoverageLeafRepair {
    History(CreateDailyHistoryLeaf),
    Heartbeat(CreateHourlyHeartbeatLeaf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoverageProbe {
    pub heartbeat_covered_now: bool,
    pub history_covered_through: DateTime<Utc>,
    pub heartbeats_covered_through: DateTime<Utc>,
    pub class_faults: Vec<CoverageClassFault>,
    pub leaf_repairs: Vec<CoverageLeafRepair>,
}

impl CoverageProbe {
    pub fn is_conformant(&self) -> bool {
        self.class_faults.is_empty() && self.leaf_repairs.is_empty()
    }
}

#[derive(Debug, FromRow)]
struct CoverageProbeRaw {
    heartbeat_covered_now: bool,
    history_covered_through: DateTime<Utc>,
    heartbeats_covered_through: DateTime<Utc>,
    fault_kind: Option<String>,
    leaf_kind: Option<String>,
    class_key: Option<String>,
    leaf_name: Option<String>,
    lower_anchor: Option<DateTime<Utc>>,
    upper_anchor: Option<DateTime<Utc>>,
    detail: Option<String>,
}

struct ExpectedFiniteClasses {
    class_keys: Vec<String>,
    durations_us: Vec<i64>,
    parent_names: Vec<String>,
}

fn expected_finite_classes(
    declared_classes: &[DeclaredRetentionClass],
) -> Result<ExpectedFiniteClasses, HistoryError> {
    let mut expected = BTreeMap::from([(
        DEFAULT_RETENTION_CLASS_KEY.to_owned(),
        Duration::days(DEFAULT_RETENTION_DURATION_DAYS),
    )]);
    for declared in declared_classes {
        if declared.duration <= Duration::zero() {
            return Err(HistoryError::contract(format!(
                "retention class {:?} has a nonpositive duration",
                declared.class_key
            )));
        }
        match expected.get(&declared.class_key) {
            Some(duration) if *duration != declared.duration => {
                return Err(HistoryError::contract(format!(
                    "retention class {:?} has conflicting declared durations",
                    declared.class_key
                )));
            }
            Some(_) => {}
            None => {
                expected.insert(declared.class_key.clone(), declared.duration);
            }
        }
    }

    let mut class_keys = Vec::with_capacity(expected.len());
    let mut durations_us = Vec::with_capacity(expected.len());
    let mut parent_names = Vec::with_capacity(expected.len());
    for (class_key, duration) in expected {
        let duration_us = duration.num_microseconds().ok_or_else(|| {
            HistoryError::contract("retention duration is outside the supported interval range")
        })?;
        class_keys.push(class_key.clone());
        durations_us.push(duration_us);
        parent_names.push(finite_class_parent_name(&class_key)?);
    }
    Ok(ExpectedFiniteClasses {
        class_keys,
        durations_us,
        parent_names,
    })
}

fn decode_leaf_repair(row: &CoverageProbeRaw) -> Result<CoverageLeafRepair, HistoryError> {
    let leaf_name = row
        .leaf_name
        .as_deref()
        .ok_or_else(|| HistoryError::contract("coverage leaf fault has no leaf name"))?;
    let class_key = row
        .class_key
        .as_deref()
        .ok_or_else(|| HistoryError::contract("coverage leaf fault has no class key"))?;
    let lower = row
        .lower_anchor
        .ok_or_else(|| HistoryError::contract("coverage leaf fault has no lower bound"))?;
    let upper = row
        .upper_anchor
        .ok_or_else(|| HistoryError::contract("coverage leaf fault has no upper bound"))?;
    let bounds =
        LeafBounds::new(lower, upper).map_err(|error| HistoryError::contract(error.to_string()))?;
    let leaf = LeafRef::new(leaf_name, class_key, bounds)
        .map_err(|error| HistoryError::contract(error.to_string()))?;
    match row.leaf_kind.as_deref() {
        Some("history") => Ok(CoverageLeafRepair::History(
            CreateDailyHistoryLeaf::new(leaf)
                .map_err(|error| HistoryError::contract(error.to_string()))?,
        )),
        Some("heartbeat") => Ok(CoverageLeafRepair::Heartbeat(
            CreateHourlyHeartbeatLeaf::new(leaf)?,
        )),
        _ => Err(HistoryError::contract(
            "coverage leaf fault has an unknown leaf kind",
        )),
    }
}

pub(crate) async fn probe_partition_coverage(
    connection: &mut PgConnection,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_classes: &[DeclaredRetentionClass],
) -> Result<CoverageProbe, HistoryError> {
    let expected = expected_finite_classes(declared_classes)?;
    let history_horizon = i64::from(history_horizon_days);
    let heartbeat_horizon = i64::from(heartbeat_horizon_hours);
    let sql = format!(
        r#"
WITH utc_timezone AS MATERIALIZED (
    SELECT set_config('timezone', 'UTC', true) AS value
),
db_clock AS MATERIALIZED (
    SELECT statement_timestamp() AS database_now
    FROM utc_timezone
    WHERE utc_timezone.value = 'UTC'
),
requested_history AS (
    SELECT * FROM unnest($1::text[], $2::bigint[], $3::text[])
        AS requested(class_key, duration_us, parent_name)
),
history_keys AS (
    SELECT class_key FROM requested_history
    UNION
    SELECT class_key FROM {RETENTION_CLASSES}
    WHERE duration IS NOT NULL AND class_key <> $4
    UNION
    SELECT $5::text
),
history_classes AS (
    SELECT
        'history'::text AS leaf_kind,
        keys.class_key,
        CASE WHEN keys.class_key = $5 THEN $6::text
             ELSE COALESCE(requested.parent_name, actual.finite_parent_name)
        END AS parent_name,
        $7::bigint AS horizon,
        interval '1 day' AS leaf_interval,
        CASE
            WHEN actual.class_key IS NULL THEN false
            WHEN keys.class_key = $5 THEN
                actual.duration IS NULL
                AND actual.partition_interval IS NULL
                AND actual.finite_parent_name IS NULL
            ELSE
                actual.duration IS NOT NULL
                AND actual.partition_interval = interval '1 day'
                AND actual.finite_parent_name = COALESCE(
                    requested.parent_name, actual.finite_parent_name
                )
                AND (
                    requested.duration_us IS NULL
                    OR actual.duration = requested.duration_us * interval '1 microsecond'
                )
        END
        AND parent.oid IS NOT NULL
        AND parent.relkind = 'p'
        AND octet_length(
            CASE WHEN keys.class_key = $5 THEN $6::text
                 ELSE COALESCE(requested.parent_name, actual.finite_parent_name)
            END
        ) <= 39
        AND (
            CASE WHEN keys.class_key = $5 THEN $6::text
                 ELSE COALESCE(requested.parent_name, actual.finite_parent_name)
            END
        ) ~ '^[a-z][a-z0-9_]*$'
        AND EXISTS (
            SELECT 1 FROM pg_inherits AS parent_attachment
            WHERE parent_attachment.inhparent = to_regclass($8)
              AND parent_attachment.inhrelid = parent.oid
              AND NOT parent_attachment.inhdetachpending
        ) AS class_conformant,
        true::boolean AS class_usable,
        CASE
            WHEN actual.class_key IS NULL THEN 'retention class is absent'
            WHEN keys.class_key = $5 AND NOT (
                actual.duration IS NULL
                AND actual.partition_interval IS NULL
                AND actual.finite_parent_name IS NULL
            ) THEN 'forever retention class has invalid metadata'
            WHEN keys.class_key <> $5 AND NOT (
                actual.duration IS NOT NULL
                AND actual.partition_interval = interval '1 day'
                AND actual.finite_parent_name = COALESCE(
                    requested.parent_name, actual.finite_parent_name
                )
                AND (
                    requested.duration_us IS NULL
                    OR actual.duration = requested.duration_us * interval '1 microsecond'
                )
            ) THEN 'finite retention class has invalid metadata'
            WHEN parent.oid IS NULL THEN 'history parent relation is absent'
            WHEN parent.relkind <> 'p' THEN 'history parent relation is not partitioned'
            WHEN octet_length(
                CASE WHEN keys.class_key = $5 THEN $6::text
                     ELSE COALESCE(requested.parent_name, actual.finite_parent_name)
                END
            ) > 39 THEN 'history parent name cannot form safe leaf index names'
            WHEN NOT (
                CASE WHEN keys.class_key = $5 THEN $6::text
                     ELSE COALESCE(requested.parent_name, actual.finite_parent_name)
                END
            ) ~ '^[a-z][a-z0-9_]*$' THEN 'history parent name is not a safe identifier'
            ELSE 'history parent is not attached to the task-history root'
        END AS fault_detail
    FROM history_keys AS keys
    LEFT JOIN requested_history AS requested USING (class_key)
    LEFT JOIN {RETENTION_CLASSES} AS actual USING (class_key)
    LEFT JOIN pg_class AS parent ON parent.oid = to_regclass(
        CASE WHEN keys.class_key = $5 THEN $6::text
             ELSE COALESCE(requested.parent_name, actual.finite_parent_name)
        END
    )
),
heartbeat_class AS (
    SELECT
        'heartbeat'::text AS leaf_kind,
        $4::text AS class_key,
        $9::text AS parent_name,
        $10::bigint AS horizon,
        interval '1 hour' AS leaf_interval,
        actual.class_key IS NOT NULL
        AND actual.duration = $10::bigint * interval '1 hour'
        AND actual.partition_interval = interval '1 hour'
        AND actual.finite_parent_name = $9
        AND parent.oid IS NOT NULL
        AND parent.relkind = 'p' AS class_conformant,
        actual.class_key IS NOT NULL
        AND actual.partition_interval = interval '1 hour'
        AND actual.finite_parent_name = $9
        AND parent.oid IS NOT NULL
        AND parent.relkind = 'p' AS class_usable,
        CASE
            WHEN actual.class_key IS NULL THEN 'heartbeat retention class is absent'
            WHEN NOT (
                actual.duration = $10::bigint * interval '1 hour'
                AND actual.partition_interval = interval '1 hour'
                AND actual.finite_parent_name = $9
            ) THEN 'heartbeat retention class has invalid metadata'
            WHEN parent.oid IS NULL THEN 'heartbeat parent relation is absent'
            ELSE 'heartbeat parent relation is not partitioned'
        END AS fault_detail
    FROM (SELECT 1) AS singleton
    LEFT JOIN {RETENTION_CLASSES} AS actual ON actual.class_key = $4
    LEFT JOIN pg_class AS parent ON parent.oid = to_regclass($9)
),
classes AS (
    SELECT * FROM history_classes
    UNION ALL
    SELECT * FROM heartbeat_class
),
desired AS (
    SELECT
        classes.*,
        CASE classes.leaf_kind
            WHEN 'history' THEN
                date_trunc('day', db_clock.database_now, 'UTC')
                + series.value * classes.leaf_interval
            ELSE
                date_trunc('hour', db_clock.database_now, 'UTC')
                + series.value * classes.leaf_interval
        END AS lower_anchor
    FROM classes
    CROSS JOIN db_clock
    CROSS JOIN LATERAL generate_series(0, classes.horizon) AS series(value)
),
named_desired AS (
    SELECT
        desired.*,
        desired.lower_anchor + desired.leaf_interval AS upper_anchor,
        desired.parent_name || '_' ||
        CASE desired.leaf_kind
            WHEN 'history' THEN to_char(
                desired.lower_anchor AT TIME ZONE 'UTC', 'YYYY_MM_DD'
            )
            ELSE to_char(
                desired.lower_anchor AT TIME ZONE 'UTC', 'YYYY_MM_DD_HH24'
            )
        END AS leaf_name
    FROM desired
),
leaf_state AS (
    SELECT
        desired.*,
        catalog.leaf_name IS NOT NULL
        AND catalog.parent_name = desired.parent_name
        AND catalog.class_key = desired.class_key
        AND catalog.lower_anchor = desired.lower_anchor
        AND catalog.upper_anchor = desired.upper_anchor
        AND catalog.index_schema_version = {INDEX_SCHEMA_VERSION}
        AND catalog.detached_at IS NULL
        AND catalog.dropped_at IS NULL
        AND leaf.oid IS NOT NULL
        AND attachment.inhrelid IS NOT NULL
        AND NOT attachment.inhdetachpending
        AND pg_get_expr(leaf.relpartbound, leaf.oid) = catalog.partition_bound
        AND pg_get_expr(leaf.relpartbound, leaf.oid) = format(
            'FOR VALUES FROM (%L) TO (%L)',
            desired.lower_anchor,
            desired.upper_anchor
        )
        AND EXISTS (
            SELECT 1
            FROM pg_index AS id_index
            JOIN pg_class AS id_relation
              ON id_relation.oid = id_index.indexrelid
            JOIN pg_am AS id_method
              ON id_method.oid = id_relation.relam
            WHERE id_index.indexrelid = to_regclass(catalog.id_index_name)
              AND id_index.indrelid = leaf.oid
              AND id_method.amname = 'btree'
              AND id_index.indisvalid
              AND id_index.indisready
              AND id_index.indislive
              AND NOT id_index.indisunique
              AND NOT id_index.indisprimary
              AND NOT id_index.indisexclusion
              AND id_index.indpred IS NULL
              AND id_index.indexprs IS NULL
              AND (
                  (
                      desired.leaf_kind = 'history'
                      AND id_index.indnkeyatts = 1
                      AND id_index.indnatts = 1
                      AND id_index.indkey[0] = (
                          SELECT attribute.attnum
                          FROM pg_attribute AS attribute
                          WHERE attribute.attrelid = leaf.oid
                            AND attribute.attname = 'task_id'
                      )
                      AND id_index.indcollation[0] = (
                          SELECT attribute.attcollation
                          FROM pg_attribute AS attribute
                          WHERE attribute.attrelid = leaf.oid
                            AND attribute.attname = 'task_id'
                      )
                      AND pg_get_indexdef(id_index.indexrelid, 1, false) = 'task_id'
                      AND pg_get_indexdef(id_index.indexrelid)
                          LIKE '% USING btree (task_id)'
                      AND id_index.indoption[0] = 0
                  )
                  OR
                  (
                      desired.leaf_kind = 'heartbeat'
                      AND id_index.indnkeyatts = 3
                      AND id_index.indnatts = 3
                      AND id_index.indkey[0] = (
                          SELECT attribute.attnum
                          FROM pg_attribute AS attribute
                          WHERE attribute.attrelid = leaf.oid
                            AND attribute.attname = 'task_id'
                      )
                      AND id_index.indkey[1] = (
                          SELECT attribute.attnum
                          FROM pg_attribute AS attribute
                          WHERE attribute.attrelid = leaf.oid
                            AND attribute.attname = 'role'
                      )
                      AND id_index.indkey[2] = (
                          SELECT attribute.attnum
                          FROM pg_attribute AS attribute
                          WHERE attribute.attrelid = leaf.oid
                            AND attribute.attname = 'sent_at'
                      )
                      AND id_index.indcollation[0] = (
                          SELECT attribute.attcollation
                          FROM pg_attribute AS attribute
                          WHERE attribute.attrelid = leaf.oid
                            AND attribute.attname = 'task_id'
                      )
                      AND id_index.indcollation[1] = (
                          SELECT attribute.attcollation
                          FROM pg_attribute AS attribute
                          WHERE attribute.attrelid = leaf.oid
                            AND attribute.attname = 'role'
                      )
                      AND id_index.indcollation[2] = (
                          SELECT attribute.attcollation
                          FROM pg_attribute AS attribute
                          WHERE attribute.attrelid = leaf.oid
                            AND attribute.attname = 'sent_at'
                      )
                      AND pg_get_indexdef(id_index.indexrelid, 1, false) = 'task_id'
                      AND pg_get_indexdef(id_index.indexrelid, 2, false) = 'role'
                      AND pg_get_indexdef(id_index.indexrelid, 3, false) = 'sent_at'
                      AND pg_get_indexdef(id_index.indexrelid)
                          LIKE '% USING btree (task_id, role, sent_at DESC)'
                      AND id_index.indoption[0] = 0
                      AND id_index.indoption[1] = 0
                      AND id_index.indoption[2] = 3
                  )
              )
        )
        AND (
            desired.leaf_kind = 'heartbeat'
            OR EXISTS (
                SELECT 1
                FROM pg_index AS ordering_index
                JOIN pg_class AS ordering_relation
                  ON ordering_relation.oid = ordering_index.indexrelid
                JOIN pg_am AS ordering_method
                  ON ordering_method.oid = ordering_relation.relam
                WHERE ordering_index.indrelid = leaf.oid
                  AND ordering_method.amname = 'btree'
                  AND ordering_index.indisvalid
                  AND ordering_index.indisready
                  AND ordering_index.indislive
                  AND NOT ordering_index.indisunique
                  AND NOT ordering_index.indisprimary
                  AND NOT ordering_index.indisexclusion
                  AND ordering_index.indpred IS NULL
                  AND ordering_index.indexprs IS NULL
                  AND ordering_index.indnkeyatts = 1
                  AND ordering_index.indnatts = 1
                  AND ordering_index.indkey[0] = (
                      SELECT attribute.attnum
                      FROM pg_attribute AS attribute
                      WHERE attribute.attrelid = leaf.oid
                        AND attribute.attname = 'enqueued_at'
                  )
                  AND ordering_index.indcollation[0] = (
                      SELECT attribute.attcollation
                      FROM pg_attribute AS attribute
                      WHERE attribute.attrelid = leaf.oid
                        AND attribute.attname = 'enqueued_at'
                  )
                  AND pg_get_indexdef(
                      ordering_index.indexrelid, 1, false
                  ) = 'enqueued_at'
                  AND pg_get_indexdef(ordering_index.indexrelid)
                      LIKE '% USING btree (enqueued_at)'
                  AND ordering_index.indoption[0] = 0
            )
        ) AS leaf_conformant
    FROM named_desired AS desired
    LEFT JOIN {LEAF_CATALOG} AS catalog
      ON catalog.leaf_name = desired.leaf_name
    LEFT JOIN pg_class AS leaf
      ON leaf.oid = to_regclass(desired.leaf_name)
    LEFT JOIN pg_inherits AS attachment
      ON attachment.inhparent = to_regclass(desired.parent_name)
     AND attachment.inhrelid = leaf.oid
),
summary AS (
    SELECT
        db_clock.database_now,
        COALESCE(bool_and(
            leaf_state.class_usable AND leaf_state.leaf_conformant
        ) FILTER (
            WHERE leaf_state.leaf_kind = 'heartbeat'
              AND leaf_state.lower_anchor = date_trunc(
                  'hour', db_clock.database_now, 'UTC'
              )
        ), false) AS heartbeat_covered_now,
        date_trunc('day', db_clock.database_now, 'UTC')
            + ($7::bigint + 1) * interval '1 day' AS history_covered_through,
        date_trunc('hour', db_clock.database_now, 'UTC')
            + ($10::bigint + 1) * interval '1 hour' AS heartbeats_covered_through
    FROM db_clock
    CROSS JOIN leaf_state
    GROUP BY db_clock.database_now
),
faults AS (
    SELECT
        'class'::text AS fault_kind,
        classes.leaf_kind,
        classes.class_key,
        NULL::text AS leaf_name,
        NULL::timestamptz AS lower_anchor,
        NULL::timestamptz AS upper_anchor,
        classes.fault_detail AS detail
    FROM classes
    WHERE NOT classes.class_conformant
    UNION ALL
    SELECT
        'leaf'::text,
        leaf_state.leaf_kind,
        leaf_state.class_key,
        leaf_state.leaf_name,
        leaf_state.lower_anchor,
        leaf_state.upper_anchor,
        'required leaf is absent or nonconformant'::text
    FROM leaf_state
    WHERE leaf_state.class_conformant AND NOT leaf_state.leaf_conformant
)
SELECT
    summary.database_now,
    summary.heartbeat_covered_now,
    summary.history_covered_through,
    summary.heartbeats_covered_through,
    NULL::text AS fault_kind,
    NULL::text AS leaf_kind,
    NULL::text AS class_key,
    NULL::text AS leaf_name,
    NULL::timestamptz AS lower_anchor,
    NULL::timestamptz AS upper_anchor,
    NULL::text AS detail
FROM summary
UNION ALL
SELECT
    summary.database_now,
    summary.heartbeat_covered_now,
    summary.history_covered_through,
    summary.heartbeats_covered_through,
    faults.fault_kind,
    faults.leaf_kind,
    faults.class_key,
    faults.leaf_name,
    faults.lower_anchor,
    faults.upper_anchor,
    faults.detail
FROM summary
CROSS JOIN faults
ORDER BY fault_kind NULLS FIRST, class_key NULLS FIRST, lower_anchor NULLS FIRST
"#
    );

    let rows = sqlx::query_as::<_, CoverageProbeRaw>(&sql)
        .bind(expected.class_keys)
        .bind(expected.durations_us)
        .bind(expected.parent_names)
        .bind(HEARTBEAT_CLASS_KEY)
        .bind(FOREVER_CLASS_KEY)
        .bind(TASK_HISTORY_FOREVER)
        .bind(history_horizon)
        .bind(TASK_HISTORY_PARENT)
        .bind(HEARTBEATS_TABLE)
        .bind(heartbeat_horizon)
        .fetch_all(&mut *connection)
        .await?;
    let summary = rows
        .first()
        .ok_or_else(|| HistoryError::contract("coverage probe returned no summary row"))?;
    let mut class_faults = Vec::new();
    let mut leaf_repairs = Vec::new();
    for row in &rows {
        match row.fault_kind.as_deref() {
            None => {}
            Some("class") => class_faults.push(CoverageClassFault {
                class_key: row
                    .class_key
                    .clone()
                    .ok_or_else(|| HistoryError::contract("class fault has no class key"))?,
                detail: row
                    .detail
                    .clone()
                    .ok_or_else(|| HistoryError::contract("class fault has no detail"))?,
            }),
            Some("leaf") => leaf_repairs.push(decode_leaf_repair(row)?),
            Some(_) => {
                return Err(HistoryError::contract(
                    "coverage probe returned an unknown fault kind",
                ));
            }
        }
    }
    Ok(CoverageProbe {
        heartbeat_covered_now: summary.heartbeat_covered_now,
        history_covered_through: summary.history_covered_through,
        heartbeats_covered_through: summary.heartbeats_covered_through,
        class_faults,
        leaf_repairs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_classes_deduplicate_equal_declarations_and_reject_conflicts() {
        let matching = DeclaredRetentionClass {
            class_key: DEFAULT_RETENTION_CLASS_KEY.to_owned(),
            duration: Duration::days(DEFAULT_RETENTION_DURATION_DAYS),
        };
        let expected = expected_finite_classes(&[matching]).unwrap();
        assert_eq!(expected.class_keys, vec![DEFAULT_RETENTION_CLASS_KEY]);

        let conflicting = DeclaredRetentionClass {
            class_key: DEFAULT_RETENTION_CLASS_KEY.to_owned(),
            duration: Duration::days(7),
        };
        assert!(expected_finite_classes(&[conflicting]).is_err());
    }
}
