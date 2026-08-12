//! Read-only partition health survey.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgConnection};

use crate::core::history::commands::CollectPartitionHealth;
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{LEAF_CATALOG, TASK_HISTORY_FOREVER, WORKFLOW_PHASE2_PENDING};
use crate::core::history::outcomes::{
    CatalogConflictKind, ClassCoverage, HealthFault, PartitionHealthReport,
};

use super::catalog::{database_now, pin_utc_timezone, read_retention_class, restore_timezone};

pub const COVERAGE_FLOOR_INTERVALS: i64 = 2;

#[derive(Debug, FromRow)]
struct LeafSurvey {
    leaf_name: String,
    lower_anchor: DateTime<Utc>,
    upper_anchor: DateTime<Utc>,
    cataloged_bound: String,
    relation_exists: bool,
    actual_bound: Option<String>,
    id_index_exists: bool,
    detach_pending: Option<bool>,
    blocker_count: i64,
}

pub async fn collect_partition_health(
    connection: &mut PgConnection,
    command: &CollectPartitionHealth,
) -> Result<PartitionHealthReport, HistoryError> {
    let now = database_now(connection).await?;
    let Some(retention) = read_retention_class(connection, command.class_key()).await? else {
        return Ok(PartitionHealthReport {
            class_key: command.class_key().to_owned(),
            checked_at: now,
            coverage: None,
            faults: vec![HealthFault::RetentionClassAbsent {
                class_key: command.class_key().to_owned(),
            }],
        });
    };
    let (duration, parent_name) = match (
        command.class_key(),
        retention.duration,
        retention.finite_parent_name,
    ) {
        (FOREVER_CLASS_KEY, None, _) => (None, TASK_HISTORY_FOREVER.to_owned()),
        (_, None, _) => {
            return Ok(PartitionHealthReport {
                class_key: command.class_key().to_owned(),
                checked_at: now,
                coverage: None,
                faults: Vec::new(),
            });
        }
        (_, Some(duration), Some(parent)) => (Some(duration), parent),
        (_, Some(_), None) => {
            return Err(HistoryError::contract(
                "finite retention class has no physical parent",
            ));
        }
    };
    let survey = survey_attached_leaves(connection, command.class_key(), &parent_name).await?;
    let mut faults = Vec::new();
    let mut attached = 0_i64;
    let mut coverage_until = None;
    let mut complete_future = 0_i64;
    let mut detachable = 0_i64;
    let mut blocked = 0_i64;
    for leaf in survey {
        if !leaf.relation_exists {
            faults.push(HealthFault::LeafNonconformant {
                leaf_name: leaf.leaf_name,
                kind: CatalogConflictKind::PhysicalNonconformant,
                detail: "cataloged attached leaf has no relation".to_owned(),
            });
            continue;
        }
        match leaf.detach_pending {
            None => {
                faults.push(HealthFault::LeafNonconformant {
                    leaf_name: leaf.leaf_name,
                    kind: CatalogConflictKind::MetadataMismatch,
                    detail: "catalog believes the leaf is attached but the relation is not a partition of its parent".to_owned(),
                });
                continue;
            }
            Some(true) => {
                faults.push(HealthFault::DetachAwaitingFinalize {
                    leaf_name: leaf.leaf_name,
                });
                continue;
            }
            Some(false) => {}
        }
        if leaf.actual_bound.as_deref() != Some(leaf.cataloged_bound.as_str())
            || !leaf.id_index_exists
        {
            faults.push(HealthFault::LeafNonconformant {
                leaf_name: leaf.leaf_name,
                kind: CatalogConflictKind::PhysicalNonconformant,
                detail: "attached leaf bound or task-ID index disagrees with catalog".to_owned(),
            });
            continue;
        }
        attached += 1;
        coverage_until = Some(
            coverage_until.map_or(leaf.upper_anchor, |prior: DateTime<Utc>| {
                prior.max(leaf.upper_anchor)
            }),
        );
        if leaf.lower_anchor >= now {
            complete_future += 1;
        }
        if leaf.blocker_count > 0 {
            blocked += 1;
        } else if duration.is_some_and(|value| leaf.upper_anchor + value <= now) {
            detachable += 1;
        }
    }
    if complete_future < COVERAGE_FLOOR_INTERVALS {
        faults.push(HealthFault::CoverageBelowFloor {
            class_key: command.class_key().to_owned(),
            complete_future_intervals: complete_future,
            coverage_until,
        });
    }
    if command.application_managed() {
        if let Some(fault) = check_ddl_privileges(connection, &parent_name).await? {
            faults.push(fault);
        }
    }
    Ok(PartitionHealthReport {
        class_key: command.class_key().to_owned(),
        checked_at: now,
        coverage: Some(ClassCoverage {
            class_key: command.class_key().to_owned(),
            attached_leaf_count: attached,
            coverage_until,
            complete_future_intervals: complete_future,
            detachable_leaf_count: detachable,
            pending_blocked_leaf_count: blocked,
        }),
        faults,
    })
}

async fn survey_attached_leaves(
    connection: &mut PgConnection,
    class_key: &str,
    parent_name: &str,
) -> Result<Vec<LeafSurvey>, HistoryError> {
    let prior = pin_utc_timezone(connection).await?;
    let sql = format!(
        "SELECT c.leaf_name, c.lower_anchor, c.upper_anchor,
                c.partition_bound AS cataloged_bound,
                to_regclass(c.leaf_name) IS NOT NULL AS relation_exists,
                (SELECT pg_get_expr(pc.relpartbound, pc.oid) FROM pg_class AS pc
                 WHERE pc.oid = to_regclass(c.leaf_name)) AS actual_bound,
                to_regclass(c.id_index_name) IS NOT NULL AS id_index_exists,
                (SELECT i.inhdetachpending FROM pg_inherits AS i
                 WHERE i.inhparent = to_regclass($2)
                   AND i.inhrelid = to_regclass(c.leaf_name)) AS detach_pending,
                (SELECT count(*) FROM {WORKFLOW_PHASE2_PENDING} AS p
                 WHERE p.recovery_source = 'HISTORY'
                   AND p.history_class = c.class_key
                   AND p.history_anchor >= c.lower_anchor
                   AND p.history_anchor < c.upper_anchor) AS blocker_count
         FROM {LEAF_CATALOG} AS c
         WHERE c.class_key = $1 AND c.detached_at IS NULL AND c.dropped_at IS NULL
         ORDER BY c.lower_anchor"
    );
    let result = sqlx::query_as::<_, LeafSurvey>(&sql)
        .bind(class_key)
        .bind(parent_name)
        .fetch_all(&mut *connection)
        .await;
    let restored = restore_timezone(connection, &prior).await;
    let rows = result?;
    restored?;
    Ok(rows)
}

#[derive(FromRow)]
struct PrivilegeRow {
    schema_create: bool,
    parent_exists: bool,
    owns_parent: Option<bool>,
}

async fn check_ddl_privileges(
    connection: &mut PgConnection,
    parent_name: &str,
) -> Result<Option<HealthFault>, HistoryError> {
    let row: PrivilegeRow = sqlx::query_as(
        "SELECT has_schema_privilege(current_user, current_schema, 'CREATE')
                    AS schema_create,
                to_regclass($1) IS NOT NULL AS parent_exists,
                pg_has_role(current_user,
                    (SELECT relowner FROM pg_class WHERE oid = to_regclass($1)),
                    'USAGE') AS owns_parent",
    )
    .bind(parent_name)
    .fetch_one(connection)
    .await?;
    if !row.parent_exists {
        return Err(HistoryError::HistoryParentAbsent(format!(
            "finite history parent {parent_name:?} does not exist"
        )));
    }
    let owns_parent = row.owns_parent.unwrap_or(false);
    if row.schema_create && owns_parent {
        Ok(None)
    } else {
        Ok(Some(HealthFault::MissingDdlPrivilege {
            schema_create: row.schema_create,
            owns_parent,
        }))
    }
}
