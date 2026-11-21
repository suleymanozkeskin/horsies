//! The staged lookup locks only the leaves it probes.
//!
//! Python specification: the prototype row
//! `test_task_history_identity_prototypes.py::test_staged_lookup_uses_static_newest_first_leaf_probes`
//! (removed with the prototype harness in b866b246), here on the readers that
//! `horsies_render_history_reader` (`0008_history_maintenance_functions.sql`)
//! renders. For a UUIDv7 id the reader probes the live table, then the
//! leaves whose upper anchor is after the birth minus the clock bound, in
//! probe order, then (when those miss) the other leaves, newest first. It
//! stops at the first leaf that holds the id.
//!
//! Every birth below is at noon, 12 hours from any leaf bound, so the clock
//! bound of the reader does not change which leaves a lookup probes.

use std::collections::BTreeSet;

use super::*;
use crate::core::history::names::{LIVE_TASKS, TASK_DETAIL_FUNCTION, TASK_HISTORY_FOREVER_LEAF};
use crate::test_support::sql::{self, TestPool};

const CLASS_KEY: &str = "standard_30d";

/// The tables and partitioned tables of the current schema that this backend
/// holds a lock on, by name.
const HELD_RELATIONS_SQL: &str = "SELECT relation.relname::text
     FROM pg_locks AS held
     JOIN pg_class AS relation ON relation.oid = held.relation
     WHERE held.pid = pg_backend_pid()
       AND relation.relnamespace = current_schema()::regnamespace
       AND relation.relkind IN ('r', 'p')";

/// The leaves a lookup of an id born at `birth` probes, in order: the
/// manifest leaves whose upper anchor is after `birth` in probe order, then
/// the other manifest leaves, newest first. Read in its own statements, so
/// that the lookup's transaction holds no lock of these reads.
async fn probe_order(pool: &TestPool, birth: DateTime<Utc>) -> Vec<String> {
    let pruned: Vec<String> = sql::query_scalar(
        "SELECT leaf_name FROM horsies_task_lookup_manifest
         WHERE upper_anchor > $1 ORDER BY probe_position",
    )
    .bind(birth)
    .fetch_all(pool)
    .await
    .expect("read the pruned probes");
    let fallback: Vec<String> = sql::query_scalar(
        "SELECT leaf_name FROM horsies_task_lookup_manifest
         WHERE upper_anchor <= $1 ORDER BY probe_position DESC",
    )
    .bind(birth)
    .fetch_all(pool)
    .await
    .expect("read the fallback probes");
    pruned.into_iter().chain(fallback).collect()
}

/// The live table and the leaves of `order` up to the leaf that holds the
/// id, or every leaf of `order` when none does.
fn expected_locks(order: &[String], holder: Option<&str>) -> BTreeSet<String> {
    let probed = match holder {
        Some(holder) => {
            let end = order
                .iter()
                .position(|leaf| leaf == holder)
                .unwrap_or_else(|| panic!("{holder} is not in the probe order {order:?}"));
            &order[..=end]
        }
        None => order,
    };
    std::iter::once(LIVE_TASKS.to_owned())
        .chain(probed.iter().cloned())
        .collect()
}

/// One detail lookup in its own transaction: the location it answers and
/// the relations the transaction holds a lock on after it.
async fn detail_lookup_locks(pool: &TestPool, task_id: Uuid) -> (Option<String>, BTreeSet<String>) {
    let mut transaction = pool.begin().await.expect("begin the lookup");
    let location: Option<String> =
        sql::query_scalar(&format!("SELECT location FROM {TASK_DETAIL_FUNCTION}($1)"))
            .bind(task_id)
            .fetch_optional(&mut *transaction)
            .await
            .expect("call the detail reader");
    let held: Vec<String> = sql::query_scalar(HELD_RELATIONS_SQL)
        .fetch_all(&mut *transaction)
        .await
        .expect("read the held relation locks");
    transaction.rollback().await.expect("end the lookup");
    (location, held.into_iter().collect())
}

/// Over five daily leaves of one class: a live id locks the live table only;
/// an id in the newest or in the oldest leaf locks the leaves up to its own
/// (the forever leaf first); an id whose birth is two days after its leaf (a
/// fast producer clock) locks the pruned leaves and the fallback leaves down
/// to its own, and not the older ones; an absent id locks every leaf.
#[tokio::test]
async fn the_staged_lookup_locks_only_the_leaves_it_probes() {
    let database = TestDatabase::create().await;
    let mut transaction = database.pool.begin().await.expect("begin the setup");
    let today = utc_day(database_now(&mut transaction).await);
    let parent = register_test_class(&mut transaction, CLASS_KEY, 30).await;
    // Oldest first: 20 to 16 days ago, all inside the 30-day period.
    let days: Vec<DateTime<Utc>> = (16..=20)
        .rev()
        .map(|ago| today - Duration::days(ago))
        .collect();
    let mut leaves = Vec::new();
    for day in &days {
        leaves.push(create_test_leaf(&mut transaction, &parent, CLASS_KEY, *day).await);
    }
    let noon = |day: DateTime<Utc>| day + Duration::hours(12);
    let newest_id = v7_with_birth(noon(days[4]));
    let oldest_id = v7_with_birth(noon(days[0]));
    let fast_clock_id = v7_with_birth(noon(days[3]));
    let live_id = v7_with_birth(noon(days[2]));
    let absent_birth = noon(days[2]) + Duration::minutes(1);
    let absent_id = v7_with_birth(absent_birth);
    for (task_id, terminal_day) in [
        (newest_id, days[4]),
        (oldest_id, days[0]),
        (fast_clock_id, days[1]),
    ] {
        seed_history_row(
            &mut transaction,
            task_id,
            CLASS_KEY,
            noon(terminal_day) + Duration::hours(1),
            "COMPLETED",
            "COMPLETE_FUSED",
        )
        .await;
    }
    seed_live_row(&mut transaction, live_id, CLASS_KEY, noon(days[2])).await;
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish the readers");
    transaction.commit().await.expect("commit the setup");
    let names: Vec<&str> = leaves.iter().map(LeafRef::leaf_name).collect();

    let (location, locks) = detail_lookup_locks(&database.pool, live_id).await;
    assert_eq!(location.as_deref(), Some("LIVE"));
    assert_eq!(locks, BTreeSet::from([LIVE_TASKS.to_owned()]));

    let (location, locks) = detail_lookup_locks(&database.pool, newest_id).await;
    assert_eq!(location.as_deref(), Some("HISTORY"));
    let order = probe_order(&database.pool, noon(days[4])).await;
    assert_eq!(locks, expected_locks(&order, Some(names[4])));
    assert!(locks.contains(TASK_HISTORY_FOREVER_LEAF), "{locks:?}");
    assert!(!locks.contains(names[3]), "{locks:?}");

    let (location, locks) = detail_lookup_locks(&database.pool, oldest_id).await;
    assert_eq!(location.as_deref(), Some("HISTORY"));
    let order = probe_order(&database.pool, noon(days[0])).await;
    assert_eq!(locks, expected_locks(&order, Some(names[0])));
    assert!(!locks.contains(names[1]), "{locks:?}");

    let (location, locks) = detail_lookup_locks(&database.pool, fast_clock_id).await;
    assert_eq!(location.as_deref(), Some("HISTORY"));
    let order = probe_order(&database.pool, noon(days[3])).await;
    assert_eq!(locks, expected_locks(&order, Some(names[1])));
    for probed in &names[1..] {
        assert!(locks.contains(*probed), "{probed}: {locks:?}");
    }
    assert!(!locks.contains(names[0]), "{locks:?}");

    let (location, locks) = detail_lookup_locks(&database.pool, absent_id).await;
    assert_eq!(location, None);
    let order = probe_order(&database.pool, absent_birth).await;
    assert_eq!(locks, expected_locks(&order, None));
    for probed in &names {
        assert!(locks.contains(*probed), "{probed}: {locks:?}");
    }
    database.drop().await;
}
