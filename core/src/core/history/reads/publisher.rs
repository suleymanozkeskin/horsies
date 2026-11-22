//! The history reader publisher: the loader side of the publication seam.
//!
//! The readers are rendered in the database by
//! `horsies_publish_history_readers()`
//! (`core/baseline/0008_history_maintenance_functions.sql`), the
//! one renderer of every client. One call, in the caller's transaction,
//! rewrites the manifest table from the leaf catalog, replaces the four
//! readers and stamps each with the manifest it was rendered from. A call on
//! current readers changes nothing.
//!
//! The manifest table, not the reader source, answers `references_leaf`.

use crate::broker::core_rows::{column, one_row, query_in, StatementLog};
use crate::core::history::errors::HistoryError;
#[cfg(test)]
use crate::core::history::names::TASK_LOOKUP_MANIFEST;
#[cfg(test)]
use crate::core::history::names::{
    TASK_DETAIL_FUNCTION, TASK_DUPLICATE_GUARD_FUNCTION, TASK_PROVENANCE_FUNCTION,
    TASK_RESULT_FUNCTION,
};
use crate::core::history::partitions::publication::LoaderRepublished;
use crate::db::{Transaction, TypedStatement};
#[cfg(test)]
use crate::test_support::sql::{self, CallError, TestConnection};

const PUBLISH_READERS_SQL: &str =
    "SELECT outcome, absent_leaves FROM horsies_publish_history_readers()";

/// `horsies_publish_history_readers()` on the statement layer.
pub(crate) static PUBLISH_READERS: TypedStatement =
    TypedStatement::new("publish_history_readers", PUBLISH_READERS_SQL, &[]);

#[cfg(test)]
const READERS_CURRENT_SQL: &str = "SELECT horsies_history_readers_current()";

/// The signatures of the readers that `horsies_publish_history_readers`
/// publishes, as `horsies_history_reader_signatures()` lists them (Python
/// `PUBLISHED_READER_SIGNATURES`).
#[cfg(test)]
pub(crate) fn rendered_reader_signatures() -> [String; 4] {
    [
        format!("{TASK_DUPLICATE_GUARD_FUNCTION}(uuid)"),
        format!("{TASK_PROVENANCE_FUNCTION}(uuid)"),
        format!("{TASK_DETAIL_FUNCTION}(uuid)"),
        format!("{TASK_RESULT_FUNCTION}(uuid)"),
    ]
}

/// The publisher of the history readers. The core publishes in its own
/// transactions ([`publish_readers_in`]); the tests reach the same functions
/// on a test session through the methods below.
#[derive(Debug, Default, Clone, Copy)]
pub struct StagedLoaderPublisher;

#[cfg(test)]
impl StagedLoaderPublisher {
    /// Publishes the readers for the attached history leaves on a test
    /// session: one statement, the statement of [`publish_readers_in`].
    ///
    /// Leaves that the catalog records as attached and whose relation no
    /// longer exists are left out of the readers and returned, in name
    /// order. A database error aborts the caller's transaction: `25000` when
    /// the transaction is not READ COMMITTED, `42P01` when no attached leaf
    /// resolves its parent.
    ///
    /// Errors: [`CallError`]; `Contract` for an unknown outcome or a NULL
    /// absent-leaf list.
    pub(crate) async fn republish(
        &self,
        connection: &mut TestConnection,
    ) -> Result<LoaderRepublished, CallError> {
        let (outcome, absent_leaves): (String, Option<Vec<String>>) =
            sql::query_as(PUBLISH_READERS_SQL)
                .fetch_one(&mut *connection)
                .await
                .map_err(CallError::Database)?;
        republished_of(&outcome, absent_leaves).map_err(CallError::Contract)
    }

    /// Whether the published manifest still names `leaf_name`.
    ///
    /// Errors: [`CallError`].
    pub(crate) async fn references_leaf(
        &self,
        connection: &mut TestConnection,
        leaf_name: &str,
    ) -> Result<bool, CallError> {
        let sql_text = format!(
            "SELECT EXISTS (
                 SELECT 1 FROM {TASK_LOOKUP_MANIFEST} WHERE leaf_name = $1
             )"
        );
        sql::query_scalar(&sql_text)
            .bind(leaf_name)
            .fetch_one(&mut *connection)
            .await
            .map_err(CallError::Database)
    }

    /// True unless `horsies_history_readers_current()` holds: the published
    /// manifest equals the catalog's reader manifest and each of the four
    /// readers carries its stamp. One statement; reads only.
    ///
    /// Errors: [`CallError`]; `Contract` for a NULL answer.
    pub(crate) async fn needs_republication(
        &self,
        connection: &mut TestConnection,
    ) -> Result<bool, CallError> {
        let current: Option<bool> = sql::query_scalar(READERS_CURRENT_SQL)
            .fetch_one(&mut *connection)
            .await
            .map_err(CallError::Database)?;
        match current {
            Some(current) => Ok(!current),
            None => Err(CallError::Contract(HistoryError::contract(
                "reader freshness check returned NULL, not a boolean",
            ))),
        }
    }
}

/// Publishes the readers for the attached history leaves in the caller's
/// core transaction: one statement, the core form of
/// [`StagedLoaderPublisher::republish`], with the same outcomes and errors.
/// A database error aborts the caller's transaction (`25000` when it is not
/// READ COMMITTED, `42P01` when no attached leaf resolves its parent).
///
/// Errors: `HistoryError::Statement`, `HistoryError::RowDecode`,
/// `HistoryError::Contract` (an unknown outcome or a NULL absent-leaf list).
pub(crate) async fn publish_readers_in(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
) -> Result<LoaderRepublished, HistoryError> {
    let statement = &PUBLISH_READERS;
    let rows = query_in(transaction, log, &statement.bind().finish()).await?;
    let row = one_row(rows, statement)?;
    let outcome: String = column(&row, statement, "outcome")?;
    let absent_leaves: Option<Vec<String>> = column(&row, statement, "absent_leaves")?;
    republished_of(&outcome, absent_leaves)
}

/// The publication of one `horsies_publish_history_readers` row. Pure.
///
/// Errors: `HistoryError::Contract` for an outcome other than `PUBLISHED`
/// and `ALREADY_CURRENT`, or a NULL absent-leaf list.
fn republished_of(
    outcome: &str,
    absent_leaves: Option<Vec<String>>,
) -> Result<LoaderRepublished, HistoryError> {
    match outcome {
        "PUBLISHED" | "ALREADY_CURRENT" => {}
        other => {
            return Err(HistoryError::contract(format!(
                "reader publication returned unknown outcome {other:?}"
            )));
        }
    }
    let absent_leaves = absent_leaves
        .ok_or_else(|| HistoryError::contract("reader publication returned no absent-leaf list"))?;
    Ok(LoaderRepublished { absent_leaves })
}

/// Published manifest entries whose relation no longer resolves, in name
/// order, on a test session. It names what the published readers probe and
/// cannot reach, which includes leaves the catalog has since recorded as
/// dropped. Empty on a healthy database. Reads only.
///
/// # Panics
///
/// When the statement fails: the test cannot run.
#[cfg(test)]
pub(crate) async fn published_manifest_absent_leaves(
    connection: &mut TestConnection,
) -> Vec<String> {
    let sql_text = format!(
        "SELECT leaf_name FROM {TASK_LOOKUP_MANIFEST}
         WHERE to_regclass(leaf_name) IS NULL ORDER BY leaf_name"
    );
    sql::query_scalar(&sql_text)
        .fetch_all(&mut *connection)
        .await
        .expect("read the absent manifest leaves")
}

#[cfg(test)]
mod publication_contract_tests {
    use super::*;
    use crate::test_support::sql;

    /// A publication that fails inside the caller's transaction leaves the
    /// manifest and the four readers as they were once the caller rolls back.
    #[tokio::test]
    async fn failed_publication_restores_manifest_and_reader_definitions_on_rollback() {
        let pool = crate::test_support::migrated_pool().await;
        let mut tx = pool.begin().await.unwrap();
        let manifest_sql = "SELECT jsonb_agg(to_jsonb(m) ORDER BY probe_position) FROM horsies_task_lookup_manifest m";
        let before: serde_json::Value = sql::query_scalar(manifest_sql)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert!(before.as_array().unwrap().len() > 1);
        let readers_sql = "SELECT jsonb_agg(jsonb_build_array(proname, pg_get_functiondef(oid)) ORDER BY proname) FROM pg_proc WHERE proname IN ('horsies_task_history_duplicate_staged', 'horsies_task_provenance_staged', 'horsies_task_detail_staged', 'horsies_task_result_staged')";
        let readers_before: serde_json::Value = sql::query_scalar(readers_sql)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(readers_before.as_array().unwrap().len(), 4);
        // A reader without the manifest stamp makes the next call publish
        // instead of answering ALREADY_CURRENT.
        sql::query("COMMENT ON FUNCTION horsies_task_detail_staged(uuid) IS 'stale'")
            .execute(&mut *tx)
            .await
            .unwrap();
        sql::query("ALTER TABLE horsies_task_lookup_manifest ADD CONSTRAINT test_reject_manifest CHECK (probe_position < 1) NOT VALID").execute(&mut *tx).await.unwrap();
        sql::query("SAVEPOINT publication_attempt")
            .execute(&mut *tx)
            .await
            .unwrap();
        assert!(StagedLoaderPublisher.republish(&mut tx).await.is_err());
        sql::query("ROLLBACK TO SAVEPOINT publication_attempt")
            .execute(&mut *tx)
            .await
            .unwrap();
        let after: serde_json::Value = sql::query_scalar(manifest_sql)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let readers_after: serde_json::Value = sql::query_scalar(readers_sql)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(before, after);
        assert_eq!(readers_before, readers_after);
        tx.rollback().await.unwrap();
    }
}

#[cfg(test)]
mod republished_of_tests {
    use super::*;

    /// Both outcomes of `horsies_publish_history_readers` give the absent
    /// leaves; another outcome and a NULL list break the contract.
    #[test]
    fn each_publication_outcome_and_each_refused_row() {
        for outcome in ["PUBLISHED", "ALREADY_CURRENT"] {
            let republished =
                republished_of(outcome, Some(vec!["leaf_a".to_owned()])).expect("a known outcome");
            assert_eq!(republished.absent_leaves, vec!["leaf_a".to_owned()]);
        }
        assert!(matches!(
            republished_of("SKIPPED", Some(Vec::new())),
            Err(HistoryError::Contract(_))
        ));
        assert!(matches!(
            republished_of("PUBLISHED", None),
            Err(HistoryError::Contract(_))
        ));
    }
}
