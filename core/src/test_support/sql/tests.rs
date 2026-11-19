//! The test client: declared types, the fetch forms, the command tag count,
//! transactions and savepoints, the checkout bound, the closed pool and the
//! listener.

use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;

use super::*;
use crate::test_support::TestDatabase;

/// A pool on a new copy of the test template.
async fn pool_on_new_database() -> (TestDatabase, TestPool) {
    let database = TestDatabase::create().await;
    let pool = TestPool::connect(database.url())
        .await
        .expect("connect the test pool");
    (database, pool)
}

async fn create_numbers_table(pool: &TestPool) {
    let _created = raw_sql("CREATE TABLE test_sql_numbers (n int4 NOT NULL)")
        .execute(pool)
        .await
        .expect("create the numbers table");
}

async fn numbers_count(pool: &TestPool) -> i64 {
    query_scalar("SELECT count(*) FROM test_sql_numbers")
        .fetch_one(pool)
        .await
        .expect("count the numbers")
}

/// Each Rust type declares the PostgreSQL type that sqlx declared for it, so
/// the server resolves `$n` from the bound value and not from its use.
#[tokio::test]
async fn a_bound_value_declares_the_type_that_sqlx_declared() {
    let (_database, pool) = pool_on_new_database().await;
    let declared: Vec<String> = query_as::<(
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    )>(
        "SELECT pg_typeof($1)::text, pg_typeof($2)::text, pg_typeof($3)::text,
                pg_typeof($4)::text, pg_typeof($5)::text, pg_typeof($6)::text,
                pg_typeof($7)::text, pg_typeof($8)::text, pg_typeof($9)::text,
                pg_typeof($10)::text, pg_typeof($11)::text, pg_typeof($12)::text,
                pg_typeof($13)::text",
    )
    .bind(1_i32)
    .bind(1_i64)
    .bind("text")
    .bind(String::from("text"))
    .bind(Uuid::new_v4())
    .bind(Utc::now())
    .bind(serde_json::json!({"key": 1}))
    .bind(vec![1_u8, 2])
    .bind(vec![Uuid::new_v4()])
    .bind(None::<i64>)
    .bind(true)
    .bind(1.5_f64)
    .bind(2_i16)
    .fetch_one(&pool)
    .await
    .map(|row| {
        vec![
            row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10, row.11,
            row.12,
        ]
    })
    .expect("read the declared types");
    assert_eq!(
        declared,
        [
            "integer",
            "bigint",
            "text",
            "text",
            "uuid",
            "timestamp with time zone",
            "jsonb",
            "bytea",
            "uuid[]",
            "bigint",
            "boolean",
            "double precision",
            "smallint",
        ]
    );
}

/// `fetch_one` takes the first of several rows; with no row it fails with
/// `RowNotFound`, where `fetch_optional` gives `None`.
#[tokio::test]
async fn fetch_one_takes_the_first_row_and_fails_on_none() {
    let (_database, pool) = pool_on_new_database().await;
    let first: i32 = query_scalar("SELECT n FROM generate_series(1, 3) AS n ORDER BY n")
        .fetch_one(&pool)
        .await
        .expect("read the first row");
    assert_eq!(first, 1);
    let all: Vec<i32> = query_scalar("SELECT n FROM generate_series(1, 3) AS n ORDER BY n")
        .fetch_all(&pool)
        .await
        .expect("read every row");
    assert_eq!(all, [1, 2, 3]);
    let none = query("SELECT 1 WHERE false").fetch_one(&pool).await;
    assert!(matches!(none, Err(Error::RowNotFound)), "{none:?}");
    let optional: Option<i32> = query_scalar("SELECT 1 WHERE false")
        .fetch_optional(&pool)
        .await
        .expect("read no row");
    assert_eq!(optional, None);
}

/// A column that does not decode into the asked type fails with `Decode`
/// naming the column.
#[tokio::test]
async fn a_column_of_another_type_fails_to_decode() {
    let (_database, pool) = pool_on_new_database().await;
    let refused = query_scalar::<i64>("SELECT 1::int4").fetch_one(&pool).await;
    match refused {
        Err(Error::Decode { column, .. }) => assert_eq!(column, "0"),
        other => panic!("expected a decode error, got {other:?}"),
    }
}

/// A refused statement carries the server's SQLSTATE.
#[tokio::test]
async fn a_refused_statement_carries_its_sqlstate() {
    let (_database, pool) = pool_on_new_database().await;
    let refused = query("SELECT 1 / 0").execute(&pool).await;
    let error = refused.expect_err("division by zero is refused");
    assert_eq!(error.sqlstate(), Some("22012"));
}

/// `execute` returns the count of the command tag; `raw_sql` runs every
/// statement of its text and returns the sum of their counts.
#[tokio::test]
async fn execute_returns_the_count_of_the_command_tag() {
    let (_database, pool) = pool_on_new_database().await;
    create_numbers_table(&pool).await;
    let inserted = query("INSERT INTO test_sql_numbers SELECT generate_series(1, $1)")
        .bind(3_i32)
        .execute(&pool)
        .await
        .expect("insert three rows");
    assert_eq!(inserted.rows_affected(), 3);
    let script = raw_sql(
        "INSERT INTO test_sql_numbers VALUES (10), (11);
         UPDATE test_sql_numbers SET n = n + 1 WHERE n >= 10",
    )
    .execute(&pool)
    .await
    .expect("run the script");
    assert_eq!(script.rows_affected(), 4);
    assert_eq!(numbers_count(&pool).await, 5);
}

/// A committed transaction stays; a rolled back one leaves nothing.
#[tokio::test]
async fn a_committed_transaction_stays_and_a_rolled_back_one_leaves_nothing() {
    let (_database, pool) = pool_on_new_database().await;
    create_numbers_table(&pool).await;
    let mut committed = pool.begin().await.expect("begin");
    let _inserted = query("INSERT INTO test_sql_numbers VALUES (1)")
        .execute(&mut *committed)
        .await
        .expect("insert in the transaction");
    committed.commit().await.expect("commit");
    let mut rolled_back = pool.begin().await.expect("begin");
    let _inserted = query("INSERT INTO test_sql_numbers VALUES (2)")
        .execute(&mut *rolled_back)
        .await
        .expect("insert in the transaction");
    rolled_back.rollback().await.expect("roll back");
    assert_eq!(numbers_count(&pool).await, 1);
}

/// A transaction of a pool that the test drops open is rolled back with its
/// session.
#[tokio::test]
async fn a_pool_transaction_dropped_open_is_rolled_back() {
    let (_database, pool) = pool_on_new_database().await;
    create_numbers_table(&pool).await;
    let mut dropped = pool.begin().await.expect("begin");
    let _inserted = query("INSERT INTO test_sql_numbers VALUES (1)")
        .execute(&mut *dropped)
        .await
        .expect("insert in the transaction");
    drop(dropped);
    assert_eq!(numbers_count(&pool).await, 0);
}

/// A transaction of the test's own session that the test drops open is
/// rolled back before the next statement of that session.
#[tokio::test]
async fn a_session_transaction_dropped_open_is_rolled_back_before_the_next_statement() {
    let (database, pool) = pool_on_new_database().await;
    create_numbers_table(&pool).await;
    let mut session = TestConnection::connect(database.url())
        .await
        .expect("connect a session");
    let mut dropped = session.begin().await.expect("begin");
    let _inserted = query("INSERT INTO test_sql_numbers VALUES (1)")
        .execute(&mut *dropped)
        .await
        .expect("insert in the transaction");
    drop(dropped);
    let in_transaction: Option<i64> = query_scalar("SELECT txid_current_if_assigned()")
        .fetch_one(&mut session)
        .await
        .expect("read the session's transaction");
    assert_eq!(in_transaction, None);
    assert_eq!(numbers_count(&pool).await, 0);
}

/// A transaction opened inside an open one is a savepoint: its rollback
/// undoes its own statements only.
#[tokio::test]
async fn a_nested_transaction_is_a_savepoint() {
    let (_database, pool) = pool_on_new_database().await;
    create_numbers_table(&pool).await;
    let mut outer = pool.begin().await.expect("begin");
    let _first = query("INSERT INTO test_sql_numbers VALUES (1)")
        .execute(&mut *outer)
        .await
        .expect("insert in the transaction");
    let mut inner = outer.begin().await.expect("savepoint");
    let _second = query("INSERT INTO test_sql_numbers VALUES (2)")
        .execute(&mut *inner)
        .await
        .expect("insert in the savepoint");
    inner.rollback().await.expect("roll back to the savepoint");
    outer.commit().await.expect("commit");
    let kept: Vec<i32> = query_scalar("SELECT n FROM test_sql_numbers ORDER BY n")
        .fetch_all(&pool)
        .await
        .expect("read the numbers");
    assert_eq!(kept, [1]);
}

/// A checkout waits for a free session at most for the pool's bound.
#[tokio::test]
async fn a_checkout_waits_at_most_the_pool_bound() {
    let database = TestDatabase::create().await;
    let pool = TestPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(100))
        .connect(database.url())
        .await
        .expect("connect the test pool");
    let held = pool.acquire().await.expect("check out the only session");
    let waited = pool.acquire().await;
    assert!(matches!(waited, Err(Error::PoolTimedOut(_))));
    drop(held);
    let _again = pool.acquire().await.expect("the returned session is free");
}

/// A closed pool refuses checkouts.
#[tokio::test]
async fn a_closed_pool_refuses_checkouts() {
    let (_database, pool) = pool_on_new_database().await;
    pool.close().await;
    assert!(pool.is_closed());
    let refused = pool.acquire().await;
    assert!(matches!(refused, Err(Error::PoolClosed)));
}

/// A close waits until every checked-out session is returned, as sqlx's
/// close did.
#[tokio::test]
async fn a_close_waits_for_the_checked_out_sessions() {
    let (_database, pool) = pool_on_new_database().await;
    let held = pool.acquire().await.expect("check out a session");
    assert_eq!((pool.size(), pool.num_idle()), (1, 0));
    let closing = tokio::spawn({
        let pool = pool.clone();
        async move { pool.close().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !closing.is_finished(),
        "the close returned while a session was out"
    );
    drop(held);
    tokio::time::timeout(Duration::from_secs(5), closing)
        .await
        .expect("the close returns once the session is back")
        .expect("join the close");
    assert!(pool.is_closed());
}

/// A new session of a pool runs the pool's session setup first.
#[tokio::test]
async fn a_new_session_runs_the_session_setup() {
    let database = TestDatabase::create().await;
    let pool = TestPoolOptions::new()
        .session_setup("SET TIME ZONE 'Asia/Tokyo'")
        .connect(database.url())
        .await
        .expect("connect the test pool");
    let zone: String = query_scalar("SHOW TimeZone")
        .fetch_one(&pool)
        .await
        .expect("read the session time zone");
    assert_eq!(zone, "Asia/Tokyo");
}

/// A listener receives the notifications of its channel in order.
#[tokio::test]
async fn a_listener_receives_the_notifications_of_its_channel() {
    let (_database, pool) = pool_on_new_database().await;
    let mut listener = TestListener::connect_with(&pool)
        .await
        .expect("connect the listener");
    listener
        .listen("test_sql_channel")
        .await
        .expect("listen on the channel");
    for payload in ["first", "second"] {
        let _sent = query("SELECT pg_notify($1, $2)")
            .bind("test_sql_channel")
            .bind(payload)
            .execute(&pool)
            .await
            .expect("notify");
    }
    for expected in ["first", "second"] {
        let notification = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .expect("the notification arrives within five seconds")
            .expect("the listener's session is open");
        assert_eq!(notification.channel(), "test_sql_channel");
        assert_eq!(notification.payload(), expected);
    }
}
