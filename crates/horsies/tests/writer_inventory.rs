//! Structural guard over terminal writers after the v35 live/history split.
//!
//! Scans `src/**/*.rs` for SQL statements that set a terminal status
//! (COMPLETED / FAILED / CANCELLED / EXPIRED) and pins them to a frozen
//! empty allowlist. The database half scans the complete public-function
//! catalog, pins the shared move plus all fifteen wire functions as the exact
//! terminal-writer set, and proves the live status CHECK admits only PENDING,
//! CLAIMED, and RUNNING.
//!
//! Matches inside `#[cfg(test)]` regions are exempt from the stamp
//! requirement here; test seeds are swept when the CHECK lands.

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

use horsies::run_horsies_migrations;

const TERMINAL_SET_MARKERS: [&str; 4] = [
    "status = 'COMPLETED'",
    "status = 'FAILED'",
    "status = 'CANCELLED'",
    "status = 'EXPIRED'",
];

/// (file relative to src/, expected terminal-writer statement count).
///
/// NO production statement in `src/**` may set a terminal status on
/// `horsies_tasks`: every terminal transition executes through the v35 move
/// program and deletes its live row.
const ALLOWLIST: [(&str, usize); 0] = [];

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read src dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Terminal-writer SET clauses in `text`: for each `UPDATE horsies_tasks`
/// occurrence, the window up to the first `WHERE` is its SET clause; the
/// statement is a terminal writer when that window sets a terminal status.
fn terminal_set_clauses(text: &str) -> Vec<&str> {
    let mut clauses = Vec::new();
    let mut from = 0;
    while let Some(pos) = text[from..].find("UPDATE horsies_tasks") {
        let start = from + pos;
        let end = text[start..]
            .find("WHERE")
            .map_or(text.len(), |w| start + w);
        let clause = &text[start..end];
        if TERMINAL_SET_MARKERS.iter().any(|m| clause.contains(m)) {
            clauses.push(clause);
        }
        from = start + "UPDATE horsies_tasks".len();
    }
    clauses
}

fn compact_sql(text: &str) -> String {
    text.chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_uppercase()
}

fn updates_live_to_terminal_status(compact: &str) -> bool {
    let mut rest = compact;
    while let Some(offset) = rest.find("UPDATEHORSIES_TASKS") {
        let statement = &rest[offset..];
        let statement = statement
            .find(';')
            .map_or(statement, |end| &statement[..end]);
        let set_clause = statement
            .find("SET")
            .map_or("", |start| &statement[start + "SET".len()..]);
        let set_clause = set_clause
            .find("WHERE")
            .map_or(set_clause, |end| &set_clause[..end]);
        if set_clause.contains("STATUS")
            && ["'COMPLETED'", "'FAILED'", "'CANCELLED'", "'EXPIRED'"]
                .iter()
                .any(|status| set_clause.contains(status))
        {
            return true;
        }
        rest = &rest[offset + "UPDATEHORSIES_TASKS".len()..];
    }
    false
}

#[test]
fn production_rust_has_no_inline_live_terminal_writers() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_sources(&src, &mut files);

    let mut seen: Vec<(String, usize)> = Vec::new();
    for path in &files {
        let text = fs::read_to_string(path).expect("read source file");
        let rel = path
            .strip_prefix(&src)
            .expect("under src/")
            .to_string_lossy()
            .replace('\\', "/");

        // Production region = everything before the first #[cfg(test)]; the
        // writer constants and inline statements all precede test modules.
        let prod_end = text.find("#[cfg(test)]").unwrap_or(text.len());

        let prod_clauses = terminal_set_clauses(&text[..prod_end]);
        if !prod_clauses.is_empty() {
            seen.push((rel, prod_clauses.len()));
        }
    }

    seen.sort();
    let mut expected: Vec<(String, usize)> = ALLOWLIST
        .iter()
        .map(|(f, n)| ((*f).to_owned(), *n))
        .collect();
    expected.sort();
    assert_eq!(
        seen, expected,
        "terminal writers on horsies_tasks changed; a new terminal-status \
         writer belongs in the terminalization layer, not inline SQL. If \
         this change is deliberate, update the allowlist."
    );
}

fn database_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root");
    let contents = fs::read_to_string(root.join(".env")).expect("read workspace .env");
    let password = contents
        .lines()
        .filter_map(|line| line.split_once('='))
        .find_map(|(key, value)| (key.trim() == "DB_PASSWORD").then(|| value.trim()))
        .expect("DB_PASSWORD in workspace .env");
    format!("postgresql://postgres:{password}@localhost:5432/horsies-rust-port")
}

#[tokio::test]
async fn v35_catalog_has_only_move_program_terminal_writers_and_a_live_only_check() {
    let base_options = PgConnectOptions::from_str(&database_url()).expect("test database URL");
    let admin_options = base_options.clone().database("postgres");
    let database_name = format!("horsies_p5_writer_{}", Uuid::new_v4().simple());
    let mut admin = PgConnection::connect_with(&admin_options)
        .await
        .expect("connect admin database");
    sqlx::query(&format!("CREATE DATABASE \"{database_name}\""))
        .execute(&mut admin)
        .await
        .expect("create writer-inventory database");
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(base_options.database(&database_name))
        .await
        .expect("connect writer-inventory database");
    run_horsies_migrations(&pool)
        .await
        .expect("migrate writer-inventory database");

    // (catalog identity, delegates to shared move, directly inserts history
    // and deletes live). Any other function with one of those properties, or
    // any function that updates live storage to a terminal status, is an
    // unreviewed terminal writer and changes the catalog-wide inventory.
    const EXPECTED_WRITERS: [(&str, bool, bool); 16] = [
        (
            "horsies_move_task_to_history(uuid,text,text,timestamp with time zone,text,text,text)",
            false,
            true,
        ),
        ("horsies_complete_locked_task(uuid,text,text)", true, false),
        (
            "horsies_complete_task_fused(uuid,text,timestamp with time zone,text,text,text)",
            true,
            false,
        ),
        (
            "horsies_fail_locked_task(uuid,text,text,text,text)",
            true,
            false,
        ),
        (
            "horsies_fail_stale_task(uuid,integer,integer,text,text,text)",
            true,
            false,
        ),
        (
            "horsies_expire_owned_claim(uuid,text,text,text)",
            true,
            false,
        ),
        (
            "horsies_expire_pending_tasks(integer,text,text)",
            false,
            true,
        ),
        ("horsies_cancel_locked_task(uuid,text[])", true, false),
        (
            "horsies_cancel_owned_orphan(uuid,text,timestamp with time zone)",
            true,
            false,
        ),
        ("horsies_cancel_orphaned_tasks(integer)", false, true),
        (
            "horsies_abandon_owned_node(uuid,text,timestamp with time zone)",
            true,
            false,
        ),
        (
            "horsies_abandon_owned_nodes(uuid[],timestamp with time zone[],text)",
            false,
            true,
        ),
        (
            "horsies_abandon_nodes_of_paused_workflows(uuid[])",
            false,
            true,
        ),
        (
            "horsies_cancel_owned_node(uuid,text,timestamp with time zone,boolean)",
            true,
            false,
        ),
        (
            "horsies_cancel_owned_nodes(uuid[],timestamp with time zone[],text)",
            false,
            true,
        ),
        (
            "horsies_cancel_nodes_of_cancelled_workflow(uuid[])",
            false,
            true,
        ),
    ];
    let rows = sqlx::query(
        "SELECT p.oid::regprocedure::text AS identity,
                p.proname,
                pg_get_functiondef(p.oid) AS definition
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'public' AND p.prokind = 'f'
         ORDER BY identity",
    )
    .fetch_all(&pool)
    .await
    .expect("public function definitions");
    let mut actual = Vec::new();
    for row in rows {
        let name: String = row.get("proname");
        let identity: String = row.get("identity");
        let definition: String = row.get("definition");
        let compact = compact_sql(&definition);
        let delegates = name != "horsies_move_task_to_history"
            && compact.contains("HORSIES_MOVE_TASK_TO_HISTORY(");
        let inserts_history = compact.contains("INSERTINTOHORSIES_TASK_HISTORY(");
        let deletes_live = compact.contains("DELETEFROMHORSIES_TASKS");
        let terminal_live_update = updates_live_to_terminal_status(&compact);
        if delegates || inserts_history || deletes_live || terminal_live_update {
            actual.push((
                identity,
                delegates,
                inserts_history && deletes_live,
                terminal_live_update,
            ));
        }
    }
    actual.sort_by(|left, right| left.0.cmp(&right.0));
    let mut expected = EXPECTED_WRITERS.to_vec();
    expected.sort_by_key(|(identity, _, _)| *identity);
    assert_eq!(
        actual.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(identity, _, _)| *identity)
            .collect::<Vec<_>>(),
        "the catalog-wide terminal-writer set changed"
    );
    for (
        (identity, delegates, direct, terminal_live_update),
        (_, expected_delegates, expected_direct),
    ) in actual.iter().zip(expected)
    {
        assert_eq!(*delegates, expected_delegates, "{identity} delegation");
        assert_eq!(*direct, expected_direct, "{identity} direct move");
        assert!(
            !terminal_live_update,
            "{identity} must not leave a terminal row in live storage"
        );
    }

    let status_checks: Vec<String> = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(con.oid)
         FROM pg_constraint con
         JOIN pg_attribute att
           ON att.attrelid = con.conrelid AND att.attname = 'status'
         WHERE con.conrelid = 'horsies_tasks'::regclass
           AND con.contype = 'c'
           AND att.attnum = ANY(con.conkey)",
    )
    .fetch_all(&pool)
    .await
    .expect("live status checks");
    assert_eq!(status_checks.len(), 1);
    let live_only = &status_checks[0];
    for live in ["PENDING", "CLAIMED", "RUNNING"] {
        assert!(live_only.contains(live));
    }
    for terminal in ["COMPLETED", "FAILED", "CANCELLED", "EXPIRED"] {
        assert!(!live_only.contains(terminal));
    }

    pool.close().await;
    sqlx::query(&format!("DROP DATABASE \"{database_name}\""))
        .execute(&mut admin)
        .await
        .expect("drop writer-inventory database");
}
