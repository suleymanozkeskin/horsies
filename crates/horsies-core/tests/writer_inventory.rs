//! Structural guard over terminal writers after the v35 live/history split.
//!
//! Scans `src/**/*.rs` for SQL statements that set a terminal status
//! (COMPLETED / FAILED / CANCELLED / EXPIRED) and pins them to a frozen
//! empty allowlist. The database half scans the complete public-function
//! catalog, pins the two shared moves plus every function that calls one of
//! them as the exact terminal-writer set, and proves the live status CHECK
//! admits only PENDING, CLAIMED, and RUNNING.
//!
//! Matches inside `#[cfg(test)]` regions are exempt from the stamp
//! requirement here, and so are files that compile only under `cfg(test)`:
//! the file of a module declared `#[cfg(test)] mod name;` and every file
//! under that module. Test seeds are swept when the CHECK lands.

use std::fs;
use std::path::{Path, PathBuf};

use horsies_core::PostgresBroker;

/// The test databases of the core (`src/test_support/database.rs`).
#[allow(dead_code)]
#[path = "../src/test_support/database.rs"]
mod test_database;

/// Installs the schema into the test template of this process through the
/// public API.
async fn install_template(url: &str) {
    let broker = PostgresBroker::connect(url)
        .await
        .expect("connect the template broker");
    broker
        .migrate()
        .await
        .expect("install the schema into the test template");
    broker.close().await;
}

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

/// The two shared moves to history: the only functions that insert history
/// rows and delete live rows themselves.
const SHARED_MOVES: [&str; 2] = [
    "horsies_move_task_to_history",
    "horsies_move_tasks_to_history",
];

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

/// An attribute line that compiles the item after it only under `cfg(test)`.
fn is_test_only_attribute(line: &str) -> bool {
    line == "#[cfg(test)]" || line.starts_with("#[cfg(all(test,")
}

/// The name of an out-of-line module declaration (`mod name;`, with an
/// optional visibility), or `None` for any other line.
fn out_of_line_module_name(line: &str) -> Option<&str> {
    let declaration = ["pub(crate) ", "pub(super) ", "pub "]
        .iter()
        .find_map(|visibility| line.strip_prefix(visibility))
        .unwrap_or(line);
    declaration
        .strip_prefix("mod ")?
        .strip_suffix(';')
        .filter(|name| !name.is_empty() && !name.contains(' '))
}

/// The directory that holds the out-of-line submodules of the module whose
/// file is `file`: the file's own directory for `mod.rs` and `lib.rs`, else
/// a directory named after the file.
fn submodule_directory(file: &Path) -> PathBuf {
    let parent = file.parent().expect("a source file has a directory");
    match file.file_name().and_then(|name| name.to_str()) {
        Some("mod.rs" | "lib.rs" | "main.rs") => parent.to_path_buf(),
        _ => parent.join(file.file_stem().expect("a source file has a stem")),
    }
}

/// The file of each module that `file` declares `#[cfg(test)] mod name;`.
/// A `#[path = "..."]` attribute between the two names the file relative to
/// the declaring file's directory.
fn test_only_module_files(file: &Path, text: &str) -> Vec<PathBuf> {
    let lines: Vec<&str> = text.lines().map(str::trim).collect();
    let mut modules = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if !is_test_only_attribute(line) {
            continue;
        }
        let mut path_attribute = None;
        let mut next = index + 1;
        while let Some(attribute) = lines.get(next).filter(|line| line.starts_with("#[")) {
            if let Some(value) = attribute
                .strip_prefix("#[path = \"")
                .and_then(|rest| rest.strip_suffix("\"]"))
            {
                path_attribute = Some(value);
            }
            next += 1;
        }
        let Some(name) = lines
            .get(next)
            .and_then(|line| out_of_line_module_name(line))
        else {
            continue;
        };
        let module_file = match path_attribute {
            Some(relative) => file
                .parent()
                .expect("a source file has a directory")
                .join(relative),
            None => {
                let directory = submodule_directory(file);
                let flat = directory.join(format!("{name}.rs"));
                match flat.is_file() {
                    true => flat,
                    false => directory.join(name).join("mod.rs"),
                }
            }
        };
        modules.push(module_file);
    }
    modules
}

/// Files under `src/` that compile only under `cfg(test)`: each test-only
/// module file and every file under its submodule directory.
fn test_only_files(files: &[PathBuf]) -> Vec<PathBuf> {
    let mut module_files = Vec::new();
    for path in files {
        let text = fs::read_to_string(path).expect("read source file");
        module_files.extend(test_only_module_files(path, &text));
    }
    let directories: Vec<PathBuf> = module_files
        .iter()
        .map(|file| submodule_directory(file))
        .collect();
    files
        .iter()
        .filter(|path| {
            module_files.contains(path)
                || directories
                    .iter()
                    .any(|directory| path.starts_with(directory))
        })
        .cloned()
        .collect()
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
    let test_only = test_only_files(&files);
    // A known positive: the lifecycle ports are a test-only module whose
    // file sets a terminal status to assert the live CHECK refuses it.
    assert!(
        test_only.contains(&src.join("broker/lifecycle_tests/terminalization_functions.rs")),
        "the test-only module scan missed broker/lifecycle_tests"
    );

    let mut seen: Vec<(String, usize)> = Vec::new();
    for path in files.iter().filter(|path| !test_only.contains(path)) {
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

#[tokio::test]
async fn v35_catalog_has_only_move_program_terminal_writers_and_a_live_only_check() {
    let database = test_database::TestDatabase::create().await;

    // (catalog identity, delegates to a shared move, directly inserts history
    // and deletes live). The two shared moves (0006_history_functions.sql)
    // are the only direct writers: the single move takes one locked row, the
    // batch move a locked id array. Every other writer calls one of them: the
    // lifecycle writes of 0004_lifecycle_functions.sql and the interim
    // terminal writes of 0003_interim_functions.sql. Any other function with
    // one of those properties, or any function that updates live storage to a
    // terminal status, is an unreviewed terminal writer and changes the
    // catalog-wide inventory.
    const EXPECTED_WRITERS: [(&str, bool, bool); 17] = [
        (
            "horsies_move_task_to_history(horsies_tasks,text,text,timestamp with time zone,text,text,text)",
            false,
            true,
        ),
        (
            "horsies_move_tasks_to_history(uuid[],text,text,timestamp with time zone,text,text,text)",
            false,
            true,
        ),
        (
            "horsies_complete_task_fused(uuid,text,timestamp with time zone,text,boolean)",
            true,
            false,
        ),
        (
            "horsies_finish_workflow_success(uuid,text,timestamp with time zone,text)",
            true,
            false,
        ),
        (
            "horsies_finish_failed_attempt(uuid,text,timestamp with time zone,text,integer,double precision,text,text,text,text,text,text,text,text,boolean)",
            true,
            false,
        ),
        (
            "horsies_finish_stale_attempt(uuid,text,integer,integer,text,timestamp with time zone,text,integer,double precision,text,text,text,text,text)",
            true,
            false,
        ),
        (
            "horsies_cancel_task(uuid,boolean)",
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
            true,
            false,
        ),
        (
            "horsies_cancel_owned_orphan(uuid,text,timestamp with time zone)",
            true,
            false,
        ),
        (
            "horsies_cancel_orphaned_tasks(integer)",
            true,
            false,
        ),
        (
            "horsies_abandon_owned_node(uuid,text,timestamp with time zone)",
            true,
            false,
        ),
        (
            "horsies_abandon_owned_nodes(uuid[],timestamp with time zone[],text)",
            true,
            false,
        ),
        (
            "horsies_abandon_nodes_of_paused_workflows(uuid[])",
            true,
            false,
        ),
        (
            "horsies_cancel_owned_node(uuid,text,timestamp with time zone,boolean)",
            true,
            false,
        ),
        (
            "horsies_cancel_owned_nodes(uuid[],timestamp with time zone[],text)",
            true,
            false,
        ),
        (
            "horsies_cancel_nodes_of_cancelled_workflow(uuid[])",
            true,
            false,
        ),
    ];
    let rows = database
        .client()
        .query(
            "SELECT p.oid::regprocedure::text AS identity,
                    p.proname::text AS proname,
                    pg_get_functiondef(p.oid) AS definition
             FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = 'public' AND p.prokind = 'f'
             ORDER BY identity",
            &[],
        )
        .await
        .expect("public function definitions");
    let mut actual = Vec::new();
    for row in rows {
        let name: String = row.get("proname");
        let identity: String = row.get("identity");
        let definition: String = row.get("definition");
        let compact = compact_sql(&definition);
        let delegates = !SHARED_MOVES.contains(&name.as_str())
            && SHARED_MOVES.iter().any(|shared_move| {
                compact.contains(&format!("{}(", shared_move.to_ascii_uppercase()))
            });
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

    let status_checks: Vec<String> = database
        .client()
        .query(
            "SELECT pg_get_constraintdef(con.oid)
             FROM pg_constraint con
             JOIN pg_attribute att
               ON att.attrelid = con.conrelid AND att.attname = 'status'
             WHERE con.conrelid = 'horsies_tasks'::regclass
               AND con.contype = 'c'
               AND att.attnum = ANY(con.conkey)",
            &[],
        )
        .await
        .expect("live status checks")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(status_checks.len(), 1);
    let live_only = &status_checks[0];
    for live in ["PENDING", "CLAIMED", "RUNNING"] {
        assert!(live_only.contains(live));
    }
    for terminal in ["COMPLETED", "FAILED", "CANCELLED", "EXPIRED"] {
        assert!(!live_only.contains(terminal));
    }

    database.drop().await;
}
