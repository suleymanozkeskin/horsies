//! Structural guard over terminal-status writers on `horsies_tasks`.
//!
//! Scans `src/**/*.rs` for SQL statements that set a terminal status
//! (COMPLETED / FAILED / CANCELLED / EXPIRED) and pins them to a frozen
//! allowlist: a new terminal writer anywhere else fails this test and must
//! either be added here deliberately or (once the terminalization operations
//! land) be expressed through them. Every allowlisted writer must stamp
//! `terminal_at = NOW()` in the same SET clause — the 0031 CHECK constraint
//! rejects terminal rows without a terminal instant.
//!
//! Matches inside `#[cfg(test)]` regions are exempt from the stamp
//! requirement here; test seeds are swept when the CHECK lands.

use std::fs;
use std::path::{Path, PathBuf};

const TERMINAL_SET_MARKERS: [&str; 4] = [
    "status = 'COMPLETED'",
    "status = 'FAILED'",
    "status = 'CANCELLED'",
    "status = 'EXPIRED'",
];

/// (file relative to src/, expected terminal-writer statement count).
const ALLOWLIST: [(&str, usize); 3] = [
    ("broker/postgres.rs", 11),
    ("worker/recovery.rs", 2),
    ("workflow_engine/lifecycle.rs", 2),
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

#[test]
fn terminal_writers_are_allowlisted_and_stamp_terminal_at() {
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
            for clause in &prod_clauses {
                assert!(
                    clause.contains("terminal_at = NOW()"),
                    "{rel}: terminal writer must stamp terminal_at = NOW() \
                     in the same SET clause:\n{clause}"
                );
            }
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
