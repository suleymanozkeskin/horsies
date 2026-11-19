//! A seed of test code into a partitioned table at an instant other than
//! the database clock goes through a guard of `src/test_support/history.rs`
//! (`covered_beat_instant`, `covered_history_instant`), which creates the
//! leaf of the instant first. Without the guard such a seed fails with
//! SQLSTATE 23514 only when its test runs in the first minutes of a UTC hour
//! or day; this scan fails it at any time of day.
//!
//! The scan lexes each `.rs` file of the crate into token trees
//! (`proc-macro2`). Test code is every file under `tests/`, every item under
//! `#[cfg(test)]` or `#[cfg(all(test, ..))]`, and every module file that the
//! module tree of `src/lib.rs` reaches through such an item. In test code each
//! string literal is read as SQL, with `{NAME}` replaced by the text of the
//! string constant `NAME` of the crate; the body of a dollar-quoted string in
//! it is read as SQL too. A write is `INSERT INTO <table> (<columns>)
//! VALUES (..) | SELECT ..`, or `UPDATE <table> SET ..` that sets the key, on
//! a table of a partitioned family:
//!
//! - `horsies_heartbeats` and its leaves `horsies_heartbeats_*`: key `sent_at`;
//! - `horsies_task_history` and its leaves `horsies_task_history_*`, other
//!   than the plain tables of [`HISTORY_PLAIN_TABLES`]: key `terminal_at`.
//!
//! A key value is the database clock when it is `NOW()`, `CURRENT_TIMESTAMP`,
//! `statement_timestamp()`, `transaction_timestamp()`, `clock_timestamp()`,
//! `NULL`, or a `CASE` whose results are all of these: a coverage pass that
//! ran before covers it. Any other key value (a parameter, arithmetic on the
//! clock, a column) needs a guard. The function that holds the literal, or,
//! for the literal of a `const` or `static` item, every function of its file
//! that names the item, must name a guard of the table's family
//! ([`Family::guards`]). A write into a family table that the scan cannot
//! read (no column list, a tuple `SET`, `COPY`, `MERGE`, a table name that
//! is a placeholder) is a finding too. [`GUARD_FILE`] holds the guards and is
//! not scanned: its own tests seed without a guard on purpose. Nor is this
//! file, whose planted seeds are data.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use proc_macro2::{Delimiter, TokenStream, TokenTree};

/// The deepest directory level under `src/` or `tests/` that the file walk
/// enters.
const MAX_DIRECTORY_DEPTH: usize = 16;
/// The deepest nesting of token trees that the walk enters.
const MAX_TREE_DEPTH: usize = 256;
/// The deepest nesting of dollar-quoted SQL bodies that the reader enters.
const MAX_SQL_NESTING: usize = 8;
/// The most module files that the module tree may reach.
const MAX_MODULE_FILES: usize = 4_096;

/// The file of the guards, relative to the crate.
const GUARD_FILE: &str = "src/test_support/history.rs";

/// This file, relative to the crate: its planted seeds are data.
const SCAN_FILE: &str = "tests/partition_seed_guard.rs";

/// The tables named `horsies_task_history_*` that are not partitions of the
/// history.
const HISTORY_PLAIN_TABLES: [&str; 2] = [
    "horsies_task_history_leaf_catalog",
    "horsies_task_history_clock_exceptions",
];

/// The functions of the database clock, lower case.
const CLOCK_FUNCTIONS: [&str; 4] = [
    "now",
    "statement_timestamp",
    "transaction_timestamp",
    "clock_timestamp",
];

/// A partitioned family of tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    /// `horsies_heartbeats`, hourly leaves by `sent_at`.
    Heartbeats,
    /// `horsies_task_history`, leaves by class and by `terminal_at`.
    History,
}

impl Family {
    /// The family of the table `name` (lower case, unqualified).
    fn of_table(name: &str) -> Option<Family> {
        let heartbeats = name == "horsies_heartbeats" || name.starts_with("horsies_heartbeats_");
        let history = name == "horsies_task_history"
            || (name.starts_with("horsies_task_history_") && !HISTORY_PLAIN_TABLES.contains(&name));
        match (heartbeats, history) {
            (true, _) => Some(Family::Heartbeats),
            (false, true) => Some(Family::History),
            (false, false) => None,
        }
    }

    /// The range key of the family's leaves.
    fn key(self) -> &'static str {
        match self {
            Family::Heartbeats => "sent_at",
            Family::History => "terminal_at",
        }
    }

    /// The names that show a function goes through the family's guard: the
    /// guard and the type that only the guard constructs.
    fn guards(self) -> [&'static str; 2] {
        match self {
            Family::Heartbeats => ["covered_beat_instant", "CoveredBeatInstant"],
            Family::History => ["covered_history_instant", "CoveredHistoryInstant"],
        }
    }
}

// ---------------------------------------------------------------- SQL

/// One token of SQL text.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlToken {
    /// A keyword or an identifier, lower case; a quoted identifier as written.
    Word(String),
    /// `(`, `)`, `,`, `;`, `.` or `=`.
    Punct(char),
    /// Any other operator, a number, a parameter `$n`, a string constant or a
    /// `{..}` placeholder left by the substitution: text that is no clock.
    Other(String),
}

/// What a write sets its key to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum KeyValue {
    /// The database clock, or no instant (`NULL`).
    Clock,
    /// Any other value, as its SQL text.
    Instant(String),
}

/// One write into a table of a partitioned family.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlWrite {
    /// The write sets the key to each of `values` (one per row of `VALUES`).
    Keyed {
        table: String,
        family: Family,
        values: Vec<KeyValue>,
    },
    /// The scan cannot read the key of the write.
    Unreadable { table: String, cause: &'static str },
}

/// The tokens of `text`; the tokens of each dollar-quoted body go to
/// `bodies`. Comments are left out.
fn sql_tokens(text: &str, bodies: &mut Vec<String>) -> Vec<SqlToken> {
    let characters: Vec<char> = text.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < characters.len() {
        let character = characters[index];
        let rest = &characters[index..];
        index += match character {
            _ if character.is_whitespace() => 1,
            '-' if rest.get(1) == Some(&'-') => line_end(rest),
            '/' if rest.get(1) == Some(&'*') => block_comment_end(rest),
            '\'' => quoted_end(rest, '\'', &mut tokens),
            '"' => quoted_identifier_end(rest, &mut tokens),
            '$' => dollar_end(rest, &mut tokens, bodies),
            '{' => placeholder_end(rest, &mut tokens),
            '(' | ')' | ',' | ';' | '.' | '=' => {
                tokens.push(SqlToken::Punct(character));
                1
            }
            _ if character.is_alphanumeric() || character == '_' => word_end(rest, &mut tokens),
            _ => {
                tokens.push(SqlToken::Other(character.to_string()));
                1
            }
        };
    }
    tokens
}

fn line_end(rest: &[char]) -> usize {
    rest.iter()
        .position(|character| *character == '\n')
        .unwrap_or(rest.len())
}

fn block_comment_end(rest: &[char]) -> usize {
    rest.windows(2)
        .skip(2)
        .position(|pair| pair == ['*', '/'])
        .map_or(rest.len(), |position| position + 4)
}

/// A string constant: `''` inside it is a quote. Pushes it as `Other`.
fn quoted_end(rest: &[char], quote: char, tokens: &mut Vec<SqlToken>) -> usize {
    let mut index = 1;
    while index < rest.len() {
        match (rest[index] == quote, rest.get(index + 1) == Some(&quote)) {
            (true, true) => index += 2,
            (true, false) => break,
            (false, _) => index += 1,
        }
    }
    let end = (index + 1).min(rest.len());
    tokens.push(SqlToken::Other(rest[..end].iter().collect()));
    end
}

/// A quoted identifier: pushed as a `Word` with its text as written.
fn quoted_identifier_end(rest: &[char], tokens: &mut Vec<SqlToken>) -> usize {
    let close = rest[1..]
        .iter()
        .position(|character| *character == '"')
        .map_or(rest.len(), |position| position + 2);
    let name: String = rest[1..close.saturating_sub(1).max(1)].iter().collect();
    tokens.push(SqlToken::Word(name));
    close
}

/// A parameter `$n` (pushed as `Other`), or a dollar-quoted string
/// `$tag$..$tag$` whose body goes to `bodies`.
fn dollar_end(rest: &[char], tokens: &mut Vec<SqlToken>, bodies: &mut Vec<String>) -> usize {
    let tag_length = rest[1..]
        .iter()
        .position(|character| !(character.is_alphanumeric() || *character == '_'))
        .unwrap_or(rest.len() - 1);
    let tag_end = 1 + tag_length;
    let opens_quote = rest.get(tag_end) == Some(&'$')
        && rest[1..tag_end]
            .first()
            .is_none_or(|first| !first.is_ascii_digit());
    if !opens_quote {
        tokens.push(SqlToken::Other(rest[..tag_end].iter().collect()));
        return tag_end;
    }
    let delimiter: Vec<char> = rest[..=tag_end].to_vec();
    let body_start = tag_end + 1;
    let body_length = rest[body_start..]
        .windows(delimiter.len())
        .position(|window| window == delimiter.as_slice())
        .unwrap_or(rest.len() - body_start);
    bodies.push(rest[body_start..body_start + body_length].iter().collect());
    tokens.push(SqlToken::Other("$$".to_owned()));
    (body_start + body_length + delimiter.len()).min(rest.len())
}

/// A `{..}` placeholder of a format string that the substitution left:
/// pushed as one `Other` token with its braces.
fn placeholder_end(rest: &[char], tokens: &mut Vec<SqlToken>) -> usize {
    let end = rest
        .iter()
        .position(|character| *character == '}')
        .map_or(1, |position| position + 1);
    tokens.push(SqlToken::Other(rest[..end].iter().collect()));
    end
}

fn word_end(rest: &[char], tokens: &mut Vec<SqlToken>) -> usize {
    let length = rest
        .iter()
        .position(|character| !(character.is_alphanumeric() || *character == '_'))
        .unwrap_or(rest.len());
    tokens.push(SqlToken::Word(
        rest[..length].iter().collect::<String>().to_lowercase(),
    ));
    length
}

fn is_word(token: Option<&SqlToken>, word: &str) -> bool {
    matches!(token, Some(SqlToken::Word(text)) if text == word)
}

fn is_punct(token: Option<&SqlToken>, punct: char) -> bool {
    matches!(token, Some(SqlToken::Punct(character)) if *character == punct)
}

/// The writes into family tables of the SQL `text`, with those of its
/// dollar-quoted bodies.
fn sql_writes(text: &str, nesting: usize) -> Vec<SqlWrite> {
    assert!(
        nesting <= MAX_SQL_NESTING,
        "dollar-quoted SQL nests deeper than {MAX_SQL_NESTING} levels"
    );
    let mut bodies = Vec::new();
    let tokens = sql_tokens(text, &mut bodies);
    let mut writes = Vec::new();
    for index in 0..tokens.len() {
        let write = match &tokens[index] {
            SqlToken::Word(word) if word == "insert" && is_word(tokens.get(index + 1), "into") => {
                insert_write(&tokens, index + 2)
            }
            SqlToken::Word(word) if word == "update" => update_write(&tokens, index + 1),
            SqlToken::Word(word) if word == "copy" || word == "merge" => {
                other_write(&tokens, index + 1)
            }
            _ => None,
        };
        writes.extend(write);
    }
    for body in bodies {
        writes.extend(sql_writes(&body, nesting + 1));
    }
    writes
}

/// The table named at `index` (`name` or `schema.name`), its family, and
/// the index after it. A placeholder in place of the name yields its text
/// and no family.
fn table_at(tokens: &[SqlToken], index: usize) -> Option<(String, Option<Family>, usize)> {
    match (tokens.get(index), is_punct(tokens.get(index + 1), '.')) {
        (Some(SqlToken::Word(_)), true) => table_at(tokens, index + 2),
        (Some(SqlToken::Word(name)), false) => {
            Some((name.clone(), Family::of_table(name), index + 1))
        }
        (Some(SqlToken::Other(text)), _) if text.starts_with('{') => {
            Some((text.clone(), None, index + 1))
        }
        _ => None,
    }
}

/// `COPY <table>` and `MERGE INTO <table>`: unreadable for a family table.
fn other_write(tokens: &[SqlToken], index: usize) -> Option<SqlWrite> {
    let start = if is_word(tokens.get(index), "into") {
        index + 1
    } else {
        index
    };
    let (table, family, _) = table_at(tokens, start)?;
    family.map(|_| SqlWrite::Unreadable {
        table,
        cause: "COPY and MERGE seeds cannot be read",
    })
}

/// The index of the token that closes the parenthesis at `open`.
fn closing(tokens: &[SqlToken], open: usize) -> Option<usize> {
    let mut depth = 0_usize;
    for (offset, token) in tokens[open..].iter().enumerate() {
        match token {
            SqlToken::Punct('(') => depth += 1,
            SqlToken::Punct(')') => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(open + offset);
                }
            }
            _ => {}
        }
    }
    None
}

/// `tokens` split at its commas outside parentheses.
fn split_commas(tokens: &[SqlToken]) -> Vec<&[SqlToken]> {
    let mut parts = Vec::new();
    let mut depth = 0_usize;
    let mut start = 0;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            SqlToken::Punct('(') => depth += 1,
            SqlToken::Punct(')') => depth = depth.saturating_sub(1),
            SqlToken::Punct(',') if depth == 0 => {
                parts.push(&tokens[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&tokens[start..]);
    parts
}

/// The index of the first token from `start` on, outside parentheses, that
/// is one of `words` or the punctuation `;`, else the end.
fn stop_at(tokens: &[SqlToken], start: usize, words: &[&str]) -> usize {
    let mut depth = 0_usize;
    for (offset, token) in tokens[start..].iter().enumerate() {
        match token {
            SqlToken::Punct('(') => depth += 1,
            SqlToken::Punct(')') if depth == 0 => return start + offset,
            SqlToken::Punct(')') => depth -= 1,
            SqlToken::Punct(';') if depth == 0 => return start + offset,
            SqlToken::Word(word) if depth == 0 && words.contains(&word.as_str()) => {
                return start + offset;
            }
            _ => {}
        }
    }
    tokens.len()
}

/// The write of `INSERT INTO` whose table starts at `index`.
fn insert_write(tokens: &[SqlToken], index: usize) -> Option<SqlWrite> {
    let (table, family, mut next) = table_at(tokens, index)?;
    let unreadable = |cause| SqlWrite::Unreadable {
        table: table.clone(),
        cause,
    };
    let Some(family) = family else {
        return table
            .starts_with('{')
            .then(|| unreadable("the table name is a placeholder that names no constant"));
    };
    if is_word(tokens.get(next), "as") {
        next += 2;
    }
    match (tokens.get(next), is_punct(tokens.get(next), '(')) {
        (None, _) => return None,
        (Some(_), false) => return Some(unreadable("an INSERT without a column list")),
        (Some(_), true) => {}
    }
    let close = closing(tokens, next)?;
    let columns = split_commas(&tokens[next + 1..close]);
    let Some(position) = columns
        .iter()
        .position(|column| matches!(column, [SqlToken::Word(name)] if name == family.key()))
    else {
        return Some(unreadable("an INSERT whose column list has no key"));
    };
    let rows = match tokens.get(close + 1) {
        Some(SqlToken::Word(word)) if word == "values" => values_rows(tokens, close + 2),
        Some(SqlToken::Word(word)) if word == "select" => vec![select_list(tokens, close + 2)],
        _ => return Some(unreadable("an INSERT that is neither VALUES nor SELECT")),
    };
    let values: Option<Vec<KeyValue>> = rows
        .iter()
        .map(|row| row.get(position).map(|value| key_value(value)))
        .collect();
    Some(match values {
        Some(values) => SqlWrite::Keyed {
            table,
            family,
            values,
        },
        None => unreadable("a row has fewer values than columns"),
    })
}

/// The rows of `VALUES (..), (..)` from `index` on.
fn values_rows(tokens: &[SqlToken], mut index: usize) -> Vec<Vec<&[SqlToken]>> {
    let mut rows = Vec::new();
    while let Some(close) = is_punct(tokens.get(index), '(')
        .then(|| closing(tokens, index))
        .flatten()
    {
        rows.push(split_commas(&tokens[index + 1..close]));
        index = close + 1;
        match is_punct(tokens.get(index), ',') {
            true => index += 1,
            false => break,
        }
    }
    rows
}

/// The output expressions of a `SELECT` list from `index` on.
fn select_list(tokens: &[SqlToken], index: usize) -> Vec<&[SqlToken]> {
    let start = if is_word(tokens.get(index), "distinct") {
        index + 1
    } else {
        index
    };
    let end = stop_at(
        tokens,
        start,
        &[
            "from",
            "where",
            "union",
            "returning",
            "on",
            "group",
            "order",
            "limit",
        ],
    );
    split_commas(&tokens[start..end])
}

/// The write of the `UPDATE` keyword at `index - 1`, when it starts an
/// `UPDATE <table> [[AS] alias] SET` statement on a family table.
fn update_write(tokens: &[SqlToken], index: usize) -> Option<SqlWrite> {
    let start = if is_word(tokens.get(index), "only") {
        index + 1
    } else {
        index
    };
    let (table, family, mut next) = table_at(tokens, start)?;
    let unreadable = |cause| SqlWrite::Unreadable {
        table: table.clone(),
        cause,
    };
    let Some(family) = family else {
        let names_a_statement = table.starts_with('{') && is_word(tokens.get(next), "set");
        return names_a_statement
            .then(|| unreadable("the table name is a placeholder that names no constant"));
    };
    if is_word(tokens.get(next), "as") {
        next += 1;
    }
    if !is_word(tokens.get(next), "set") {
        next += 1;
    }
    if !is_word(tokens.get(next), "set") {
        return Some(unreadable(
            "an UPDATE of a family table that the scan cannot read",
        ));
    }
    if is_punct(tokens.get(next + 1), '(') {
        return Some(unreadable("an UPDATE that sets a tuple"));
    }
    let end = stop_at(tokens, next + 1, &["from", "where", "returning"]);
    let values: Vec<KeyValue> = split_commas(&tokens[next + 1..end])
        .into_iter()
        .filter_map(|assignment| match assignment {
            [SqlToken::Word(column), SqlToken::Punct('='), value @ ..]
                if column == family.key() =>
            {
                Some(key_value(value))
            }
            _ => None,
        })
        .collect();
    (!values.is_empty()).then_some(SqlWrite::Keyed {
        table,
        family,
        values,
    })
}

/// What the key expression `value` is.
fn key_value(value: &[SqlToken]) -> KeyValue {
    match is_clock(value) {
        true => KeyValue::Clock,
        false => KeyValue::Instant(sql_text(value)),
    }
}

fn sql_text(tokens: &[SqlToken]) -> String {
    tokens
        .iter()
        .map(|token| match token {
            SqlToken::Word(text) | SqlToken::Other(text) => text.clone(),
            SqlToken::Punct(character) => character.to_string(),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether `value` is the database clock or `NULL`: a clock function, a
/// `CASE` whose results all are, either in parentheses.
fn is_clock(value: &[SqlToken]) -> bool {
    match value {
        [SqlToken::Punct('('), inner @ .., SqlToken::Punct(')')]
            if closing(value, 0) == Some(value.len() - 1) =>
        {
            is_clock(inner)
        }
        [SqlToken::Word(word)] => word == "current_timestamp" || word == "null",
        [SqlToken::Word(function), SqlToken::Punct('('), SqlToken::Punct(')')] => {
            CLOCK_FUNCTIONS.contains(&function.as_str())
        }
        [SqlToken::Word(case), .., SqlToken::Word(end)] if case == "case" && end == "end" => {
            case_results(&value[1..value.len() - 1])
                .iter()
                .all(|result| is_clock(result))
        }
        _ => false,
    }
}

/// The result expressions of the inside of a `CASE .. END`: each after
/// `THEN` and after `ELSE`. A `CASE` without `ELSE` also yields `NULL`,
/// which is no instant.
fn case_results(inside: &[SqlToken]) -> Vec<&[SqlToken]> {
    let mut results = Vec::new();
    let mut index = 0;
    while index < inside.len() {
        let after = index + 1;
        match &inside[index] {
            SqlToken::Word(word) if word == "then" || word == "else" => {
                let end = stop_at(inside, after, &["when", "else"]);
                results.push(&inside[after..end]);
                index = end;
            }
            _ => index = stop_at(inside, after, &["then", "else"]),
        }
    }
    results
}

// ---------------------------------------------------------------- Rust

/// What holds a string literal.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Holder {
    /// The function of this index in [`SourceFile::functions`].
    Function(usize),
    /// The `const` or `static` item of this name.
    Item(String),
    /// Neither: an attribute, a module-level macro call.
    Other,
}

/// One string literal of test code.
#[derive(Debug)]
struct StringLiteral {
    line: usize,
    text: String,
    holder: Holder,
}

/// One function: the names in its signature and body, and the `{NAME}`
/// captures of its string literals.
#[derive(Debug, Default)]
struct Function {
    names: BTreeSet<String>,
}

/// A module declared `mod name;` in a file.
#[derive(Debug)]
struct ModuleDeclaration {
    /// The inline modules around the declaration, outermost first.
    inline_path: Vec<String>,
    name: String,
    /// The value of a `#[path = ".."]` attribute.
    path_attribute: Option<String>,
    /// The declaration compiles only under `cfg(test)`.
    test: bool,
}

/// What the scan reads of one file.
#[derive(Debug, Default)]
struct SourceFile {
    literals: Vec<StringLiteral>,
    functions: Vec<Function>,
    /// Each `const` or `static` item whose value is one string literal.
    string_items: Vec<(String, String)>,
    modules: Vec<ModuleDeclaration>,
}

/// Where the walk is.
#[derive(Debug, Clone, Default)]
struct Context {
    test: bool,
    function: Option<usize>,
    item: Option<String>,
    inline_path: Vec<String>,
}

fn decoded_string(literal: &proc_macro2::Literal) -> Option<String> {
    match syn::Lit::new(literal.clone()) {
        syn::Lit::Str(text) => Some(text.value()),
        _ => None,
    }
}

/// Whether the attribute group `#[..]` is `cfg(test)` or
/// `cfg(all(test, ..))`.
fn is_cfg_test(attribute: &proc_macro2::Group) -> bool {
    let tokens: Vec<TokenTree> = attribute.stream().into_iter().collect();
    let [TokenTree::Ident(cfg), TokenTree::Group(condition)] = tokens.as_slice() else {
        return false;
    };
    let inner: Vec<TokenTree> = condition.stream().into_iter().collect();
    let names_test = |tree: &TokenTree| matches!(tree, TokenTree::Ident(name) if name == "test");
    *cfg == "cfg"
        && match inner.as_slice() {
            [single] => names_test(single),
            [TokenTree::Ident(all), TokenTree::Group(terms)] if all == "all" => {
                terms.stream().into_iter().any(|tree| names_test(&tree))
            }
            _ => false,
        }
}

/// The value of a `path = ".."` attribute group.
fn path_attribute(attribute: &proc_macro2::Group) -> Option<String> {
    let tokens: Vec<TokenTree> = attribute.stream().into_iter().collect();
    match tokens.as_slice() {
        [TokenTree::Ident(path), TokenTree::Punct(equals), TokenTree::Literal(value)]
            if path == "path" && equals.as_char() == '=' =>
        {
            decoded_string(value)
        }
        _ => None,
    }
}

/// The index of the first brace group or `;` after `start` on this level:
/// the end of the item that starts at `start`.
fn item_end(tokens: &[TokenTree], start: usize) -> Option<usize> {
    tokens[start..]
        .iter()
        .position(|tree| match tree {
            TokenTree::Group(group) => group.delimiter() == Delimiter::Brace,
            TokenTree::Punct(punct) => punct.as_char() == ';',
            _ => false,
        })
        .map(|offset| start + offset)
}

/// Every identifier in `tokens` and in the groups in them, and every
/// `{NAME}` capture of their string literals.
fn names_in(tokens: &[TokenTree], names: &mut BTreeSet<String>, depth: usize) {
    assert!(
        depth <= MAX_TREE_DEPTH,
        "token trees nest deeper than {MAX_TREE_DEPTH} levels"
    );
    for tree in tokens {
        match tree {
            TokenTree::Ident(name) => {
                names.insert(name.to_string());
            }
            TokenTree::Group(group) => {
                let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                names_in(&inner, names, depth + 1);
            }
            TokenTree::Literal(literal) => {
                names.extend(
                    decoded_string(literal)
                        .iter()
                        .flat_map(|text| captures(text)),
                );
            }
            TokenTree::Punct(_) => {}
        }
    }
}

/// The `{NAME}` captures of a format string: `NAME` is an upper-case
/// identifier.
fn captures(text: &str) -> Vec<String> {
    text.split('{')
        .skip(1)
        .filter_map(|after| after.split_once('}'))
        .map(|(name, _)| name.to_owned())
        .filter(|name| {
            !name.is_empty()
                && name.chars().all(|character| {
                    character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
                })
        })
        .collect()
}

/// Walks one level of token trees: records the literals, functions,
/// string items and module declarations of `file`.
fn walk(tokens: &[TokenTree], context: &Context, file: &mut SourceFile, depth: usize) {
    assert!(
        depth <= MAX_TREE_DEPTH,
        "token trees nest deeper than {MAX_TREE_DEPTH} levels"
    );
    let mut context = context.clone();
    let mut pending_test = false;
    let mut pending_path = None;
    let mut index = 0;
    while index < tokens.len() {
        if let Some((attribute, next)) = attribute_at(tokens, index) {
            match (attribute.inner, is_cfg_test(&attribute.group)) {
                (true, true) => context.test = true,
                (false, true) => pending_test = true,
                (_, false) => pending_path = path_attribute(&attribute.group).or(pending_path),
            }
            index = next;
            continue;
        }
        let item_context = Context {
            test: context.test || pending_test,
            ..context.clone()
        };
        let next = match item_at(tokens, index) {
            Some(Item::Function(name_index)) => {
                function_item(tokens, index, name_index, &item_context, file, depth)
            }
            Some(Item::Value(name)) => value_item(tokens, index, name, &item_context, file, depth),
            Some(Item::Module(name)) => module_item(
                tokens,
                index,
                name,
                pending_path.take(),
                &item_context,
                file,
                depth,
            ),
            None => other_tree(tokens, index, &item_context, pending_test, file, depth),
        };
        pending_test = pending_test && !next.ends_item;
        index = next.index;
    }
}

/// An attribute `#[..]` or `#![..]` at an index.
struct Attribute {
    group: proc_macro2::Group,
    inner: bool,
}

/// The attribute at `index` and the index after it.
fn attribute_at(tokens: &[TokenTree], index: usize) -> Option<(Attribute, usize)> {
    let TokenTree::Punct(hash) = &tokens[index] else {
        return None;
    };
    if hash.as_char() != '#' {
        return None;
    }
    let (inner, group_index) = match tokens.get(index + 1) {
        Some(TokenTree::Punct(bang)) if bang.as_char() == '!' => (true, index + 2),
        _ => (false, index + 1),
    };
    match tokens.get(group_index) {
        Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Bracket => Some((
            Attribute {
                group: group.clone(),
                inner,
            },
            group_index + 1,
        )),
        _ => None,
    }
}

/// The words after `const` or `static` that do not name a value item.
const NOT_VALUE_NAMES: [&str; 5] = ["fn", "mut", "unsafe", "async", "extern"];

/// An item that starts at an index.
enum Item {
    /// `fn <name>`: the index of the name.
    Function(usize),
    /// `const <NAME>` or `static <NAME>`.
    Value(String),
    /// `mod <name>`.
    Module(String),
}

fn item_at(tokens: &[TokenTree], index: usize) -> Option<Item> {
    let (TokenTree::Ident(keyword), Some(TokenTree::Ident(name))) =
        (&tokens[index], tokens.get(index + 1))
    else {
        return None;
    };
    match keyword.to_string().as_str() {
        "fn" => Some(Item::Function(index + 1)),
        "const" | "static" if !NOT_VALUE_NAMES.contains(&name.to_string().as_str()) => {
            Some(Item::Value(name.to_string()))
        }
        "mod" => Some(Item::Module(name.to_string())),
        _ => None,
    }
}

/// Where the walk goes on after an item, and whether the item ended there.
struct Next {
    index: usize,
    ends_item: bool,
}

fn function_item(
    tokens: &[TokenTree],
    start: usize,
    name_index: usize,
    context: &Context,
    file: &mut SourceFile,
    depth: usize,
) -> Next {
    let Some(end) = item_end(tokens, name_index) else {
        return Next {
            index: tokens.len(),
            ends_item: true,
        };
    };
    let mut function = Function::default();
    names_in(&tokens[start..=end], &mut function.names, depth);
    file.functions.push(function);
    let inner_context = Context {
        function: Some(file.functions.len() - 1),
        item: None,
        ..context.clone()
    };
    walk(
        &tokens[name_index + 1..end],
        &inner_context,
        file,
        depth + 1,
    );
    if let TokenTree::Group(body) = &tokens[end] {
        let inner: Vec<TokenTree> = body.stream().into_iter().collect();
        walk(&inner, &inner_context, file, depth + 1);
    }
    Next {
        index: end + 1,
        ends_item: true,
    }
}

fn value_item(
    tokens: &[TokenTree],
    start: usize,
    name: String,
    context: &Context,
    file: &mut SourceFile,
    depth: usize,
) -> Next {
    let end = tokens[start..]
        .iter()
        .position(|tree| matches!(tree, TokenTree::Punct(punct) if punct.as_char() == ';'))
        .map_or(tokens.len(), |offset| start + offset);
    let value = &tokens[start..end];
    let equals = value
        .iter()
        .position(|tree| matches!(tree, TokenTree::Punct(punct) if punct.as_char() == '='));
    if let Some([TokenTree::Literal(literal)]) = equals.map(|position| &value[position + 1..]) {
        if let Some(text) = decoded_string(literal) {
            file.string_items.push((name.clone(), text));
        }
    }
    let inner_context = match context.function {
        Some(_) => context.clone(),
        None => Context {
            item: Some(name),
            ..context.clone()
        },
    };
    walk(&tokens[start + 2..end], &inner_context, file, depth + 1);
    Next {
        index: end + 1,
        ends_item: true,
    }
}

fn module_item(
    tokens: &[TokenTree],
    start: usize,
    name: String,
    path_attribute: Option<String>,
    context: &Context,
    file: &mut SourceFile,
    depth: usize,
) -> Next {
    match tokens.get(start + 2) {
        Some(TokenTree::Group(body)) if body.delimiter() == Delimiter::Brace => {
            let mut inner_context = context.clone();
            inner_context.inline_path.push(name);
            let inner: Vec<TokenTree> = body.stream().into_iter().collect();
            walk(&inner, &inner_context, file, depth + 1);
        }
        _ => file.modules.push(ModuleDeclaration {
            inline_path: context.inline_path.clone(),
            name,
            path_attribute,
            test: context.test,
        }),
    }
    Next {
        index: start + 3,
        ends_item: true,
    }
}

/// Any other tree: a literal is recorded, a group is walked. A brace group
/// or a `;` ends a pending `#[cfg(test)]` item.
fn other_tree(
    tokens: &[TokenTree],
    index: usize,
    context: &Context,
    pending_test: bool,
    file: &mut SourceFile,
    depth: usize,
) -> Next {
    let ends_item = match &tokens[index] {
        TokenTree::Literal(literal) => {
            if let (true, Some(text)) = (context.test, decoded_string(literal)) {
                file.literals.push(StringLiteral {
                    line: literal.span().start().line,
                    text,
                    holder: match (&context.function, &context.item) {
                        (Some(function), _) => Holder::Function(*function),
                        (None, Some(item)) => Holder::Item(item.clone()),
                        (None, None) => Holder::Other,
                    },
                });
            }
            false
        }
        TokenTree::Group(group) => {
            let inner: Vec<TokenTree> = group.stream().into_iter().collect();
            walk(&inner, context, file, depth + 1);
            group.delimiter() == Delimiter::Brace
        }
        TokenTree::Punct(punct) => punct.as_char() == ';',
        TokenTree::Ident(_) => false,
    };
    Next {
        index: index + 1,
        ends_item: ends_item && pending_test,
    }
}

/// The scan of one source text whose code is test code when `test`.
fn read_source(source: &str, test: bool) -> SourceFile {
    let tokens: Vec<TokenTree> = TokenStream::from_str(source)
        .unwrap_or_else(|error| panic!("the source does not lex: {error:?}"))
        .into_iter()
        .collect();
    let mut file = SourceFile::default();
    walk(
        &tokens,
        &Context {
            test,
            ..Context::default()
        },
        &mut file,
        0,
    );
    file
}

// ---------------------------------------------------------------- the rule

/// A seed that breaks the rule.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Finding {
    file: String,
    line: usize,
    table: String,
    cause: String,
}

/// What the rule saw in the scanned files.
#[derive(Debug, Default, PartialEq, Eq)]
struct Tally {
    clock_writes: usize,
    guarded_writes: BTreeMap<&'static str, usize>,
    findings: Vec<Finding>,
}

/// `text` with each `{NAME}` replaced by the one text of the string item
/// `NAME`; a name with no text or with two texts stays.
fn substituted(text: &str, items: &BTreeMap<String, BTreeSet<String>>) -> String {
    let mut text = text.to_owned();
    for name in captures(&text) {
        if let Some([value]) = items
            .get(&name)
            .map(|values| values.iter().collect::<Vec<_>>())
            .as_deref()
        {
            text = text.replace(&format!("{{{name}}}"), value);
        }
    }
    text
}

/// Whether every function that the holder makes responsible names a guard
/// of `family`: the function of the literal, or each function of the file
/// that names the item. An item that no function of its file names fails.
fn guarded(file: &SourceFile, holder: &Holder, family: Family) -> bool {
    let names_guard = |function: &Function| {
        family
            .guards()
            .iter()
            .any(|guard| function.names.contains(*guard))
    };
    match holder {
        Holder::Function(index) => names_guard(&file.functions[*index]),
        Holder::Item(item) => {
            let users: Vec<&Function> = file
                .functions
                .iter()
                .filter(|function| function.names.contains(item))
                .collect();
            !users.is_empty() && users.into_iter().all(names_guard)
        }
        Holder::Other => false,
    }
}

/// Applies the rule to the literals of `file`, named `name`.
fn tally_file(
    name: &str,
    file: &SourceFile,
    items: &BTreeMap<String, BTreeSet<String>>,
    tally: &mut Tally,
) {
    for literal in &file.literals {
        for write in sql_writes(&substituted(&literal.text, items), 0) {
            let finding = |table: String, cause: String| Finding {
                file: name.to_owned(),
                line: literal.line,
                table,
                cause,
            };
            match write {
                SqlWrite::Unreadable { table, cause } => {
                    tally.findings.push(finding(table, cause.to_owned()));
                }
                SqlWrite::Keyed {
                    table,
                    family,
                    values,
                } => {
                    let instants: Vec<String> = values
                        .into_iter()
                        .filter_map(|value| match value {
                            KeyValue::Clock => None,
                            KeyValue::Instant(text) => Some(text),
                        })
                        .collect();
                    match (instants.is_empty(), guarded(file, &literal.holder, family)) {
                        (true, _) => tally.clock_writes += 1,
                        (false, true) => {
                            *tally.guarded_writes.entry(family.key()).or_default() += 1;
                        }
                        (false, false) => tally.findings.push(finding(
                            table,
                            format!(
                                "{} = {} is not the database clock, and no function that runs \
                                 it calls {}",
                                family.key(),
                                instants.join(" | "),
                                family.guards()[0]
                            ),
                        )),
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------- the files

/// The crate folder.
fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_text(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// Every `.rs` file under `directory`, `depth` levels below the walk's root.
fn rust_files(directory: &Path, depth: usize, files: &mut Vec<PathBuf>) {
    assert!(
        depth <= MAX_DIRECTORY_DEPTH,
        "{} is more than {MAX_DIRECTORY_DEPTH} levels deep",
        directory.display()
    );
    let entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read the directory {}: {error}", directory.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| panic!("read an entry of {}: {error}", directory.display()))
            .path();
        let is_rust = path.extension().is_some_and(|extension| extension == "rs");
        match (path.is_dir(), is_rust) {
            (true, _) => rust_files(&path, depth + 1, files),
            (false, true) => files.push(path),
            (false, false) => {}
        }
    }
}

/// The folder of the submodules of the module whose file is `file`.
fn submodule_directory(file: &Path) -> PathBuf {
    let parent = file.parent().expect("a source file has a folder");
    match file.file_name().and_then(|name| name.to_str()) {
        Some("mod.rs" | "lib.rs" | "main.rs") => parent.to_path_buf(),
        _ => parent.join(file.file_stem().expect("a source file has a stem")),
    }
}

/// The file of the module that `declaration` of `file` declares.
fn module_file(file: &Path, declaration: &ModuleDeclaration) -> PathBuf {
    let folder = declaration
        .inline_path
        .iter()
        .fold(submodule_directory(file), |folder, name| folder.join(name));
    match (
        &declaration.path_attribute,
        declaration.inline_path.is_empty(),
    ) {
        (Some(path), true) => file
            .parent()
            .expect("a source file has a folder")
            .join(path),
        (Some(path), false) => panic!(
            "{}: #[path = {path:?}] inside an inline module is not resolved by this scan",
            file.display()
        ),
        (None, _) => {
            let flat = folder.join(format!("{}.rs", declaration.name));
            match flat.exists() {
                true => flat,
                false => folder.join(&declaration.name).join("mod.rs"),
            }
        }
    }
}

/// Every file of the module tree of `src/lib.rs`, read, with whether it
/// compiles only under `cfg(test)`.
fn module_tree(root: &Path) -> BTreeMap<PathBuf, SourceFile> {
    let mut files = BTreeMap::new();
    let mut pending = vec![(root.join("src/lib.rs"), false)];
    while let Some((path, test)) = pending.pop() {
        assert!(
            files.len() < MAX_MODULE_FILES,
            "the module tree reaches more than {MAX_MODULE_FILES} files"
        );
        let file = read_source(&read_text(&path), test);
        for declaration in &file.modules {
            pending.push((module_file(&path, declaration), test || declaration.test));
        }
        files.insert(path, file);
    }
    files
}

/// The rule over the crate: the module tree of `src/`, and `tests/`.
fn tally_crate() -> Tally {
    let root = crate_root();
    let mut sources = module_tree(&root);
    let mut on_disk = Vec::new();
    rust_files(&root.join("src"), 0, &mut on_disk);
    let unreached: Vec<&PathBuf> = on_disk
        .iter()
        .filter(|path| !sources.contains_key(*path))
        .collect();
    assert!(
        unreached.is_empty(),
        "files the module tree does not reach: {unreached:?}"
    );
    let mut test_files = Vec::new();
    rust_files(&root.join("tests"), 0, &mut test_files);
    for path in test_files {
        let file = read_source(&read_text(&path), true);
        sources.insert(path, file);
    }
    let mut items: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (name, text) in sources.values().flat_map(|file| file.string_items.iter()) {
        items.entry(name.clone()).or_default().insert(text.clone());
    }
    let mut tally = Tally::default();
    for (path, file) in &sources {
        let name = path
            .strip_prefix(&root)
            .expect("a scanned file is in the crate")
            .to_string_lossy()
            .into_owned();
        if name != GUARD_FILE && name != SCAN_FILE {
            tally_file(&name, file, &items, &mut tally);
        }
    }
    tally
}

#[test]
fn every_partition_seed_of_test_code_is_the_clock_or_goes_through_a_guard() {
    let tally = tally_crate();

    let report: Vec<String> = tally
        .findings
        .iter()
        .map(|finding| {
            format!(
                "{}:{}: {}: {}",
                finding.file, finding.line, finding.table, finding.cause
            )
        })
        .collect();
    assert!(
        report.is_empty(),
        "unguarded partition seeds:\n{}",
        report.join("\n")
    );
    assert!(
        tally.clock_writes > 0,
        "the scan read no seed at the clock: {tally:?}"
    );
    assert!(
        tally.guarded_writes.contains_key("sent_at")
            && tally.guarded_writes.contains_key("terminal_at"),
        "the scan read no guarded seed of each family: {tally:?}"
    );
}

// ---------------------------------------------------------------- planted seeds

/// The rule over one planted test-code source.
fn tally_planted(source: &str) -> Tally {
    let file = read_source(source, false);
    let mut items: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (name, text) in &file.string_items {
        items.entry(name.clone()).or_default().insert(text.clone());
    }
    let mut tally = Tally::default();
    tally_file("planted.rs", &file, &items, &mut tally);
    tally
}

fn finding_lines(tally: &Tally) -> Vec<usize> {
    tally.findings.iter().map(|finding| finding.line).collect()
}

/// Unguarded seeds of test code, one per line from line 5 to line 13.
const UNGUARDED: &str = r#"
const PARENT: &str = "horsies_task_history";
#[cfg(test)]
mod tests {
    fn a() { query("INSERT INTO horsies_heartbeats (task_id, sent_at) VALUES ($1, NOW() - interval '5 minutes')"); }
    fn b(at: DateTime<Utc>) { query("INSERT INTO horsies_heartbeats (task_id, sent_at) VALUES ($1, $2)").bind(at); }
    fn c() { query(&format!("UPDATE {PARENT} SET status = 'X', terminal_at = $2 WHERE task_id = $1")); }
    fn d() { query("insert into public.horsies_heartbeats_2026_01_01_00 (sent_at, task_id) select t.claimed_at, t.id from horsies_tasks t"); }
    fn e() { query("UPDATE horsies_task_history SET terminal_at = CASE WHEN $2 THEN NOW() ELSE $3 END"); }
    fn f() { query("CREATE FUNCTION g() RETURNS void LANGUAGE sql AS $$ INSERT INTO horsies_task_history (terminal_at) VALUES (now() - interval '1 day') $$"); }
    fn g() { query("INSERT INTO horsies_heartbeats SELECT * FROM other"); }
    fn h() { query("UPDATE {UNKNOWN_TABLE} SET terminal_at = $1"); }
    fn i() { let _covered = covered_beat_instant(&pool, SeedInstant::At(at)).await; query("UPDATE horsies_task_history SET terminal_at = $1"); }
}
"#;

#[test]
fn each_planted_unguarded_seed_is_found() {
    let tally = tally_planted(UNGUARDED);

    assert_eq!(
        finding_lines(&tally),
        (5..=13).collect::<Vec<_>>(),
        "{tally:?}"
    );
}

/// An item whose SQL a function without a guard runs: found at the line of
/// the item.
const UNGUARDED_ITEM: &str = r#"
#[cfg(test)]
mod tests {
    const BEAT_SQL: &str = "INSERT INTO horsies_heartbeats (task_id, role, sent_at) VALUES ($1, $2, CAST($3 AS timestamptz))";
    async fn guarded(at: DateTime<Utc>) { let covered = covered_beat_instant(&pool, SeedInstant::At(at)).await; run(BEAT_SQL, covered.at()); }
    async fn unguarded(at: DateTime<Utc>) { run(BEAT_SQL, at); }
}
"#;

#[test]
fn an_item_that_one_unguarded_function_runs_is_found() {
    let tally = tally_planted(UNGUARDED_ITEM);

    assert_eq!(finding_lines(&tally), vec![4], "{tally:?}");
}

/// Seeds that keep the rule, and seeds outside test code.
const KEPT: &str = r#"
const HEARTBEATS: &str = "horsies_heartbeats";
fn product() { query("INSERT INTO horsies_heartbeats (task_id, sent_at) VALUES ($1, $2)"); }
#[cfg(test)]
mod tests {
    fn a() { query("INSERT INTO horsies_heartbeats (task_id, sent_at) VALUES ($1, NOW()), ($2, statement_timestamp())"); }
    fn b() { query(&format!("INSERT INTO {HEARTBEATS} (sent_at, task_id) VALUES (CURRENT_TIMESTAMP, $1)")); }
    fn c() { query("INSERT INTO horsies_task_history (task_id, terminal_at) SELECT id, CASE WHEN $2 = 'NOW' THEN NOW() END FROM horsies_tasks"); }
    async fn d(at: DateTime<Utc>) { let covered = covered_history_instant(&pool, "standard_30d", SeedInstant::At(at)).await; query("UPDATE horsies_task_history SET terminal_at = $2 WHERE task_id = $1").bind(covered.at()); }
    async fn e(covered: CoveredBeatInstant) { query("INSERT INTO horsies_heartbeats (task_id, sent_at) VALUES ($1, $2)").bind(covered.at()); }
    fn f() { query("UPDATE horsies_task_history SET status = 'BOGUS' WHERE task_id = $1"); }
    fn g() { query("INSERT INTO horsies_task_history_clock_exceptions (task_id, terminal_at) VALUES ($1, $2)"); }
    fn h() { query("SELECT 1 FROM horsies_tasks FOR UPDATE; -- INSERT INTO horsies_heartbeats (sent_at) VALUES ($1)"); }
    fn i() { query("INSERT INTO horsies_tasks (id, sent_at) VALUES ($1, 'it''s INSERT INTO horsies_heartbeats (sent_at) VALUES ($2)')"); }
}
fn after_the_test_module() { query("UPDATE horsies_task_history SET terminal_at = $1"); }
"#;

#[test]
fn seeds_at_the_clock_guarded_seeds_and_code_outside_tests_pass() {
    let tally = tally_planted(KEPT);

    assert!(tally.findings.is_empty(), "{tally:?}");
    assert_eq!(tally.clock_writes, 3, "{tally:?}");
    assert_eq!(tally.guarded_writes.values().sum::<usize>(), 2, "{tally:?}");
}

/// A guard named in another family does not guard the seed.
const OTHER_FAMILY_GUARD: &str = r#"
#[cfg(test)]
fn seed(at: DateTime<Utc>) {
    let _covered = covered_beat_instant(&pool, SeedInstant::At(at)).await;
    query("UPDATE horsies_task_history SET terminal_at = $1");
}
"#;

#[test]
fn the_guard_of_the_other_family_does_not_guard_a_seed() {
    let tally = tally_planted(OTHER_FAMILY_GUARD);

    assert_eq!(finding_lines(&tally), vec![5], "{tally:?}");
}
