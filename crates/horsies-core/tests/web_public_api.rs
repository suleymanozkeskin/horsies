//! The public `web` API of the core names no axum type. The handler's
//! interface does not depend on one framework: a host framework mounts
//! `MonitoringHandler` through `RequestParts` and `ResponseParts`, whose
//! types come from the `http` and `bytes` crates. axum stays internal to the
//! `web` feature, in the router behind `MonitoringHandler::handle`.
//! Through the `impl` blocks the scan also reads the public methods of
//! types that the API does not export; it is stricter than the API.
//!
//! The public API is what `src/web/mod.rs` re-exports (`pub use`). The scan
//! lexes `src/web/mod.rs` and each module of `src/web/` that it re-exports
//! from into token trees (`proc-macro2`), and checks every public item of
//! those files:
//!
//! - the path of each `pub use`;
//! - the signature of each `pub fn`, at the top level and in `impl` blocks;
//! - each `pub const`, `pub static` and `pub type`;
//! - the whole of each `pub enum` and `pub trait`;
//! - the generics and each `pub` field of each `pub struct`.
//!
//! An item fails when one of its identifiers is `axum`, or a name that its
//! file imports from `axum`. An item with a restricted visibility
//! (`pub(crate)`, `pub(super)`) is not public and is not checked.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use proc_macro2::{Delimiter, Spacing, TokenStream, TokenTree};

/// The crate whose types the public `web` API must not name.
const FRAMEWORK: &str = "axum";

/// The deepest nesting of token trees that a check enters.
const MAX_TREE_DEPTH: usize = 64;

/// The file of the `web` module `name` (`mod.rs` or `<module>.rs`).
fn web_file(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("web")
        .join(name)
}

/// The top-level token trees of `path`.
///
/// Panics: a file that does not read or does not lex (the test cannot run).
fn lex(path: &Path) -> Vec<TokenTree> {
    let text =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    TokenStream::from_str(&text)
        .unwrap_or_else(|error| panic!("lex {}: {error:?}", path.display()))
        .into_iter()
        .collect()
}

fn is_ident(tree: &TokenTree, word: &str) -> bool {
    matches!(tree, TokenTree::Ident(ident) if ident == word)
}

fn is_punct(tree: &TokenTree, ch: char) -> bool {
    matches!(tree, TokenTree::Punct(punct) if punct.as_char() == ch)
}

fn is_group(tree: &TokenTree, delimiter: Delimiter) -> bool {
    matches!(tree, TokenTree::Group(group) if group.delimiter() == delimiter)
}

/// Whether the token at `index` starts a path separator `::`.
fn starts_path_separator(trees: &[TokenTree], index: usize) -> bool {
    match (trees.get(index), trees.get(index + 1)) {
        (Some(TokenTree::Punct(first)), Some(TokenTree::Punct(second))) => {
            first.as_char() == ':' && first.spacing() == Spacing::Joint && second.as_char() == ':'
        }
        _ => false,
    }
}

/// The index after the item that starts at `start`: after its first
/// top-level `;`, or after its first top-level `{ ... }` group. `None` when
/// the item runs to the end of `trees`.
fn item_end(trees: &[TokenTree], start: usize) -> Option<usize> {
    trees[start..]
        .iter()
        .position(|tree| is_punct(tree, ';') || is_group(tree, Delimiter::Brace))
        .map(|offset| start + offset + 1)
}

/// The index after the attributes (`#[...]`, `#![...]`) that start at
/// `index`.
fn skip_attributes(trees: &[TokenTree], mut index: usize) -> usize {
    loop {
        match (trees.get(index), trees.get(index + 1), trees.get(index + 2)) {
            (Some(hash), Some(group), _)
                if is_punct(hash, '#') && is_group(group, Delimiter::Bracket) =>
            {
                index += 2;
            }
            (Some(hash), Some(bang), Some(group))
                if is_punct(hash, '#')
                    && is_punct(bang, '!')
                    && is_group(group, Delimiter::Bracket) =>
            {
                index += 3;
            }
            _ => return index,
        }
    }
}

/// Whether the visibility at `index` is plain `pub`: `pub` and no
/// restriction group after it.
fn is_plain_pub(trees: &[TokenTree], index: usize) -> bool {
    match trees.get(index) {
        Some(tree) if is_ident(tree, "pub") => {
            !matches!(trees.get(index + 1), Some(next) if is_group(next, Delimiter::Parenthesis))
        }
        _ => false,
    }
}

/// The names that a file imports from `axum`: the last name of each path of
/// each `use axum::...` (any visibility), its alias after `as`, or the
/// module before a `self`.
fn framework_imports(trees: &[TokenTree]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut index = 0;
    while index < trees.len() {
        let start = skip_attributes(trees, index);
        let mut cursor = start;
        if matches!(trees.get(cursor), Some(tree) if is_ident(tree, "pub")) {
            cursor += 1;
            if matches!(trees.get(cursor), Some(tree) if is_group(tree, Delimiter::Parenthesis)) {
                cursor += 1;
            }
        }
        let is_framework_use = matches!(trees.get(cursor), Some(tree) if is_ident(tree, "use"))
            && matches!(trees.get(cursor + 1), Some(tree) if is_ident(tree, FRAMEWORK));
        let end = item_end(trees, start.max(index)).unwrap_or(trees.len());
        if is_framework_use {
            collect_use_leaves(&trees[cursor + 1..end], None, &mut names, 0);
        }
        index = end.max(start + 1);
    }
    names
}

/// The leaf names of a use tree, as [`framework_imports`] reads them.
/// `parent` is the path segment before a `{ ... }` group: the module that a
/// `self` in the group names.
fn collect_use_leaves(
    trees: &[TokenTree],
    parent: Option<String>,
    names: &mut BTreeSet<String>,
    depth: usize,
) {
    assert!(
        depth < MAX_TREE_DEPTH,
        "a use tree nests past {MAX_TREE_DEPTH} levels"
    );
    let mut previous_segment = parent;
    let mut index = 0;
    while index < trees.len() {
        match &trees[index] {
            TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => {
                let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                collect_use_leaves(&inner, previous_segment.clone(), names, depth + 1);
            }
            TokenTree::Ident(ident) if ident == "as" => {
                if let Some(TokenTree::Ident(alias)) = trees.get(index + 1) {
                    names.insert(alias.to_string());
                    index += 1;
                }
            }
            TokenTree::Ident(ident) if ident == "self" => {
                if let Some(segment) = &previous_segment {
                    names.insert(segment.clone());
                }
            }
            TokenTree::Ident(ident) => {
                let renamed = matches!(trees.get(index + 1), Some(next) if is_ident(next, "as"));
                match starts_path_separator(trees, index + 1) {
                    true => previous_segment = Some(ident.to_string()),
                    false if renamed => {}
                    false => {
                        names.insert(ident.to_string());
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }
}

/// The identifiers of `trees` that `forbidden` holds, nested groups
/// included.
fn forbidden_names(trees: &[TokenTree], forbidden: &BTreeSet<String>, depth: usize) -> Vec<String> {
    assert!(
        depth < MAX_TREE_DEPTH,
        "an item nests past {MAX_TREE_DEPTH} levels"
    );
    trees
        .iter()
        .flat_map(|tree| match tree {
            TokenTree::Ident(ident) if forbidden.contains(&ident.to_string()) => {
                vec![ident.to_string()]
            }
            TokenTree::Group(group) => {
                let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                forbidden_names(&inner, forbidden, depth + 1)
            }
            _ => Vec::new(),
        })
        .collect()
}

/// What the check reads of a public item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ItemKind {
    /// A function (`fn`, `async fn`, `const fn`): its signature.
    Function,
    /// A struct: its generics and its `pub` fields.
    Struct,
    /// Any other item: every token.
    Whole,
}

/// The kind of the public item `item`: a function when `fn` comes before
/// its first `{ ... }` group, a struct when `struct` does.
fn item_kind(item: &[TokenTree]) -> ItemKind {
    let head = item
        .iter()
        .take_while(|tree| !is_group(tree, Delimiter::Brace));
    let mut kind = ItemKind::Whole;
    for tree in head {
        match (is_ident(tree, "fn"), is_ident(tree, "struct")) {
            (true, _) => return ItemKind::Function,
            (false, true) => kind = ItemKind::Struct,
            (false, false) => {}
        }
    }
    kind
}

/// The tokens of the public item `item` (from its `pub` to its end) that
/// the check reads ([`ItemKind`]).
fn checked_tokens(item: &[TokenTree]) -> Vec<TokenTree> {
    match item_kind(item) {
        ItemKind::Function => item
            .iter()
            .take_while(|tree| !is_group(tree, Delimiter::Brace))
            .cloned()
            .collect(),
        ItemKind::Struct => struct_tokens(item),
        ItemKind::Whole => item.to_vec(),
    }
}

/// `trees` split at the commas outside angle brackets: the fields of a
/// struct body, whose types may hold `<A, B>`. A `>` after a joint `-` is
/// an arrow (`fn() -> T`), not a bracket.
fn split_fields(trees: &[TokenTree]) -> Vec<&[TokenTree]> {
    let mut fields = Vec::new();
    let mut angle_depth: usize = 0;
    let mut field_start = 0;
    for (index, tree) in trees.iter().enumerate() {
        let arrow = matches!(
            (index.checked_sub(1).and_then(|before| trees.get(before)), tree),
            (Some(TokenTree::Punct(dash)), TokenTree::Punct(close))
                if dash.as_char() == '-' && dash.spacing() == Spacing::Joint && close.as_char() == '>'
        );
        match tree {
            TokenTree::Punct(punct) if punct.as_char() == '<' => angle_depth += 1,
            TokenTree::Punct(punct) if punct.as_char() == '>' && !arrow => {
                angle_depth = angle_depth.saturating_sub(1);
            }
            TokenTree::Punct(punct) if punct.as_char() == ',' && angle_depth == 0 => {
                fields.push(&trees[field_start..index]);
                field_start = index + 1;
            }
            _ => {}
        }
    }
    fields.push(&trees[field_start..]);
    fields
}

/// The generics of a struct and its `pub` fields, named or positional.
fn struct_tokens(item: &[TokenTree]) -> Vec<TokenTree> {
    let mut tokens = Vec::new();
    for tree in item {
        match tree {
            TokenTree::Group(group)
                if matches!(group.delimiter(), Delimiter::Brace | Delimiter::Parenthesis) =>
            {
                let fields: Vec<TokenTree> = group.stream().into_iter().collect();
                for field in split_fields(&fields) {
                    let start = skip_attributes(field, 0);
                    if is_plain_pub(field, start) {
                        tokens.extend(field[start..].iter().cloned());
                    }
                }
            }
            other => tokens.push(other.clone()),
        }
    }
    tokens
}

/// The public items of `trees` (top level and in `impl` blocks), each as its
/// tokens from `pub` to its end.
fn public_items(trees: &[TokenTree], depth: usize) -> Vec<Vec<TokenTree>> {
    assert!(
        depth < MAX_TREE_DEPTH,
        "impl blocks nest past {MAX_TREE_DEPTH} levels"
    );
    let mut items = Vec::new();
    let mut index = 0;
    while index < trees.len() {
        let start = skip_attributes(trees, index);
        let Some(end) = item_end(trees, start) else {
            break;
        };
        let item = &trees[start..end];
        if is_plain_pub(trees, start) {
            items.push(item.to_vec());
        }
        if matches!(item.first(), Some(tree) if is_ident(tree, "impl")) {
            if let Some(TokenTree::Group(body)) = item.last() {
                let inner: Vec<TokenTree> = body.stream().into_iter().collect();
                items.extend(public_items(&inner, depth + 1));
            }
        }
        index = end;
    }
    items
}

/// The modules of `src/web/` that the `pub use` items of `mod.rs` re-export
/// from, and the violations of those `pub use` items.
fn reexports(mod_trees: &[TokenTree]) -> (BTreeSet<String>, Vec<String>) {
    let forbidden = BTreeSet::from([FRAMEWORK.to_owned()]);
    let mut modules = BTreeSet::new();
    let mut violations = Vec::new();
    for item in public_items(mod_trees, 0) {
        if !matches!(item.get(1), Some(tree) if is_ident(tree, "use")) {
            continue;
        }
        let path = TokenStream::from_iter(item.iter().cloned()).to_string();
        if !forbidden_names(&item, &forbidden, 0).is_empty() {
            violations.push(format!("mod.rs: {path}"));
        }
        if let Some(TokenTree::Ident(first)) = item.get(2) {
            let module = first.to_string();
            if web_file(&format!("{module}.rs")).is_file() {
                modules.insert(module);
            }
        }
    }
    (modules, violations)
}

#[test]
fn the_public_web_api_names_no_axum_type() {
    let mod_trees = lex(&web_file("mod.rs"));
    let (modules, mut violations) = reexports(&mod_trees);
    assert!(
        modules.contains("handler"),
        "the scan must reach the module of MonitoringHandler; it found {modules:?}"
    );
    for module in &modules {
        let trees = lex(&web_file(&format!("{module}.rs")));
        let mut forbidden = framework_imports(&trees);
        forbidden.insert(FRAMEWORK.to_owned());
        for item in public_items(&trees, 0) {
            let named = forbidden_names(&checked_tokens(&item), &forbidden, 0);
            if !named.is_empty() {
                let head = TokenStream::from_iter(item.iter().take(6).cloned()).to_string();
                violations.push(format!("{module}.rs: `{head} ...` names {named:?}"));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "public items of the web module that name axum types:\n{}",
        violations.join("\n")
    );
}

/// The scan finds an axum name in a public item, and passes over a private
/// one and an internal import.
#[test]
fn the_scan_flags_a_public_item_that_names_an_imported_axum_type() {
    let source = "
        use axum::Router;
        use axum::http::{self, StatusCode as Code};
        pub struct Exposed { pub routers: HashMap<String, Router>, hidden: Router }
        pub struct Hidden { hidden: Router }
        pub fn status() -> Code { Code::OK }
        pub(crate) fn internal() -> Router { Router::new() }
        impl Hidden { pub fn module() -> http::Method { todo!() } fn private() -> Router { todo!() } }
    ";
    let trees: Vec<TokenTree> = TokenStream::from_str(source)
        .expect("the sample lexes")
        .into_iter()
        .collect();
    let mut forbidden = framework_imports(&trees);
    forbidden.insert(FRAMEWORK.to_owned());
    assert_eq!(
        forbidden,
        BTreeSet::from([
            "Code".to_owned(),
            "Router".to_owned(),
            "axum".to_owned(),
            "http".to_owned(),
        ])
    );
    let flagged: Vec<Vec<String>> = public_items(&trees, 0)
        .iter()
        .map(|item| forbidden_names(&checked_tokens(item), &forbidden, 0))
        .collect();
    assert_eq!(
        flagged,
        vec![
            vec!["Router".to_owned()],
            Vec::new(),
            vec!["Code".to_owned()],
            vec!["http".to_owned()],
        ]
    );
}
