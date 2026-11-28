//! No production path of the core or of the Rust client reads a JSON text into
//! a `serde_json::Value`, except the sites of [`KNOWN_SITES`], each with the
//! reason why it may.
//!
//! Without `arbitrary-precision`, a `serde_json::Value` holds an integer
//! outside the `i64` and `u64` range as an `f64`. A user value (a task input,
//! a result, a dependency result, a workflow output, the output of a
//! sub-workflow summary, the `data` of an error) that passes through one loses
//! digits. The user-value paths keep the value as its text (`JsonText`,
//! `&RawValue`) or decode the asked type from the text.
//!
//! The scan lexes each production module of `core/src` and
//! `clients/rust/horsies/src` into token trees (`proc-macro2`): `lib.rs`, then each
//! file that a `mod name;` declares, and skips each item that `#[cfg(test)]`
//! gates. A site is one of [`SiteKind`]. A site is keyed by its file, its
//! enclosing item (`module::Type::function`) and its kind, with a count. The
//! test fails on a key or a count that the list does not hold, and on a listed
//! key that the scan no longer finds: a new read into `Value` is classified
//! in the list, in the same change.
//!
//! A row decoder is a function of the two crates with a `FromSql` or
//! `FromSqlOwned` bound (the statement layer's `column` and `scalar`, and each
//! reader that wraps them), or the method `try_get` of a row. Its call names
//! no type: the scan takes the type from the turbofish, from the type of a
//! typed `let`, or from the field that the call fills, of a struct or variant
//! literal or a tuple constructor (`Self` is the type of its `impl`). A first
//! pass over every module file collects the row decoders and the fields whose
//! type names `Value`; a second pass finds the sites. The scan does not see a
//! row decoder whose type comes from a function argument, a return type, a
//! method chain or an untyped `let`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use proc_macro2::{Delimiter, Spacing, TokenStream, TokenTree};

/// The deepest nesting of token trees that the walk enters.
const MAX_TREE_DEPTH: usize = 256;
/// The most module files that one crate may have.
const MAX_MODULE_FILES: usize = 2_000;

/// The functions whose turbofish names the type that they read.
const READERS: [&str; 13] = [
    "from_str",
    "from_slice",
    "from_reader",
    "from_json_str",
    "from_json_bytes",
    "from_json_bytes_with",
    "decode_checked",
    "task_result_from_json_str",
    "decode_task_result",
    "decode",
    "into_iter",
    "try_get",
    "get",
];

/// The functions that make the value of a typed `let` a read.
const LET_READERS: [&str; 12] = [
    "from_str",
    "from_slice",
    "from_reader",
    "from_value",
    "from_json_str",
    "from_json_bytes",
    "from_json_bytes_with",
    "decode_checked",
    "task_result_from_json_str",
    "decode_task_result",
    "decode",
    "deserialize",
];

/// The words before an item keyword that keep the attributes of the item.
const QUALIFIERS: [&str; 7] = [
    "pub", "async", "unsafe", "extern", "default", "const", "static",
];

/// The bounds that make a function a row decoder.
const DECODER_BOUNDS: [&str; 2] = ["FromSql", "FromSqlOwned"];

/// The method of a row that decodes a column (`Row::try_get`).
const ROW_METHOD: &str = "try_get";

/// The form of a read into `Value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SiteKind {
    /// A call of a reader or a row decoder whose turbofish names `Value`
    /// (`from_str::<serde_json::Value>(..)`,
    /// `column::<serde_json::Value>(..)`), or a `::deserialize(..)` call on a
    /// path whose turbofish names it.
    TurbofishValue,
    /// A `let` whose type names `Value` and whose value calls a reader or a
    /// row decoder (`let value: serde_json::Value = from_json_str(text)?`).
    TypedLetParse,
    /// `Value::deserialize(..)`.
    ValueDeserialize,
    /// A call of `from_value`, which decodes a type from a `Value`.
    FromValue,
    /// A field whose type names `Value`, of a type that derives `Deserialize`
    /// or `FromRow`.
    DeserializeField,
    /// A call of a row decoder that fills a field whose type names `Value`:
    /// a field of a struct or variant literal
    /// (`Stored { state: column(row, statement, "state")? }`), or an argument
    /// of a tuple struct or variant (`State(column(row, statement, "state")?)`).
    RowDecoderField,
}

/// Where a site is.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SiteKey {
    /// The file, from the repository root.
    file: String,
    /// The enclosing items, `::`-joined; empty at the top of a file.
    item: String,
    kind: SiteKind,
}

/// Why a listed site may read into `Value`.
#[derive(Debug, Clone, Copy)]
enum Allowed {
    /// The value is not a user value.
    NotUserValue(&'static str),
    /// A user value on no production path.
    NoProductionCaller(&'static str),
}

impl Allowed {
    fn reason(self) -> &'static str {
        match self {
            Self::NotUserValue(reason) | Self::NoProductionCaller(reason) => reason,
        }
    }
}

/// One listed site.
struct Known {
    file: &'static str,
    item: &'static str,
    kind: SiteKind,
    count: usize,
    allowed: Allowed,
}

const CORE: &str = "core/src/";
const CLIENT: &str = "clients/rust/horsies/src/";

/// Every read into `Value` of the production modules, with its reason.
const KNOWN_SITES: &[Known] = &[
    Known {
        file: "core/src/broker/class_coverage_reads.rs",
        item: "LiveEntry",
        kind: SiteKind::DeserializeField,
        count: 1,
        allowed: Allowed::NotUserValue("the class capacity of a live worker"),
    },
    Known {
        file: "core/src/broker/class_coverage_reads.rs",
        item: "decoded_part",
        kind: SiteKind::FromValue,
        count: 1,
        allowed: Allowed::NotUserValue("a part of the class coverage read"),
    },
    Known {
        file: "core/src/broker/class_coverage_reads.rs",
        item: "coverage_row_from",
        kind: SiteKind::RowDecoderField,
        count: 2,
        allowed: Allowed::NotUserValue(
            "the live workers and the pending streams of the class coverage read",
        ),
    },
    Known {
        file: "core/src/broker/class_coverage_reads.rs",
        item: "PostgresBroker::report_row",
        kind: SiteKind::RowDecoderField,
        count: 3,
        allowed: Allowed::NotUserValue(
            "the live workers, the pending heads and the rows in flight of the class report",
        ),
    },
    Known {
        file: "core/src/broker/core_rows.rs",
        item: "stored_worker_state",
        kind: SiteKind::RowDecoderField,
        count: 5,
        allowed: Allowed::NotUserValue(
            "the queue priorities, the queue concurrency limits, the recovery config, the down \
             slots and the class capacity of a worker state",
        ),
    },
    Known {
        file: "core/src/broker/postgres.rs",
        item: "decode_pong",
        kind: SiteKind::FromValue,
        count: 1,
        allowed: Allowed::NotUserValue("the payload of a worker pong"),
    },
    Known {
        file: "core/src/broker/postgres.rs",
        item: "decode_pong",
        kind: SiteKind::TypedLetParse,
        count: 1,
        allowed: Allowed::NotUserValue("the payload of a worker pong"),
    },
    Known {
        file: "core/src/core/config/broker.rs",
        item: "PostgresConfig::deserialize",
        kind: SiteKind::FromValue,
        count: 1,
        allowed: Allowed::NotUserValue("the broker config"),
    },
    Known {
        file: "core/src/core/config/broker.rs",
        item: "PostgresConfig::deserialize",
        kind: SiteKind::ValueDeserialize,
        count: 1,
        allowed: Allowed::NotUserValue("the broker config"),
    },
    Known {
        file: "core/src/core/config/recovery.rs",
        item: "RecoveryConfig::deserialize",
        kind: SiteKind::FromValue,
        count: 1,
        allowed: Allowed::NotUserValue("the recovery config"),
    },
    Known {
        file: "core/src/core/config/recovery.rs",
        item: "RecoveryConfig::deserialize",
        kind: SiteKind::ValueDeserialize,
        count: 1,
        allowed: Allowed::NotUserValue("the recovery config"),
    },
    Known {
        file: "core/src/core/config/retention.rs",
        item: "RetentionConfig::deserialize",
        kind: SiteKind::FromValue,
        count: 1,
        allowed: Allowed::NotUserValue("the retention config"),
    },
    Known {
        file: "core/src/core/config/retention.rs",
        item: "RetentionConfig::deserialize",
        kind: SiteKind::ValueDeserialize,
        count: 1,
        allowed: Allowed::NotUserValue("the retention config"),
    },
    Known {
        file: "core/src/core/history/archive/attempts.rs",
        item: "decode_payload",
        kind: SiteKind::TypedLetParse,
        count: 1,
        allowed: Allowed::NotUserValue("the attempt archive of a history row, before its version decodes"),
    },
    Known {
        file: "core/src/core/history/enqueue.rs",
        item: "parse_optional_object",
        kind: SiteKind::TypedLetParse,
        count: 1,
        allowed: Allowed::NotUserValue(
            "the task options; an integer outside the i64 and u64 range is refused before the parse",
        ),
    },
    Known {
        file: "core/src/core/history/reads/aggregates.rs",
        item: "plan_rows_from_explain_text",
        kind: SiteKind::TypedLetParse,
        count: 1,
        allowed: Allowed::NotUserValue("the EXPLAIN output of a statement"),
    },
    Known {
        file: "core/src/core/history/rerun/input_envelope.rs",
        item: "decode_input_envelope",
        kind: SiteKind::TypedLetParse,
        count: 1,
        allowed: Allowed::NoProductionCaller(
            "the version 1 input codec of the history vectors; a rerun reads the input as its \
             text (`decode_framed_rerun_input`); this codec refuses an integer outside the i64 \
             and u64 range",
        ),
    },
    Known {
        file: "core/src/core/lifecycle/attempt_outcomes.rs",
        item: "shared_columns",
        kind: SiteKind::RowDecoderField,
        count: 1,
        allowed: Allowed::NotUserValue("the guard evidence of an outcome row"),
    },
    Known {
        file: "core/src/core/lifecycle/outcomes.rs",
        item: "raw_outcome_row",
        kind: SiteKind::RowDecoderField,
        count: 1,
        allowed: Allowed::NotUserValue("the guard evidence of an outcome row"),
    },
    Known {
        file: "core/src/core/task/error.rs",
        item: "TaskErrorCode::deserialize",
        kind: SiteKind::FromValue,
        count: 1,
        allowed: Allowed::NotUserValue("the code of an error"),
    },
    Known {
        file: "core/src/core/task/error.rs",
        item: "TaskErrorCode::deserialize",
        kind: SiteKind::ValueDeserialize,
        count: 1,
        allowed: Allowed::NotUserValue("the code of an error"),
    },
    Known {
        file: "core/src/monitoring/fleet/decode.rs",
        item: "LiveSlotsEntry",
        kind: SiteKind::DeserializeField,
        count: 1,
        allowed: Allowed::NotUserValue("the down slots of a live worker"),
    },
    Known {
        file: "core/src/monitoring/fleet/decode.rs",
        item: "oldest_pending_row",
        kind: SiteKind::RowDecoderField,
        count: 1,
        allowed: Allowed::NotUserValue("the pending heads of the fleet read"),
    },
    Known {
        file: "core/src/monitoring/fleet/decode.rs",
        item: "workers_row",
        kind: SiteKind::RowDecoderField,
        count: 2,
        allowed: Allowed::NotUserValue("the live workers and the rows in flight of the fleet read"),
    },
    Known {
        file: "core/src/worker/execution.rs",
        item: "parse::WorkflowCtxPayload",
        kind: SiteKind::DeserializeField,
        count: 2,
        allowed: Allowed::NotUserValue(
            "the task names of the context, and each stored summary as a JSON string, which \
             holds the summary text as it is",
        ),
    },
    Known {
        file: "core/src/web/request_body.rs",
        item: "parse_action_body",
        kind: SiteKind::TurbofishValue,
        count: 1,
        allowed: Allowed::NotUserValue(
            "a web action request body; read as a JSON object only to name an unknown key before the typed parse",
        ),
    },
    Known {
        file: "core/src/worker/executor/protocol/metadata.rs",
        item: "parse_metadata",
        kind: SiteKind::TurbofishValue,
        count: 1,
        allowed: Allowed::NotUserValue(
            "the metadata of a frame; a result envelope travels in the body",
        ),
    },
    Known {
        file: "core/src/worker/finalize_batch/statement.rs",
        item: "raw_outcome",
        kind: SiteKind::RowDecoderField,
        count: 1,
        allowed: Allowed::NotUserValue("the guard evidence of an outcome row"),
    },
    Known {
        file: "core/src/workflow_engine/args_merge.rs",
        item: "read_args_from",
        kind: SiteKind::TypedLetParse,
        count: 1,
        allowed: Allowed::NotUserValue("the args_from map of a node: kwarg name to node index"),
    },
    Known {
        file: "clients/rust/horsies/src/workflow/handle.rs",
        item: "node_info",
        kind: SiteKind::FromValue,
        count: 1,
        allowed: Allowed::NotUserValue("the stored status text of a node"),
    },
];

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

/// The delimiter of a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delim {
    Paren,
    Bracket,
    Brace,
    Invisible,
}

/// A token tree, with the parts that the scan reads.
#[derive(Debug, Clone)]
enum Token {
    Ident(String),
    Punct { ch: char, joint: bool },
    Literal(String),
    Group { delim: Delim, tokens: Vec<Token> },
}

/// The tokens of `stream`, `depth` levels down.
fn tokens_of(stream: TokenStream, depth: usize) -> Vec<Token> {
    assert!(
        depth <= MAX_TREE_DEPTH,
        "token trees nest deeper than {MAX_TREE_DEPTH} levels"
    );
    stream
        .into_iter()
        .map(|tree| match tree {
            TokenTree::Ident(ident) => Token::Ident(ident.to_string()),
            TokenTree::Punct(punct) => Token::Punct {
                ch: punct.as_char(),
                joint: punct.spacing() == Spacing::Joint,
            },
            TokenTree::Literal(literal) => Token::Literal(literal.to_string()),
            TokenTree::Group(group) => Token::Group {
                delim: match group.delimiter() {
                    Delimiter::Parenthesis => Delim::Paren,
                    Delimiter::Bracket => Delim::Bracket,
                    Delimiter::Brace => Delim::Brace,
                    Delimiter::None => Delim::Invisible,
                },
                tokens: tokens_of(group.stream(), depth + 1),
            },
        })
        .collect()
}

fn is_punct(token: Option<&Token>, expected: char) -> bool {
    matches!(token, Some(Token::Punct { ch, .. }) if *ch == expected)
}

fn is_ident(token: Option<&Token>, expected: &str) -> bool {
    matches!(token, Some(Token::Ident(name)) if name == expected)
}

fn ident_name(token: Option<&Token>) -> Option<&str> {
    match token {
        Some(Token::Ident(name)) => Some(name.as_str()),
        _ => None,
    }
}

fn group_of(token: Option<&Token>, expected: Delim) -> Option<&[Token]> {
    match token {
        Some(Token::Group { delim, tokens }) if *delim == expected => Some(tokens.as_slice()),
        _ => None,
    }
}

/// Whether `tokens`, at any depth, hold an identifier in `names`.
fn names_any(tokens: &[Token], names: &[&str]) -> bool {
    tokens.iter().any(|token| match token {
        Token::Ident(name) => names.contains(&name.as_str()),
        Token::Group { tokens, .. } => names_any(tokens, names),
        Token::Punct { .. } | Token::Literal(_) => false,
    })
}

/// Whether `tokens`, at any depth, name `Value`.
fn names_value(tokens: &[Token]) -> bool {
    names_any(tokens, &["Value"])
}

/// Whether the `>` at `index` ends a `->`.
fn is_arrow_head(tokens: &[Token], index: usize) -> bool {
    index > 0
        && matches!(
            &tokens[index - 1],
            Token::Punct {
                ch: '-',
                joint: true
            }
        )
}

/// The index of the `>` that closes the `<` at `open`.
fn matching_angle(tokens: &[Token], open: usize) -> Option<usize> {
    let mut depth: usize = 0;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        match token {
            Token::Punct { ch: '<', .. } => depth += 1,
            Token::Punct { ch: '>', .. } if !is_arrow_head(tokens, index) => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

/// The parts of `tokens` between the commas outside `<...>`, without empty
/// parts.
fn comma_segments(tokens: &[Token]) -> Vec<&[Token]> {
    let mut segments = Vec::new();
    let mut depth: usize = 0;
    let mut start = 0;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Token::Punct { ch: '<', .. } => depth += 1,
            Token::Punct { ch: '>', .. } if !is_arrow_head(tokens, index) && depth > 0 => {
                depth -= 1;
            }
            Token::Punct { ch: ',', .. } if depth == 0 => {
                segments.push(&tokens[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    segments.push(&tokens[start..]);
    segments.retain(|segment| !segment.is_empty());
    segments
}

/// Whether the single `:` at `index` is not a half of `::`.
fn is_single_colon(tokens: &[Token], index: usize) -> bool {
    is_punct(tokens.get(index), ':')
        && !is_punct(tokens.get(index + 1), ':')
        && !(index > 0 && is_punct(tokens.get(index - 1), ':'))
}

// ---------------------------------------------------------------------------
// Attributes and items
// ---------------------------------------------------------------------------

/// Whether the attribute `tokens` (inside `#[...]`) is `cfg(test)` or
/// `cfg(all(test, ...))`.
fn is_test_cfg(tokens: &[Token]) -> bool {
    let (true, Some(predicate)) = (
        tokens.len() == 2 && is_ident(tokens.first(), "cfg"),
        group_of(tokens.get(1), Delim::Paren),
    ) else {
        return false;
    };
    match predicate {
        [Token::Ident(name)] => name == "test",
        [Token::Ident(name), Token::Group {
            delim: Delim::Paren,
            tokens: inner,
        }] if name == "all" => inner.iter().any(|token| is_ident(Some(token), "test")),
        _ => false,
    }
}

/// The file of a `#[path = "..."]` attribute.
fn path_attribute(tokens: &[Token]) -> Option<String> {
    match tokens {
        [Token::Ident(name), Token::Punct { ch: '=', .. }, Token::Literal(text)]
            if name == "path" =>
        {
            Some(text.trim_matches('"').to_owned())
        }
        _ => None,
    }
}

/// Whether the attribute derives `Deserialize` or `FromRow`.
fn derives_reader(tokens: &[Token]) -> bool {
    match tokens {
        [Token::Ident(name), Token::Group {
            delim: Delim::Paren,
            tokens: derived,
        }] if name == "derive" => derived
            .iter()
            .any(|token| is_ident(Some(token), "Deserialize") || is_ident(Some(token), "FromRow")),
        _ => false,
    }
}

/// One past the item that starts at `start`: its first brace group, or its `;`.
fn item_end(tokens: &[Token], start: usize) -> usize {
    tokens
        .iter()
        .enumerate()
        .skip(start)
        .find(|(_, token)| {
            matches!(
                token,
                Token::Group {
                    delim: Delim::Brace,
                    ..
                } | Token::Punct { ch: ';', .. }
            )
        })
        .map_or(tokens.len(), |(index, _)| index + 1)
}

/// The name of the self type of the `impl` at `start`, and the index of its body.
fn impl_self_name(tokens: &[Token], start: usize) -> Option<(String, usize)> {
    let mut from = start + 1;
    if is_punct(tokens.get(from), '<') {
        from = matching_angle(tokens, from)? + 1;
    }
    let body =
        (from..tokens.len()).find(|index| group_of(tokens.get(*index), Delim::Brace).is_some())?;
    let mut depth: i64 = 0;
    let mut for_at = None;
    for (index, token) in tokens.iter().enumerate().take(body).skip(from) {
        match token {
            Token::Punct { ch: '<', .. } => depth += 1,
            Token::Punct { ch: '>', .. } => depth -= 1,
            Token::Ident(name) if depth == 0 && name == "for" => {
                for_at = Some(index);
                break;
            }
            _ => {}
        }
    }
    let name_from = for_at.map_or(from, |at| at + 1);
    let name = tokens[name_from..body]
        .iter()
        .find_map(|token| match token {
            Token::Ident(name) if name != "dyn" && name != "mut" => Some(name.clone()),
            _ => None,
        })?;
    Some((name, body))
}

/// The index after the attributes (`#[...]`) at the start of `segment`.
fn after_attributes(segment: &[Token]) -> usize {
    let mut start = 0;
    while is_punct(segment.get(start), '#')
        && group_of(segment.get(start + 1), Delim::Bracket).is_some()
    {
        start += 2;
    }
    start
}

/// The tokens after the attributes and `name :` of a named field: the type
/// of a field of a definition, or the value of a field of a literal.
fn field_type(segment: &[Token]) -> &[Token] {
    let start = after_attributes(segment);
    (start..segment.len())
        .find(|index| is_single_colon(segment, *index))
        .map_or(&segment[segment.len()..], |colon| &segment[colon + 1..])
}

/// The name of a named field: the last identifier before its single `:`.
fn field_name(segment: &[Token]) -> Option<&str> {
    let colon = (0..segment.len()).find(|index| is_single_colon(segment, *index))?;
    segment[..colon]
        .iter()
        .rev()
        .find_map(|token| ident_name(Some(token)))
}

/// The name of an enum variant: its first identifier after its attributes.
fn variant_name(variant: &[Token]) -> Option<&str> {
    ident_name(variant.get(after_attributes(variant)))
}

/// One field of a struct, enum or union definition.
struct Field<'t> {
    /// The struct, or the variant of the enum.
    owner: &'t str,
    /// The name of the field, or the index of a tuple field.
    key: String,
    /// The type tokens.
    ty: &'t [Token],
}

/// The fields of `owner` in the named-field body `body`.
fn named_fields<'t>(owner: &'t str, body: &'t [Token]) -> Vec<Field<'t>> {
    comma_segments(body)
        .into_iter()
        .filter_map(|segment| {
            Some(Field {
                owner,
                key: field_name(segment)?.to_owned(),
                ty: field_type(segment),
            })
        })
        .collect()
}

/// The fields of `owner` in the tuple body `body`, keyed by index.
fn tuple_fields<'t>(owner: &'t str, body: &'t [Token]) -> Vec<Field<'t>> {
    comma_segments(body)
        .into_iter()
        .enumerate()
        .map(|(position, ty)| Field {
            owner,
            key: position.to_string(),
            ty,
        })
        .collect()
}

/// The fields of one variant of an enum.
fn variant_fields(variant: &[Token]) -> Vec<Field<'_>> {
    let Some(owner) = variant_name(variant) else {
        return Vec::new();
    };
    variant
        .iter()
        .flat_map(|token| match token {
            Token::Group {
                delim: Delim::Brace,
                tokens: fields,
            } => named_fields(owner, fields),
            Token::Group {
                delim: Delim::Paren,
                tokens: fields,
            } => tuple_fields(owner, fields),
            Token::Group {
                delim: Delim::Bracket | Delim::Invisible,
                ..
            }
            | Token::Ident(_)
            | Token::Punct { .. }
            | Token::Literal(_) => Vec::new(),
        })
        .collect()
}

/// The fields of the struct, enum or union whose keyword is at `start`, and
/// the index after the item.
fn data_fields(tokens: &[Token], start: usize) -> (Vec<Field<'_>>, usize) {
    let Some(body_at) = (start + 2..tokens.len()).find(|index| {
        matches!(
            &tokens[*index],
            Token::Group {
                delim: Delim::Brace | Delim::Paren,
                ..
            } | Token::Punct { ch: ';', .. }
        )
    }) else {
        return (Vec::new(), tokens.len());
    };
    let (delim, body) = match &tokens[body_at] {
        Token::Group {
            delim,
            tokens: body_tokens,
        } => (*delim, body_tokens.as_slice()),
        Token::Ident(_) | Token::Punct { .. } | Token::Literal(_) => {
            return (Vec::new(), body_at + 1)
        }
    };
    let end = match is_punct(tokens.get(body_at + 1), ';') {
        true => body_at + 2,
        false => body_at + 1,
    };
    let name = ident_name(tokens.get(start + 1)).unwrap_or_default();
    let fields = match (is_ident(tokens.get(start), "enum"), delim) {
        (true, _) => comma_segments(body)
            .into_iter()
            .flat_map(variant_fields)
            .collect(),
        (false, Delim::Brace) => named_fields(name, body),
        (false, _) => tuple_fields(name, body),
    };
    (fields, end)
}

/// Whether the function whose signature after its name is `signature` takes
/// `self`.
fn has_receiver(signature: &[Token]) -> bool {
    let parameters_at = match is_punct(signature.first(), '<') {
        true => matching_angle(signature, 0).map_or(signature.len(), |close| close + 1),
        false => 0,
    };
    group_of(signature.get(parameters_at), Delim::Paren)
        .and_then(|parameters| comma_segments(parameters).first().copied())
        .is_some_and(|first| first.iter().any(|token| is_ident(Some(token), "self")))
}

/// Whether the name at `index` is called: `name(..)` or `name::<..>(..)`.
fn is_called(tokens: &[Token], index: usize) -> bool {
    let turbofish = is_punct(tokens.get(index + 1), ':')
        && is_punct(tokens.get(index + 2), ':')
        && is_punct(tokens.get(index + 3), '<');
    let arguments_at = match turbofish {
        true => matching_angle(tokens, index + 3).map_or(tokens.len(), |close| close + 1),
        false => index + 1,
    };
    group_of(tokens.get(arguments_at), Delim::Paren).is_some()
}

// ---------------------------------------------------------------------------
// The walk
// ---------------------------------------------------------------------------

/// The row decoders, and the fields whose type names `Value`, that module
/// files define.
#[derive(Debug, Default)]
struct RowTypes {
    /// The functions with a decoder bound and no `self`.
    decoder_functions: BTreeSet<String>,
    /// The methods with a decoder bound.
    decoder_methods: BTreeSet<String>,
    /// The fields whose type names `Value`, by struct or variant name: the
    /// name of each field, or the index of a tuple field.
    value_fields: BTreeMap<String, BTreeSet<String>>,
}

impl RowTypes {
    fn extend(&mut self, other: RowTypes) {
        self.decoder_functions.extend(other.decoder_functions);
        self.decoder_methods.extend(other.decoder_methods);
        for (owner, keys) in other.value_fields {
            self.value_fields.entry(owner).or_default().extend(keys);
        }
    }
}

/// The sites, the declared modules and the row types of one file.
struct FileScan<'k> {
    /// The row types of every scanned file; empty in the first pass.
    known: &'k RowTypes,
    sites: BTreeMap<(String, SiteKind), usize>,
    /// Each `mod name;` with no `#[cfg(test)]`: the name, the `#[path]` file,
    /// and the inline modules around it.
    modules: Vec<(String, Option<String>, Vec<String>)>,
    /// The row types that this file defines.
    defined: RowTypes,
}

impl<'k> FileScan<'k> {
    fn new(known: &'k RowTypes) -> Self {
        Self {
            known,
            sites: BTreeMap::new(),
            modules: Vec::new(),
            defined: RowTypes::default(),
        }
    }

    fn add(&mut self, scope: &[String], kind: SiteKind) {
        *self.sites.entry((scope.join("::"), kind)).or_insert(0) += 1;
    }

    /// The sites of `tokens`, in the items `scope` and the inline modules
    /// `modules`. `self_type` is the type of the enclosing `impl`.
    fn walk(
        &mut self,
        tokens: &[Token],
        scope: &[String],
        modules: &[String],
        self_type: Option<&str>,
        depth: usize,
    ) {
        assert!(
            depth <= MAX_TREE_DEPTH,
            "the walk nests deeper than {MAX_TREE_DEPTH} levels"
        );
        let mut index = 0;
        let mut attributes: Vec<&[Token]> = Vec::new();
        let mut path: Option<String> = None;
        let mut derives = false;
        while index < tokens.len() {
            let token = &tokens[index];
            if is_punct(Some(token), '#')
                && is_punct(tokens.get(index + 1), '!')
                && group_of(tokens.get(index + 2), Delim::Bracket).is_some()
            {
                index += 3;
                continue;
            }
            if let (true, Some(attribute)) = (
                is_punct(Some(token), '#'),
                group_of(tokens.get(index + 1), Delim::Bracket),
            ) {
                attributes.push(attribute);
                index += 2;
                continue;
            }
            if !attributes.is_empty() {
                if attributes.iter().any(|attribute| is_test_cfg(attribute)) {
                    attributes.clear();
                    index = item_end(tokens, index);
                    continue;
                }
                path = attributes
                    .iter()
                    .find_map(|attribute| path_attribute(attribute));
                derives = attributes.iter().any(|attribute| derives_reader(attribute));
                attributes.clear();
            }
            if let Some(word) = ident_name(Some(token)).filter(|word| QUALIFIERS.contains(word)) {
                index += 1;
                if word == "pub" && group_of(tokens.get(index), Delim::Paren).is_some() {
                    index += 1;
                }
                continue;
            }
            let (item_path, item_derives) = (path.take(), derives);
            derives = false;
            if let Some(next) = self.item(
                tokens,
                index,
                scope,
                modules,
                self_type,
                depth,
                item_path,
                item_derives,
            ) {
                index = next;
                continue;
            }
            self.sites_at(tokens, index, scope, self_type);
            if let Token::Group { tokens: inner, .. } = token {
                self.walk(inner, scope, modules, self_type, depth + 1);
            }
            index += 1;
        }
    }

    /// The item that starts at `index`, walked: a module, a function, an
    /// `impl` or a data type. Returns the index after it, or `None` when no
    /// item starts there.
    fn item(
        &mut self,
        tokens: &[Token],
        index: usize,
        scope: &[String],
        modules: &[String],
        self_type: Option<&str>,
        depth: usize,
        path: Option<String>,
        derives: bool,
    ) -> Option<usize> {
        let keyword = ident_name(tokens.get(index))?;
        let name = ident_name(tokens.get(index + 1));
        match (keyword, name) {
            ("mod", Some(name)) => {
                if is_punct(tokens.get(index + 2), ';') {
                    self.modules.push((name.to_owned(), path, modules.to_vec()));
                    return Some(index + 3);
                }
                let body = group_of(tokens.get(index + 2), Delim::Brace)?;
                let inner_scope = with_name(scope, name);
                let inner_modules = with_name(modules, name);
                self.walk(body, &inner_scope, &inner_modules, None, depth + 1);
                Some(index + 3)
            }
            ("fn", Some(name)) => {
                let end = (index + 2..tokens.len())
                    .find(|at| {
                        group_of(tokens.get(*at), Delim::Brace).is_some()
                            || is_punct(tokens.get(*at), ';')
                    })
                    .unwrap_or(tokens.len());
                self.define_decoder(name, tokens.get(index + 2..end).unwrap_or_default());
                if let Some(body) = group_of(tokens.get(end), Delim::Brace) {
                    self.walk(body, &with_name(scope, name), modules, self_type, depth + 1);
                }
                Some(end + 1)
            }
            ("impl", _) => {
                let (self_name, body_at) = impl_self_name(tokens, index)?;
                let body = group_of(tokens.get(body_at), Delim::Brace)?;
                let inner_scope = with_name(scope, &self_name);
                self.walk(
                    body,
                    &inner_scope,
                    modules,
                    Some(self_name.as_str()),
                    depth + 1,
                );
                Some(body_at + 1)
            }
            ("struct" | "enum" | "union", Some(name)) => {
                let (fields, end) = data_fields(tokens, index);
                let inner_scope = with_name(scope, name);
                for field in fields.iter().filter(|field| names_value(field.ty)) {
                    if derives {
                        self.add(&inner_scope, SiteKind::DeserializeField);
                    }
                    self.defined
                        .value_fields
                        .entry(field.owner.to_owned())
                        .or_default()
                        .insert(field.key.clone());
                }
                Some(end)
            }
            _ => None,
        }
    }

    /// Records the function `name` as a row decoder when its signature (the
    /// tokens after its name) names a decoder bound.
    fn define_decoder(&mut self, name: &str, signature: &[Token]) {
        if !names_any(signature, &DECODER_BOUNDS) {
            return;
        }
        let decoders = match has_receiver(signature) {
            true => &mut self.defined.decoder_methods,
            false => &mut self.defined.decoder_functions,
        };
        decoders.insert(name.to_owned());
    }

    /// Whether the name at `index` is a row decoder in its call form: a
    /// method after a `.`, a function otherwise.
    fn names_decoder(&self, tokens: &[Token], index: usize) -> bool {
        let Some(name) = ident_name(tokens.get(index)) else {
            return false;
        };
        match index > 0 && is_punct(tokens.get(index - 1), '.') {
            true => name == ROW_METHOD || self.known.decoder_methods.contains(name),
            false => self.known.decoder_functions.contains(name),
        }
    }

    /// The calls of a row decoder in `tokens`, at any depth.
    fn decoder_calls(&self, tokens: &[Token]) -> usize {
        tokens
            .iter()
            .enumerate()
            .map(|(index, token)| match token {
                Token::Group { tokens: inner, .. } => self.decoder_calls(inner),
                Token::Ident(_) => {
                    usize::from(self.names_decoder(tokens, index) && is_called(tokens, index))
                }
                Token::Punct { .. } | Token::Literal(_) => 0,
            })
            .sum()
    }

    /// The calls of a row decoder that fill a field whose type names `Value`,
    /// in the struct or variant literal, or the tuple constructor, at
    /// `index`. `Self` is `self_type`.
    fn decoder_fields(&self, tokens: &[Token], index: usize, self_type: Option<&str>) -> usize {
        let owner = match (ident_name(tokens.get(index)), self_type) {
            (Some("Self"), Some(self_type)) => self_type,
            (Some(name), _) => name,
            (None, _) => return 0,
        };
        let Some(keys) = self.known.value_fields.get(owner) else {
            return 0;
        };
        match tokens.get(index + 1) {
            Some(Token::Group {
                delim: Delim::Brace,
                tokens: fields,
            }) => comma_segments(fields)
                .into_iter()
                .filter(|field| field_name(field).is_some_and(|name| keys.contains(name)))
                .map(|field| self.decoder_calls(field_type(field)))
                .sum(),
            Some(Token::Group {
                delim: Delim::Paren,
                tokens: arguments,
            }) => comma_segments(arguments)
                .into_iter()
                .enumerate()
                .filter(|(position, _)| keys.contains(&position.to_string()))
                .map(|(_, argument)| self.decoder_calls(argument))
                .sum(),
            Some(
                Token::Group {
                    delim: Delim::Bracket | Delim::Invisible,
                    ..
                }
                | Token::Ident(_)
                | Token::Punct { .. }
                | Token::Literal(_),
            )
            | None => 0,
        }
    }

    /// Whether the `let` at `start` has a type that names `Value` and a value
    /// that calls a reader or a row decoder.
    fn is_typed_let_parse(&self, tokens: &[Token], start: usize) -> bool {
        let end = (start..tokens.len())
            .find(|index| is_punct(tokens.get(*index), ';'))
            .unwrap_or(tokens.len());
        let statement = &tokens[start + 1..end];
        let colon = (0..statement.len()).find(|index| is_single_colon(statement, *index));
        let equals = (0..statement.len()).find(|index| {
            let after_joint_punct =
                *index > 0 && matches!(&statement[*index - 1], Token::Punct { joint: true, .. });
            is_punct(statement.get(*index), '=')
                && !after_joint_punct
                && !is_punct(statement.get(index + 1), '=')
                && !is_punct(statement.get(index + 1), '>')
        });
        match (colon, equals) {
            (Some(colon), Some(equals)) if colon < equals => {
                let value = &statement[equals + 1..];
                names_value(&statement[colon + 1..equals])
                    && (names_any(value, &LET_READERS) || self.decoder_calls(value) > 0)
            }
            _ => false,
        }
    }

    /// The sites that start at `index`. `Self` is `self_type`.
    fn sites_at(
        &mut self,
        tokens: &[Token],
        index: usize,
        scope: &[String],
        self_type: Option<&str>,
    ) {
        let token = tokens.get(index);
        if is_ident(token, "let") && self.is_typed_let_parse(tokens, index) {
            self.add(scope, SiteKind::TypedLetParse);
        }
        if let Some(name) = ident_name(token).filter(|name| *name != "from_value") {
            let turbofish = is_punct(tokens.get(index + 1), ':')
                && is_punct(tokens.get(index + 2), ':')
                && is_punct(tokens.get(index + 3), '<');
            if let Some(close) = turbofish
                .then(|| matching_angle(tokens, index + 3))
                .flatten()
            {
                let call = (READERS.contains(&name) || self.names_decoder(tokens, index))
                    && group_of(tokens.get(close + 1), Delim::Paren).is_some();
                let deserialize = is_punct(tokens.get(close + 1), ':')
                    && is_punct(tokens.get(close + 2), ':')
                    && is_ident(tokens.get(close + 3), "deserialize")
                    && group_of(tokens.get(close + 4), Delim::Paren).is_some();
                if names_value(&tokens[index + 4..close]) && (call || deserialize) {
                    self.add(scope, SiteKind::TurbofishValue);
                }
            }
        }
        if is_ident(token, "Value")
            && is_punct(tokens.get(index + 1), ':')
            && is_punct(tokens.get(index + 2), ':')
            && is_ident(tokens.get(index + 3), "deserialize")
        {
            self.add(scope, SiteKind::ValueDeserialize);
        }
        if is_ident(token, "from_value")
            && (group_of(tokens.get(index + 1), Delim::Paren).is_some()
                || is_punct(tokens.get(index + 1), ':'))
        {
            self.add(scope, SiteKind::FromValue);
        }
        for _ in 0..self.decoder_fields(tokens, index, self_type) {
            self.add(scope, SiteKind::RowDecoderField);
        }
    }
}

/// `names` and then `name`.
fn with_name(names: &[String], name: &str) -> Vec<String> {
    let mut joined = names.to_vec();
    joined.push(name.to_owned());
    joined
}

/// The sites of the Rust source `source`, by enclosing item and kind, with
/// the modules and the row types that it declares. `known` holds the row
/// types of every scanned file.
fn scan_source<'k>(source: &str, known: &'k RowTypes) -> FileScan<'k> {
    let stream =
        TokenStream::from_str(source).unwrap_or_else(|error| panic!("the source lexes: {error:?}"));
    let tokens = tokens_of(stream, 0);
    let mut scan = FileScan::new(known);
    scan.walk(&tokens, &[], &[], None, 0);
    scan
}

/// The file of the module `name` declared in a file whose children live in
/// `child_dir` and that is in `file_dir`.
fn module_file(name: &str, path: Option<&str>, child_dir: &Path, file_dir: &Path) -> PathBuf {
    if let Some(path) = path {
        return file_dir.join(path);
    }
    let flat = child_dir.join(format!("{name}.rs"));
    let nested = child_dir.join(name).join("mod.rs");
    match (flat.exists(), nested.exists()) {
        (true, _) => flat,
        (false, true) => nested,
        (false, false) => panic!("no file for the module {name} in {}", child_dir.display()),
    }
}

/// The repository-relative text of `path`, with `/` between the parts.
fn relative_text(root: &Path, path: &Path) -> String {
    let relative = path
        .strip_prefix(root)
        .unwrap_or_else(|_| panic!("{} is under {}", path.display(), root.display()));
    relative
        .components()
        .map(|part| part.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// A production module file: its path from the repository root, and its
/// source.
struct ModuleFile {
    path: String,
    source: String,
}

/// The production module files of the crate whose `src` folder is `src`,
/// from the repository root `root`. Adds the row types that the files
/// define to `row_types`.
fn crate_files(root: &Path, src: &str, row_types: &mut RowTypes) -> Vec<ModuleFile> {
    let none_known = RowTypes::default();
    let mut pending = vec![root.join(src).join("lib.rs")];
    let mut seen: Vec<PathBuf> = Vec::new();
    let mut files = Vec::new();
    while let Some(file) = pending.pop() {
        if seen.contains(&file) {
            continue;
        }
        assert!(
            seen.len() < MAX_MODULE_FILES,
            "{src} has more than {MAX_MODULE_FILES} module files"
        );
        seen.push(file.clone());
        let source = fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("read {}: {error}", file.display()));
        let scan = scan_source(&source, &none_known);
        let file_dir = file
            .parent()
            .expect("a module file is in a folder")
            .to_path_buf();
        let stem = file
            .file_stem()
            .expect("a module file has a name")
            .to_string_lossy()
            .into_owned();
        let child_dir = match stem.as_str() {
            "lib" | "mod" => file_dir.clone(),
            _ => file_dir.join(&stem),
        };
        for (name, path, inline) in &scan.modules {
            let inline_dir = inline
                .iter()
                .fold(child_dir.clone(), |dir, module| dir.join(module));
            let inline_file_dir = match inline.is_empty() {
                true => file_dir.clone(),
                false => inline_dir.clone(),
            };
            pending.push(module_file(
                name,
                path.as_deref(),
                &inline_dir,
                &inline_file_dir,
            ));
        }
        row_types.extend(scan.defined);
        files.push(ModuleFile {
            path: relative_text(root, &file),
            source,
        });
    }
    files
}

/// The sites of every production module of the core and the Rust client,
/// keyed from the repository root `root`: a first pass collects the module
/// files and their row types, a second pass finds the sites.
fn scan_crates(root: &Path) -> BTreeMap<SiteKey, usize> {
    let mut row_types = RowTypes::default();
    let mut files = Vec::new();
    for src in [CORE, CLIENT] {
        files.extend(crate_files(root, src.trim_end_matches('/'), &mut row_types));
    }
    let mut sites = BTreeMap::new();
    for file in &files {
        for ((item, kind), count) in scan_source(&file.source, &row_types).sites {
            let key = SiteKey {
                file: file.path.clone(),
                item,
                kind,
            };
            *sites.entry(key).or_insert(0) += count;
        }
    }
    sites
}

#[test]
fn every_read_into_value_of_the_core_and_the_rust_client_is_listed() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let found = scan_crates(&root);
    let known: BTreeMap<SiteKey, usize> = KNOWN_SITES
        .iter()
        .map(|site| {
            let key = SiteKey {
                file: site.file.to_owned(),
                item: site.item.to_owned(),
                kind: site.kind,
            };
            (key, site.count)
        })
        .collect();
    let unlisted: Vec<(&SiteKey, &usize)> = found
        .iter()
        .filter(|(key, count)| known.get(*key) != Some(*count))
        .collect();
    let stale: Vec<(&SiteKey, &usize)> = known
        .iter()
        .filter(|(key, count)| found.get(*key) != Some(*count))
        .collect();
    assert!(
        unlisted.is_empty() && stale.is_empty(),
        "a read into serde_json::Value changed. A user value keeps its text (JsonText, &RawValue) \
         or decodes its type from the text; any other read is listed in KNOWN_SITES with its \
         reason.\nfound, not listed: {unlisted:#?}\nlisted, not found: {stale:#?}"
    );
}

#[test]
fn each_listed_site_has_a_reason_and_a_file_of_a_scanned_crate() {
    assert_eq!(KNOWN_SITES.len(), 30);
    for site in KNOWN_SITES {
        assert!(!site.allowed.reason().is_empty(), "{}", site.file);
        assert!(site.count > 0, "{}", site.file);
        assert!(
            site.file.starts_with(CORE) || site.file.starts_with(CLIENT),
            "{}",
            site.file
        );
    }
}

/// One site of each kind, and the forms that are not sites.
const FIXTURE: &str = r#"
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub(crate) struct Payload {
    #[serde(default)]
    names: HashMap<String, serde_json::Value>,
    texts: BTreeMap<String, Box<RawValue>>,
}

#[derive(Serialize)]
struct Written {
    value: Value,
}

#[derive(Deserialize)]
enum Entry {
    Kept(Box<RawValue>),
    Parsed { value: Value },
}

pub fn reads(text: &str) {
    let value: serde_json::Value = serde_json::from_str(text).unwrap();
    let parsed = serde_json::from_str::<TaskResult<serde_json::Value>>(text);
    let built: Vec<Value> = vec![Value::Null];
    let error = TaskResult::<serde_json::Value>::Err(error);
    let kept = serde_json::from_str::<TaskResult<JsonText>>(text);
}

impl<'de> Deserialize<'de> for Code {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        serde_json::from_value(value).map_err(D::Error::custom)
    }
}

mod inline {
    fn decode(bytes: &[u8]) {
        let members = BTreeMap::<String, TaskOutcome<serde_json::Value, CustomCode>>::deserialize(d);
    }
    mod declared;
}

#[cfg(test)]
mod tests {
    fn hidden(text: &str) {
        let value: Value = serde_json::from_str(text).unwrap();
    }
}

#[cfg(test)]
mod test_file;
"#;

/// The sites of `scan`, in key order.
fn sites_of<'s>(scan: &'s FileScan<'_>) -> Vec<(&'s str, SiteKind, usize)> {
    scan.sites
        .iter()
        .map(|((item, kind), count)| (item.as_str(), *kind, *count))
        .collect()
}

#[test]
fn each_kind_of_read_is_a_site_and_the_other_forms_are_not() {
    let none_known = RowTypes::default();
    let known = scan_source(FIXTURE, &none_known).defined;
    let scan = scan_source(FIXTURE, &known);
    let sites = sites_of(&scan);
    assert_eq!(
        sites,
        [
            ("Code::deserialize", SiteKind::ValueDeserialize, 1),
            ("Code::deserialize", SiteKind::FromValue, 1),
            ("Entry", SiteKind::DeserializeField, 1),
            ("Payload", SiteKind::DeserializeField, 1),
            ("inline::decode", SiteKind::TurbofishValue, 1),
            ("reads", SiteKind::TurbofishValue, 1),
            ("reads", SiteKind::TypedLetParse, 1),
        ]
    );
    assert_eq!(
        scan.modules,
        [("declared".to_owned(), None, vec!["inline".to_owned()])]
    );
}

/// Planted reads through row decoders: one site of each form, and the forms
/// that are not sites (a decoder into a field that does not name `Value`, a
/// map lookup into a `Value` field).
const ROW_FIXTURE: &str = r#"
pub(crate) fn column<'row, T: FromSql<'row>>(
    row: &'row Row,
    statement: &TypedStatement,
    column: &'static str,
) -> Result<T, RowDecodeError> {
    row.try_get(column).map_err(|cause| decode_error(statement, column, cause))
}

fn owned<T: FromSqlOwned>(row: &Row, index: usize) -> Result<T, Error> {
    row.try_get(index)
}

struct Reader;

impl Reader {
    fn cell<'row, T: FromSql<'row>>(&self, row: &'row Row, name: &'static str) -> Result<T, Error> {
        row.try_get(name)
    }
}

struct Stored {
    state: Option<serde_json::Value>,
    name: String,
}

struct Wrapped(Value);

enum Read {
    Parsed { value: Value },
    Kept(String, Value),
}

fn stored(row: &Row, statement: &TypedStatement) -> Result<Stored, RowDecodeError> {
    Ok(Stored {
        state: column(row, statement, "state")?,
        name: column(row, statement, "name")?,
    })
}

impl Stored {
    fn read(row: &Row, reader: &Reader) -> Result<Self, Error> {
        Ok(Self {
            state: Some(reader.cell(row, "state")?),
            name: owned(row, 1)?,
        })
    }
}

fn forms(row: &Row, statement: &TypedStatement, map: &Map<String, Value>) -> Result<(), Error> {
    let wrapped = Wrapped(owned(row, 0)?);
    let parsed = Read::Parsed { value: row.try_get("value")? };
    let kept = Read::Kept(column(row, statement, "name")?, column(row, statement, "value")?);
    let typed: Value = column(row, statement, "typed")?;
    let fished = column::<serde_json::Value>(row, statement, "fished")?;
    let looked_up = Stored { state: map.get("state").cloned(), name: String::new() };
    Ok(())
}
"#;

#[test]
fn a_row_decoder_read_into_value_is_a_site_and_the_other_forms_are_not() {
    let none_known = RowTypes::default();
    let known = scan_source(ROW_FIXTURE, &none_known).defined;
    assert_eq!(
        known
            .decoder_functions
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["column", "owned"]
    );
    assert_eq!(
        known
            .decoder_methods
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["cell"]
    );
    let scan = scan_source(ROW_FIXTURE, &known);
    assert_eq!(
        sites_of(&scan),
        [
            ("Stored::read", SiteKind::RowDecoderField, 1),
            ("forms", SiteKind::TurbofishValue, 1),
            ("forms", SiteKind::TypedLetParse, 1),
            ("forms", SiteKind::RowDecoderField, 3),
            ("stored", SiteKind::RowDecoderField, 1),
        ]
    );
}
