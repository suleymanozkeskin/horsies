//! Each `tracing` event of the core that carries an `error` field writes that
//! error in its message. The log bridge makes the message the text of the
//! Python `LogRecord` and each other field an attribute of it, so a handler
//! that writes `getMessage()` shows the cause only when the message holds it.
//!
//! The scan lexes each file under `src/` into token trees (`proc-macro2`) and
//! walks every tree, the bodies of other macros included. An event is a call
//! `tracing::<name>!(...)` or `<name>!(...)` with the name `trace`, `debug`,
//! `info`, `warn`, `error` or `event`; every level is checked. The arguments
//! of a call split at their top-level commas into directives (`target:`,
//! `parent:`, `name:`), the level of `event!`, the fields, the message literal
//! and the format arguments. An `error` field passes when a placeholder of
//! the message renders the expression of the field: an implicit capture
//! (`{error}`), a named argument (`{cause}` with `cause = error`) or a
//! positional argument (`{}` or `{0}` with `error`). Expressions compare as
//! token text.
//!
//! A second rule holds for the files of the worker parent that act on a
//! claimed attempt ([`is_attempt_scoped`]): an event with a `task_id` field
//! also has an `attempt` field, so each log record of an attempt names it.

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use proc_macro2::{Delimiter, LexError, Spacing, TokenStream, TokenTree};

/// The deepest directory level under `src/` that the file walk enters.
const MAX_DIRECTORY_DEPTH: usize = 16;
/// The deepest nesting of token trees that the walk enters.
const MAX_TREE_DEPTH: usize = 256;

/// A macro name that starts an event.
#[derive(Debug, Clone, Copy)]
enum EventMacro {
    /// `trace!`, `debug!`, `info!`, `warn!`, `error!`: the name is the level.
    Level,
    /// `event!`: the level is the first argument after the directives.
    Event,
}

/// One field of an event: its name and the token text of its value.
#[derive(Debug)]
struct Field {
    name: String,
    value: String,
}

/// The message of an event.
#[derive(Debug)]
enum Message {
    /// The call has no message literal.
    Absent,
    /// The decoded literal, and the token text of each expression that its
    /// placeholders render.
    Literal { text: String, rendered: Vec<String> },
}

/// One event call site.
#[derive(Debug)]
struct Event {
    /// The line of the macro name.
    line: usize,
    fields: Vec<Field>,
    message: Message,
}

/// An `error` field whose expression no placeholder of the message renders.
#[derive(Debug, PartialEq, Eq)]
struct Finding {
    line: usize,
    value: String,
}

/// What a placeholder takes its value from.
enum Argument {
    /// `{}`: the next positional argument.
    Next,
    /// `{0}`: the positional argument at the index.
    Index(usize),
    /// `{name}`: the named argument, or the variable it captures.
    Name(String),
}

/// One placeholder of a format string.
struct Placeholder {
    argument: Argument,
    /// The spec has the precision `.*`, which takes the next positional
    /// argument before the value.
    precision_from_next: bool,
}

/// Every `.rs` file under `directory`, which is `depth` levels below `src/`.
fn rust_files(directory: &Path, depth: usize, files: &mut Vec<PathBuf>) {
    assert!(
        depth <= MAX_DIRECTORY_DEPTH,
        "{} is more than {MAX_DIRECTORY_DEPTH} levels below src/",
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

/// The event call sites of `source`, in source order. Fails when `source`
/// is not a stream of Rust tokens.
fn events_of(source: &str) -> Result<Vec<Event>, LexError> {
    let tokens: Vec<TokenTree> = TokenStream::from_str(source)?.into_iter().collect();
    let mut events = Vec::new();
    walk(&tokens, 0, &mut events);
    Ok(events)
}

/// The events of `tokens` and of every group in them.
fn walk(tokens: &[TokenTree], depth: usize, events: &mut Vec<Event>) {
    assert!(
        depth <= MAX_TREE_DEPTH,
        "token trees nest deeper than {MAX_TREE_DEPTH} levels"
    );
    for (index, token) in tokens.iter().enumerate() {
        if let Some(event) = event_at(tokens, index) {
            events.push(event);
        }
        if let TokenTree::Group(group) = token {
            let inner: Vec<TokenTree> = group.stream().into_iter().collect();
            walk(&inner, depth + 1, events);
        }
    }
}

/// The event whose macro name is `tokens[index]`, when it starts one.
fn event_at(tokens: &[TokenTree], index: usize) -> Option<Event> {
    let TokenTree::Ident(name) = &tokens[index] else {
        return None;
    };
    let kind = event_macro(&name.to_string())?;
    let (Some(TokenTree::Punct(bang)), Some(TokenTree::Group(arguments))) =
        (tokens.get(index + 1), tokens.get(index + 2))
    else {
        return None;
    };
    match bang.as_char() == '!' && names_tracing(tokens, index) {
        true => Some(event_of(name.span().start().line, kind, arguments.stream())),
        false => None,
    }
}

/// The kind of event that the macro `name` starts, if any.
fn event_macro(name: &str) -> Option<EventMacro> {
    match name {
        "trace" | "debug" | "info" | "warn" | "error" => Some(EventMacro::Level),
        "event" => Some(EventMacro::Event),
        _ => None,
    }
}

/// Whether the path of the macro named at `index` is `tracing::<name>`
/// (with or without a leading `::`) or the bare name.
fn names_tracing(tokens: &[TokenTree], index: usize) -> bool {
    let after_path = index
        .checked_sub(1)
        .and_then(|before| tokens.get(before))
        .is_some_and(|token| matches!(token, TokenTree::Punct(colon) if colon.as_char() == ':'));
    match after_path {
        false => true,
        true => matches!(
            index.checked_sub(3).and_then(|at| tokens.get(at)),
            Some(TokenTree::Ident(crate_name)) if *crate_name == "tracing"
        ),
    }
}

/// The fields and the message of the call whose arguments are `arguments`.
fn event_of(line: usize, kind: EventMacro, arguments: TokenStream) -> Event {
    let segments = comma_segments(arguments);
    let after_directives = segments
        .iter()
        .position(|segment| !is_directive(segment))
        .unwrap_or(segments.len());
    let first_field = match kind {
        EventMacro::Level => after_directives,
        EventMacro::Event => (after_directives + 1).min(segments.len()),
    };
    let mut fields = Vec::new();
    for (offset, segment) in segments[first_field..].iter().enumerate() {
        if let Some(text) = message_literal(segment) {
            let rendered = rendered_expressions(&text, &segments[first_field + offset + 1..]);
            return Event {
                line,
                fields,
                message: Message::Literal { text, rendered },
            };
        }
        match segment.as_slice() {
            [TokenTree::Group(set)] if set.delimiter() == Delimiter::Brace => fields.extend(
                comma_segments(set.stream())
                    .iter()
                    .map(Vec::as_slice)
                    .map(field_of),
            ),
            _ => fields.push(field_of(segment)),
        }
    }
    Event {
        line,
        fields,
        message: Message::Absent,
    }
}

/// The parts of `stream` between its top-level commas, without empty parts.
fn comma_segments(stream: TokenStream) -> Vec<Vec<TokenTree>> {
    let mut segments = vec![Vec::new()];
    for token in stream {
        let is_comma = matches!(&token, TokenTree::Punct(punct) if punct.as_char() == ',');
        match is_comma {
            true => segments.push(Vec::new()),
            false => segments
                .last_mut()
                .expect("the first segment is there")
                .push(token),
        }
    }
    segments.retain(|segment| !segment.is_empty());
    segments
}

/// Whether `segment` is a directive: `target:`, `parent:` or `name:`.
fn is_directive(segment: &[TokenTree]) -> bool {
    matches!(
        segment,
        [TokenTree::Ident(name), TokenTree::Punct(colon), ..]
            if colon.as_char() == ':'
                && colon.spacing() == Spacing::Alone
                && ["target", "parent", "name"]
                    .iter()
                    .any(|directive| *name == *directive)
    )
}

/// The decoded text of `segment` when it is one string literal: the message.
fn message_literal(segment: &[TokenTree]) -> Option<String> {
    match segment {
        [TokenTree::Literal(literal)] => match syn::Lit::new(literal.clone()) {
            syn::Lit::Str(text) => Some(text.value()),
            _ => None,
        },
        _ => None,
    }
}

/// The field of `segment`: `name = value`, `name = %value`, `name = ?value`,
/// `%name`, `?name` or `name`. A shorthand field's value is its name.
fn field_of(segment: &[TokenTree]) -> Field {
    let body = without_sigil(segment);
    let equals = body
        .iter()
        .position(|token| matches!(token, TokenTree::Punct(punct) if punct.as_char() == '='));
    match equals {
        Some(at) => Field {
            name: token_text(&body[..at]),
            value: token_text(without_sigil(&body[at + 1..])),
        },
        None => Field {
            name: token_text(body),
            value: token_text(body),
        },
    }
}

/// `tokens` without a leading `%` or `?`.
fn without_sigil(tokens: &[TokenTree]) -> &[TokenTree] {
    match tokens {
        [TokenTree::Punct(sigil), rest @ ..] if matches!(sigil.as_char(), '%' | '?') => rest,
        _ => tokens,
    }
}

/// The text of `tokens` as one token stream writes it.
fn token_text(tokens: &[TokenTree]) -> String {
    tokens.iter().cloned().collect::<TokenStream>().to_string()
}

/// The token text of each expression that a placeholder of `text` renders,
/// with the format arguments `arguments`.
fn rendered_expressions(text: &str, arguments: &[Vec<TokenTree>]) -> Vec<String> {
    let mut named: Vec<(String, String)> = Vec::new();
    let mut positional: Vec<String> = Vec::new();
    for argument in arguments {
        match argument.as_slice() {
            [TokenTree::Ident(name), TokenTree::Punct(equals), value @ ..]
                if equals.as_char() == '='
                    && equals.spacing() == Spacing::Alone
                    && !value.is_empty() =>
            {
                named.push((name.to_string(), token_text(value)));
            }
            _ => positional.push(token_text(argument)),
        }
    }
    let mut next = 0;
    let mut rendered = Vec::new();
    for placeholder in placeholders(text) {
        if placeholder.precision_from_next {
            next += 1;
        }
        let expression = match placeholder.argument {
            Argument::Next => {
                next += 1;
                positional.get(next - 1).cloned()
            }
            Argument::Index(index) => positional.get(index).cloned(),
            Argument::Name(name) => Some(
                named
                    .iter()
                    .find(|(named_as, _)| *named_as == name)
                    .map_or(name.clone(), |(_, value)| value.clone()),
            ),
        };
        rendered.extend(expression);
    }
    rendered
}

/// The placeholders of the format string `text`, in order. `{{` and `}}`
/// are text.
fn placeholders(text: &str) -> Vec<Placeholder> {
    let characters: Vec<char> = text.chars().collect();
    let mut found = Vec::new();
    let mut at = 0;
    while at < characters.len() {
        match (characters[at], characters.get(at + 1)) {
            ('{', Some('{')) | ('}', Some('}')) => at += 2,
            ('{', _) => {
                let Some(length) = characters[at..]
                    .iter()
                    .position(|character| *character == '}')
                else {
                    // An open brace with no close does not compile.
                    break;
                };
                let inner: String = characters[at + 1..at + length].iter().collect();
                found.push(placeholder_of(&inner));
                at += length + 1;
            }
            _ => at += 1,
        }
    }
    found
}

/// The placeholder whose text between the braces is `inner`.
fn placeholder_of(inner: &str) -> Placeholder {
    let (argument, spec) = inner.split_once(':').unwrap_or((inner, ""));
    let argument = match argument.trim() {
        "" => Argument::Next,
        index if index.chars().all(|character| character.is_ascii_digit()) => {
            Argument::Index(index.parse().expect("an index of digits parses"))
        }
        name => Argument::Name(name.to_owned()),
    };
    Placeholder {
        argument,
        precision_from_next: spec.contains(".*"),
    }
}

/// Whether `event` has a field named `error`.
fn has_error_field(event: &Event) -> bool {
    event.fields.iter().any(|field| field.name == "error")
}

/// The `error` fields of `events` that no placeholder of their message
/// renders.
fn findings(events: &[Event]) -> Vec<Finding> {
    let mut found = Vec::new();
    for event in events {
        let rendered: &[String] = match &event.message {
            Message::Absent => &[],
            Message::Literal { rendered, .. } => rendered,
        };
        found.extend(
            event
                .fields
                .iter()
                .filter(|field| field.name == "error" && !rendered.contains(&field.value))
                .map(|field| Finding {
                    line: event.line,
                    value: field.value.clone(),
                }),
        );
    }
    found
}

#[test]
fn every_error_field_of_an_event_of_the_core_is_written_in_its_message() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let known_site = src.join("worker").join("finalize_batch.rs");
    let mut files = Vec::new();
    rust_files(&src, 0, &mut files);
    files.sort();
    let mut error_events = 0;
    let mut known_site_found = false;
    let mut report = Vec::new();
    for path in &files {
        let source = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        let events =
            events_of(&source).unwrap_or_else(|error| panic!("lex {}: {error}", path.display()));
        error_events += events.iter().filter(|event| has_error_field(event)).count();
        known_site_found |= *path == known_site
            && events.iter().any(|event| {
                has_error_field(event)
                    && matches!(
                        &event.message,
                        Message::Literal { text, .. }
                            if text.starts_with("the finalize batch write failed")
                    )
            });
        let relative = path.strip_prefix(&src).expect("a file under src/");
        report.extend(findings(&events).into_iter().map(|finding| {
            format!(
                "{}:{}: error = {}",
                relative.display(),
                finding.line,
                finding.value
            )
        }));
    }
    // A known positive: the scan reaches the event of the finalize batch
    // whose error field the message renders.
    assert!(
        known_site_found,
        "the scan of {} files found no `error` event of the finalize batch write in {}",
        files.len(),
        known_site.display()
    );
    assert!(
        report.is_empty(),
        "{} of {error_events} events with an `error` field leave the error out of their \
         message; write it there (for example `\"...: {{error}}\"`), keeping the field:\n{}",
        report.len(),
        report.join("\n")
    );
}

/// The event of `worker/finalize_batch.rs` before #1676: its message leaves
/// the error out.
const UNFIXED_EVENT: &str = r#"
fn write_and_answer(rows: &[u8], error: &str) {
    tracing::error!(
        rows = rows.len(),
        %error,
        "the finalize batch write failed; every row of it gets the error",
    );
}
"#;

#[test]
fn an_event_whose_message_leaves_its_error_out_is_found() {
    let events = events_of(UNFIXED_EVENT).expect("the fixture lexes");

    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(
        findings(&events),
        [Finding {
            line: 3,
            value: "error".to_owned(),
        }]
    );
}

/// One event per line, each with a message that renders its error.
const RENDERED_FORMS: &str = r#"
fn forms(error: &str, e: &str, other: &str, failure: Failure, wait: Wait) {
    tracing::warn!(%error, "a write failed: {error}");
    tracing::warn!(error = %e, "a write failed: {e}");
    tracing::warn!(error = ?e, "a write failed: {e:?}");
    tracing::warn!(error = %failure.detail(), "a write failed: {}", failure.detail());
    tracing::warn!(error = %e, "a write failed: {cause}", cause = e);
    tracing::warn!(error = %e, "{} failed: {0}, {1}", e, other);
    tracing::warn!(target: "core", error = %e, "a write failed: {e}");
    tracing::event!(tracing::Level::WARN, error = %e, "a write failed: {e}");
    tracing::warn!(error = %e, "{{e}} is text; {e} is a placeholder");
    tracing::warn!(error = %e, "{:.*} then {}", 2, 1.5, e);
    tracing::warn!(error = %e, "a write failed: \
                                {e}");
    tokio::select! { _ = wait => { tracing::error!(error = %e, "a write failed: {e}"); } }
}
"#;

#[test]
fn each_placeholder_form_that_renders_the_error_passes() {
    let events = events_of(RENDERED_FORMS).expect("the fixture lexes");

    assert_eq!(events.len(), 12, "{events:?}");
    assert!(events.iter().all(has_error_field), "{events:?}");
    assert!(findings(&events).is_empty(), "{events:?}");
}

/// One event per line, from line 3 to line 12, each with a message that
/// leaves its error out.
const LEFT_OUT_FORMS: &str = r#"
fn forms(error: &str, e: &str, other: &str, wait: Wait) {
    tracing::warn!(error = %e, "a write failed");
    tracing::warn!(error = %e, "a write failed: {{e}}");
    tracing::warn!(error = %e, "a write failed: {}", other);
    tracing::warn!(error = %e, "a write failed: {error}");
    tracing::warn!(error = %e);
    tracing::event!(tracing::Level::ERROR, error = %e, "a write failed");
    warn!(error = %e, "a write failed");
    tracing::warn!({ error = %e }, "a write failed");
    tokio::select! { _ = wait => { tracing::error!(error = %e, "a write failed"); } }
    tracing::debug!(error = %e, "a write failed");
}
"#;

#[test]
fn each_form_that_leaves_the_error_out_is_found() {
    let events = events_of(LEFT_OUT_FORMS).expect("the fixture lexes");

    let expected: Vec<Finding> = (3..=12)
        .map(|line| Finding {
            line,
            value: "e".to_owned(),
        })
        .collect();
    assert_eq!(findings(&events), expected, "{events:?}");
}

/// Calls of other macros, an attribute and a string: none is an event.
const OTHER_MACROS: &str = r##"
#[derive(thiserror::Error)]
enum Failure {
    #[error("a write failed")]
    Write,
}
fn calls(e: &str) {
    log::error!("a write failed: {}", e);
    other::warn!(error = %e, "a write failed");
    let text = "tracing::warn!(error = %e, \"a write failed\")";
}
"##;

#[test]
fn calls_of_other_macros_are_not_events() {
    let events = events_of(OTHER_MACROS).expect("the fixture lexes");

    assert!(events.is_empty(), "{events:?}");
}

/// Whether the file at `relative` (under `src/worker/`) acts on a claimed
/// attempt of the worker parent: every file but the tests, the reaper
/// (`recovery.rs`, `recovery/`), the scheduler (`scheduler/`), the finalize
/// and phase-2 steps that the reaper shares (`execution.rs`, `execution/`),
/// and the child host, which runs in the child (`executor/child_host/`).
fn is_attempt_scoped(relative: &Path) -> bool {
    let first = relative
        .components()
        .next()
        .and_then(|component| component.as_os_str().to_str())
        .unwrap_or_default();
    let name = relative
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let is_test = name == "tests.rs"
        || name.ends_with("_tests.rs")
        || relative
            .components()
            .any(|component| component.as_os_str() == "tests");
    let shared_with_the_reaper = matches!(
        first,
        "recovery.rs" | "recovery" | "scheduler" | "execution.rs" | "execution"
    );
    !is_test && !shared_with_the_reaper && !relative.starts_with("executor/child_host")
}

fn has_field(event: &Event, name: &str) -> bool {
    event.fields.iter().any(|field| field.name == name)
}

/// The lines of the events of `events` with a `task_id` field and no
/// `attempt` field.
fn missing_attempts(events: &[Event]) -> Vec<usize> {
    events
        .iter()
        .filter(|event| has_field(event, "task_id") && !has_field(event, "attempt"))
        .map(|event| event.line)
        .collect()
}

#[test]
fn every_event_of_an_attempt_of_the_worker_names_its_attempt() {
    let worker = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("worker");
    let known_site = worker.join("executor").join("slots").join("outcome.rs");
    let mut files = Vec::new();
    rust_files(&worker, 1, &mut files);
    files.retain(|path| {
        is_attempt_scoped(
            path.strip_prefix(&worker)
                .expect("a file under src/worker/"),
        )
    });
    files.sort();
    let mut task_events = 0;
    let mut known_site_found = false;
    let mut report = Vec::new();
    for path in &files {
        let source = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        let events =
            events_of(&source).unwrap_or_else(|error| panic!("lex {}: {error}", path.display()));
        task_events += events
            .iter()
            .filter(|event| has_field(event, "task_id"))
            .count();
        known_site_found |= *path == known_site
            && events
                .iter()
                .any(|event| has_field(event, "task_id") && has_field(event, "attempt"));
        let relative = path
            .strip_prefix(&worker)
            .expect("a file under src/worker/");
        report.extend(
            missing_attempts(&events)
                .into_iter()
                .map(|line| format!("{}:{line}", relative.display())),
        );
    }
    // A known positive: the scan reaches the events of the outcome writer,
    // which name the attempt of their task.
    assert!(
        known_site_found,
        "the scan of {} files found no event with task_id and attempt in {}",
        files.len(),
        known_site.display()
    );
    assert!(
        report.is_empty(),
        "{} of {task_events} events with a task_id field have no attempt field; add \
         `attempt = task.retry.attempt().get()` and `trace_id = task.trace_id.log_field()`:\n{}",
        report.len(),
        report.join("\n")
    );
}

/// One event per line from line 3 to line 6: the first and the third name
/// their task and not its attempt.
const EVENTS_OF_AN_ATTEMPT: &str = r#"
fn sites(task: &Task, task_id: Uuid) {
    tracing::warn!(task_id = %task.task_id, "a write failed");
    tracing::warn!(%task_id, attempt = task.attempt, "a write failed");
    tracing::debug!(task_id, "a run ended");
    tracing::warn!(task_id = %task_id, attempt = 2, trace_id = task.trace_id, "a write failed");
    tracing::info!(attempt = 2, "no task is named");
}
"#;

#[test]
fn an_event_that_names_its_task_and_not_its_attempt_is_found() {
    let events = events_of(EVENTS_OF_AN_ATTEMPT).expect("the fixture lexes");

    assert_eq!(events.len(), 5, "{events:?}");
    assert_eq!(missing_attempts(&events), vec![3, 5]);
}

#[test]
fn the_reaper_the_scheduler_the_child_host_and_the_tests_are_not_attempt_scoped() {
    for scoped in [
        "worker.rs",
        "claim_pass.rs",
        "children.rs",
        "executor/slots/outcome.rs",
        "executor/slots/isolated.rs",
    ] {
        assert!(is_attempt_scoped(Path::new(scoped)), "{scoped}");
    }
    for outside in [
        "recovery.rs",
        "recovery/owner_gone.rs",
        "scheduler/service.rs",
        "execution.rs",
        "execution/parse.rs",
        "executor/child_host/run.rs",
        "executor/slots/settle_tests.rs",
        "executor/slots/tests.rs",
        "executor/slots/tests/settle_tests.rs",
    ] {
        assert!(!is_attempt_scoped(Path::new(outside)), "{outside}");
    }
}
