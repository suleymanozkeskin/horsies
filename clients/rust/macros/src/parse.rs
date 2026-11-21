//! The attributes of `#[horsies::task("name", ...)]` and
//! `#[horsies::blocking_task("name", ...)]`, and the rules that hold
//! without the function: bounds, repeats, and the pairs that go together.
//! The rules that need the function (its asyncness against the execution
//! class) are in `codegen`.

use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, ExprArray, Ident, Lit, LitInt, LitStr, Token};

/// The smallest `timeout_ms` (Python `MIN_TASK_TIMEOUT_MS`). The largest is
/// `u32::MAX` (Python `MAX_TASK_TIMEOUT_MS`), the bound of the parse.
const MIN_TIMEOUT_MS: u32 = 1_000;

/// The smallest `cancel_grace_ms` (Python `MIN_CANCEL_GRACE_MS`).
const MIN_CANCEL_GRACE_MS: u32 = 100;

/// The largest `cancel_grace_ms` (Python `MAX_CANCEL_GRACE_MS`).
const MAX_CANCEL_GRACE_MS: u32 = 10_000;

const KNOWN_ATTRIBUTES: &str = "queue, retry_policy, auto_retry_for, timeout_ms, execution, \
                                cancel_grace_ms, workflow_ctx";

/// How a worker runs the task (Python `Execution`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionClass {
    /// One task at a time in its own child. The default.
    Isolated,
    /// An `async fn` on a seat of a shared async child.
    SharedAsync,
    /// A blocking `fn` on a seat thread of a shared thread child.
    SharedThread,
}

impl ExecutionClass {
    /// The attribute value.
    pub fn attribute_value(self) -> &'static str {
        match self {
            Self::Isolated => "isolated",
            Self::SharedAsync => "shared_async",
            Self::SharedThread => "shared_thread",
        }
    }
}

/// The execution class and where it was written.
#[derive(Debug, Clone, Copy)]
pub struct ExecutionAttr {
    pub class: ExecutionClass,
    /// The span of the value; `call_site` when the class is the default.
    pub span: Span,
}

/// One validated millisecond value and where it was written.
#[derive(Debug, Clone, Copy)]
pub struct Milliseconds {
    pub value: u32,
    pub span: Span,
}

/// One `auto_retry_for` entry.
#[derive(Debug, Clone)]
pub struct RetryCode {
    pub code: LitStr,
}

/// The retry of a task: a policy with the codes that retry, or none. Python
/// keeps the codes inside the policy, with at least one code, so one without
/// the other is refused.
#[derive(Debug, Clone)]
pub enum Retry {
    NoRetry,
    Policy {
        policy: Box<Expr>,
        codes: Vec<RetryCode>,
    },
}

/// The validated attributes of a task.
pub struct TaskAttrs {
    /// The task name: not empty.
    pub name: LitStr,
    pub queue: Option<LitStr>,
    pub retry: Retry,
    /// From [`MIN_TIMEOUT_MS`] to `u32::MAX`.
    pub timeout_ms: Option<Milliseconds>,
    pub execution: ExecutionAttr,
    /// From [`MIN_CANCEL_GRACE_MS`] to [`MAX_CANCEL_GRACE_MS`], on a
    /// `shared_async` task only.
    pub cancel_grace_ms: Option<Milliseconds>,
    pub workflow_ctx: bool,
}

/// The attributes as written, before the rules between them.
#[derive(Default)]
struct WrittenAttrs {
    queue: Option<LitStr>,
    retry_policy: Option<(Ident, Expr)>,
    auto_retry_for: Option<(Ident, Vec<RetryCode>)>,
    timeout_ms: Option<Milliseconds>,
    execution: Option<ExecutionAttr>,
    cancel_grace_ms: Option<Milliseconds>,
    workflow_ctx: bool,
}

impl Parse for TaskAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: LitStr = input.parse()?;
        if name.value().is_empty() {
            return Err(syn::Error::new(name.span(), "task name must not be empty"));
        }
        let mut written = WrittenAttrs::default();
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            parse_one(input, &key, &mut written)?;
        }
        if !input.is_empty() {
            return Err(input.error("expected `,` between task attributes"));
        }
        resolve(name, written)
    }
}

/// Parse the value of `key` into `written`. Refuses a repeated key, an
/// unknown key and `good_until`.
fn parse_one(input: ParseStream, key: &Ident, written: &mut WrittenAttrs) -> syn::Result<()> {
    let key_text = key.to_string();
    if key_text == "workflow_ctx" {
        refuse_repeat(written.workflow_ctx, key)?;
        written.workflow_ctx = true;
        return Ok(());
    }
    input.parse::<Token![=]>()?;
    match key_text.as_str() {
        "queue" => {
            refuse_repeat(written.queue.is_some(), key)?;
            written.queue = Some(input.parse::<LitStr>()?);
        }
        "retry_policy" => {
            refuse_repeat(written.retry_policy.is_some(), key)?;
            written.retry_policy = Some((key.clone(), input.parse::<Expr>()?));
        }
        "auto_retry_for" => {
            refuse_repeat(written.auto_retry_for.is_some(), key)?;
            let codes = retry_codes(&input.parse::<ExprArray>()?)?;
            written.auto_retry_for = Some((key.clone(), codes));
        }
        "timeout_ms" => {
            refuse_repeat(written.timeout_ms.is_some(), key)?;
            written.timeout_ms = Some(bounded_ms(
                &input.parse::<LitInt>()?,
                "timeout_ms",
                MIN_TIMEOUT_MS,
                u32::MAX,
            )?);
        }
        "execution" => {
            refuse_repeat(written.execution.is_some(), key)?;
            written.execution = Some(execution_class(&input.parse::<Ident>()?)?);
        }
        "cancel_grace_ms" => {
            refuse_repeat(written.cancel_grace_ms.is_some(), key)?;
            written.cancel_grace_ms = Some(bounded_ms(
                &input.parse::<LitInt>()?,
                "cancel_grace_ms",
                MIN_CANCEL_GRACE_MS,
                MAX_CANCEL_GRACE_MS,
            )?);
        }
        "good_until" => {
            let _value = input.parse::<Expr>()?;
            return Err(syn::Error::new(
                key.span(),
                "`good_until` is not a task attribute: a deadline fixed at registration is \
                 in the past for every later send; set it per send with \
                 TaskSendOptions::good_until, or per workflow node",
            ));
        }
        _ => {
            return Err(syn::Error::new(
                key.span(),
                format!("unknown attribute `{key}`; expected one of: {KNOWN_ATTRIBUTES}"),
            ));
        }
    }
    Ok(())
}

fn refuse_repeat(already: bool, key: &Ident) -> syn::Result<()> {
    match already {
        true => Err(syn::Error::new(
            key.span(),
            format!("duplicate `{key}` attribute"),
        )),
        false => Ok(()),
    }
}

/// Each entry of `auto_retry_for` is a string literal. Whether it names a
/// built-in code or a code of the task's code type is checked at compile
/// time by a `const` assertion in the expansion: this crate has no copy of
/// the built-in codes.
fn retry_codes(array: &ExprArray) -> syn::Result<Vec<RetryCode>> {
    array
        .elems
        .iter()
        .map(|element| match element {
            Expr::Lit(literal) => match &literal.lit {
                Lit::Str(code) => Ok(RetryCode { code: code.clone() }),
                other => Err(syn::Error::new_spanned(
                    other,
                    "an `auto_retry_for` entry is a code name in a string literal",
                )),
            },
            other => Err(syn::Error::new_spanned(
                other,
                "an `auto_retry_for` entry is a code name in a string literal",
            )),
        })
        .collect()
}

fn bounded_ms(literal: &LitInt, field: &str, min: u32, max: u32) -> syn::Result<Milliseconds> {
    let value = literal.base10_parse::<u32>().map_err(|_| {
        syn::Error::new(
            literal.span(),
            format!("`{field}` must be from {min} to {max}"),
        )
    })?;
    match (min..=max).contains(&value) {
        true => Ok(Milliseconds {
            value,
            span: literal.span(),
        }),
        false => Err(syn::Error::new(
            literal.span(),
            format!("`{field}` must be from {min} to {max} (got {value})"),
        )),
    }
}

fn execution_class(value: &Ident) -> syn::Result<ExecutionAttr> {
    let class = match value.to_string().as_str() {
        "isolated" => ExecutionClass::Isolated,
        "shared_async" => ExecutionClass::SharedAsync,
        "shared_thread" => ExecutionClass::SharedThread,
        _ => {
            return Err(syn::Error::new(
                value.span(),
                format!(
                    "unknown execution class `{value}`; expected isolated, shared_async or \
                     shared_thread"
                ),
            ))
        }
    };
    Ok(ExecutionAttr {
        class,
        span: value.span(),
    })
}

/// The rules between attributes (Python `_check_cancel_grace` and the
/// timeout rule of a shared class):
/// - a shared class needs `timeout_ms`;
/// - `cancel_grace_ms` is for `shared_async` only;
/// - `retry_policy` and `auto_retry_for` go together, with at least one
///   code.
fn resolve(name: LitStr, written: WrittenAttrs) -> syn::Result<TaskAttrs> {
    let execution = written.execution.unwrap_or(ExecutionAttr {
        class: ExecutionClass::Isolated,
        span: Span::call_site(),
    });
    match (execution.class, written.timeout_ms) {
        (ExecutionClass::SharedAsync | ExecutionClass::SharedThread, None) => {
            return Err(syn::Error::new(
                execution.span,
                format!(
                    "execution = {} needs `timeout_ms`: a shared child runs other tasks next \
                     to this one, and the deadline is what ends a task that does not return",
                    execution.class.attribute_value()
                ),
            ));
        }
        (ExecutionClass::Isolated, _)
        | (ExecutionClass::SharedAsync | ExecutionClass::SharedThread, Some(_)) => {}
    }
    match (execution.class, written.cancel_grace_ms) {
        (ExecutionClass::Isolated | ExecutionClass::SharedThread, Some(grace)) => {
            return Err(syn::Error::new(
                grace.span,
                format!(
                    "`cancel_grace_ms` is for execution = shared_async only; this task is {}",
                    execution.class.attribute_value()
                ),
            ));
        }
        (ExecutionClass::SharedAsync, _) | (_, None) => {}
    }
    Ok(TaskAttrs {
        name,
        queue: written.queue,
        retry: retry(written.retry_policy, written.auto_retry_for)?,
        timeout_ms: written.timeout_ms,
        execution,
        cancel_grace_ms: written.cancel_grace_ms,
        workflow_ctx: written.workflow_ctx,
    })
}

fn retry(
    policy: Option<(Ident, Expr)>,
    codes: Option<(Ident, Vec<RetryCode>)>,
) -> syn::Result<Retry> {
    match (policy, codes) {
        (None, None) => Ok(Retry::NoRetry),
        (Some((key, _policy)), None) => Err(syn::Error::new(
            key.span(),
            "`retry_policy` needs `auto_retry_for`: the codes whose failures retry",
        )),
        (None, Some((key, _codes))) => Err(syn::Error::new(
            key.span(),
            "`auto_retry_for` needs `retry_policy`: without a policy a task does not retry",
        )),
        (Some(_), Some((key, codes))) if codes.is_empty() => Err(syn::Error::new(
            key.span(),
            "`auto_retry_for` needs at least one code",
        )),
        (Some((_, policy)), Some((_, codes))) => Ok(Retry::Policy {
            policy: Box::new(policy),
            codes,
        }),
    }
}
