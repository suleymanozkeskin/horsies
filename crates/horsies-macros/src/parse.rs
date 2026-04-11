use syn::parse::{Parse, ParseStream};
use syn::{Expr, ExprArray, Ident, LitStr, Token};

/// Parsed attributes from `#[horsies::task("name", queue = "...", ...)]`.
pub struct TaskAttrs {
    /// Required: the task name.
    pub name: LitStr,
    /// Optional: target queue.
    pub queue: Option<LitStr>,
    /// Optional: retry policy expression.
    pub retry_policy: Option<Expr>,
    /// Optional: auto_retry_for list of string codes.
    pub auto_retry_for: Option<ExprArray>,
    /// Optional: mark this task as accepting workflow context injection.
    pub workflow_ctx: bool,
}

impl Parse for TaskAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // First positional argument: task name (required).
        let name: LitStr = input.parse()?;

        let mut queue = None;
        let mut retry_policy = None;
        let mut auto_retry_for = None;
        let mut workflow_ctx = false;

        // Parse optional keyword arguments.
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;

            // Allow trailing comma.
            if input.is_empty() {
                break;
            }

            let key: Ident = input.parse()?;

            // `workflow_ctx` is a bare flag — no `= value`.
            if key == "workflow_ctx" {
                if workflow_ctx {
                    return Err(syn::Error::new(
                        key.span(),
                        "duplicate `workflow_ctx` attribute",
                    ));
                }
                workflow_ctx = true;
                continue;
            }

            input.parse::<Token![=]>()?;

            match key.to_string().as_str() {
                "queue" => {
                    if queue.is_some() {
                        return Err(syn::Error::new(key.span(), "duplicate `queue` attribute"));
                    }
                    queue = Some(input.parse::<LitStr>()?);
                }
                "retry_policy" => {
                    if retry_policy.is_some() {
                        return Err(syn::Error::new(
                            key.span(),
                            "duplicate `retry_policy` attribute",
                        ));
                    }
                    retry_policy = Some(input.parse::<Expr>()?);
                }
                "good_until" => {
                    let _expr = input.parse::<Expr>()?;
                    return Err(syn::Error::new(
                        key.span(),
                        "`good_until` is not accepted on `#[task]` because it would be \
                         evaluated at registration time; use \
                         task.with_options(TaskSendOptions::new().good_until(deadline)).send(args) \
                         for ad-hoc sends, or task::node()?.good_until(deadline) for workflow tasks",
                    ));
                }
                "auto_retry_for" => {
                    if auto_retry_for.is_some() {
                        return Err(syn::Error::new(
                            key.span(),
                            "duplicate `auto_retry_for` attribute",
                        ));
                    }
                    auto_retry_for = Some(input.parse::<ExprArray>()?);
                }
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown attribute `{}`; expected one of: queue, retry_policy, auto_retry_for, workflow_ctx",
                            key,
                        ),
                    ));
                }
            }
        }

        if name.value().is_empty() {
            return Err(syn::Error::new(name.span(), "task name must not be empty"));
        }

        Ok(TaskAttrs {
            name,
            queue,
            retry_policy,
            auto_retry_for,
            workflow_ctx,
        })
    }
}
