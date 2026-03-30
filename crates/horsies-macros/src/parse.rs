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
    /// Optional: good_until expression.
    pub good_until: Option<Expr>,
    /// Optional: auto_retry_for list of string codes.
    pub auto_retry_for: Option<ExprArray>,
}

impl Parse for TaskAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // First positional argument: task name (required).
        let name: LitStr = input.parse()?;

        let mut queue = None;
        let mut retry_policy = None;
        let mut good_until = None;
        let mut auto_retry_for = None;

        // Parse optional keyword arguments.
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;

            // Allow trailing comma.
            if input.is_empty() {
                break;
            }

            let key: Ident = input.parse()?;
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
                    if good_until.is_some() {
                        return Err(syn::Error::new(
                            key.span(),
                            "duplicate `good_until` attribute",
                        ));
                    }
                    good_until = Some(input.parse::<Expr>()?);
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
                            "unknown attribute `{}`; expected one of: queue, retry_policy, good_until, auto_retry_for",
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
            good_until,
            auto_retry_for,
        })
    }
}
