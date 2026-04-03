use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemFn};

mod codegen;
mod parse;

/// Attribute macro for defining an async horsies task.
///
/// # Example
///
/// ```ignore
/// #[horsies::task("validate_order", queue = "standard")]
/// async fn validate_order(order: Order) -> Result<ValidatedOrder, TaskError> {
///     Ok(ValidatedOrder { ... })
/// }
/// ```
///
/// Tasks that need to start a dynamic workflow at runtime may inject
/// `horsies::TaskRuntime` as the first parameter:
///
/// ```ignore
/// #[horsies::task("scrape_detail")]
/// async fn scrape_detail(
///     rt: horsies::TaskRuntime,
///     input: ScrapeInput,
/// ) -> Result<(), TaskError> {
///     if let Some(spec) = build_enrichment_spec(&input)? {
///         rt.start::<()>(spec).await?;
///     }
///     Ok(())
/// }
/// ```
///
/// Generates a companion `#[doc(hidden)]` module with a `register()`
/// function. Register via:
///
/// ```ignore
/// let task = validate_order::register(&mut app)?;
/// ```
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = parse_macro_input!(attr as parse::TaskAttrs);
    let func = parse_macro_input!(item as ItemFn);

    match codegen::generate_task(attrs, func, /* blocking */ false) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Attribute macro for defining a blocking (CPU-bound) horsies task.
///
/// Same as `#[horsies::task]` but generates a `blocking_task_fn!` registration
/// instead of `async_task_fn!`.
///
/// ```ignore
/// #[horsies::blocking_task("cpu_heavy", queue = "background")]
/// fn cpu_heavy(input: HeavyInput) -> Result<HeavyOutput, TaskError> {
///     Ok(HeavyOutput { ... })
/// }
/// ```
#[proc_macro_attribute]
pub fn blocking_task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = parse_macro_input!(attr as parse::TaskAttrs);
    let func = parse_macro_input!(item as ItemFn);

    match codegen::generate_task(attrs, func, /* blocking */ true) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
