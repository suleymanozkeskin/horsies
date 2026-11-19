//! The derives that `horsies-core` uses for its own facts: the static data of
//! its closed code enums (`CodeCatalog`), the Python classes of its error
//! enums (`ErrorCatalog`) and the values of their instances
//! (`ErrorClassValues`), and the Python form of its manifest types
//! (`ManifestSchema`). The task and workflow macros of the Rust client are
//! in `horsies-macros`.

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod code_catalog;
mod error_catalog;
mod manifest_schema;

/// Derive the static catalog data of a closed code enum of horsies-core:
/// its members, their wire values and their docs. `horsies-codegen` renders
/// the Python enum classes from it.
///
/// ```ignore
/// /// Categorized broker operation failure codes.
/// #[derive(horsies_core_derive::CodeCatalog)]
/// #[code_catalog(module = "broker", base = "str_enum")]
/// pub enum BrokerErrorCode {
///     /// Schema initialization failed.
///     SchemaInitFailed,
/// }
/// ```
///
/// Emits `impl horsies_core::catalog::CodeEnum for BrokerErrorCode`.
///
/// A lifecycle status marks its terminal variants with
/// `#[code_catalog(terminal)]` and names their Python constant with
/// `#[code_catalog(terminal_states = "...")]` on the enum.
#[proc_macro_derive(CodeCatalog, attributes(code_catalog))]
pub fn derive_code_catalog(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match code_catalog::derive_code_catalog(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive the Python class of each variant of an error enum of
/// horsies-core: its name, its doc and its slots. `horsies-codegen` renders
/// the frozen dataclasses and the union from it. See the `error_catalog`
/// module for the attributes.
///
/// ```ignore
/// #[derive(horsies_core_derive::ErrorCatalog)]
/// #[error_catalog(module = "broker_errors", code = "BrokerErrorCode",
///     union = "BrokerCoreError", union_doc = "Every core failure.")]
/// pub enum BrokerOperationError {
///     /// The task id holds a different command. Nothing was written.
///     #[error_class(class = "BrokerPayloadMismatch", code = "PayloadMismatch", never_retryable)]
///     PayloadMismatch {
///         #[error_slot(python = "Str", hidden)]
///         message: String,
///     },
/// }
/// ```
///
/// Emits `impl horsies_core::error_catalog::ErrorEnum` for the enum.
#[proc_macro_derive(ErrorCatalog, attributes(error_catalog, error_class, error_slot))]
pub fn derive_error_catalog(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match error_catalog::derive_error_catalog(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive the values of the instance slots of an error enum's value, by
/// name, in the order of its Python class (the class that `ErrorCatalog`
/// records). A client builds the class of the value from them. Each variant
/// with `exception` has one `#[error_slot(cause)]` field, whose type
/// implements `horsies_core::error_catalog::ExceptionCause`.
///
/// ```ignore
/// #[derive(horsies_core_derive::ErrorCatalog, horsies_core_derive::ErrorClassValues)]
/// #[error_catalog(module = "monitoring_errors", code = "TaskActionErrorCode",
///     union = "TaskActionCoreError", union_doc = "Every core failure.")]
/// pub enum TaskActionError {
///     /// The cancel statement failed.
///     #[error_class(class = "TaskActionDbOperationFailed", code = "DbOperationFailed",
///         exception = "BaseException")]
///     DbOperationFailed {
///         #[error_slot(python = "Str", hidden)]
///         message: String,
///         #[error_slot(cause)]
///         cause: BrokerError,
///     },
/// }
/// ```
///
/// Emits `impl horsies_core::error_catalog::ErrorClassValues` for the enum.
#[proc_macro_derive(ErrorClassValues, attributes(error_catalog, error_class, error_slot))]
pub fn derive_error_class_values(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match error_catalog::derive_error_class_values(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive the Python form of a manifest type, read by the Python code
/// generator.
///
/// A struct of named fields is a frozen dataclass; an enum with a serde
/// `tag` is one dataclass per variant and a union alias; an enum of unit
/// variants is a `Literal` alias. Each field type implements
/// `horsies_core::manifest_schema::ManifestValue`.
///
/// ```ignore
/// /// Where the session pool connects.
/// #[derive(serde::Deserialize, horsies_core_derive::ManifestSchema)]
/// #[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
/// pub enum SessionDatabase {
///     SameAsDatabaseUrl,
///     Separate { url: DatabaseUrl },
/// }
/// ```
///
/// Emits `impl horsies_core::manifest_schema::ManifestValue` and
/// `impl horsies_core::manifest_schema::ManifestItem` for the type.
#[proc_macro_derive(ManifestSchema)]
pub fn derive_manifest_schema(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match manifest_schema::derive_manifest_schema(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
