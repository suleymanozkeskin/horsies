//! The derives that `horsies-core` uses for its own facts: the static data of
//! its closed code enums (`CodeCatalog`), the error classes of its error
//! enums (`ErrorCatalog`) and the values of their instances
//! (`ErrorClassValues`), the schema of its manifest types
//! (`ManifestSchema`), and the wire codec and schema of its event
//! types (`EventSchema`). The task and workflow macros of the Rust client
//! are in `horsies-macros`.

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod code_catalog;
mod error_catalog;
mod event_schema;
mod manifest_schema;

/// Derive the static catalog data of a closed code enum of horsies-core:
/// its members, their names, their wire values and their docs.
/// `horsies-codegen` renders the enum types of each client from it.
///
/// ```ignore
/// /// Categorized broker operation failure codes.
/// #[derive(horsies_core_derive::CodeCatalog)]
/// #[code_catalog(module = "broker", values = "equal_text")]
/// pub enum DatabaseErrorCode {
///     /// Schema initialization failed.
///     SchemaInitFailed,
/// }
/// ```
///
/// Emits `impl horsies_core::catalog::CodeEnum for DatabaseErrorCode`.
///
/// The name of a member is the variant name in SCREAMING_SNAKE_CASE.
/// Its wire value is the same text, unless the variant sets
/// `#[code_catalog(value = "...")]` (`HRS-001` for `WorkflowNoName`).
///
/// A lifecycle status marks its terminal variants with
/// `#[code_catalog(terminal)]` and names their constant with
/// `#[code_catalog(terminal_states = "...")]` on the enum.
#[proc_macro_derive(CodeCatalog, attributes(code_catalog))]
pub fn derive_code_catalog(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match code_catalog::derive_code_catalog(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive the error class of each variant of an error enum of
/// horsies-core: its name, its doc and its slots. `horsies-codegen` renders
/// the classes and the union of each client from it. See the `error_catalog`
/// module for the attributes.
///
/// ```ignore
/// #[derive(horsies_core_derive::ErrorCatalog)]
/// #[error_catalog(module = "broker_errors", code = "DatabaseErrorCode",
///     union = "DatabaseCoreError", union_doc = "Every core failure.")]
/// pub enum DatabaseOperationError {
///     /// The task id holds a different command. Nothing was written.
///     #[error_class(class = "DatabasePayloadMismatch", code = "PayloadMismatch", never_retryable)]
///     PayloadMismatch {
///         #[error_slot(slot_type = "Str", hidden)]
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
/// name, in the order of its class (the class that `ErrorCatalog`
/// records). A client builds the class of the value from them. Each variant
/// with `cause = "..."` has one `#[error_slot(cause)]` field, whose type
/// implements `horsies_core::error_catalog::ExceptionCause`.
///
/// ```ignore
/// #[derive(horsies_core_derive::ErrorCatalog, horsies_core_derive::ErrorClassValues)]
/// #[error_catalog(module = "monitoring_errors", code = "TaskActionErrorCode",
///     union = "TaskActionCoreError", union_doc = "Every core failure.")]
/// pub enum TaskActionError {
///     /// The cancel statement failed.
///     #[error_class(class = "TaskActionDatabaseOperationFailed", code = "DatabaseOperationFailed",
///         cause = "required")]
///     DatabaseOperationFailed {
///         #[error_slot(slot_type = "Str", hidden)]
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

/// Derive the schema of a manifest type, read by the code generators.
///
/// A struct of named fields is a record; an enum with a serde `tag` is one
/// record per variant and a union; an enum of unit variants is a set of
/// names. Each field type implements
/// `horsies_core::manifest_schema::ManifestValue`, or the field names its
/// integer bound with `#[manifest_schema(int_bound = <IntBound constant>)]`.
/// A generic type lists its uses with `#[manifest_schema(uses(A, B))]` and
/// is recorded once per use. See the `manifest_schema` module for the serde
/// attributes that the derive reads.
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
#[proc_macro_derive(ManifestSchema, attributes(manifest_schema))]
pub fn derive_manifest_schema(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match manifest_schema::derive_manifest_schema(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive the wire codec and the schema of a type of the event
/// contract (`horsies_core::events`).
///
/// A struct of named fields is an event type: wire form `[tag, field, ...]`.
/// An enum of unit variants is a choice: wire form the variant name in
/// SCREAMING_SNAKE_CASE. An enum with a variant of named fields is a
/// variant type: wire form `[variant name, field, ...]`. Each field type
/// implements `horsies_core::events::EventValue`.
///
/// ```ignore
/// /// When the attempt started.
/// #[derive(horsies_core_derive::EventSchema)]
/// pub enum AttemptStart {
///     /// The child reported the time of its start write.
///     Reported { started_at: DateTime<Utc> },
///     /// The child reported no start time.
///     NotReported,
/// }
/// ```
///
/// Emits `impl horsies_core::events::EventRecord` for a struct, and
/// `impl horsies_core::events::EventValue` and
/// `impl horsies_core::events::EventNamedValue` for an enum. A field with
/// `#[event_schema(internal)]` is read only by the handler child.
#[proc_macro_derive(EventSchema, attributes(event_schema))]
pub fn derive_event_schema(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match event_schema::derive_event_schema(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
