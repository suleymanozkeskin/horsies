//! `#[derive(ErrorCatalog)]`: the Python class of each variant of an error
//! enum, read by the Python code generator.
//!
//! The enum is the one source. Container attribute:
//! `#[error_catalog(module = "...", code = "...", union = "...", union_doc = "...")]`:
//! the module under `horsies._catalog`, the code enum (a path in scope), and
//! the name and doc of the union of every class.
//!
//! Variant attribute: `#[error_class(class = "...", ...)]` with
//!
//! - `code = "Variant"`: `code` is a `ClassVar` member, the variant
//!   `Variant` of the code enum; or `code_field = "field"` and
//!   `code_subset = "Name"`: `code` is the field `field`, a `Literal`
//!   subset `Name` that converts into the code enum (`From`);
//! - `never_retryable`: `retryable: ClassVar[bool] = False`;
//! - `exception = "BaseException"` or `exception = "BaseException | NoCause"`:
//!   the last slot, `exception`, the core's exception class of the cause.
//!
//! The variant's `///` lines are the class doc. Each named field other than
//! the code field has `#[error_slot(python = "Type")]`, with `hidden` for
//! `field(repr=False)`, or `#[error_slot(skip)]` for a field that only feeds
//! the exception (a cause or a source). `Type` is a variant of
//! `horsies_core::error_catalog::SlotType`.

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{Attribute, Data, DataEnum, DeriveInput, Fields, Ident, LitStr, Path, Variant};

use crate::code_catalog::{doc_text, screaming_snake_case};

/// The Python types that a field slot can have: the variants of
/// `SlotType`.
const SLOT_TYPES: &[&str] = &[
    "Str",
    "Bool",
    "Int",
    "Uuid",
    "UuidText",
    "TaskStatus",
    "TaskSendPayload",
];

/// The container settings.
struct CatalogSettings {
    module: String,
    code: Path,
    union: String,
    union_doc: String,
}

/// How a class holds its code.
enum CodeSource {
    /// A `ClassVar` member: the variant `Ident` of the code enum.
    Member(Ident),
    /// An instance field of the variant, typed by a `Literal` subset.
    Field { field: Ident, subset: String },
}

/// Whether a class never clears.
enum Retry {
    NeverRetryable,
    ByField,
}

/// The declared type of the `exception` slot, when the class has one.
enum ExceptionSlot {
    Absent,
    Exception,
    ExceptionOrNoCause,
}

/// The settings of one variant.
struct ClassSettings {
    class: String,
    code: CodeSource,
    retry: Retry,
    exception: ExceptionSlot,
}

pub fn derive_error_catalog(input: DeriveInput) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "ErrorCatalog can only be derived for an enum without generic parameters",
        ));
    }
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            ident,
            "ErrorCatalog can only be derived for an enum",
        ));
    };
    let settings = catalog_settings(&input.attrs, ident)?;
    let classes = read_classes(data, &settings.code)?;
    let CatalogSettings {
        module,
        code,
        union,
        union_doc,
    } = settings;
    let records = classes.iter().map(|class| &class.record);
    let code_arms = classes.iter().map(|class| &class.code_arm);
    let class_arms = classes.iter().map(|class| &class.class_arm);
    Ok(quote! {
        impl ::horsies_core::error_catalog::ErrorEnum for #ident {
            type Code = #code;

            const ERROR_CATALOG: ::horsies_core::error_catalog::ErrorCatalog =
                ::horsies_core::error_catalog::ErrorCatalog {
                    python_module: #module,
                    code_catalog: <#code as ::horsies_core::catalog::CodeEnum>::CATALOG,
                    union: #union,
                    union_doc: #union_doc,
                    classes: &[#(#records,)*],
                };

            fn error_code(&self) -> #code {
                match self {
                    #(#code_arms,)*
                }
            }

            fn python_class(&self) -> &'static str {
                match self {
                    #(#class_arms,)*
                }
            }
        }
    })
}

/// The generated parts of one variant.
struct ClassParts {
    record: TokenStream,
    code_arm: TokenStream,
    class_arm: TokenStream,
}

fn read_classes(data: &DataEnum, code: &Path) -> syn::Result<Vec<ClassParts>> {
    data.variants
        .iter()
        .map(|variant| class_parts(variant, code))
        .collect()
}

fn class_parts(variant: &Variant, code: &Path) -> syn::Result<ClassParts> {
    let settings = class_settings(variant)?;
    let variant_ident = &variant.ident;
    let class = &settings.class;
    let doc = doc_text(&variant.attrs);
    let mut slots: Vec<TokenStream> = Vec::new();
    let code_arm = match &settings.code {
        CodeSource::Member(member) => {
            let value = screaming_snake_case(&member.to_string());
            slots.push(
                quote!(::horsies_core::error_catalog::ErrorSlot::CodeMember { member: #value }),
            );
            quote!(Self::#variant_ident { .. } => #code::#member)
        }
        CodeSource::Field { field, subset } => {
            slots.push(
                quote!(::horsies_core::error_catalog::ErrorSlot::CodeSubset { subset: #subset }),
            );
            quote!(Self::#variant_ident { #field, .. } => ::core::convert::From::from(*#field))
        }
    };
    match settings.retry {
        Retry::NeverRetryable => {
            slots.push(quote!(
                ::horsies_core::error_catalog::ErrorSlot::NeverRetryable
            ));
        }
        Retry::ByField => {}
    }
    let code_field = match &settings.code {
        CodeSource::Field { field, .. } => Some(field.to_string()),
        CodeSource::Member(_) => None,
    };
    slots.extend(field_slots(variant, code_field.as_deref())?);
    match settings.exception {
        ExceptionSlot::Absent => {}
        ExceptionSlot::Exception => slots.push(quote!(
            ::horsies_core::error_catalog::ErrorSlot::Exception {
                python_type: ::horsies_core::error_catalog::ExceptionType::Exception,
            }
        )),
        ExceptionSlot::ExceptionOrNoCause => slots.push(quote!(
            ::horsies_core::error_catalog::ErrorSlot::Exception {
                python_type: ::horsies_core::error_catalog::ExceptionType::ExceptionOrNoCause,
            }
        )),
    }
    Ok(ClassParts {
        record: quote!(::horsies_core::error_catalog::ErrorClass {
            python_class: #class,
            doc: #doc,
            slots: &[#(#slots,)*],
        }),
        code_arm,
        class_arm: quote!(Self::#variant_ident { .. } => #class),
    })
}

/// The slots of the named fields of `variant`, in declaration order,
/// without the code field.
fn field_slots(variant: &Variant, code_field: Option<&str>) -> syn::Result<Vec<TokenStream>> {
    let fields = match &variant.fields {
        Fields::Named(named) => named.named.iter().collect::<Vec<_>>(),
        Fields::Unit => Vec::new(),
        Fields::Unnamed(_) => {
            return Err(syn::Error::new_spanned(
                variant,
                "an ErrorCatalog variant has named fields or none",
            ))
        }
    };
    let mut slots = Vec::with_capacity(fields.len());
    for field in fields {
        let Some(name) = &field.ident else {
            return Err(syn::Error::new_spanned(field, "a named field has a name"));
        };
        if code_field == Some(name.to_string().as_str()) {
            continue;
        }
        match slot_of_field(&field.attrs, name)? {
            FieldSlot::Skip => {}
            FieldSlot::Slot { python_type, repr } => {
                let name_text = name.to_string();
                let python_type = Ident::new(&python_type, Span::call_site());
                let repr = match repr {
                    Repr::Shown => quote!(::horsies_core::error_catalog::SlotRepr::Shown),
                    Repr::Hidden => quote!(::horsies_core::error_catalog::SlotRepr::Hidden),
                };
                slots.push(quote!(::horsies_core::error_catalog::ErrorSlot::Field {
                    name: #name_text,
                    python_type: ::horsies_core::error_catalog::SlotType::#python_type,
                    repr: #repr,
                }));
            }
        }
    }
    Ok(slots)
}

/// Whether `repr` shows a field.
enum Repr {
    Shown,
    Hidden,
}

/// What a field gives the class.
enum FieldSlot {
    Skip,
    Slot { python_type: String, repr: Repr },
}

fn slot_of_field(attrs: &[Attribute], name: &Ident) -> syn::Result<FieldSlot> {
    let mut python_type: Option<String> = None;
    let mut repr = Repr::Shown;
    let mut skip = false;
    let mut seen = false;
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("error_slot"))
    {
        seen = true;
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            match key.as_str() {
                "python" => {
                    let value: LitStr = meta.value()?.parse()?;
                    match SLOT_TYPES.contains(&value.value().as_str()) {
                        true => python_type = Some(value.value()),
                        false => {
                            return Err(syn::Error::new_spanned(
                                &value,
                                format!("python must be one of {SLOT_TYPES:?}"),
                            ))
                        }
                    }
                }
                "hidden" => repr = Repr::Hidden,
                "skip" => skip = true,
                _ => return Err(meta.error("the error_slot keys are python, hidden and skip")),
            }
            Ok(())
        })?;
    }
    match (seen, skip, python_type) {
        (false, _, _) => Err(syn::Error::new_spanned(
            name,
            "each field of an ErrorCatalog variant has #[error_slot(python = \"...\")] or #[error_slot(skip)]",
        )),
        (true, true, None) => Ok(FieldSlot::Skip),
        (true, false, Some(python_type)) => Ok(FieldSlot::Slot { python_type, repr }),
        (true, true, Some(_)) => Err(syn::Error::new_spanned(
            name,
            "error_slot takes skip or python, not both",
        )),
        (true, false, None) => Err(syn::Error::new_spanned(
            name,
            "error_slot needs python = \"...\" or skip",
        )),
    }
}

fn class_settings(variant: &Variant) -> syn::Result<ClassSettings> {
    let mut class: Option<String> = None;
    let mut member: Option<Ident> = None;
    let mut code_field: Option<Ident> = None;
    let mut code_subset: Option<String> = None;
    let mut retry = Retry::ByField;
    let mut exception = ExceptionSlot::Absent;
    for attr in variant
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("error_class"))
    {
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            match key.as_str() {
                "never_retryable" => retry = Retry::NeverRetryable,
                "class" => class = Some(meta.value()?.parse::<LitStr>()?.value()),
                "code" => member = Some(meta.value()?.parse::<LitStr>()?.parse::<Ident>()?),
                "code_field" => {
                    code_field = Some(meta.value()?.parse::<LitStr>()?.parse::<Ident>()?);
                }
                "code_subset" => code_subset = Some(meta.value()?.parse::<LitStr>()?.value()),
                "exception" => {
                    let value: LitStr = meta.value()?.parse()?;
                    exception = match value.value().as_str() {
                        "BaseException" => ExceptionSlot::Exception,
                        "BaseException | NoCause" => ExceptionSlot::ExceptionOrNoCause,
                        _ => {
                            return Err(syn::Error::new_spanned(
                                &value,
                                "exception must be \"BaseException\" or \"BaseException | NoCause\"",
                            ))
                        }
                    };
                }
                _ => {
                    return Err(meta.error(
                        "the error_class keys are class, code, code_field, code_subset, never_retryable and exception",
                    ))
                }
            }
            Ok(())
        })?;
    }
    let Some(class) = class else {
        return Err(syn::Error::new_spanned(
            &variant.ident,
            "each variant has #[error_class(class = \"...\")]",
        ));
    };
    let code = match (member, code_field, code_subset) {
        (Some(member), None, None) => CodeSource::Member(member),
        (None, Some(field), Some(subset)) => CodeSource::Field { field, subset },
        (_, _, _) => {
            return Err(syn::Error::new_spanned(
                &variant.ident,
                "error_class takes code, or code_field with code_subset",
            ))
        }
    };
    Ok(ClassSettings {
        class,
        code,
        retry,
        exception,
    })
}

fn catalog_settings(attrs: &[Attribute], ident: &Ident) -> syn::Result<CatalogSettings> {
    let mut module: Option<String> = None;
    let mut code: Option<Path> = None;
    let mut union: Option<String> = None;
    let mut union_doc: Option<String> = None;
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("error_catalog"))
    {
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            let value: LitStr = meta.value()?.parse()?;
            match key.as_str() {
                "module" => module = Some(value.value()),
                "code" => code = Some(value.parse::<Path>()?),
                "union" => union = Some(value.value()),
                "union_doc" => union_doc = Some(value.value()),
                _ => {
                    return Err(
                        meta.error("the error_catalog keys are module, code, union and union_doc")
                    )
                }
            }
            Ok(())
        })?;
    }
    match (module, code, union, union_doc) {
        (Some(module), Some(code), Some(union), Some(union_doc)) => Ok(CatalogSettings {
            module,
            code,
            union,
            union_doc,
        }),
        (_, _, _, _) => Err(syn::Error::new_spanned(
            ident,
            "ErrorCatalog needs #[error_catalog(module = \"...\", code = \"...\", union = \"...\", union_doc = \"...\")]",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::derive_error_catalog;

    /// The derive output as text without spaces, or the error message.
    fn derive_text(input: syn::DeriveInput) -> Result<String, String> {
        derive_error_catalog(input)
            .map(|tokens| tokens.to_string().replace(' ', ""))
            .map_err(|error| error.to_string())
    }

    #[test]
    fn a_class_records_its_code_its_retry_its_fields_and_its_cause() {
        let text = derive_text(syn::parse_quote! {
            #[error_catalog(module = "broker_errors", code = "BrokerErrorCode", union = "BrokerCoreError", union_doc = "Every core failure.")]
            enum BrokerError {
                /// The id holds another command.
                #[error_class(class = "BrokerPayloadMismatch", code = "PayloadMismatch", never_retryable)]
                PayloadMismatch {
                    #[error_slot(python = "Str", hidden)]
                    message: String,
                },
                /// A broker operation failed.
                #[error_class(class = "BrokerOperationFailed", code_field = "code", code_subset = "BrokerOperationFailedCode", exception = "BaseException")]
                OperationFailed {
                    code: BrokerOperationFailedCode,
                    #[error_slot(python = "Str", hidden)]
                    message: String,
                    #[error_slot(python = "Bool")]
                    retryable: bool,
                    #[error_slot(skip)]
                    cause: FailureCause,
                },
            }
        })
        .expect("the derive accepts the enum");
        assert!(text.contains("python_module:\"broker_errors\""), "{text}");
        assert!(text.contains(
            "python_class:\"BrokerPayloadMismatch\",doc:\"Theidholdsanothercommand.\",slots:&[::horsies_core::error_catalog::ErrorSlot::CodeMember{member:\"PAYLOAD_MISMATCH\"},::horsies_core::error_catalog::ErrorSlot::NeverRetryable,::horsies_core::error_catalog::ErrorSlot::Field{name:\"message\",python_type:::horsies_core::error_catalog::SlotType::Str,repr:::horsies_core::error_catalog::SlotRepr::Hidden,},]"
        ), "{text}");
        assert!(text.contains(
            "ErrorSlot::CodeSubset{subset:\"BrokerOperationFailedCode\"},::horsies_core::error_catalog::ErrorSlot::Field{name:\"message\""
        ), "{text}");
        assert!(text.contains(
            "ErrorSlot::Field{name:\"retryable\",python_type:::horsies_core::error_catalog::SlotType::Bool,repr:::horsies_core::error_catalog::SlotRepr::Shown,},::horsies_core::error_catalog::ErrorSlot::Exception{python_type:::horsies_core::error_catalog::ExceptionType::Exception,},]"
        ), "{text}");
        assert!(
            text.contains("Self::PayloadMismatch{..}=>BrokerErrorCode::PayloadMismatch"),
            "{text}"
        );
        assert!(
            text.contains("Self::OperationFailed{code,..}=>::core::convert::From::from(*code)"),
            "{text}"
        );
        assert!(
            text.contains("Self::OperationFailed{..}=>\"BrokerOperationFailed\""),
            "{text}"
        );
    }

    #[test]
    fn a_field_without_its_slot_is_refused() {
        let error = derive_text(syn::parse_quote! {
            #[error_catalog(module = "m", code = "Code", union = "U", union_doc = "u")]
            enum Failure {
                #[error_class(class = "Failed", code = "Failed")]
                Failed { message: String },
            }
        })
        .expect_err("the derive refuses the enum");
        assert!(error.contains("#[error_slot"), "{error}");
    }

    #[test]
    fn a_slot_type_outside_the_closed_set_is_refused() {
        let error = derive_text(syn::parse_quote! {
            #[error_catalog(module = "m", code = "Code", union = "U", union_doc = "u")]
            enum Failure {
                #[error_class(class = "Failed", code = "Failed")]
                Failed {
                    #[error_slot(python = "dict")]
                    data: String,
                },
            }
        })
        .expect_err("the derive refuses the enum");
        assert!(error.contains("python must be one of"), "{error}");
    }

    #[test]
    fn a_code_and_a_code_field_together_are_refused() {
        let error = derive_text(syn::parse_quote! {
            #[error_catalog(module = "m", code = "Code", union = "U", union_doc = "u")]
            enum Failure {
                #[error_class(class = "Failed", code = "Failed", code_field = "code", code_subset = "Sub")]
                Failed { code: Sub },
            }
        })
        .expect_err("the derive refuses the enum");
        assert!(error.contains("code_field with code_subset"), "{error}");
    }

    #[test]
    fn a_variant_without_a_class_is_refused() {
        let error = derive_text(syn::parse_quote! {
            #[error_catalog(module = "m", code = "Code", union = "U", union_doc = "u")]
            enum Failure {
                Failed,
            }
        })
        .expect_err("the derive refuses the enum");
        assert!(error.contains("class = "), "{error}");
    }
}
