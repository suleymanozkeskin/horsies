//! `#[derive(CodeCatalog)]`: the static data of a closed code enum, read by
//! the Python code generator.
//!
//! The enum is the one source: its unit variants are the members, their
//! `///` comments are the member docs, and the enum's `///` comment is the
//! class doc. The Python member name of a variant is its name in
//! SCREAMING_SNAKE_CASE (serde's `rename_all = "SCREAMING_SNAKE_CASE"` rule).
//! Its wire value is the same text, unless the variant sets
//! `#[code_catalog(value = "...")]`.
//!
//! Container attribute: `#[code_catalog(module = "...", base = "...")]`,
//! with `base` one of `str_enum` (`class X(str, Enum)`) and `enum`
//! (`class X(Enum)`), and an optional `class = "..."` when the Python class
//! name differs from the enum name.
//!
//! A lifecycle status names its terminal members: each terminal variant
//! sets `#[code_catalog(terminal)]`, and the enum sets
//! `terminal_states = "..."`, the Python name of the frozenset constant of
//! those members. One without the other is refused. The constant holds the
//! Python member names of the terminal variants.
//!
//! A member that only a blocking call form gives (a sync call of the Python
//! client, a blocking call of the Rust client) sets
//! `#[code_catalog(blocking_form_only)]`; its `forms` is
//! `CallForms::BlockingFormOnly`. Every other member is `CallForms::AnyForm`.
//! The key marks a variant; on the enum it is refused.

use proc_macro2::TokenStream;
use quote::quote;
use syn::meta::ParseNestedMeta;
use syn::{Attribute, Data, DataEnum, DeriveInput, Expr, ExprLit, Fields, Lit, LitStr, Meta};

/// The Python base class of a generated enum.
enum PythonBase {
    StrEnum,
    PlainEnum,
}

/// The container settings of one catalog.
struct ContainerSettings {
    module: String,
    class: Option<String>,
    base: PythonBase,
    terminal_states: TerminalConstant,
}

/// The `terminal_states` key of the container.
enum TerminalConstant {
    NotNamed,
    Named(String),
}

/// Whether a variant is a terminal state of its lifecycle.
#[derive(Clone, Copy)]
enum MemberRole {
    Terminal,
    NotTerminal,
}

/// The call forms that give a member's code.
#[derive(Clone, Copy)]
enum MemberForms {
    AnyForm,
    BlockingFormOnly,
}

/// The settings of one variant.
struct VariantSettings {
    value: Option<String>,
    role: MemberRole,
    forms: MemberForms,
}

pub fn derive_code_catalog(input: DeriveInput) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "CodeCatalog can only be derived for an enum without generic parameters",
        ));
    }
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            ident,
            "CodeCatalog can only be derived for an enum of unit variants",
        ));
    };
    let settings = container_settings(&input.attrs, ident)?;
    let class = settings.class.unwrap_or_else(|| ident.to_string());
    let module = settings.module;
    let base = match settings.base {
        PythonBase::StrEnum => quote!(::horsies_core::catalog::PythonEnumBase::StrEnum),
        PythonBase::PlainEnum => quote!(::horsies_core::catalog::PythonEnumBase::PlainEnum),
    };
    let enum_doc = doc_text(&input.attrs);

    let Members {
        variants,
        names,
        values,
        docs,
        forms,
        terminal_names,
    } = read_members(data)?;
    let terminal_states = terminal_states(settings.terminal_states, &terminal_names, ident)?;

    Ok(quote! {
        impl ::horsies_core::catalog::CodeEnum for #ident {
            const CATALOG: ::horsies_core::catalog::CodeCatalog =
                ::horsies_core::catalog::CodeCatalog {
                    python_module: #module,
                    python_class: #class,
                    python_base: #base,
                    doc: #enum_doc,
                    members: &[
                        #(::horsies_core::catalog::CatalogMember {
                            name: #names,
                            value: #values,
                            doc: #docs,
                            forms: #forms,
                        },)*
                    ],
                    terminal_states: #terminal_states,
                };

            const VARIANTS: &'static [Self] = &[#(Self::#variants,)*];

            fn wire_value(&self) -> &'static str {
                match self {
                    #(Self::#variants => #values,)*
                }
            }
        }
    })
}

/// The members of an enum, in declaration order.
struct Members<'a> {
    variants: Vec<&'a syn::Ident>,
    /// The Python member names.
    names: Vec<String>,
    /// The wire values.
    values: Vec<String>,
    docs: Vec<String>,
    /// The `CallForms` value of each member.
    forms: Vec<TokenStream>,
    /// The Python member names of the `#[code_catalog(terminal)]` variants.
    terminal_names: Vec<String>,
}

fn read_members(data: &DataEnum) -> syn::Result<Members<'_>> {
    let mut members = Members {
        variants: Vec::with_capacity(data.variants.len()),
        names: Vec::with_capacity(data.variants.len()),
        values: Vec::with_capacity(data.variants.len()),
        docs: Vec::with_capacity(data.variants.len()),
        forms: Vec::with_capacity(data.variants.len()),
        terminal_names: Vec::new(),
    };
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "CodeCatalog members must be unit variants",
            ));
        }
        let variant_settings = variant_settings(&variant.attrs)?;
        let name = screaming_snake_case(&variant.ident.to_string());
        let value = match variant_settings.value {
            Some(value) => value,
            None => name.clone(),
        };
        match variant_settings.role {
            MemberRole::Terminal => members.terminal_names.push(name.clone()),
            MemberRole::NotTerminal => {}
        }
        members.variants.push(&variant.ident);
        members.names.push(name);
        members.values.push(value);
        members.docs.push(doc_text(&variant.attrs));
        members.forms.push(match variant_settings.forms {
            MemberForms::AnyForm => quote!(::horsies_core::catalog::CallForms::AnyForm),
            MemberForms::BlockingFormOnly => {
                quote!(::horsies_core::catalog::CallForms::BlockingFormOnly)
            }
        });
    }
    Ok(members)
}

/// The `terminal_states` field of the catalog. The constant and the terminal
/// variants come together or not at all.
fn terminal_states(
    constant: TerminalConstant,
    terminal_names: &[String],
    ident: &syn::Ident,
) -> syn::Result<TokenStream> {
    match (constant, terminal_names.is_empty()) {
        (TerminalConstant::NotNamed, true) => {
            Ok(quote!(::horsies_core::catalog::TerminalStates::NotALifecycle))
        }
        (TerminalConstant::Named(constant), false) => Ok(quote!(
            ::horsies_core::catalog::TerminalStates::Lifecycle {
                constant: #constant,
                members: &[#(#terminal_names,)*],
            }
        )),
        (TerminalConstant::Named(_), true) => Err(syn::Error::new_spanned(
            ident,
            "terminal_states names a constant, but no variant has #[code_catalog(terminal)]",
        )),
        (TerminalConstant::NotNamed, false) => Err(syn::Error::new_spanned(
            ident,
            "a variant has #[code_catalog(terminal)]; the enum needs #[code_catalog(terminal_states = \"...\")]",
        )),
    }
}

fn container_settings(attrs: &[Attribute], ident: &syn::Ident) -> syn::Result<ContainerSettings> {
    let mut module: Option<String> = None;
    let mut class: Option<String> = None;
    let mut base: Option<PythonBase> = None;
    let mut terminal_states = TerminalConstant::NotNamed;
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("code_catalog"))
    {
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            match key.as_str() {
                "module" => module = Some(text_value(&meta)?.value()),
                "class" => class = Some(text_value(&meta)?.value()),
                "terminal_states" => {
                    terminal_states = TerminalConstant::Named(text_value(&meta)?.value());
                }
                "base" => {
                    let value = text_value(&meta)?;
                    base = Some(match value.value().as_str() {
                        "str_enum" => PythonBase::StrEnum,
                        "enum" => PythonBase::PlainEnum,
                        _ => {
                            return Err(syn::Error::new_spanned(
                                &value,
                                "base must be \"str_enum\" or \"enum\"",
                            ))
                        }
                    });
                }
                "value" | "terminal" | "blocking_form_only" => {
                    return Err(meta.error(format!(
                        "{key} is a code_catalog key of a variant, not of the enum"
                    )))
                }
                _ => {
                    return Err(meta.error(
                        "unknown code_catalog key; the keys are module, class, base and terminal_states",
                    ))
                }
            }
            Ok(())
        })?;
    }
    match (module, base) {
        (Some(module), Some(base)) => Ok(ContainerSettings {
            module,
            class,
            base,
            terminal_states,
        }),
        (None, _) | (_, None) => Err(syn::Error::new_spanned(
            ident,
            "CodeCatalog needs #[code_catalog(module = \"...\", base = \"...\")]",
        )),
    }
}

/// The text literal after `key =` of a `#[code_catalog(...)]` key.
fn text_value(meta: &ParseNestedMeta<'_>) -> syn::Result<LitStr> {
    meta.value()?.parse()
}

/// The settings of a variant's `#[code_catalog(...)]`: `value = "..."`, when
/// set, the `terminal` flag and the `blocking_form_only` flag.
fn variant_settings(attrs: &[Attribute]) -> syn::Result<VariantSettings> {
    let mut value: Option<String> = None;
    let mut role = MemberRole::NotTerminal;
    let mut forms = MemberForms::AnyForm;
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("code_catalog"))
    {
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            match key.as_str() {
                "value" => {
                    value = Some(text_value(&meta)?.value());
                    Ok(())
                }
                "terminal" => {
                    role = MemberRole::Terminal;
                    Ok(())
                }
                "blocking_form_only" => {
                    forms = MemberForms::BlockingFormOnly;
                    Ok(())
                }
                _ => Err(meta.error(
                    "the code_catalog keys of a variant are value, terminal and blocking_form_only",
                )),
            }
        })?;
    }
    Ok(VariantSettings { value, role, forms })
}

/// The `///` lines of an item, one leading space removed, joined by newlines.
pub(crate) fn doc_text(attrs: &[Attribute]) -> String {
    let lines: Vec<String> = attrs
        .iter()
        .filter(|attr| attr.path().is_ident("doc"))
        .filter_map(|attr| match &attr.meta {
            Meta::NameValue(name_value) => match &name_value.value {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(text),
                    ..
                }) => {
                    let line = text.value();
                    Some(line.strip_prefix(' ').unwrap_or(&line).to_owned())
                }
                _ => None,
            },
            Meta::Path(_) | Meta::List(_) => None,
        })
        .collect();
    lines.join("\n")
}

/// serde's SCREAMING_SNAKE_CASE rule for a variant name: an underscore
/// before each uppercase letter but the first, then all uppercase.
pub(crate) fn screaming_snake_case(variant: &str) -> String {
    let mut out = String::with_capacity(variant.len() + 8);
    for (position, character) in variant.char_indices() {
        if position > 0 && character.is_uppercase() {
            out.push('_');
        }
        out.push(character.to_ascii_uppercase());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{derive_code_catalog, screaming_snake_case};

    /// The derive output as text without spaces, or the error message.
    fn derive_text(input: syn::DeriveInput) -> Result<String, String> {
        derive_code_catalog(input)
            .map(|tokens| tokens.to_string().replace(' ', ""))
            .map_err(|error| error.to_string())
    }

    #[test]
    fn terminal_variants_and_their_constant_give_a_lifecycle() {
        let text = derive_text(syn::parse_quote! {
            #[code_catalog(module = "status", base = "enum", terminal_states = "RUN_TERMINAL_STATES")]
            enum RunStatus {
                Pending,
                #[code_catalog(terminal)]
                Completed,
                #[code_catalog(terminal, value = "GAVE_UP")]
                Failed,
            }
        })
        .expect("the derive accepts the enum");
        assert!(text.contains(
            "terminal_states:::horsies_core::catalog::TerminalStates::Lifecycle{constant:\"RUN_TERMINAL_STATES\",members:&[\"COMPLETED\",\"FAILED\",],}"
        ));
    }

    #[test]
    fn a_member_is_named_after_its_variant_and_a_value_key_sets_only_its_value() {
        let text = derive_text(syn::parse_quote! {
            #[code_catalog(module = "error_codes", base = "str_enum")]
            enum StartCode {
                /// The workflow has no name.
                #[code_catalog(value = "HRS-001")]
                WorkflowNoName,
                QueueFull,
            }
        })
        .expect("the derive accepts the enum");
        assert!(text.contains(
            "::horsies_core::catalog::CatalogMember{name:\"WORKFLOW_NO_NAME\",value:\"HRS-001\",doc:\"Theworkflowhasnoname.\",forms:::horsies_core::catalog::CallForms::AnyForm,}"
        ));
        assert!(text.contains(
            "::horsies_core::catalog::CatalogMember{name:\"QUEUE_FULL\",value:\"QUEUE_FULL\",doc:\"\",forms:::horsies_core::catalog::CallForms::AnyForm,}"
        ));
        assert!(text.contains("Self::WorkflowNoName=>\"HRS-001\""));
    }

    #[test]
    fn an_enum_without_terminal_variants_is_not_a_lifecycle() {
        let text = derive_text(syn::parse_quote! {
            #[code_catalog(module = "broker", base = "str_enum")]
            enum BrokerCode {
                EnqueueFailed,
            }
        })
        .expect("the derive accepts the enum");
        assert!(
            text.contains("terminal_states:::horsies_core::catalog::TerminalStates::NotALifecycle")
        );
    }

    #[test]
    fn a_terminal_variant_without_the_constant_is_refused() {
        let error = derive_text(syn::parse_quote! {
            #[code_catalog(module = "status", base = "enum")]
            enum RunStatus {
                Pending,
                #[code_catalog(terminal)]
                Completed,
            }
        })
        .expect_err("the derive refuses the enum");
        assert!(error.contains("terminal_states"), "{error}");
    }

    #[test]
    fn a_constant_without_terminal_variants_is_refused() {
        let error = derive_text(syn::parse_quote! {
            #[code_catalog(module = "status", base = "enum", terminal_states = "RUN_TERMINAL_STATES")]
            enum RunStatus {
                Pending,
            }
        })
        .expect_err("the derive refuses the enum");
        assert!(error.contains("no variant has"), "{error}");
    }

    #[test]
    fn an_unknown_variant_key_is_refused() {
        let error = derive_text(syn::parse_quote! {
            #[code_catalog(module = "status", base = "enum")]
            enum RunStatus {
                #[code_catalog(closing)]
                Pending,
            }
        })
        .expect_err("the derive refuses the enum");
        assert!(
            error.contains("value, terminal and blocking_form_only"),
            "{error}"
        );
    }

    #[test]
    fn a_blocking_form_only_variant_is_marked_and_every_other_member_is_any_form() {
        let text = derive_text(syn::parse_quote! {
            #[code_catalog(module = "task_send", base = "str_enum")]
            enum SendCode {
                #[code_catalog(blocking_form_only)]
                AsyncContext,
                EnqueueFailed,
            }
        })
        .expect("the derive accepts the enum");
        assert!(text.contains(
            "::horsies_core::catalog::CatalogMember{name:\"ASYNC_CONTEXT\",value:\"ASYNC_CONTEXT\",doc:\"\",forms:::horsies_core::catalog::CallForms::BlockingFormOnly,}"
        ));
        assert!(text.contains(
            "::horsies_core::catalog::CatalogMember{name:\"ENQUEUE_FAILED\",value:\"ENQUEUE_FAILED\",doc:\"\",forms:::horsies_core::catalog::CallForms::AnyForm,}"
        ));
    }

    #[test]
    fn blocking_form_only_on_the_enum_is_refused() {
        let error = derive_text(syn::parse_quote! {
            #[code_catalog(module = "task_send", base = "str_enum", blocking_form_only)]
            enum SendCode {
                AsyncContext,
            }
        })
        .expect_err("the derive refuses the enum");
        assert!(
            error
                .contains("blocking_form_only is a code_catalog key of a variant, not of the enum"),
            "{error}"
        );
    }

    #[test]
    fn variant_names_follow_the_serde_rule() {
        assert_eq!(screaming_snake_case("DbPingFailed"), "DB_PING_FAILED");
        assert_eq!(screaming_snake_case("Pending"), "PENDING");
        assert_eq!(
            screaming_snake_case("WorkflowSuccessCaseNotMet"),
            "WORKFLOW_SUCCESS_CASE_NOT_MET"
        );
        assert_eq!(screaming_snake_case("Phase2Failed"), "PHASE2_FAILED");
    }
}
