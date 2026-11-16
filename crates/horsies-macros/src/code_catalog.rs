//! `#[derive(CodeCatalog)]`: the static data of a closed code enum, read by
//! the Python code generator.
//!
//! The enum is the one source: its unit variants are the members, their
//! `///` comments are the member docs, and the enum's `///` comment is the
//! class doc. The wire value of a variant is its name in
//! SCREAMING_SNAKE_CASE (serde's `rename_all = "SCREAMING_SNAKE_CASE"` rule),
//! unless the variant sets `#[code_catalog(value = "...")]`.
//!
//! Container attribute: `#[code_catalog(module = "...", base = "...")]`,
//! with `base` one of `str_enum` (`class X(str, Enum)`) and `enum`
//! (`class X(Enum)`), and an optional `class = "..."` when the Python class
//! name differs from the enum name.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Expr, ExprLit, Fields, Lit, LitStr, Meta};

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

    let mut variants = Vec::with_capacity(data.variants.len());
    let mut values = Vec::with_capacity(data.variants.len());
    let mut docs = Vec::with_capacity(data.variants.len());
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "CodeCatalog members must be unit variants",
            ));
        }
        let value = match variant_value(&variant.attrs)? {
            Some(value) => value,
            None => screaming_snake_case(&variant.ident.to_string()),
        };
        variants.push(&variant.ident);
        values.push(value);
        docs.push(doc_text(&variant.attrs));
    }

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
                            value: #values,
                            doc: #docs,
                        },)*
                    ],
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

fn container_settings(attrs: &[Attribute], ident: &syn::Ident) -> syn::Result<ContainerSettings> {
    let mut module: Option<String> = None;
    let mut class: Option<String> = None;
    let mut base: Option<PythonBase> = None;
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
            let value: LitStr = meta.value()?.parse()?;
            match key.as_str() {
                "module" => module = Some(value.value()),
                "class" => class = Some(value.value()),
                "base" => {
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
                _ => {
                    return Err(
                        meta.error("unknown code_catalog key; the keys are module, class and base")
                    )
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
        }),
        (None, _) | (_, None) => Err(syn::Error::new_spanned(
            ident,
            "CodeCatalog needs #[code_catalog(module = \"...\", base = \"...\")]",
        )),
    }
}

/// The `value` of a variant's `#[code_catalog(value = "...")]`, if set.
fn variant_value(attrs: &[Attribute]) -> syn::Result<Option<String>> {
    let mut value: Option<String> = None;
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("code_catalog"))
    {
        attr.parse_nested_meta(|meta| match meta.path.is_ident("value") {
            true => {
                let literal: LitStr = meta.value()?.parse()?;
                value = Some(literal.value());
                Ok(())
            }
            false => Err(meta.error("the only code_catalog key of a variant is value")),
        })?;
    }
    Ok(value)
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
    use super::screaming_snake_case;

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
