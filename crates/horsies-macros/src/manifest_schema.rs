//! `#[derive(ManifestSchema)]`: the Python form of a manifest type, read
//! from the type and its serde attributes.
//!
//! - A struct of named fields becomes a `Record`: one field per Rust field,
//!   its Python type from `ManifestValue` of the field type.
//! - An enum with `#[serde(tag = "...")]` becomes `Tagged`: one dataclass per
//!   variant, named `<Enum><Variant>`, whose tag field holds the variant's
//!   wire name. Variants are unit variants or have named fields.
//! - An enum without a tag becomes `Choice`; every variant must be a unit
//!   variant.
//!
//! The wire name of a variant follows serde: its Rust name, or its name in
//! SCREAMING_SNAKE_CASE under `rename_all = "SCREAMING_SNAKE_CASE"`. Serde
//! attributes that change the wire form in other ways (`rename`,
//! `untagged`, `content`, `transparent`, `try_from`, `from`, `into`, and
//! any serde attribute on a field or a variant) are refused: such a type
//! implements `ManifestValue` by hand.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, Data, DataEnum, DeriveInput, Fields, FieldsNamed, LitStr};

use crate::code_catalog::{doc_text, screaming_snake_case};

/// How serde names the variants of an enum.
enum VariantNames {
    AsWritten,
    ScreamingSnakeCase,
}

/// The serde settings of a container that the derive reads.
struct SerdeSettings {
    tag: Option<String>,
    variant_names: VariantNames,
}

pub fn derive_manifest_schema(input: DeriveInput) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "ManifestSchema can only be derived for a type without generic parameters",
        ));
    }
    let python_name = ident.to_string();
    let doc = doc_text(&input.attrs);
    let settings = serde_settings(&input.attrs)?;
    let shape = match &input.data {
        Data::Struct(data) => record_shape(ident, &data.fields, settings.tag.as_ref())?,
        Data::Enum(data) => match &settings.tag {
            Some(tag) => tagged_shape(&python_name, tag, &settings.variant_names, data)?,
            None => choice_shape(&settings.variant_names, data)?,
        },
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                ident,
                "ManifestSchema can only be derived for a struct or an enum",
            ))
        }
    };

    Ok(quote! {
        impl ::horsies_core::manifest_schema::ManifestValue for #ident {
            const PYTHON: ::horsies_core::manifest_schema::PythonValue =
                ::horsies_core::manifest_schema::PythonValue::Named(#python_name);
        }

        impl ::horsies_core::manifest_schema::ManifestItem for #ident {
            const ITEM: ::horsies_core::manifest_schema::ManifestType =
                ::horsies_core::manifest_schema::ManifestType {
                    python_name: #python_name,
                    doc: #doc,
                    shape: #shape,
                };
        }
    })
}

fn record_shape(
    ident: &syn::Ident,
    fields: &Fields,
    tag: Option<&String>,
) -> syn::Result<TokenStream> {
    let named = match (fields, tag) {
        (Fields::Named(named), None) => named,
        (Fields::Named(_), Some(_)) => {
            return Err(syn::Error::new_spanned(
                ident,
                "a serde tag on a struct is not a manifest form",
            ))
        }
        (Fields::Unnamed(_) | Fields::Unit, _) => {
            return Err(syn::Error::new_spanned(
                ident,
                "ManifestSchema needs named fields; a newtype implements ManifestValue by hand",
            ))
        }
    };
    let fields = field_list(named)?;
    Ok(quote! {
        ::horsies_core::manifest_schema::ManifestShape::Record { fields: #fields }
    })
}

fn tagged_shape(
    enum_name: &str,
    tag: &str,
    names: &VariantNames,
    data: &DataEnum,
) -> syn::Result<TokenStream> {
    let mut variants = Vec::with_capacity(data.variants.len());
    for variant in &data.variants {
        refuse_serde_attributes(&variant.attrs)?;
        let fields = match &variant.fields {
            Fields::Unit => quote!(&[]),
            Fields::Named(fields) => field_list(fields)?,
            Fields::Unnamed(_) => {
                return Err(syn::Error::new_spanned(
                    variant,
                    "a variant of a tagged manifest enum is a unit variant or has named fields",
                ))
            }
        };
        let rust_name = variant.ident.to_string();
        let python_class = format!("{enum_name}{rust_name}");
        let tag_value = wire_name(&rust_name, names);
        let doc = doc_text(&variant.attrs);
        variants.push(quote! {
            ::horsies_core::manifest_schema::TaggedVariant {
                python_class: #python_class,
                tag_value: #tag_value,
                doc: #doc,
                fields: #fields,
            }
        });
    }
    Ok(quote! {
        ::horsies_core::manifest_schema::ManifestShape::Tagged {
            tag_field: #tag,
            variants: &[#(#variants,)*],
        }
    })
}

fn choice_shape(names: &VariantNames, data: &DataEnum) -> syn::Result<TokenStream> {
    let mut values = Vec::with_capacity(data.variants.len());
    for variant in &data.variants {
        refuse_serde_attributes(&variant.attrs)?;
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "an enum without a serde tag must have unit variants only",
            ));
        }
        let value = wire_name(&variant.ident.to_string(), names);
        let doc = doc_text(&variant.attrs);
        values.push(quote! {
            ::horsies_core::manifest_schema::ChoiceValue { value: #value, doc: #doc }
        });
    }
    Ok(quote! {
        ::horsies_core::manifest_schema::ManifestShape::Choice { values: &[#(#values,)*] }
    })
}

/// `&[ManifestField { .. }, ..]` of named fields.
fn field_list(fields: &FieldsNamed) -> syn::Result<TokenStream> {
    let mut items = Vec::with_capacity(fields.named.len());
    for field in &fields.named {
        refuse_serde_attributes(&field.attrs)?;
        let Some(field_ident) = &field.ident else {
            return Err(syn::Error::new_spanned(field, "a named field has a name"));
        };
        let name = field_ident.to_string();
        let doc = doc_text(&field.attrs);
        let field_type = &field.ty;
        items.push(quote! {
            ::horsies_core::manifest_schema::ManifestField {
                name: #name,
                doc: #doc,
                value: <#field_type as ::horsies_core::manifest_schema::ManifestValue>::PYTHON,
            }
        });
    }
    Ok(quote!(&[#(#items,)*]))
}

fn wire_name(rust_name: &str, names: &VariantNames) -> String {
    match names {
        VariantNames::AsWritten => rust_name.to_owned(),
        VariantNames::ScreamingSnakeCase => screaming_snake_case(rust_name),
    }
}

/// Read `tag` and `rename_all` of the container's serde attributes; refuse
/// the attributes that change the wire form in another way.
fn serde_settings(attrs: &[Attribute]) -> syn::Result<SerdeSettings> {
    let mut tag: Option<String> = None;
    let mut variant_names = VariantNames::AsWritten;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            match key.as_str() {
                "tag" => {
                    let value: LitStr = meta.value()?.parse()?;
                    tag = Some(value.value());
                    Ok(())
                }
                "rename_all" => {
                    let value: LitStr = meta.value()?.parse()?;
                    variant_names = parse_variant_names(&value)?;
                    Ok(())
                }
                "deny_unknown_fields" => Ok(()),
                _ => Err(meta.error(
                    "ManifestSchema reads only the serde keys tag, rename_all and \
                     deny_unknown_fields; implement ManifestValue by hand",
                )),
            }
        })?;
    }
    Ok(SerdeSettings { tag, variant_names })
}

fn parse_variant_names(value: &LitStr) -> syn::Result<VariantNames> {
    match value.value().as_str() {
        "SCREAMING_SNAKE_CASE" => Ok(VariantNames::ScreamingSnakeCase),
        _ => Err(syn::Error::new_spanned(
            value,
            "ManifestSchema reads only rename_all = \"SCREAMING_SNAKE_CASE\"",
        )),
    }
}

/// A serde attribute on a field or a variant changes its wire form, which
/// the derive does not model.
fn refuse_serde_attributes(attrs: &[Attribute]) -> syn::Result<()> {
    match attrs.iter().find(|attr| attr.path().is_ident("serde")) {
        Some(attr) => Err(syn::Error::new_spanned(
            attr,
            "ManifestSchema does not read serde attributes of a field or a variant",
        )),
        None => Ok(()),
    }
}
