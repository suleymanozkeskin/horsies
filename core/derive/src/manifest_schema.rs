//! `#[derive(ManifestSchema)]`: the form of a manifest type, read from the
//! type, its serde attributes and its `#[manifest_schema(...)]` attributes.
//!
//! - A struct of named fields becomes a `Record`: one field per Rust field.
//! - An enum with `#[serde(tag = "...")]` becomes `Tagged`: one dataclass per
//!   variant, named `<Enum><Variant>`, whose tag field holds the variant's
//!   wire name. A variant is a unit variant, has named fields, or is a
//!   newtype variant whose one field is a record type: its members after the
//!   tag are that record's fields (`VariantFields::OfRecord`).
//! - An enum without a tag becomes `Choice`; every variant must be a unit
//!   variant.
//!
//! A field's value is `ManifestValue::VALUE_TYPE` of its Rust type. An integer
//! field, or a length of time in whole seconds, whose bound is a rule of the
//! type that holds it names that bound,
//! `#[manifest_schema(int_bound = <IntBound constant>)]`; its value is then
//! `bounded_int::<FieldType>(bound)`.
//!
//! A generic type has one type parameter and lists its uses,
//! `#[manifest_schema(uses(Weekday, Month))]`. The derive records the type
//! once per use, named `<Type><Use>` (`CronEnumTermWeekday`), with the type
//! parameter read as the use.
//!
//! The wire name of a variant follows serde: its Rust name, or its name in
//! `snake_case` or `SCREAMING_SNAKE_CASE` under `rename_all`. Serde
//! attributes that change the wire form in other ways (`rename`,
//! `untagged`, `content`, `transparent`, `try_from`, `from`, `into`, and
//! any other serde attribute on a field or a variant) are refused: such a
//! type implements `ManifestValue` by hand.
//!
//! The serde attributes of a field that the derive reads:
//! - `skip_serializing_if = "Option::is_none"`: it changes only Rust's own
//!   serialization. The reader takes a missing member and `null` alike.
//! - `default = "<const fn>"`: the reader's value for a missing member,
//!   recorded with `MemberDefault::int`. The function is a `const fn`, so
//!   the build knows the value. A bare `default` is refused.
//!
//! One serde attribute of a variant is read: `deserialize_with` with a
//! function named `no_member_after_the_tag`, on a unit variant of a tagged
//! enum. serde skips the members after the tag of such a variant, also
//! under `deny_unknown_fields`; the function refuses them. The wire form
//! stays the tag alone.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Attribute, Data, DataEnum, DeriveInput, Fields, FieldsNamed, FieldsUnnamed, GenericParam,
    Generics, LitStr, Path, PathArguments,
};

use crate::code_catalog::{doc_text, screaming_snake_case};

/// How serde names the variants of an enum.
enum VariantNames {
    AsWritten,
    SnakeCase,
    ScreamingSnakeCase,
}

/// The serde settings of a container that the derive reads.
struct SerdeSettings {
    tag: Option<String>,
    variant_names: VariantNames,
}

/// The type parameter of the derived type.
enum TypeParameter {
    NoParameter,
    One(syn::Ident),
}

/// Where the value of a field comes from.
enum FieldBound {
    /// `ManifestValue::VALUE_TYPE` of the field type.
    FromType,
    /// `bounded_int::<FieldType>(bound)`: the field names its bound.
    Named(Path),
}

/// The serde default of a field.
enum FieldDefault {
    NoDefault,
    /// `default = "<const fn>"`.
    Function(Path),
}

/// The one function that `skip_serializing_if` may name.
const OPTION_IS_NONE: &str = "Option::is_none";

pub fn derive_manifest_schema(input: DeriveInput) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let doc = doc_text(&input.attrs);
    let settings = serde_settings(&input.attrs)?;
    let uses = generic_uses(&input.attrs)?;
    match (type_parameter(&input.generics)?, uses.as_slice()) {
        (TypeParameter::NoParameter, []) => {
            let type_name = ident.to_string();
            let shape = shape(&input, &type_name, &settings)?;
            Ok(schema_impls(&quote!(#ident), &type_name, &doc, &shape))
        }
        (TypeParameter::NoParameter, [_, ..]) => Err(syn::Error::new_spanned(
            ident,
            "uses(...) lists the uses of a generic type; this type has no type parameter",
        )),
        (TypeParameter::One(_), []) => Err(syn::Error::new_spanned(
            &input.generics,
            "a generic manifest type lists its uses: #[manifest_schema(uses(...))]",
        )),
        (TypeParameter::One(parameter), [_, ..]) => {
            let mut impls = TokenStream::new();
            for used in &uses {
                let type_name = format!("{ident}{}", use_name(used)?);
                let shape = shape(&input, &type_name, &settings)?;
                let schema = schema_impls(&quote!(#ident<#parameter>), &type_name, &doc, &shape);
                impls.extend(quote! {
                    const _: () = {
                        type #parameter = #used;
                        #schema
                    };
                });
            }
            Ok(impls)
        }
    }
}

/// The `ManifestValue` and `ManifestItem` impls of `self_type`.
fn schema_impls(
    self_type: &TokenStream,
    type_name: &str,
    doc: &str,
    shape: &TokenStream,
) -> TokenStream {
    quote! {
        impl ::horsies_core::manifest_schema::ManifestValue for #self_type {
            const VALUE_TYPE: ::horsies_core::manifest_schema::ValueType =
                ::horsies_core::manifest_schema::ValueType::Named(#type_name);
        }

        impl ::horsies_core::manifest_schema::ManifestItem for #self_type {
            const ITEM: ::horsies_core::manifest_schema::ManifestType =
                ::horsies_core::manifest_schema::ManifestType {
                    type_name: #type_name,
                    doc: #doc,
                    shape: #shape,
                };
        }
    }
}

/// The `ManifestShape` of the derived type, whose name is `type_name`.
fn shape(
    input: &DeriveInput,
    type_name: &str,
    settings: &SerdeSettings,
) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    match &input.data {
        Data::Struct(data) => record_shape(ident, &data.fields, settings.tag.as_ref()),
        Data::Enum(data) => match &settings.tag {
            Some(tag) => tagged_shape(type_name, tag, &settings.variant_names, data),
            None => choice_shape(&settings.variant_names, data),
        },
        Data::Union(_) => Err(syn::Error::new_spanned(
            ident,
            "ManifestSchema can only be derived for a struct or an enum",
        )),
    }
}

/// The type parameter of `generics`: none, or one type parameter. A
/// lifetime, a const parameter or a second parameter is refused.
fn type_parameter(generics: &Generics) -> syn::Result<TypeParameter> {
    let mut parameters = generics.params.iter();
    match (parameters.next(), parameters.next()) {
        (None, _) => Ok(TypeParameter::NoParameter),
        (Some(GenericParam::Type(parameter)), None) => {
            Ok(TypeParameter::One(parameter.ident.clone()))
        }
        (Some(_), _) => Err(syn::Error::new_spanned(
            generics,
            "ManifestSchema reads one type parameter at most, and no lifetime or const parameter",
        )),
    }
}

/// The name of a use in the name of the recorded type: the last segment of
/// its path, which takes no arguments.
fn use_name(used: &Path) -> syn::Result<String> {
    match used.segments.last() {
        Some(segment) if matches!(segment.arguments, PathArguments::None) => {
            Ok(segment.ident.to_string())
        }
        Some(_) | None => Err(syn::Error::new_spanned(
            used,
            "a use of a generic manifest type is a type name without arguments",
        )),
    }
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
        refuse_schema_attributes(&variant.attrs, "a variant")?;
        let fields = match &variant.fields {
            Fields::Unit => {
                check_unit_variant_attributes(&variant.attrs)?;
                quote!(::horsies_core::manifest_schema::VariantFields::Own(&[]))
            }
            Fields::Named(fields) => {
                refuse_serde_attributes(&variant.attrs, "a variant")?;
                let list = field_list(fields)?;
                quote!(::horsies_core::manifest_schema::VariantFields::Own(#list))
            }
            Fields::Unnamed(fields) => {
                refuse_serde_attributes(&variant.attrs, "a variant")?;
                let record = newtype_record(variant, fields)?;
                quote!(::horsies_core::manifest_schema::VariantFields::OfRecord(
                    ::horsies_core::manifest_schema::record_name(
                        <#record as ::horsies_core::manifest_schema::ManifestItem>::ITEM
                    )
                ))
            }
        };
        let rust_name = variant.ident.to_string();
        let type_name = format!("{enum_name}{rust_name}");
        let tag_value = wire_name(&rust_name, names);
        let doc = doc_text(&variant.attrs);
        variants.push(quote! {
            ::horsies_core::manifest_schema::TaggedVariant {
                type_name: #type_name,
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

/// The type of the one field of a newtype variant: a record type, whose
/// fields are the members of the variant after its tag.
fn newtype_record<'a>(
    variant: &syn::Variant,
    fields: &'a FieldsUnnamed,
) -> syn::Result<&'a syn::Type> {
    let mut unnamed = fields.unnamed.iter();
    match (unnamed.next(), unnamed.next()) {
        (Some(only), None) => {
            refuse_serde_attributes(&only.attrs, "the field of a newtype variant")?;
            refuse_schema_attributes(&only.attrs, "the field of a newtype variant")?;
            Ok(&only.ty)
        }
        (None, _) | (Some(_), Some(_)) => Err(syn::Error::new_spanned(
            variant,
            "a tuple variant of a tagged manifest enum has one field, a record type",
        )),
    }
}

fn choice_shape(names: &VariantNames, data: &DataEnum) -> syn::Result<TokenStream> {
    let mut values = Vec::with_capacity(data.variants.len());
    for variant in &data.variants {
        refuse_serde_attributes(&variant.attrs, "a variant")?;
        refuse_schema_attributes(&variant.attrs, "a variant")?;
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
        let Some(field_ident) = &field.ident else {
            return Err(syn::Error::new_spanned(field, "a named field has a name"));
        };
        let name = field_ident.to_string();
        let doc = doc_text(&field.attrs);
        let field_type = &field.ty;
        let value = match field_bound(&field.attrs)? {
            FieldBound::FromType => {
                quote!(<#field_type as ::horsies_core::manifest_schema::ManifestValue>::VALUE_TYPE)
            }
            FieldBound::Named(bound) => {
                quote!(::horsies_core::manifest_schema::bounded_int::<#field_type>(#bound))
            }
        };
        let default = match field_default(&field.attrs)? {
            FieldDefault::NoDefault => {
                quote!(::horsies_core::manifest_schema::MemberDefault::NoDefault)
            }
            FieldDefault::Function(function) => quote!(
                ::horsies_core::manifest_schema::MemberDefault::int(#value, #function() as u64)
            ),
        };
        items.push(quote! {
            ::horsies_core::manifest_schema::ManifestField {
                name: #name,
                doc: #doc,
                value: #value,
                default: #default,
            }
        });
    }
    Ok(quote!(&[#(#items,)*]))
}

fn wire_name(rust_name: &str, names: &VariantNames) -> String {
    match names {
        VariantNames::AsWritten => rust_name.to_owned(),
        VariantNames::SnakeCase => snake_case(rust_name),
        VariantNames::ScreamingSnakeCase => screaming_snake_case(rust_name),
    }
}

/// serde's snake_case rule for a variant name: an underscore before each
/// uppercase letter but the first, then all lowercase.
fn snake_case(variant: &str) -> String {
    let mut out = String::with_capacity(variant.len() + 8);
    for (position, character) in variant.char_indices() {
        if position > 0 && character.is_uppercase() {
            out.push('_');
        }
        out.push(character.to_ascii_lowercase());
    }
    out
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
        "snake_case" => Ok(VariantNames::SnakeCase),
        "SCREAMING_SNAKE_CASE" => Ok(VariantNames::ScreamingSnakeCase),
        _ => Err(syn::Error::new_spanned(
            value,
            "ManifestSchema reads only rename_all = \"snake_case\" or \"SCREAMING_SNAKE_CASE\"",
        )),
    }
}

/// The uses of a generic type: `#[manifest_schema(uses(A, B))]` on the
/// type. None for a type without the attribute.
fn generic_uses(attrs: &[Attribute]) -> syn::Result<Vec<Path>> {
    let mut uses = Vec::new();
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("manifest_schema"))
    {
        attr.parse_nested_meta(|meta| match meta.path.is_ident("uses") {
            true => meta.parse_nested_meta(|used| {
                uses.push(used.path.clone());
                Ok(())
            }),
            false => Err(meta.error("the manifest_schema key of a type is uses(...)")),
        })?;
    }
    Ok(uses)
}

/// The bound of a field: `#[manifest_schema(int_bound = <IntBound
/// constant>)]`, or the form of the field type.
fn field_bound(attrs: &[Attribute]) -> syn::Result<FieldBound> {
    let mut bound = FieldBound::FromType;
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("manifest_schema"))
    {
        attr.parse_nested_meta(|meta| match meta.path.is_ident("int_bound") {
            true => {
                bound = FieldBound::Named(meta.value()?.parse()?);
                Ok(())
            }
            false => Err(meta.error("the manifest_schema key of a field is int_bound")),
        })?;
    }
    Ok(bound)
}

/// The serde default of a field. A field's serde attributes are
/// `skip_serializing_if = "Option::is_none"` and `default = "<const fn>"`;
/// any other is refused.
fn field_default(attrs: &[Attribute]) -> syn::Result<FieldDefault> {
    let mut default = FieldDefault::NoDefault;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            match key.as_str() {
                "skip_serializing_if" => {
                    let function: LitStr = meta.value()?.parse()?;
                    match function.value().as_str() {
                        OPTION_IS_NONE => Ok(()),
                        _ => Err(syn::Error::new_spanned(
                            &function,
                            "ManifestSchema reads skip_serializing_if only with \"Option::is_none\"",
                        )),
                    }
                }
                "default" => match meta.input.peek(syn::Token![=]) {
                    true => {
                        let function: LitStr = meta.value()?.parse()?;
                        default = FieldDefault::Function(function.parse()?);
                        Ok(())
                    }
                    false => Err(meta.error(
                        "ManifestSchema reads a field default only as default = \"<const fn>\"",
                    )),
                },
                _ => Err(meta.error(
                    "ManifestSchema reads only the serde keys skip_serializing_if = \
                     \"Option::is_none\" and default = \"<const fn>\" of a field",
                )),
            }
        })?;
    }
    Ok(default)
}

/// A serde attribute on a variant changes its wire form, which the derive
/// does not model.
fn refuse_serde_attributes(attrs: &[Attribute], place: &str) -> syn::Result<()> {
    match attrs.iter().find(|attr| attr.path().is_ident("serde")) {
        Some(attr) => Err(syn::Error::new_spanned(
            attr,
            format!("ManifestSchema does not read serde attributes of {place}"),
        )),
        None => Ok(()),
    }
}

/// The derive reads `#[manifest_schema(...)]` on a type and on a named
/// field only.
fn refuse_schema_attributes(attrs: &[Attribute], place: &str) -> syn::Result<()> {
    match attrs
        .iter()
        .find(|attr| attr.path().is_ident("manifest_schema"))
    {
        Some(attr) => Err(syn::Error::new_spanned(
            attr,
            format!("ManifestSchema does not read manifest_schema attributes of {place}"),
        )),
        None => Ok(()),
    }
}

/// The name of the function that refuses the members after the tag of a
/// unit variant (`horsies_core::core::config::no_member_after_the_tag`).
const NO_MEMBER_AFTER_THE_TAG: &str = "no_member_after_the_tag";

/// The serde attributes of a unit variant of a tagged enum: none, or
/// `deserialize_with` with a function named [`NO_MEMBER_AFTER_THE_TAG`].
/// That function refuses a member after the tag and keeps the wire form;
/// any other serde attribute is refused.
fn check_unit_variant_attributes(attrs: &[Attribute]) -> syn::Result<()> {
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        attr.parse_nested_meta(|meta| match meta.path.is_ident("deserialize_with") {
            true => {
                let value: LitStr = meta.value()?.parse()?;
                let function = value.value();
                let name = match function.rsplit_once("::") {
                    Some((_module, name)) => name,
                    None => function.as_str(),
                };
                match name == NO_MEMBER_AFTER_THE_TAG {
                    true => Ok(()),
                    false => Err(syn::Error::new_spanned(
                        &value,
                        "ManifestSchema reads deserialize_with on a unit variant only with \
                         the function no_member_after_the_tag",
                    )),
                }
            }
            false => Err(meta.error(
                "ManifestSchema reads only deserialize_with = \"no_member_after_the_tag\" \
                 on a unit variant",
            )),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{derive_manifest_schema, snake_case};

    /// The derive output as text without spaces, or the error message.
    fn derive_text(input: syn::DeriveInput) -> Result<String, String> {
        derive_manifest_schema(input)
            .map(|tokens| tokens.to_string().replace(' ', ""))
            .map_err(|error| error.to_string())
    }

    /// A unit variant of a tagged enum with `deserialize_with` and the
    /// function `no_member_after_the_tag`, by its name or by a path, has
    /// the schema of the same variant without the attribute.
    #[test]
    fn a_unit_variant_that_refuses_a_member_after_the_tag_keeps_its_form() {
        let plain = derive_text(syn::parse_quote! {
            #[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
            enum Recycle {
                Never,
                AfterSeconds { seconds: u32 },
            }
        })
        .expect("the derive accepts the enum");
        for function in [
            "no_member_after_the_tag",
            "crate::core::config::no_member_after_the_tag",
        ] {
            let function = syn::LitStr::new(function, proc_macro2::Span::call_site());
            let marked = derive_text(syn::parse_quote! {
                #[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
                enum Recycle {
                    #[serde(deserialize_with = #function)]
                    Never,
                    AfterSeconds { seconds: u32 },
                }
            });
            assert_eq!(marked, Ok(plain.clone()), "{}", function.value());
        }
    }

    /// Every other serde attribute of a variant is refused: another
    /// function or another key on a unit variant of a tagged enum, any
    /// attribute on a variant with fields, and any attribute on a variant
    /// of an enum without a tag.
    #[test]
    fn every_other_serde_attribute_of_a_variant_is_refused() {
        let cases: [(syn::DeriveInput, &str); 4] = [
            (
                syn::parse_quote! {
                    #[serde(tag = "kind")]
                    enum Recycle {
                        #[serde(deserialize_with = "read_never")]
                        Never,
                    }
                },
                "only with the function no_member_after_the_tag",
            ),
            (
                syn::parse_quote! {
                    #[serde(tag = "kind")]
                    enum Recycle {
                        #[serde(rename = "NEVER")]
                        Never,
                    }
                },
                "reads only deserialize_with",
            ),
            (
                syn::parse_quote! {
                    #[serde(tag = "kind")]
                    enum Recycle {
                        #[serde(deserialize_with = "no_member_after_the_tag")]
                        AfterSeconds { seconds: u32 },
                    }
                },
                "does not read serde attributes of a variant",
            ),
            (
                syn::parse_quote! {
                    enum Mode {
                        #[serde(deserialize_with = "no_member_after_the_tag")]
                        Observe,
                    }
                },
                "does not read serde attributes of a variant",
            ),
        ];
        for (input, expected) in cases {
            let refused = derive_text(input).expect_err("the derive refuses the attribute");
            assert!(refused.contains(expected), "{refused}");
        }
    }

    /// A newtype variant under a tag holds the fields of its record type.
    #[test]
    fn a_newtype_variant_under_a_tag_holds_the_fields_of_its_record() {
        let text = derive_text(syn::parse_quote! {
            #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
            enum SchedulePattern {
                Interval(IntervalSchedule),
            }
        })
        .expect("the derive accepts the enum");
        assert!(
            text.contains(
                "::horsies_core::manifest_schema::TaggedVariant{type_name:\"SchedulePatternInterval\",tag_value:\"interval\",doc:\"\",fields:::horsies_core::manifest_schema::VariantFields::OfRecord(::horsies_core::manifest_schema::record_name(<IntervalScheduleas::horsies_core::manifest_schema::ManifestItem>::ITEM)),}"
            ),
            "{text}"
        );
    }

    /// A generic enum is recorded once per use, with its type parameter
    /// read as the use.
    #[test]
    fn a_generic_enum_is_recorded_once_per_use() {
        let text = derive_text(syn::parse_quote! {
            #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
            #[manifest_schema(uses(Weekday, Month))]
            enum CronEnumTerm<T> {
                EnumValues { values: Vec<T> },
            }
        })
        .expect("the derive accepts the enum");
        for (used, name) in [
            ("Weekday", "CronEnumTermWeekday"),
            ("Month", "CronEnumTermMonth"),
        ] {
            assert!(
                text.contains(&format!("const_:()={{typeT={used};impl")),
                "{text}"
            );
            assert!(
                text.contains(&format!("ValueType::Named(\"{name}\")")),
                "{text}"
            );
            assert!(
                text.contains(&format!(
                    "type_name:\"{name}EnumValues\",tag_value:\"enum_values\""
                )),
                "{text}"
            );
        }
        assert!(text.contains("forCronEnumTerm<T>{"), "{text}");
        assert!(
            text.contains("value:<Vec<T>as::horsies_core::manifest_schema::ManifestValue>::VALUE_TYPE"),
            "{text}"
        );
    }

    /// An optional field that names its bound is an optional integer of
    /// that bound, with no serde default.
    #[test]
    fn an_optional_field_written_as_null_names_its_bound() {
        let text = derive_text(syn::parse_quote! {
            #[serde(deny_unknown_fields)]
            struct Window {
                #[serde(skip_serializing_if = "Option::is_none")]
                #[manifest_schema(int_bound = WINDOW_SECONDS)]
                seconds: Option<u32>,
            }
        })
        .expect("the derive accepts the struct");
        assert!(
            text.contains(
                "value:::horsies_core::manifest_schema::bounded_int::<Option<u32>>(WINDOW_SECONDS),default:::horsies_core::manifest_schema::MemberDefault::NoDefault,"
            ),
            "{text}"
        );
    }

    /// A length of time that names its bound takes the bound of its whole
    /// seconds: the derive writes the same call for every field type, and
    /// the field type picks the form.
    #[test]
    fn a_length_of_time_names_the_bound_of_its_seconds() {
        let text = derive_text(syn::parse_quote! {
            struct IntervalSchedule {
                #[manifest_schema(int_bound = INTERVAL_EVERY_SECONDS)]
                every: Duration,
            }
        })
        .expect("the derive accepts the struct");
        assert!(
            text.contains(
                "value:::horsies_core::manifest_schema::bounded_int::<Duration>(INTERVAL_EVERY_SECONDS),default:::horsies_core::manifest_schema::MemberDefault::NoDefault,"
            ),
            "{text}"
        );
    }

    /// A field with a serde default records the value of its const fn.
    #[test]
    fn a_field_with_a_serde_default_records_its_const_fn() {
        let text = derive_text(syn::parse_quote! {
            struct HourlySchedule {
                #[serde(default = "default_hourly_second")]
                #[manifest_schema(int_bound = HOURLY_SECOND)]
                second: u32,
            }
        })
        .expect("the derive accepts the struct");
        assert!(
            text.contains(
                "default:::horsies_core::manifest_schema::MemberDefault::int(::horsies_core::manifest_schema::bounded_int::<u32>(HOURLY_SECOND),default_hourly_second()asu64),"
            ),
            "{text}"
        );
    }

    /// A time of day takes the form of its Rust type, `NaiveTime`.
    #[test]
    fn a_time_of_day_field_takes_the_form_of_its_type() {
        let text = derive_text(syn::parse_quote! {
            struct DailySchedule {
                time: NaiveTime,
            }
        })
        .expect("the derive accepts the struct");
        assert!(
            text.contains(
                "name:\"time\",doc:\"\",value:<NaiveTimeas::horsies_core::manifest_schema::ManifestValue>::VALUE_TYPE,default:::horsies_core::manifest_schema::MemberDefault::NoDefault,"
            ),
            "{text}"
        );
    }

    /// `rename_all = "snake_case"` gives serde's snake_case names.
    #[test]
    fn a_snake_case_choice_has_serde_s_snake_case_values() {
        let text = derive_text(syn::parse_quote! {
            #[serde(rename_all = "snake_case")]
            enum Day {
                Monday,
                EveryDay,
            }
        })
        .expect("the derive accepts the enum");
        assert!(text.contains("value:\"monday\""), "{text}");
        assert!(text.contains("value:\"every_day\""), "{text}");
        assert_eq!(snake_case("ByMonthDay"), "by_month_day");
        assert_eq!(snake_case("EnumValues"), "enum_values");
    }

    /// The new keys are refused where they do not apply, and a field reads
    /// only its two serde keys.
    #[test]
    fn the_new_forms_refuse_what_they_do_not_model() {
        let cases: [(syn::DeriveInput, &str); 7] = [
            (
                syn::parse_quote! {
                    enum Term<T> {
                        Values { values: Vec<T> },
                    }
                },
                "lists its uses",
            ),
            (
                syn::parse_quote! {
                    #[manifest_schema(uses(Weekday))]
                    enum Term {
                        Every,
                    }
                },
                "this type has no type parameter",
            ),
            (
                syn::parse_quote! {
                    struct Hourly {
                        #[serde(default)]
                        second: u32,
                    }
                },
                "default = \"<const fn>\"",
            ),
            (
                syn::parse_quote! {
                    struct Interval {
                        #[serde(skip_serializing_if = "Vec::is_empty")]
                        seconds: Vec<u32>,
                    }
                },
                "only with \"Option::is_none\"",
            ),
            (
                syn::parse_quote! {
                    struct Interval {
                        #[serde(rename = "secs")]
                        seconds: u32,
                    }
                },
                "reads only the serde keys skip_serializing_if",
            ),
            (
                syn::parse_quote! {
                    struct Interval {
                        #[manifest_schema(bound = SECONDS)]
                        seconds: u32,
                    }
                },
                "the manifest_schema key of a field is int_bound",
            ),
            (
                syn::parse_quote! {
                    #[serde(tag = "type")]
                    enum Pattern {
                        Interval(IntervalSchedule, u32),
                    }
                },
                "has one field, a record type",
            ),
        ];
        for (input, expected) in cases {
            let refused = derive_text(input).expect_err("the derive refuses the input");
            assert!(refused.contains(expected), "{refused}");
        }
    }
}
