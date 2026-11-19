//! `#[derive(EventSchema)]`: the wire codec and the Python form of a type of
//! the event contract (`horsies_core::events`), read from the type itself.
//!
//! - A struct of named fields is an event type. Its wire form is
//!   `[tag, field, ...]`: the struct name, then each field in declaration
//!   order. The derive implements `EventRecord`.
//! - An enum whose variants are all unit variants is a choice. Its wire form
//!   is the variant name in SCREAMING_SNAKE_CASE. The derive implements
//!   `EventValue` and `EventNamedValue`.
//! - An enum with a variant of named fields is a variant type. Its wire form
//!   is `[variant name, field, ...]`, and `[variant name]` for a unit
//!   variant. The Python class of a variant is the enum name, then the
//!   variant name. The derive implements `EventValue` and
//!   `EventNamedValue`.
//!
//! Each field type implements `horsies_core::events::EventValue`. A field
//! with `#[event_schema(internal)]` is read only by the handler child: it is
//! on the wire and on the Python class, and it is not a field of the stable
//! contract. A tuple struct, a tuple variant, a union and a generic type are
//! refused.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Attribute, Data, DataEnum, DeriveInput, Fields, FieldsNamed, Ident};

use crate::code_catalog::{doc_text, screaming_snake_case};

/// Who reads a field.
enum Access {
    Public,
    Internal,
}

/// One named field, as the generated code needs it.
struct FieldParts<'a> {
    ident: &'a Ident,
    name: String,
    /// The local name of the field's wire item in the generated decoder.
    binding: Ident,
    schema: TokenStream,
}

pub fn derive_event_schema(input: DeriveInput) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "EventSchema can only be derived for a type without generic parameters",
        ));
    }
    refuse_container_keys(&input.attrs)?;
    let doc = doc_text(&input.attrs);
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => event_record(ident, &doc, named),
            Fields::Unnamed(_) | Fields::Unit => Err(syn::Error::new_spanned(
                ident,
                "an event type is a struct of named fields",
            )),
        },
        Data::Enum(data) => {
            let all_unit = data
                .variants
                .iter()
                .all(|variant| matches!(variant.fields, Fields::Unit));
            match (data.variants.is_empty(), all_unit) {
                (true, _) => Err(syn::Error::new_spanned(
                    ident,
                    "a type of the event contract has at least one variant",
                )),
                (false, true) => choice(ident, &doc, data),
                (false, false) => variants(ident, &doc, data),
            }
        }
        Data::Union(_) => Err(syn::Error::new_spanned(
            ident,
            "EventSchema can only be derived for a struct or an enum",
        )),
    }
}

/// `impl EventRecord` of an event struct.
fn event_record(ident: &Ident, doc: &str, named: &FieldsNamed) -> syn::Result<TokenStream> {
    let name = ident.to_string();
    let fields = field_parts(named)?;
    let count = fields.len();
    let schemas: Vec<&TokenStream> = fields.iter().map(|field| &field.schema).collect();
    let idents: Vec<&Ident> = fields.iter().map(|field| field.ident).collect();
    let names: Vec<&String> = fields.iter().map(|field| &field.name).collect();
    let bindings: Vec<&Ident> = fields.iter().map(|field| &field.binding).collect();
    Ok(quote! {
        impl ::horsies_core::events::EventRecord for #ident {
            const TYPE: ::horsies_core::events::EventType = ::horsies_core::events::EventType {
                name: #name,
                doc: #doc,
                fields: &[#(#schemas,)*],
            };

            fn fields_to_wire(
                &self,
                items: &mut ::std::vec::Vec<::horsies_core::events::WireValue>,
            ) -> ::std::result::Result<(), ::horsies_core::events::EncodeRefusal> {
                #(
                    items.push(
                        ::horsies_core::events::EventValue::to_wire(&self.#idents)
                            .map_err(|refusal| refusal.inside(#names))?,
                    );
                )*
                ::std::result::Result::Ok(())
            }

            fn from_wire_fields(
                fields: &[::horsies_core::events::WireValue],
            ) -> ::std::result::Result<Self, ::horsies_core::events::DecodeRefusal> {
                let [#(#bindings,)*] = fields else {
                    return ::std::result::Result::Err(
                        ::horsies_core::events::DecodeRefusal::item_count(#count, fields.len()),
                    );
                };
                ::std::result::Result::Ok(Self {
                    #(
                        #idents: ::horsies_core::events::EventValue::from_wire(#bindings)
                            .map_err(|refusal| refusal.inside(#names))?,
                    )*
                })
            }
        }
    })
}

/// `impl EventValue` and `impl EventNamedValue` of an enum of unit variants.
fn choice(ident: &Ident, doc: &str, data: &DataEnum) -> syn::Result<TokenStream> {
    let name = ident.to_string();
    let mut variant_idents: Vec<&Ident> = Vec::with_capacity(data.variants.len());
    let mut values: Vec<String> = Vec::with_capacity(data.variants.len());
    let mut docs: Vec<String> = Vec::with_capacity(data.variants.len());
    for variant in &data.variants {
        refuse_event_schema_keys(&variant.attrs, "a variant")?;
        variant_idents.push(&variant.ident);
        values.push(screaming_snake_case(&variant.ident.to_string()));
        docs.push(doc_text(&variant.attrs));
    }
    let named = named_impl(
        ident,
        &name,
        doc,
        quote! {
            ::horsies_core::events::NamedShape::Choice {
                values: &[#(
                    ::horsies_core::events::ChoiceValue { value: #values, doc: #docs },
                )*],
            }
        },
    );
    Ok(quote! {
        impl ::horsies_core::events::EventValue for #ident {
            const FORM: ::horsies_core::events::ValueForm =
                ::horsies_core::events::ValueForm::Named(#name);

            fn to_wire(
                &self,
            ) -> ::std::result::Result<
                ::horsies_core::events::WireValue,
                ::horsies_core::events::EncodeRefusal,
            > {
                let value: &'static str = match self {
                    #(Self::#variant_idents => #values,)*
                };
                ::std::result::Result::Ok(::horsies_core::events::WireValue::String(
                    ::std::borrow::ToOwned::to_owned(value),
                ))
            }

            fn from_wire(
                wire: &::horsies_core::events::WireValue,
            ) -> ::std::result::Result<Self, ::horsies_core::events::DecodeRefusal> {
                match ::horsies_core::events::wire_text(wire)? {
                    #(#values => ::std::result::Result::Ok(Self::#variant_idents),)*
                    other => ::std::result::Result::Err(
                        ::horsies_core::events::DecodeRefusal::unknown_name(#name, other),
                    ),
                }
            }
        }

        #named
    })
}

/// `impl EventValue` and `impl EventNamedValue` of an enum whose variants
/// are unit variants or have named fields.
fn variants(ident: &Ident, doc: &str, data: &DataEnum) -> syn::Result<TokenStream> {
    let name = ident.to_string();
    let mut schemas: Vec<TokenStream> = Vec::with_capacity(data.variants.len());
    let mut to_wire_arms: Vec<TokenStream> = Vec::with_capacity(data.variants.len());
    let mut from_wire_arms: Vec<TokenStream> = Vec::with_capacity(data.variants.len());
    for variant in &data.variants {
        refuse_event_schema_keys(&variant.attrs, "a variant")?;
        let variant_ident = &variant.ident;
        let tag = variant_ident.to_string();
        let python_class = format!("{name}{tag}");
        let variant_doc = doc_text(&variant.attrs);
        let fields = match &variant.fields {
            Fields::Unit => Vec::new(),
            Fields::Named(named) => field_parts(named)?,
            Fields::Unnamed(_) => {
                return Err(syn::Error::new_spanned(
                    variant,
                    "a variant of an event type is a unit variant or has named fields",
                ))
            }
        };
        let count = fields.len();
        let field_schemas: Vec<&TokenStream> = fields.iter().map(|field| &field.schema).collect();
        let idents: Vec<&Ident> = fields.iter().map(|field| field.ident).collect();
        let names: Vec<&String> = fields.iter().map(|field| &field.name).collect();
        let bindings: Vec<&Ident> = fields.iter().map(|field| &field.binding).collect();
        schemas.push(quote! {
            ::horsies_core::events::VariantType {
                tag: #tag,
                python_class: #python_class,
                doc: #variant_doc,
                fields: &[#(#field_schemas,)*],
            }
        });
        to_wire_arms.push(quote! {
            Self::#variant_ident { #(#idents,)* } => {
                // Not `items`: a field of that name is bound in this arm.
                let mut variant_wire_items: ::std::vec::Vec<::horsies_core::events::WireValue> =
                    ::std::vec::Vec::with_capacity(#count + 1);
                variant_wire_items.push(::horsies_core::events::WireValue::String(
                    ::std::borrow::ToOwned::to_owned(#tag),
                ));
                #(
                    variant_wire_items.push(
                        ::horsies_core::events::EventValue::to_wire(#idents)
                            .map_err(|refusal| refusal.inside(#names).inside(#tag))?,
                    );
                )*
                ::std::result::Result::Ok(::horsies_core::events::WireValue::Array(
                    variant_wire_items,
                ))
            }
        });
        from_wire_arms.push(quote! {
            #tag => {
                let [#(#bindings,)*] = fields else {
                    return ::std::result::Result::Err(
                        ::horsies_core::events::DecodeRefusal::item_count(#count, fields.len())
                            .inside(#tag),
                    );
                };
                ::std::result::Result::Ok(Self::#variant_ident {
                    #(
                        #idents: ::horsies_core::events::EventValue::from_wire(#bindings)
                            .map_err(|refusal| refusal.inside(#names).inside(#tag))?,
                    )*
                })
            }
        });
    }
    let named = named_impl(
        ident,
        &name,
        doc,
        quote! {
            ::horsies_core::events::NamedShape::Variants {
                variants: &[#(#schemas,)*],
            }
        },
    );
    Ok(quote! {
        impl ::horsies_core::events::EventValue for #ident {
            const FORM: ::horsies_core::events::ValueForm =
                ::horsies_core::events::ValueForm::Named(#name);

            fn to_wire(
                &self,
            ) -> ::std::result::Result<
                ::horsies_core::events::WireValue,
                ::horsies_core::events::EncodeRefusal,
            > {
                match self {
                    #(#to_wire_arms)*
                }
            }

            fn from_wire(
                wire: &::horsies_core::events::WireValue,
            ) -> ::std::result::Result<Self, ::horsies_core::events::DecodeRefusal> {
                let (tag, fields) = ::horsies_core::events::wire_variant(wire, #name)?;
                match tag {
                    #(#from_wire_arms)*
                    other => ::std::result::Result::Err(
                        ::horsies_core::events::DecodeRefusal::unknown_name(#name, other),
                    ),
                }
            }
        }

        #named
    })
}

/// `impl EventNamedValue` with the given shape.
fn named_impl(ident: &Ident, name: &str, doc: &str, shape: TokenStream) -> TokenStream {
    quote! {
        impl ::horsies_core::events::EventNamedValue for #ident {
            const NAMED: ::horsies_core::events::NamedType = ::horsies_core::events::NamedType {
                name: #name,
                doc: #doc,
                shape: #shape,
            };
        }
    }
}

/// The parts of each named field, in declaration order.
fn field_parts(named: &FieldsNamed) -> syn::Result<Vec<FieldParts<'_>>> {
    let mut parts = Vec::with_capacity(named.named.len());
    for field in &named.named {
        let Some(ident) = &field.ident else {
            return Err(syn::Error::new_spanned(field, "a named field has a name"));
        };
        let name = ident.to_string();
        let doc = doc_text(&field.attrs);
        let field_type = &field.ty;
        let access = match field_access(&field.attrs)? {
            Access::Public => quote!(::horsies_core::events::FieldAccess::Public),
            Access::Internal => quote!(::horsies_core::events::FieldAccess::Internal),
        };
        parts.push(FieldParts {
            ident,
            binding: format_ident!("wire_{}", name),
            schema: quote! {
                ::horsies_core::events::EventField {
                    name: #name,
                    doc: #doc,
                    form: <#field_type as ::horsies_core::events::EventValue>::FORM,
                    access: #access,
                }
            },
            name,
        });
    }
    Ok(parts)
}

/// The access of a field: `#[event_schema(internal)]` or none.
fn field_access(attrs: &[Attribute]) -> syn::Result<Access> {
    let mut access = Access::Public;
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("event_schema"))
    {
        attr.parse_nested_meta(|meta| match meta.path.is_ident("internal") {
            true => {
                access = Access::Internal;
                Ok(())
            }
            false => Err(meta.error("the only event_schema key of a field is internal")),
        })?;
    }
    Ok(access)
}

/// A type takes no `event_schema` key.
fn refuse_container_keys(attrs: &[Attribute]) -> syn::Result<()> {
    refuse_event_schema_keys(attrs, "a type")
}

/// `item` takes no `event_schema` attribute.
fn refuse_event_schema_keys(attrs: &[Attribute], item: &str) -> syn::Result<()> {
    match attrs
        .iter()
        .find(|attr| attr.path().is_ident("event_schema"))
    {
        Some(attr) => Err(syn::Error::new_spanned(
            attr,
            format!("{item} takes no event_schema attribute; only a field takes internal"),
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::derive_event_schema;

    /// The derive output as text without spaces, or the error message.
    fn derive_text(input: syn::DeriveInput) -> Result<String, String> {
        derive_event_schema(input)
            .map(|tokens| tokens.to_string().replace(' ', ""))
            .map_err(|error| error.to_string())
    }

    #[test]
    fn a_struct_is_an_event_with_its_fields_in_order() {
        let text = derive_text(syn::parse_quote! {
            /// A task was claimed.
            struct TaskClaimed {
                /// When.
                at: Time,
                task_id: Uuid,
            }
        })
        .expect("the derive accepts the struct");
        assert!(text.contains("impl::horsies_core::events::EventRecordforTaskClaimed"));
        assert!(text.contains("name:\"TaskClaimed\",doc:\"Ataskwasclaimed.\""));
        let at = text
            .find("name:\"at\"")
            .expect("the first field is recorded");
        let task_id = text
            .find("name:\"task_id\"")
            .expect("the second field is recorded");
        assert!(at < task_id);
        assert!(text.contains("let[wire_at,wire_task_id,]=fieldselse"));
        assert!(text.contains("DecodeRefusal::item_count(2usize,fields.len())"));
        assert!(text.contains("access:::horsies_core::events::FieldAccess::Public"));
    }

    #[test]
    fn an_enum_of_unit_variants_is_a_choice_of_screaming_snake_names() {
        let text = derive_text(syn::parse_quote! {
            /// Why.
            enum StopReason {
                /// The claim is gone.
                ClaimLost,
                TaskExpired,
            }
        })
        .expect("the derive accepts the enum");
        assert!(text.contains("ValueForm::Named(\"StopReason\")"));
        assert!(text.contains("Self::ClaimLost=>\"CLAIM_LOST\""));
        assert!(text.contains("\"TASK_EXPIRED\"=>::std::result::Result::Ok(Self::TaskExpired)"));
        assert!(text.contains("NamedShape::Choice"));
        assert!(text.contains("value:\"CLAIM_LOST\",doc:\"Theclaimisgone.\""));
    }

    #[test]
    fn an_enum_with_fields_is_a_variant_type_with_prefixed_python_classes() {
        let text = derive_text(syn::parse_quote! {
            enum AttemptStart {
                Reported { started_at: Time },
                NotReported,
            }
        })
        .expect("the derive accepts the enum");
        assert!(text.contains("NamedShape::Variants"));
        assert!(text.contains("tag:\"Reported\",python_class:\"AttemptStartReported\""));
        assert!(text.contains("tag:\"NotReported\",python_class:\"AttemptStartNotReported\""));
        assert!(text.contains("wire_variant(wire,\"AttemptStart\")"));
        assert!(text.contains("\"NotReported\"=>{let[]=fieldselse"));
        assert!(text.contains(".inside(\"started_at\").inside(\"Reported\")"));
    }

    #[test]
    fn an_internal_field_is_recorded_as_internal() {
        let text = derive_text(syn::parse_quote! {
            enum AttemptEnd {
                DecidedByWorker {
                    #[event_schema(internal)]
                    placed_at: Time,
                },
            }
        })
        .expect("the derive accepts the enum");
        assert!(text.contains("access:::horsies_core::events::FieldAccess::Internal"));
    }

    #[test]
    fn shapes_outside_the_contract_are_refused() {
        let cases: [(syn::DeriveInput, &str); 6] = [
            (
                syn::parse_quote! { struct Pair(u32, u32); },
                "a struct of named fields",
            ),
            (
                syn::parse_quote! { enum Code { User(String) } },
                "a unit variant or has named fields",
            ),
            (syn::parse_quote! { enum Never {} }, "at least one variant"),
            (
                syn::parse_quote! { struct Wrapped<T> { value: T } },
                "without generic parameters",
            ),
            (
                syn::parse_quote! { struct Event { #[event_schema(hidden)] at: Time } },
                "the only event_schema key of a field is internal",
            ),
            (
                syn::parse_quote! { #[event_schema(internal)] struct Event { at: Time } },
                "only a field takes internal",
            ),
        ];
        for (input, expected) in cases {
            let refused = derive_text(input).expect_err("the derive refuses the type");
            assert!(refused.contains(expected), "{refused}");
        }
    }
}
