//! The outcome of an operation: its record, or its expected failure as a
//! value.
//!
//! An operation answers `{ kind: "done", record }` or `{ kind: "failed",
//! error }`, where `error` is the raw record of a core catalog value
//! ([`RawCatalogError`]). Expected failures are values, not thrown errors.

use horsies_core::error_catalog::ErrorClassValues;
use napi::bindgen_prelude::{check_status, sys, Object, ToNapiValue};

use crate::values::catalog_values::RawCatalogError;

/// The outcome of one operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome<T> {
    Done(T),
    Failed(RawCatalogError),
}

impl<T> Outcome<T> {
    /// The failure of the operation `subject` with the catalog value `error`.
    /// The raw record is built here, on the thread that ends the operation.
    /// Pure.
    pub fn failed<E: ErrorClassValues>(subject: &str, error: &E) -> Self {
        Self::Failed(RawCatalogError::of(subject, error))
    }
}

impl<T: ToNapiValue> ToNapiValue for Outcome<T> {
    /// `{ kind: "done", record }` or `{ kind: "failed", error }`.
    unsafe fn to_napi_value(env: sys::napi_env, outcome: Self) -> napi::Result<sys::napi_value> {
        match outcome {
            Self::Done(record) => unsafe { tagged_object(env, "done", "record", record) },
            Self::Failed(error) => unsafe { tagged_object(env, "failed", "error", error) },
        }
    }
}

/// A new JavaScript object `{ kind: <kind>, <field>: <value> }`.
///
/// # Safety
/// `env` is the environment of the calling JavaScript thread, as in
/// [`ToNapiValue::to_napi_value`].
pub unsafe fn tagged_object<V: ToNapiValue>(
    env: sys::napi_env,
    kind: &str,
    field: &str,
    value: V,
) -> napi::Result<sys::napi_value> {
    let mut raw = std::ptr::null_mut();
    check_status!(
        unsafe { sys::napi_create_object(env, &mut raw) },
        "creating the object of an outcome failed"
    )?;
    let mut object = Object::from_raw(env, raw);
    object.set("kind", kind)?;
    object.set(field, value)?;
    Ok(raw)
}
