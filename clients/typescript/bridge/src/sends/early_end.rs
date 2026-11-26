//! The Promise of an operation that ends before it starts: its arguments
//! break a boundary rule, so the call resolves at once with the thrown code
//! and no core starts.

use napi::bindgen_prelude::{Object, ToNapiValue};
use napi::Env;

use crate::call::operation::{resolve, EnvelopeResolver};
use crate::call::thrown::{Envelope, NativeThrown};

/// A Promise that resolves with `thrown` at once.
///
/// Failure exits: the `napi::Error` of a deferred that Node-API cannot
/// create (the Promise does not exist then).
pub fn thrown_now<'env, T: ToNapiValue + Send + 'static>(
    env: &'env Env,
    thrown: NativeThrown,
) -> napi::Result<Object<'env>> {
    let (deferred, promise) = env.create_deferred::<Envelope<T>, EnvelopeResolver<T>>()?;
    resolve(deferred, Envelope::Thrown(thrown));
    Ok(promise)
}
