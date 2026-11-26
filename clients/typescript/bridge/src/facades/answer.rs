//! The answer of a facade call that ends before an operation starts: an
//! argument that the package should have refused, or a query that the core
//! refuses before any connect.

use napi::bindgen_prelude::{Object, ToNapiValue};
use napi::Env;

use crate::call::operation::{resolve, EnvelopeResolver};
use crate::call::thrown::Envelope;

/// A Promise that resolves at once with `envelope`. No core operation
/// starts and no slot of the calls in flight is taken.
///
/// Failure exits: the `napi::Error` of a deferred that Node-API cannot
/// create.
pub fn answered_at_once<T: ToNapiValue + Send + 'static>(
    env: &Env,
    envelope: Envelope<T>,
) -> napi::Result<Object<'_>> {
    let (deferred, promise) = env.create_deferred::<Envelope<T>, EnvelopeResolver<T>>()?;
    resolve(deferred, envelope);
    Ok(promise)
}
