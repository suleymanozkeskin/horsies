//! `probeParseWorkerManifest`: a test export that parses the whole text of a
//! worker manifest with the core's parser. A worker start builds that text
//! from the app config and the start members (`run::worker_manifest`) and
//! parses it with the same function; no export takes a whole text, so the
//! native tests of the generated manifest samples call this one.
//!
//! Only a build with the cargo feature `test-probes` has this module
//! (`bun run --cwd bridge build:dev`). A published addon does not export it.

use horsies_core::worker_manifest::parse_worker_manifest;
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;

/// How the core's parse of a worker manifest text ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerManifestProbe {
    /// The core accepted the text.
    Parsed {},
    /// The core refused the text at `field_path`, for `cause`.
    Refused { field_path: String, cause: String },
}

/// Parse `text` with the core's worker manifest parser. Pure: starts no core.
#[napi(ts_return_type = "NativeEnvelope<WorkerManifestProbe>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn probe_parse_worker_manifest(text: String) -> Envelope<WorkerManifestProbe> {
    sync_call("probe_parse_worker_manifest", || {
        Envelope::Value(match parse_worker_manifest(&text) {
            Ok(_manifest) => WorkerManifestProbe::Parsed {},
            Err(refused) => WorkerManifestProbe::Refused {
                field_path: refused.field_path,
                cause: refused.cause,
            },
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_text_that_is_not_a_manifest_is_refused() {
        let Envelope::Value(outcome) = probe_parse_worker_manifest("{}".to_owned()) else {
            panic!("the parse gives a value");
        };
        assert!(matches!(outcome, WorkerManifestProbe::Refused { .. }));
    }
}
