//! The members of a worker manifest that the start of a worker gives, not
//! the app config, and the check of a `--metrics-listen` address.
//!
//! - The package writes the start members as one JSON object with the
//!   manifest's own member names and forms (the generated `toWire`
//!   functions): `profile`, `metrics_endpoint`, `tasks`, `handlers`,
//!   `worker_ended_spans`, `child_launch`. The core's manifest parser checks
//!   each one with the rest of the manifest; this module refuses only a
//!   text that is not an object and a member that is not a start member.
//! - `parseListenAddress` is the core's rule for `HOST:PORT`
//!   (`ListenAddress::parse`), so the refusal texts are the core's.
//!
//! Both are pure: they start no core.

use horsies_core::manifest::ManifestRefused;
use horsies_core::telemetry::listen::ListenAddress;
use napi_derive::napi;
use serde_json::{Map, Value};

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;

/// The members of a worker manifest that the start gives.
pub const START_MEMBERS: [&str; 6] = [
    "profile",
    "metrics_endpoint",
    "tasks",
    "handlers",
    "worker_ended_spans",
    "child_launch",
];

/// A JSON object that holds only start members. The core's manifest parser
/// checks their values and refuses a member that is missing.
#[derive(Debug, Clone, PartialEq)]
pub struct StartPart {
    members: Map<String, Value>,
}

impl StartPart {
    /// The start members of `text`. Pure.
    ///
    /// Errors: [`ManifestRefused`] with the path `.` for a text that is not
    /// a JSON object, and with the member's name for a member that is not a
    /// start member.
    pub fn parse(text: &str) -> Result<Self, ManifestRefused> {
        let members = match serde_json::from_str::<Value>(text) {
            Ok(Value::Object(members)) => members,
            Ok(_) => return Err(refused(".", "the start members are not a JSON object")),
            Err(error) => {
                return Err(refused(
                    ".",
                    &format!("the start members do not read as JSON: {error}"),
                ))
            }
        };
        match members
            .keys()
            .find(|name| !START_MEMBERS.contains(&name.as_str()))
        {
            Some(name) => Err(refused(name, "not a member that the worker start gives")),
            None => Ok(Self { members }),
        }
    }

    /// The members, by name.
    pub fn into_members(self) -> Map<String, Value> {
        self.members
    }
}

fn refused(field_path: &str, cause: &str) -> ManifestRefused {
    ManifestRefused {
        field_path: field_path.to_owned(),
        cause: cause.to_owned(),
    }
}

/// The check of a `--metrics-listen` value.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListenAddressCheck {
    /// The host (an IPv6 address without its brackets) and the port.
    Address { host: String, port: u32 },
    /// The core's refusal text, which names the value and the rule.
    Refused { cause: String },
}

/// Check a `HOST:PORT` text with the core's rule. Pure: starts no core.
#[napi(ts_return_type = "NativeEnvelope<ListenAddressCheck>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn parse_listen_address(text: String) -> Envelope<ListenAddressCheck> {
    sync_call("parse_listen_address", || {
        Envelope::Value(listen_address_check(&text))
    })
}

/// The check of `text`. Pure.
fn listen_address_check(text: &str) -> ListenAddressCheck {
    match ListenAddress::parse(text) {
        Ok(address) => ListenAddressCheck::Address {
            host: address.host().to_owned(),
            port: u32::from(address.port()),
        },
        Err(refused) => ListenAddressCheck::Refused {
            cause: refused.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_object_of_start_members_is_kept() {
        let part = StartPart::parse(r#"{"profile": {"kind": "NAMED", "name": "io"}}"#)
            .expect("a start member");
        assert_eq!(part.into_members().len(), 1);
    }

    #[test]
    fn a_member_that_the_app_config_gives_is_refused_with_its_name() {
        let refused = StartPart::parse(r#"{"claim": {}}"#).expect_err("not a start member");
        assert_eq!(refused.field_path, "claim");
    }

    #[test]
    fn a_text_that_is_not_an_object_is_refused_at_the_document() {
        assert_eq!(
            StartPart::parse("[]")
                .expect_err("not an object")
                .field_path,
            "."
        );
        assert_eq!(StartPart::parse("{").expect_err("not JSON").field_path, ".");
    }

    #[test]
    fn a_host_and_port_is_an_address() {
        assert_eq!(
            listen_address_check("0.0.0.0:9464"),
            ListenAddressCheck::Address {
                host: "0.0.0.0".to_owned(),
                port: 9464
            }
        );
    }

    #[test]
    fn a_bracketed_ipv6_address_loses_its_brackets() {
        assert_eq!(
            listen_address_check("[::1]:9464"),
            ListenAddressCheck::Address {
                host: "::1".to_owned(),
                port: 9464
            }
        );
    }

    #[test]
    fn a_text_with_no_port_is_refused_with_the_core_text() {
        assert!(matches!(
            listen_address_check("localhost"),
            ListenAddressCheck::Refused { .. }
        ));
    }

    #[test]
    fn port_zero_is_refused() {
        assert!(matches!(
            listen_address_check("127.0.0.1:0"),
            ListenAddressCheck::Refused { .. }
        ));
    }
}
