//! The build graph of this crate has serde_json's `arbitrary_precision` on,
//! so the core and the client in it are built with the flag.

/// An integer above `u64::MAX`. Without the flag serde_json reads it as an
/// `f64` and writes `1.2345678901234568e+29`.
const BEYOND_U64: &str = "123456789012345678901234567890";

#[test]
fn serde_json_keeps_the_text_of_an_integer_beyond_u64() {
    let value: serde_json::Value = serde_json::from_str(BEYOND_U64).expect("a JSON integer");

    assert_eq!(value.to_string(), BEYOND_U64);
}
