//! C13: the `WorkflowInput` derive must emit the serde wire name for each field
//! (honoring `#[serde(rename)]` and `#[serde(rename_all)]`), not the Rust field
//! identifier. Otherwise `set`/`arg_from` write kwargs under the Rust name while
//! the input struct deserializes from the renamed key.

use horsies::WorkflowInput;
use serde::{Deserialize, Serialize};

#[derive(WorkflowInput, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CamelInput {
    user_id: i32,
    first_name: String,
}

#[derive(WorkflowInput, Serialize, Deserialize)]
struct RenamedInput {
    #[serde(rename = "id")]
    identifier: i32,
    #[serde(rename(deserialize = "amt"))]
    amount: i32,
    plain_field: i32,
}

#[test]
fn rename_all_camel_case_applies_to_wire_name() {
    assert_eq!(CamelInput::field_user_id().name(), "userId");
    assert_eq!(CamelInput::field_first_name().name(), "firstName");
}

#[test]
fn field_rename_wins_and_plain_field_is_verbatim() {
    assert_eq!(RenamedInput::field_identifier().name(), "id");
    // `rename(deserialize = "amt")` sets the wire key the struct deserializes from.
    assert_eq!(RenamedInput::field_amount().name(), "amt");
    // No rename and no rename_all: the Rust field name is used unchanged.
    assert_eq!(RenamedInput::field_plain_field().name(), "plain_field");
}
