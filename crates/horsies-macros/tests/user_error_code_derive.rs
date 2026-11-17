//! `#[derive(UserErrorCode)]`: each unit variant has one code, its name in
//! UPPER_SNAKE_CASE, and the code reads back to its variant.

use horsies::UserErrorCode;

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum ChargeCode {
    CardDeclined,
    RateLimited,
    HTTPError,
    r#Type,
}

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum NoCodes {}

#[test]
fn each_variant_has_its_upper_snake_case_code() {
    assert_eq!(
        ChargeCode::CODES,
        &["CARD_DECLINED", "RATE_LIMITED", "HTTP_ERROR", "TYPE"]
    );
    assert_eq!(ChargeCode::RateLimited.code(), "RATE_LIMITED");
    assert_eq!(ChargeCode::r#Type.code(), "TYPE");
}

#[test]
fn each_code_reads_back_to_its_variant() {
    for code in ChargeCode::CODES {
        let variant = ChargeCode::from_code(code).expect("a code of the type");
        assert_eq!(variant.code(), *code);
    }
}

#[test]
fn a_text_that_is_not_a_code_reads_as_none() {
    assert_eq!(ChargeCode::from_code("card_declined"), None);
    assert_eq!(ChargeCode::from_code("TASK_TIMEOUT"), None);
}

#[test]
fn an_enum_without_variants_has_no_code() {
    assert!(NoCodes::CODES.is_empty());
    assert_eq!(NoCodes::from_code("CARD_DECLINED"), None);
}
