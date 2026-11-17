use horsies::UserErrorCode;

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
pub enum ChargeCode {
    CardDeclined,
    RateLimited,
}

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum NoCodes {}

fn main() {
    let error = horsies::TaskError::user(ChargeCode::CardDeclined, "card declined");
    assert_eq!(error.code().text(), "CARD_DECLINED");
    assert!(NoCodes::CODES.is_empty());
}
