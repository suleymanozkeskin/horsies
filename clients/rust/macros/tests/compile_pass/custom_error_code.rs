use horsies::CustomErrorCode;

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
pub enum ChargeCode {
    CardDeclined,
    RateLimited,
}

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum NoCodes {}

fn main() {
    let error = horsies::TaskError::custom(ChargeCode::CardDeclined, "card declined");
    assert_eq!(error.code().as_str(), "CARD_DECLINED");
    assert!(NoCodes::CODES.is_empty());
}
