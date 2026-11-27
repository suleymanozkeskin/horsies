use horsies::CustomErrorCode;

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum ChargeCode {
    CardDeclined { reason: String },
}

fn main() {}
