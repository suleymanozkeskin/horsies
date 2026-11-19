use horsies::UserErrorCode;

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum ChargeCode {
    CardDeclined { reason: String },
}

fn main() {}
