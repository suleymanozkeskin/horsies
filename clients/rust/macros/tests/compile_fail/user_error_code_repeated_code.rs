// `RateLimited` and `RATELimited` are both RATE_LIMITED.
use horsies::UserErrorCode;

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
#[allow(non_camel_case_types)]
enum ChargeCode {
    RateLimited,
    RATELimited,
}

fn main() {}
