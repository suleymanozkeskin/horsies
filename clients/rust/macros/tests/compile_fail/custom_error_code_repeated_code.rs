// `RateLimited` and `RATELimited` are both RATE_LIMITED.
use horsies::CustomErrorCode;

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
#[allow(non_camel_case_types)]
enum ChargeCode {
    RateLimited,
    RATELimited,
}

fn main() {}
