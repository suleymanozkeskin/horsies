use horsies::CustomErrorCode;

#[derive(Debug, Clone, PartialEq, CustomErrorCode)]
enum ChargeCode<T> {
    CardDeclined,
    Other(std::marker::PhantomData<T>),
}

fn main() {}
