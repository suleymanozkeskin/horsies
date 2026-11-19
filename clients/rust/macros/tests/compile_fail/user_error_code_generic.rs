use horsies::UserErrorCode;

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum ChargeCode<T> {
    CardDeclined,
    Other(std::marker::PhantomData<T>),
}

fn main() {}
