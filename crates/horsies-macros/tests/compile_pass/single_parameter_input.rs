// A single parameter that is not a struct is wrapped in the generated
// `Input` struct, keyed by its name, as Python stores a keyword argument:
// `double` stores `{"x": 2}`. A single struct parameter is the input itself.
use std::convert::Infallible;

use horsies::{task, TaskError, TaskFunction};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Order {
    total: f64,
}

#[task("double")]
async fn double(x: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(x * 2)
}

#[task("price")]
async fn price(order: Order) -> Result<f64, TaskError<Infallible>> {
    Ok(order.total)
}

fn main() {
    let _ = double::Input { x: 2 };
    let _ = double::params::x();
    let _ = double::register
        as fn(
            &mut horsies::Horsies,
        ) -> Result<TaskFunction<double::Input, i64, Infallible>, horsies::HorsiesError>;
    let _ = price::register
        as fn(
            &mut horsies::Horsies,
        ) -> Result<TaskFunction<Order, f64, Infallible>, horsies::HorsiesError>;
}
