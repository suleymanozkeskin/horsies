// Every single parameter that is not `()` is wrapped in the generated
// `Input` struct, keyed by its name, as Python stores a keyword argument,
// whatever its type: `double` stores `{"x": 2}`, `price` stores
// `{"order": {"total": ...}}` and `route` stores `{"kind": "Express"}`. A
// single `()` parameter is the input `()`.
use std::convert::Infallible;

use horsies::{task, TaskError, TaskFunction};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Order {
    total: f64,
}

#[derive(Serialize, Deserialize)]
enum OrderKind {
    Express,
    Standard,
}

#[task("double")]
async fn double(x: i64) -> Result<i64, TaskError<Infallible>> {
    Ok(x * 2)
}

#[task("price")]
async fn price(order: Order) -> Result<f64, TaskError<Infallible>> {
    Ok(order.total)
}

#[task("route")]
async fn route(kind: OrderKind) -> Result<bool, TaskError<Infallible>> {
    match kind {
        OrderKind::Express => Ok(true),
        OrderKind::Standard => Ok(false),
    }
}

#[task("tick")]
async fn tick(nothing: ()) -> Result<(), TaskError<Infallible>> {
    Ok(nothing)
}

fn main() {
    let _ = double::Input { x: 2 };
    let _ = double::params::x();
    let _ = double::register
        as fn(
            &mut horsies::Horsies,
        )
            -> Result<TaskFunction<double::Input, i64, Infallible>, horsies::HorsiesError>;
    let _ = price::Input {
        order: Order { total: 1.5 },
    };
    let _ = price::params::order();
    let _ = price::register
        as fn(
            &mut horsies::Horsies,
        )
            -> Result<TaskFunction<price::Input, f64, Infallible>, horsies::HorsiesError>;
    let _ = route::Input {
        kind: OrderKind::Standard,
    };
    let _ = route::params::kind();
    let _ = route::register
        as fn(
            &mut horsies::Horsies,
        )
            -> Result<TaskFunction<route::Input, bool, Infallible>, horsies::HorsiesError>;
    let _ = tick::register
        as fn(
            &mut horsies::Horsies,
        ) -> Result<TaskFunction<(), (), Infallible>, horsies::HorsiesError>;
}
