use horsies::task;
use horsies::TaskError;

// This task is feature-gated. Both the function and the generated
// module must be compiled out when the feature is off.
// Since this is a compile-pass test, it verifies the cfg attribute
// does not cause a compilation error.
#[cfg(any())] // always false — function and module should be compiled out
#[task("gated")]
async fn gated(_: ()) -> Result<String, TaskError> {
    Ok("never".into())
}

// This task is always present.
#[task("always")]
async fn always(_: ()) -> Result<String, TaskError> {
    Ok("ok".into())
}

fn main() {
    // gated::register as fn(&mut horsies::Horsies) -> _ should NOT exist — the cfg compiled it out.
    // We can only reference always::register as fn(&mut horsies::Horsies) -> _.
    let _ = always::register as fn(&mut horsies::Horsies) -> _;
}
