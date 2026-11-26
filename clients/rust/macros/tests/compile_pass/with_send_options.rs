// The send helpers take the app that they send through; the delay is a
// send option.
use horsies::{task, AppConfig, Horsies, TaskError, TaskSendOptions, WholeSeconds};

#[task("sum_with_options")]
async fn sum_with_options(a: i32, b: i32) -> Result<i32, TaskError<std::convert::Infallible>> {
    Ok(a + b)
}

#[task("no_arg_with_options")]
async fn no_arg_with_options() -> Result<i32, TaskError<std::convert::Infallible>> {
    Ok(1)
}

fn main() {
    let Ok(app) = Horsies::new(AppConfig::for_database_url(
        "postgresql://localhost/horsies",
    )) else {
        return;
    };
    let delayed = TaskSendOptions::new().delay(WholeSeconds::from_secs(30));
    // The futures are built and dropped: no send runs.
    let _now = sum_with_options::send(&app, 1, 2);
    let _later = sum_with_options::with_options(delayed.clone()).send(&app, 1, 2);
    let _none = no_arg_with_options::send(&app);
    let _none_later = no_arg_with_options::with_options(delayed).send(&app);
}
