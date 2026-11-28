// `name::with_options(options).send(&app, ..)` is one expression: the send
// owns the options, so its future borrows only the app, also when a binding
// keeps the future and the options value is a temporary. A clone of the
// send options serves one more send.
use horsies::{
    task, AppConfig, Horsies, TaskError, TaskHandle, TaskSendError, TaskSendOptions, WholeSeconds,
};

#[task("chained_sum")]
async fn chained_sum(a: i32, b: i32) -> Result<i32, TaskError<std::convert::Infallible>> {
    Ok(a + b)
}

type SumHandle = TaskHandle<i32, std::convert::Infallible>;

/// Two sends with temporary options values, each future kept in a binding
/// before the await.
async fn send_twice(app: &Horsies) -> Result<(SumHandle, SumHandle), TaskSendError> {
    let later =
        chained_sum::with_options(TaskSendOptions::new().delay(WholeSeconds::from_secs(30)))
            .send(app, 1, 2);
    let keyed = chained_sum::with_options(TaskSendOptions::new().idempotency_key("sum-3-4"))
        .send(app, 3, 4);
    Ok((later.await?, keyed.await?))
}

/// One options value for two sends: the first send takes a clone.
async fn send_with_one_options_value(app: &Horsies) -> Result<SumHandle, TaskSendError> {
    let options = chained_sum::with_options(TaskSendOptions::new().retain_forever());
    let _first = options.clone().send(app, 5, 6).await?;
    options.send(app, 7, 8).await
}

fn main() {
    let Ok(app) = Horsies::new(AppConfig::from_database_url(
        "postgresql://localhost/horsies",
    )) else {
        return;
    };
    // The futures are built and dropped: no send runs.
    let _twice = send_twice(&app);
    let _one_value = send_with_one_options_value(&app);
    let _kept = chained_sum::with_options(TaskSendOptions::new()).send(&app, 9, 10);
}
