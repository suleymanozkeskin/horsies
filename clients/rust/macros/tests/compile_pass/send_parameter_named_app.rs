// A task parameter named `app` binds apart from the app parameter of the
// generated send helpers.
use horsies::{task, AppConfig, Horsies, TaskError, TaskSendOptions};

#[task("app_parameter")]
async fn app_parameter(app: String) -> Result<usize, TaskError<std::convert::Infallible>> {
    Ok(app.len())
}

fn main() {
    let Ok(horsies_app) = Horsies::new(AppConfig::for_database_url(
        "postgresql://localhost/horsies",
    )) else {
        return;
    };
    let _now = app_parameter::send(&horsies_app, "billing".to_owned());
    let _with_options = app_parameter::with_options(TaskSendOptions::new())
        .send(&horsies_app, "billing".to_owned());
    let _field = app_parameter::fields::app();
}
