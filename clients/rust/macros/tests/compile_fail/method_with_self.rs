use horsies::task;
use horsies::TaskError;

struct MyService;

impl MyService {
    #[task("bad")]
    async fn handle(&self, _: ()) -> Result<String, TaskError<std::convert::Infallible>> {
        Ok("nope".into())
    }
}

fn main() {}
