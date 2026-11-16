use horsies::task;
use horsies::TaskError;

struct MyService;

impl MyService {
    #[task("bad")]
    async fn handle(&self, _: ()) -> Result<String, TaskError> {
        Ok("nope".into())
    }
}

fn main() {}
