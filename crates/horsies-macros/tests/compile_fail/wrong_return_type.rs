use horsies::task;

#[task("bad")]
async fn bad(_: ()) -> String {
    "nope".into()
}

fn main() {}
