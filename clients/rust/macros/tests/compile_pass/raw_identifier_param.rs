// A raw identifier parameter is the key that serde reads: `type`.
use std::convert::Infallible;

use horsies::{task, TaskError};

#[task("labelled")]
async fn labelled(r#type: String, count: u32) -> Result<String, TaskError<Infallible>> {
    Ok(format!("{type_}:{count}", type_ = r#type))
}

fn main() {
    let _ = labelled::params::r#type();
    let _ = labelled::params::count();
    let _ = labelled::Input {
        r#type: "invoice".to_owned(),
        count: 1,
    };
}
