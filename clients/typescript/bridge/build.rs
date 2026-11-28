//! Sets the linker arguments of a Node-API addon (on macOS: the Node-API
//! symbols resolve in the host process at load).

fn main() {
    napi_build::setup();
}
