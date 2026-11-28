//! The docs hold the app as shared state of a web service
//! (`Arc<Horsies>` in the router state of axum), which needs the app to be
//! `Send` and `Sync`.

fn assert_send_and_sync<T: Send + Sync>() {}

#[test]
fn the_app_is_send_and_sync() {
    assert_send_and_sync::<horsies::Horsies>();
    assert_send_and_sync::<std::sync::Arc<horsies::Horsies>>();
}
