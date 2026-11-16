//! Embedded SPA assets and per-request runtime configuration.

use std::borrow::Cow;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::Arc;

use include_dir::{include_dir, Dir};
use percent_encoding::percent_decode_str;
use serde::Serialize;

static EMBEDDED_UI: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/webui-dist");

pub const ASSETS_MISSING_DETAIL: &str =
    "horsies web UI assets are not built. Run: cd webui && bun install && bun run build";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MonitoringUiConfig {
    pub custom_css_url: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct Asset {
    pub bytes: Cow<'static, [u8]>,
    pub content_type: String,
}

pub(crate) trait AssetStore: Send + Sync {
    fn get(&self, path: &str) -> Option<Asset>;
}

pub(crate) struct EmbeddedAssets;

impl AssetStore for EmbeddedAssets {
    fn get(&self, path: &str) -> Option<Asset> {
        let file = EMBEDDED_UI.get_file(path)?;
        Some(Asset {
            bytes: Cow::Borrowed(file.contents()),
            content_type: mime_guess::from_path(path)
                .first_or_octet_stream()
                .essence_str()
                .to_owned(),
        })
    }
}

#[cfg(test)]
pub(crate) struct MemoryAssets {
    files: HashMap<String, Asset>,
}

#[cfg(test)]
impl MemoryAssets {
    pub(crate) fn standard() -> Arc<dyn AssetStore> {
        let mut files = HashMap::new();
        files.insert(
            "index.html".to_owned(),
            Asset {
                bytes: Cow::Borrowed(
                    b"<!doctype html><html><head><title>horsies</title></head><body></body></html>",
                ),
                content_type: "text/html".to_owned(),
            },
        );
        files.insert(
            "assets/app.js".to_owned(),
            Asset {
                bytes: Cow::Borrowed(b"console.log('horsies')"),
                content_type: "text/javascript".to_owned(),
            },
        );
        Arc::new(Self { files })
    }

    pub(crate) fn empty() -> Arc<dyn AssetStore> {
        Arc::new(Self {
            files: HashMap::new(),
        })
    }
}

#[cfg(test)]
impl AssetStore for MemoryAssets {
    fn get(&self, path: &str) -> Option<Asset> {
        self.files.get(path).cloned()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeConfig<'a> {
    base_path: &'a str,
    api_base: String,
}

pub(crate) fn normalize_base_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_owned()
    } else {
        trimmed.to_owned()
    }
}

pub(crate) fn base_href(base_path: &str) -> String {
    if base_path == "/" {
        "/".to_owned()
    } else {
        format!("{base_path}/")
    }
}

pub(crate) fn inject(index_html: &str, base_path: &str, custom_css_url: Option<&str>) -> String {
    let base_path = normalize_base_path(base_path);
    let api_base = if base_path == "/" {
        "/api".to_owned()
    } else {
        format!("{base_path}/api")
    };
    let runtime = serde_json::to_string(&RuntimeConfig {
        base_path: &base_path,
        api_base,
    })
    .expect("runtime config JSON encoding cannot fail");
    let mut block = format!(
        "<base href=\"{}\"><script>window.__HORSIES_UI__ = {runtime}</script>",
        base_href(&base_path),
    );
    if let Some(url) = custom_css_url {
        block.push_str(&format!("<link rel=\"stylesheet\" href=\"{url}\">"));
    }

    if let Some(position) = head_close_position(index_html) {
        let mut injected = String::with_capacity(index_html.len() + block.len());
        injected.push_str(&index_html[..position]);
        injected.push_str(&block);
        injected.push_str(&index_html[position..]);
        injected
    } else {
        format!("{index_html}{block}")
    }
}

fn head_close_position(index_html: &str) -> Option<usize> {
    let lower = index_html.to_ascii_lowercase();
    let mut offset = 0;
    while let Some(relative) = lower[offset..].find("</head") {
        let position = offset + relative;
        let suffix = &lower[position + "</head".len()..];
        let whitespace = suffix.len() - suffix.trim_start_matches(char::is_whitespace).len();
        if suffix.as_bytes().get(whitespace) == Some(&b'>') {
            return Some(position);
        }
        offset = position + "</head".len();
    }
    None
}

pub(crate) fn safe_asset_path(request_path: &str) -> Option<String> {
    let raw = request_path.trim_start_matches('/');
    if raw.is_empty() || raw == "index.html" {
        return None;
    }
    let decoded = percent_decode_str(raw).decode_utf8().ok()?;
    if decoded.contains('\\') || decoded.contains('\0') {
        return None;
    }
    if decoded
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return None;
    }
    Some(decoded.into_owned())
}
