use burn_p2p_views::BrowserAppStaticBootstrap;
use dioxus::prelude::*;
use dioxus::ssr::render_element;

use super::theme::BROWSER_PORTAL_CSS;

fn join_asset_url(base: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") || base.is_empty() {
        return path.to_owned();
    }
    let normalized_base = base.trim_end_matches('/');
    let normalized_path = path.trim_start_matches('/');
    format!("{normalized_base}/{normalized_path}")
}

pub(super) fn render_browser_app_static_html(bootstrap: &BrowserAppStaticBootstrap) -> String {
    let bootstrap_json =
        serde_json::to_string(bootstrap).expect("static browser app bootstrap should serialize");
    let bootstrap_json = escape_script_json(&bootstrap_json);
    let module_entry = join_asset_url(&bootstrap.asset_base_url, &bootstrap.module_entry_path);
    let stylesheet = bootstrap
        .stylesheet_path
        .as_deref()
        .map(|path| join_asset_url(&bootstrap.asset_base_url, path));
    let body = render_element(rsx! {
        div { id: "burn-p2p-browser-app" }
        noscript { "JavaScript and WebAssembly are required to start burn_p2p." }
        script {
            id: "browser-app-static-bootstrap",
            r#type: "application/json",
            dangerous_inner_html: "{bootstrap_json}",
        }
        script { r#type: "module", src: module_entry.clone() }
    });
    let stylesheet_link = stylesheet
        .as_deref()
        .map(|href| {
            format!(
                "<link rel=\"stylesheet\" href=\"{}\">",
                escape_html_attr(href)
            )
        })
        .unwrap_or_default();
    format!(
        concat!(
            "<!doctype html>",
            "<html lang=\"en\">",
            "<head>",
            "<meta charset=\"utf-8\">",
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
            "<meta name=\"theme-color\" content=\"#05070c\">",
            "<title>{title}</title>",
            "<style>{style}</style>",
            "{stylesheet_link}",
            "</head>",
            "<body data-browser-app=\"static\" data-default-surface=\"{surface}\" data-default-edge-url=\"{edge_url}\">",
            "{body}",
            "</body>",
            "</html>"
        ),
        title = escape_html_text(&bootstrap.app_name),
        style = BROWSER_PORTAL_CSS,
        stylesheet_link = stylesheet_link,
        surface = escape_html_attr(bootstrap.default_surface.as_str()),
        edge_url = escape_html_attr(bootstrap.default_edge_url.as_deref().unwrap_or_default()),
        body = body,
    )
}

fn escape_html_attr(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn escape_html_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn escape_script_json(value: &str) -> String {
    value.replace("</script>", "<\\/script>")
}
